# Réponses du test

## _Utilisation de la solution (étape 1 à 3)_

### Executer les tests

```
pip install -r requirements-dev.txt
pytest [--last-failed]
```

### Exécuter l'ingestion de données 

Initialement l'ingestion de données a été developée pour être exécutée manuellement, mais j'ai ajouté un DAG Airflow pour automatiser le processus par la suite. J'ai conservé les deux méthodes pour montrer les deux approches (l'approche cli est également pratique à des fins de tests).
Ces deux approches vont aller aspirer la totalité des données pour les enregistrer dans un répertoire `tmp/<date>/`, en format Parquet.

Lancer le serveur FastAPI:

```
python -m uvicorn main:app
```

#### Manuellement

```
./src/ingestion/cli.py full-ingest
./src/ingestion/cli.py sanity-check
```

#### Avec Airflow:

```bash
export AIRFLOW_ADMIN_PASSWORD="xxx" && ./airflow_init.sh  # first run only
./airflow_start.sh  # can stop with ./airflow_terminate.sh
```

Aller à [http://localhost:8080](http://localhost:8080) et exécuter le DAG `full_ingest`.


## Questions (étapes 4 à 7)

### Étape 4

Le schéma de la base de données est assez simple, nous avons une table "facts" `listen_history` qui contient des identifiants vers les clés primaires des tables "dimension" `users` et `tracks`.
Une approche intéressante serait toutefois de ne pas créer de clé primaire/étrangère, car nous n'avons pas de garantie lorsque nous ingérons les données que ces contraintes sont respectées (nous n'avons pas de contrôle sur les données sources). 

L'intégrité des données pourrait être vérifiée en aval, lors d'une étape de sanity check par exemple. Cela fait particulièrement du sens si l'ingestion des données est très longue (à cause d'une grosse volumétrie, lenteur de l'API, etc).

Stocker les données dans un data lake (S3, GCS, etc) pourrait être une solution simple pour stocker les données brutes, c'est la solution que j'ai choisi ici. De l'analyse exploratoire pourrait ensuite être faite sur des échantillons à l'aide de notebooks Jupyter, par exemple. Si l'on voulait explorer la totalité des données, on pourrait les charger dans un data warehouse (BigQuery, Redshift, etc) pour les requêter plus facilement, ou alors utiliser des outils comme Athena ou Spark pour les requêter directement.


### Étape 5

La liste des métriques clés pourrait inclure

 - Métriques de performance de la copie de données
   - Temps de copie des données
   - Taille des données copiées
   - Nombre de lignes copiées
   - Nombre de call HTTP, de retries, etc
 - Métriques de qualité des données
   - Nombre de lignes dupliquées
   - Nombre de lignes avec des valeurs manquantes
   - Nombre de lignes avec des clés étrangères invalides
 - Métriques d'orchestration
   - Temps d'exécution de chaque tâche
   - Nombre de tâches et DAGs en échec

Ces métriques pourraient être publiées dans Cloudwatch/GC Monitoring, pour permettre de les visualiser et de mettre en place des alertes/gestion d'incident en cas de problème.
La détection d'anomalies sur ces métriques pourrait nous permettre de déployer rapidement des alertes.

### Étape 6

Les prédictions sont générées à partir du modèle entrainé, soit en temps réel (à travers une API, qui sert les dernières recommandations), ou alors en batch (plus performant/efficace mais avec un pb potentiel de fraicheur des données). 
Dans le cas d'une API, on pourrait utiliser Flask ou FastAPI (avec un serveur web + load balancer pour gérer les connexions HTTP) pour déployer notre modèle, sur des instances adpatées, ou alors utiliser un service de plus haut niveau comme SageMaker ou Vertex AI pour simplifier le déploiement. 
Dans le cas d'un batch, on pourrait utiliser un outil d'orchestration comme Airflow/StepFunctions/GC Workflows pour planifier l'exécution de notre modèle, avec encore le choix entre une solution plus manuelle et des services de plus haut niveau.

### Étape 7

Nous pouvons réutiliser notre outil d'orchestration pour automatiser le réentrainement du modèle.

L'API ne nous permet pas facilement de faire des mises à jour incrémentales, ce qui aurait pu aider côté données à identifier facilement les nouvelles données à ajouter. Cela dit un entrainement incrémental est possible.

Le pipeline pourrait ressembler à ceci:

Data capture => Sanity checks => Cleanup/Normalisation => Feature Engineering => Training => Evaluation => Deployment

Le réentrainement du modèle pourrait être déclenché par certaines métriques collectées côté client, tel que le "taux de conversion" moyen des recommendations, ou alors à intervals réguliers (tous les jours, toutes les semaines, etc). 