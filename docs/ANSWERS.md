# Réponses du test

## _Utilisation de la solution (étape 1 à 3)_

# Executer les tests

```
pip install -r requirements-dev.txt
pytest [--last-failed]
```

# Exécuter l'ingestion de données 

## Manuellement

```
./src/ingestion/cli.py full-ingest
./src/ingestion/cli.py sanity-check
```

## Avec Airflow:

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

Stocker les données dans un data lake (S3, GCS, etc) pourrait être une solution simple pour stocker les données brutes, c'est la solution que j'ai choisi ici. De l'analyse exploratoire pourrait ensuite être faite sur des échantillons à l'aide de notebooks Jupyter, par exemple. Si l'on voulait explorer la totalité des données, on pourrait les charger dans un data warehouse (BigQuery, Redshift, etc) pour les requêter plus facilement, ou alors utiliser des outils comme Athena ou Spark pour les requêter directement sur le data lake.


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

_votre réponse ici_

### Étape 7

_votre réponse ici_
