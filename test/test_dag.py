from airflow.models import DagBag


dagbag = DagBag()


def test_dag_loaded():
    dag = dagbag.get_dag(dag_id='raw_data_ingestion_dag')
    assert dag is not None
    assert dag.dag_id == 'raw_data_ingestion_dag'


def test_dag_structure():
    dag = dagbag.get_dag(dag_id='raw_data_ingestion_dag')
    tasks = dag.tasks

    task_ids = list(map(lambda task: task.task_id, tasks))
    assert len(tasks) == 5
    assert 'create_data_directory_task' in task_ids

    create_data_directory_task = dag.get_task(task_id='create_data_directory_task')
    assert len(create_data_directory_task.downstream_task_ids) == 3

    run_sanity_check_task = dag.get_task(task_id='run_sanity_check_task')
    assert len(run_sanity_check_task.upstream_task_ids) == 3
