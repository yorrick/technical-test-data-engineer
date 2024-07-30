import os
from dataclasses import asdict
from functools import partial

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime

from src.ingestion.analytics import run_sanity_check
from src.ingestion.cli import TMP_FOLDER, API_ENDPOINT
from src.ingestion.ingestion import save_to_parquet, get_items


def create_data_directory(root_path, **kwargs):
    execution_time_in_utc = datetime.fromisoformat(kwargs["ts"])
    data_directory = (
        f"{root_path}/{execution_time_in_utc.strftime('%Y-%m-%d_%H-%M-%S')}"
    )
    print(f"Creating directory {data_directory}")
    os.makedirs(data_directory, exist_ok=True)
    return data_directory


def raw_data_ingestion(
    endpoint: str, page_size: int, model: str, parquet_buffer_size: int, **kwargs
):
    data_directory = kwargs["ti"].xcom_pull(task_ids="create_data_directory_task")

    output_file = f"{data_directory}/{model}.parquet"
    print(f"Ingesting data from {endpoint} to {output_file}")
    save_to_parquet(
        get_items(endpoint, page_size),
        output_file,
        buffer_size=parquet_buffer_size,
    )


def run_sanity_check_fn(**kwargs):
    data_directory = kwargs["ti"].xcom_pull(task_ids="create_data_directory_task")
    print(f"Running sanity check on {data_directory}")

    # here we could publish those metrics to a monitoring system, then enabling alerts/incidents
    non_matching_dim_data = run_sanity_check(data_directory)
    if non_matching_dim_data.users > 0 or non_matching_dim_data.tracks > 0:
        raise ValueError(
            f"Found non-matching dimension data: {asdict(non_matching_dim_data)}"
        )


with DAG(
    "raw_data_ingestion_dag",
    schedule_interval="32 14 * * *",  # UTC
    start_date=days_ago(1),
    tags=["ingestion"],
) as dag1:
    create_data_directory_task = PythonOperator(
        task_id="create_data_directory_task",
        python_callable=partial(create_data_directory, TMP_FOLDER),
        provide_context=True,
    )

    models = ("tracks", "users", "listen_history")

    fetch_data_states = []
    for model in models:
        fn = partial(
            raw_data_ingestion,
            endpoint=f"{API_ENDPOINT}/{model}",
            page_size=30,
            model=model,
            parquet_buffer_size=500,
        )

        fetch_data_states.append(
            PythonOperator(
                task_id=f"{model}_data_ingestion",
                python_callable=fn,
                provide_context=True,
            )
        )

    run_sanity_check_task = PythonOperator(
        task_id="run_sanity_check_task",
        python_callable=run_sanity_check_fn,
        provide_context=True,
    )

    (create_data_directory_task >> fetch_data_states >> run_sanity_check_task)
