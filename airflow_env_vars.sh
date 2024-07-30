#!/bin/bash

export AIRFLOW_HOME="$PWD/.airflow"
export AIRFLOW__CORE__DAGS_FOLDER="$PWD/src/ingestion/dags"
export AIRFLOW__SCHEDULER__DAG_DIR_LIST_INTERVAL=5
export AIRFLOW__SCHEDULER__MIN_FILE_PROCESS_INTERVAL=0
export AIRFLOW__CORE__LOAD_EXAMPLES=False
