#!/bin/bash

source airflow_env_vars.sh

function check_process {
    if ps -p $1 > /dev/null; then
        echo "$2 started successfully with PID $1"
    else
        echo "Error: Failed to start $2"
        exit 1
    fi
}

echo "Starting Airflow webserver on port 8080..."
airflow webserver --port 8080 > /dev/null 2>&1 &
webserver_pid=$!
sleep 5
check_process $webserver_pid "Airflow webserver"

echo "Starting Airflow scheduler..."
airflow scheduler > /dev/null 2>&1 &
scheduler_pid=$!
sleep 5
check_process $scheduler_pid "Airflow scheduler"

echo $webserver_pid > .airflow_pids
echo $scheduler_pid >> .airflow_pids
