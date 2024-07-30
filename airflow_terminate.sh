#!/bin/bash

echo "Terminating previous Airflow processes..."

if [ ! -f .airflow_pids ]; then
    echo "No PID file found. No Airflow processes to terminate."
    exit 1
fi

while read -r pid; do
    if ps -p $pid > /dev/null; then
        echo "Terminating process with PID $pid..."
        kill $pid
        sleep 1
        if ps -p $pid > /dev/null; then
            echo "Process with PID $pid did not terminate, forcing kill..."
            kill -9 $pid
        fi
    else
        echo "Process with PID $pid is not running."
    fi
done < .airflow_pids

rm -f .airflow_pids

echo "All specified Airflow processes have been terminated."