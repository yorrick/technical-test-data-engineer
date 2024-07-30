#!/bin/bash

source airflow_env_vars.sh

airflow db init
airflow users create \
   --username admin \
   --firstname Admin \
   --lastname User \
   --role Admin \
   --email admin@example.com \
   --password ${AIRFLOW_ADMIN_PASSWORD}
