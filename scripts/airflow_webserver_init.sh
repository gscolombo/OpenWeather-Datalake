#! /bin/bash

airflow db migrate

airflow users create \
    --username admin \
    --password admin \
    --firstname Gabriel \
    --lastname Colombo \
    --email gscolombo404@gmail.com \
    --role Admin

airflow webserver