#! usr/bin/sh
airflow_path="${PWD}/airflow"

if [ ! -d "$airflow_path" ]; then
    mkdir "$airflow_path";
fi

export AIRFLOW_HOME="${PWD}/airflow"
export AIRFLOW__CORE__DAGS_FOLDER="${PWD}/src"
export AIRFLOW__CORE__LOAD_EXAMPLES="False"

airflow standalone