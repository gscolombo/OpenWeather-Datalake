k8s_yaml('k8s/airflow-deployment.yaml')

docker_build('openweather-airflow', '.')

k8s_resource('airflow', port_forwards="8080")

