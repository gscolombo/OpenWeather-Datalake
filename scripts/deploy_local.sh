#! /bin/bash

source ./.env
kubectl create secret generic openweather-api-key --from-literal=key=$API_KEY

kubectl apply -f k8s/metadata-db-pgsql-statefulset.yaml
kubectl apply -f k8s/pvc.yaml
kubectl apply -f k8s/airflow-deployment.yaml


kubectl rollout status deployment/airflow