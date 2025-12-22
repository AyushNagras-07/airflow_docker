docker compose up -d airflow-webserver airflow-scheduler postgres airflow-worker 

running postgres on cli :
    docker exec -it airflow-postgres-1 bash
then
    psql -U airflow -d airflow
you should see something like 
    airflow=#