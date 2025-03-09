#!/bin/bash

# Criar banco de dados, se não existir
if [ ! -f "/opt/airflow/airflow_db/airflow.db" ]; then
    echo "Inicializando banco de dados..."
    airflow db init
else
    echo "Banco de dados já existe, pulando inicialização."
fi

# Iniciar o Airflow Webserver e Scheduler
airflow webserver -p 8080 &
airflow scheduler &

# Aguarda a inicialização e executa as DAGs automaticamente
sleep 10
airflow dags list
airflow dags unpause etl_pipeline
airflow dags trigger etl_pipeline

# Mantém o container ativo
tail -f /dev/null
