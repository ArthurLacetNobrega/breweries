# Usar a imagem oficial do Apache Airflow
FROM apache/airflow:2.6.3

# Garantir que os comandos sejam executados como root
USER root

# Copia o script de entrada e garante permissões de execução
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

# Criar o grupo airflow caso não exista e adicionar o usuário airflow a ele
RUN groupadd --force airflow && usermod -aG airflow airflow

# Define o diretório de trabalho
WORKDIR /opt/airflow

# Criar diretórios necessários e garantir permissões
RUN mkdir -p /opt/airflow/airflow_db /opt/airflow/dags /opt/airflow/output \
    && chown -R airflow:airflow /opt/airflow \
    && chmod -R 777 /opt/airflow/output

# Copia as DAGs para o container
COPY dags /opt/airflow/dags

# Copia o arquivo de dependências
COPY requirements.txt /opt/airflow/requirements.txt

# Define o script de entrada como padrão
ENTRYPOINT ["/entrypoint.sh"]



