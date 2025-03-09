"""
Arquivo que transforma os dados da camada Silver para a camada Gold.

Objetivo:
- Ler os dados armazenados na camada Silver (Parquet).
- Criar uma visão agregada com a quantidade de cervejarias por tipo e localização (state).
- Salvar o resultado em formato Parquet na camada Gold para otimizar consultas futuras.
"""

import os
import pandas as pd

# Configurações gerais dos diretórios
SILVER_PREFIX = "/opt/airflow/data/silver/breweries"
GOLD_PREFIX = "/opt/airflow/data/gold"
SINK_FILE_NAME = "aggregated_breweries.parquet"

def parse_file(source_folder, sink_folder, sink_file_name):
    """
    Função que lê os dados da camada Silver, realiza agregação e salva na camada Gold.

    Parâmetros:
    - source_folder: Caminho da pasta onde os dados Silver estão salvos (Parquet).
    - sink_folder: Caminho da pasta onde o arquivo agregado será salvo (Gold).
    - sink_file_name: Nome do arquivo de saída contendo os dados agregados.
    """

    try:
        # Criar a pasta Gold se não existir
        os.makedirs(sink_folder, exist_ok=True)

        gold_file_path = os.path.join(sink_folder, sink_file_name)

        # Verificar se o diretório Silver existe e contém arquivos Parquet
        if not os.path.exists(source_folder) or not os.listdir(source_folder):
            raise FileNotFoundError(f"Diretório Silver está vazio ou não existe: {source_folder}")

        # Ler os dados da camada Silver (Parquet)
        df = pd.read_parquet(source_folder, engine='pyarrow')

        # Verificar se as colunas necessárias existem
        if not {'state', 'brewery_type'}.issubset(df.columns):
            raise ValueError("As colunas 'state' e 'brewery_type' são obrigatórias para a agregação.")

        # Criar a agregação contando a quantidade de cervejarias por estado e tipo
        aggregated_df = df.groupby(['state', 'brewery_type']).size().reset_index(name='brewery_count')

        # Salvar os dados agregados em formato Parquet
        aggregated_df.to_parquet(gold_file_path, engine='pyarrow')

        print(f"Dados agregados e salvos em {gold_file_path}.")

    except FileNotFoundError as e:
        print(f"Erro de arquivo: {e}")
    except ValueError as e:
        print(f"Erro nos dados: {e}")
    except ImportError:
        print(f"Erro inesperado: {e}")

if __name__ == "__main__":
    parse_file(SILVER_PREFIX, GOLD_PREFIX, SINK_FILE_NAME)
