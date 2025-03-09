"""
Arquivo que transforma os dados da camada Bronze para a camada Silver.

Objetivo:
- Ler os dados em formato JSON armazenados localmente na camada Bronze.
- Converter os dados para um formato de armazenamento colunar (Parquet).
- Particionar os dados pelo campo 'state' para otimizar consultas futuras.
"""

import os
import pandas as pd

# Configurações gerais dos diretórios
BRONZE_PREFIX = "/opt/airflow/data/bronze"
SILVER_PREFIX = "/opt/airflow/data/silver/breweries"
SOURCE_FILE_NAME = "breweries.json"
SINK_FILE_NAME = "state"

def parse_file(source_folder, sink_folder, source_file_name, sink_file_name):
    """
    Função que lê os dados da camada Bronze, transforma para Parquet e salva na camada Silver.

    Parâmetros:
    - source_folder: Caminho da pasta onde o JSON está salvo (Bronze).
    - sink_folder: Caminho da pasta onde os arquivos Parquet serão salvos (Silver).
    - source_file_name: Nome do arquivo JSON de entrada.
    - sink_file_name: Nome da partição de saída (campo 'state').
    """

    try:
        bronze_file_path = os.path.join(source_folder, source_file_name)
        silver_file_path = os.path.join(sink_folder, sink_file_name)

        print(f"Lendo arquivo: {bronze_file_path}")

        # Verificar se o arquivo existe e não está vazio
        if not os.path.exists(bronze_file_path) or os.path.getsize(bronze_file_path) == 0:
            raise FileNotFoundError(f"Arquivo JSON não encontrado ou está vazio: {bronze_file_path}")

        # Ler o JSON para um DataFrame
        df = pd.read_json(bronze_file_path)

        # Verificar se a coluna de particionamento existe
        if 'state' not in df.columns:
            raise ValueError("A coluna 'state' não existe no JSON para particionamento.")

        # Criar a pasta Silver se não existir
        os.makedirs(sink_folder, exist_ok=True)

        # Salvar os dados em formato Parquet, particionados por 'state'
        df.to_parquet(silver_file_path, engine='pyarrow', partition_cols=['state'])

        print(f"Dados transformados e salvos em {silver_file_path}, particionados por estado.")

    except FileNotFoundError as e:
        print(f"Erro de arquivo: {e}")
    except ValueError as e:
        print(f"Erro nos dados: {e}")
    except ImportError:
        print(f"Erro inesperado: {e}")

if __name__ == "__main__":
    parse_file(BRONZE_PREFIX, SILVER_PREFIX, SOURCE_FILE_NAME, SINK_FILE_NAME)
