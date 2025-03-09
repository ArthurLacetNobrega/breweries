"""
Arquivo que faz uma requisição à API, recebe os dados e os salva em formato JSON na pasta 'bronze'.
Seu objetivo é armazenar os dados da API de forma local antes de um possível processamento adicional.
"""

import os
import requests
import json

# Configurações gerais
BRONZE_PREFIX = "/opt/airflow/data/bronze"
FILE_NAME = "breweries.json"
URL = "https://api.openbrewerydb.org/v1/breweries"

def api_request(url, source_folder, file_name):
    """
    Função que faz uma requisição GET à API e salva os dados em formato JSON na pasta especificada.

    Parâmetros:
    - url: Caminho para a requisição na API com os dados desejados.
    - source_folder: Caminho da pasta onde os arquivos JSON serão salvos (Bronze).
    - file_name: Nome do arquivo JSON salvo com os dados da API.
    """
    try:
        response = requests.get(url, timeout=100)
        response.raise_for_status()

        data = response.json()

        output_folder = "/opt/airflow/output"
        os.makedirs(output_folder, exist_ok=True)
        os.makedirs(source_folder, exist_ok=True)

        file_path = os.path.join(source_folder, file_name)

        # Salvando os dados convertidos em um arquivo JSON
        with open(file_path, 'w', encoding="utf-8") as json_file:
            json.dump(data, json_file, indent=4)

        print(f"Dados salvos em {source_folder}/{file_name}")

    except requests.exceptions.RequestException as e:
        print(f"Erro na requisição à API: {e}")
    except (OSError, IOError) as e:
        print(f"Erro ao salvar o arquivo JSON: {e}")
    except ImportError:
        print(f"Erro inesperado: {e}")

if __name__ == "__main__":
    api_request(URL, BRONZE_PREFIX, FILE_NAME)
