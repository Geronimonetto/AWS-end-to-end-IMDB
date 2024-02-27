import boto3
import json
import requests
from datetime import date


gender = [27, 9648]
type_gender = 'Terror&Mistério'
api_key = "ea33ae145ce37250f8ab09b9583b7a7f"


def count_page() -> list:
    """
    :func: A função trata de busca a quantidade total de páginas da requisição para que
    a função run_data percorra os dados das páginas.
    :return: dados
    """
    url = f'https://api.themoviedb.org/3/discover/movie?page=1&api_key={api_key}&with_genres={gender}&language=pt-BR'  # URL da 1o página
    response = requests.get(url).json()
    # Lista com a quantidade de páginas return dados
    dados = [dados for dados in range(1, response.get('total_pages') + 1)]
    return dados


def send_data(json_filename: str, type_gender_func: str, number_file: int) -> None:
    """
    :func: A função recebe os dados do armazenamento temporário do lambda e envia para o S3 particionado com 100 registros cada.
    :param json_filename:
    :param type_gender_func:
    :param number_file:
    :return: None
    """
    bucket_name: str = 'data-lake-desafio-final'
    session = boto3.Session(
        aws_access_key_id='AKIAZYAXJ7C03HYOH06D',
        aws_secret_access_key='xiPEysY1rZRzuu4L1D/pJx0n21Bb5bbaB+QovuSA'
    )
    s3 = session.client('s3')  # escolhendo o recurso S3
    s3.upload_file(json_filename, bucket_name,
                   f"Raw/TMDB/JSON/Movies/{date.today().year}/{date.today().month}/{date.today().day}/{type_gender_func}/data_{number_file}.json")


def run_data(page_number: list) -> None:
    """
    :func: A função trata de receber como parâmetro a função count_page que tem como argumento uma lista com a quantidade de páginas para que percorra a quantidade de páginas modificando a URL da API.
    :param page_number: count_page()
    :return: None
    """
    movies = []
    number_file = 1
    for count in page_number:
        url = f'https://api.themoviedb.org/3/discover/movie?page={count}&api_key={api_key}&with_genres={gender}&language=pt-BR'
        response = requests.get(url)
        data = response.json()

        if response.status_code == 200:
            if 'results' in data:
                movies.extend(data['results'])
                if len(movies) == 100:
                    # Create a new JSON file for each batch of data
                    json_filename = f"/tmp/data_{number_file}.json"
                    with open(json_filename, 'w', encoding='utf-8') as data_movies:
                        json.dump(movies, data_movies, indent=5)

                    # Enviado arquivo para o S3
                    send_data(json_filename, type_gender, number_file)

                    # Limpando lista
                    number_file += 1
                    movies.clear()

        else:
            print(f"Erro : {response.status_code}")
            break


def lambda_handler(event, context) -> None:
    run_data(count_page())
    return {
        'statusCode': 200,
        'body': 'JSON enviado para o S3'
    }
    # Código executado e finalizado em 1 min e 50 segundos
