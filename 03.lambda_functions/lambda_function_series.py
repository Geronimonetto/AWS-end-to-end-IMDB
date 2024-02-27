import json
import pandas as pd
import boto3
import requests
from datetime import date

api_key = "ea33ae145ce37250f8ab09b9583b7a7f"
def id_imdb(list_id2):
    lista_id_nova = []
    for valor in list_id2:
        url = f'https://api.themoviedb.org/3/find/{valor}?api_key={api_key}&external_source=imdb_id'
        response = requests.get(url)
        data = response.json()
        if response.status_code == 200:
            if len(data['tv_results']) > 0:
                data = response.json()
                lista_id_nova.append(data['tv_results'][0].get('id'))
            else:
                continue
        else:
            continue
    return lista_id_nova
def pages_gener():
    dados = []
    url = f'https://api.themoviedb.org/3/discover/tv?api_key={api_key}&with_genres=9648'
    response = requests.get(url)
    data = response.json()
    total_pages = data['total_pages']
    for valor in range(1, total_pages + 1):
        url = f"https://api.themoviedb.org/3/discover/tv?page={valor}&api_key={api_key}&with_genres=9648"
        response = requests.get(url)
        if response.status_code == 200:
            data = response.json()
            resultados = data["results"]
            ids = [filme["id"] for filme in resultados]
            dados.extend(ids)
        else:
            continue
    return dados


def process_data(list_id):
    s3 = boto3.client('s3',
                      aws_access_key_id='AKIAZYAXJ7CO2LO2CJG2',
                      aws_secret_access_key='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'
                      )

    bucket_name = 'data-lake-desafio-final'
    s3_file_name = 'Raw/Local/CSV/Series/2023/09/26/series.csv'

    try:
        # Gêneros desejados
        generos = ['Horror', 'Mystery']

        # Obter objeto do bucket S3
        objeto = s3.get_object(Bucket=bucket_name, Key=s3_file_name)

        # Ler arquivo CSV diretamente do objeto do S3 e criar DataFrame
        df = pd.read_csv(objeto['Body'], sep='|', na_values=[r'\N'], dtype={'NomeDaColuna': str})

        # Filtrar filmes por gênero
        all_movies = df[df['genero'].isin(generos)].drop_duplicates()

        # Obter IDs únicos dos filmes
        lista_ids_unicos = all_movies['id'].unique().tolist()

        list_id.extend(id_imdb(lista_ids_unicos))
        dados = []  # Lista para adicionar os dados que vão para o JSON
        n_file = 1  # Número para ordenar o nome dos arquivo

        for id_serie in list_id:
            url = f'https://api.themoviedb.org/3/tv/{id_serie}?api_key=ea33ae145ce37250f8ab09b9583b7a7f&append_response=credits'
            response_credits = requests.get(url)
            if response_credits.status_code == 200:
                data_credits = response_credits.json()
                dados.append(data_credits)

                if len(dados) == 100:
                    json_file = json.dumps(dados, indent=4)
                    # Fazendo upload dos dados diretamente para o S3
                    s3_key = f'Raw/TMDB/JSON/Series/{date.today().year}/{date.today().month}/{date.today().day}/HorrorMystery/dados{n_file}.json'
                    s3.put_object(Bucket=bucket_name, Key=s3_key, Body=json_file)
                    n_file += 1
                    dados.clear()
            else:
                continue

        # Fazendo upload de qualquer dado restante
        if dados:
            json_file = json.dumps(dados, indent=4)
            # Fazendo upload dos dados diretamente para o S3
            s3_key = f'Raw/TMDB/JSON/Series/{date.today().year}/{date.today().month}/{date.today().day}/HorrorMystery/dados{n_file}.json'
            s3.put_object(Bucket=bucket_name, Key=s3_key, Body=json_file)
            n_file += 1
            dados.clear()
        else:
            print(f"Nenhum dado a ser enviado para o S3.")

    except Exception as e:
        print(f"Ocorreu um erro: {str(e)}")

    return {
        'statusCode': 200,
        'body': 'JSON enviado para o S3'
    }

process_data(pages_gener())