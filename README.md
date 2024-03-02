# Projeto API de filmes

O projeto é baseado em serviços de cloud e tem como principal objetivo verificar os filmes contidos em um arquivo csv, que faltam informações dentro dele, 
sendo assim necessário utilização da API do TMDB para consultas dos IDs dos filmes e obter essas informações adicionais.

Requisitos mínimos: 
- Criar API no TMDB - https://developer.themoviedb.org/reference/intro/getting-started
- Conhecimento em AWS

## Criando API no TMDB

![image](https://github.com/Geronimonetto/Projeto_API_Filmes/blob/main/01.API/01.Criando_chave_de_autentica%C3%A7%C3%A3o_api.PNG)

Contudo após a criação da API podemos verificar a documentação da API no site

### Testando API no VsCode

```python
import requests
import pandas as pd
import json
from IPython.display import display

api_key = "ea33ae145ce37250f8ab09b9583b7a7f"
url = f"https://api.themoviedb.org/3/movie/top_rated?api_key={api_key}&language=pt-BR"


class BuscaFilmes:

    def __init__(self, url: str) -> None:
        self.url = url
        self.response = requests.get(self.url)
        self.data = self.response.json()
        self.filmes = []

    def pegando_dados(self):
        for movie in self.data['results']:
            df = {'Titulo': movie['title'],
                  'Data de lançamento': movie['release_date'],
                  'Visão geral': movie['overview'],
                  'Votos': movie['vote_count'],
                  'Média de votos:': movie['vote_average']}
            self.filmes.append(df)
            with open("filmes_gerais.json", "w") as arquivo:
                json.dump(self.data, arquivo, indent=4)

    def imprimindo_dados(self):
        df = pd.DataFrame(self.filmes)
        pd.set_option('display.max_columns', None)
        pd.set_option('display.expand_frame_repr', False)
        display(df)

    def run(self):
        self.pegando_dados()
        self.imprimindo_dados()


if __name__ == "__main__":
    buscador_filmes = Busca_Filmes(url)
    buscador_filmes.run()

```

Através do teste sabemos que a API está retornando dados, e a partir disso podemos obter informações para nossa consulta e análises, 
faremos isto através do serviço da AWS Web Services o AWS Lambda, serviço que executa códigos em nuvem

Porém para executar funções no AWS Lambda precisamos importar camadas que servem como libs, isso deve ser feito para qualquer lib que quiser utilizar dentro da AWS Lambda, porém essas camadas devem ter o tamanho de 250kb se quiser utilizar o IDE


## Criar camadas no AWS Lambda

**Obs: A camada linux precisa ser criada em ambiente linux**

**Passo 1**:


![https://github.com/Geronimonetto/Projeto_API_Filmes/blob/main/02.Camada_Lambda/01.Criando%20Camada%20para%20AWS%20Lambda.PNG](https://github.com/Geronimonetto/Projeto_API_Filmes/blob/main/02.Camada_Lambda/01.Criando%20Camada%20para%20AWS%20Lambda.PNG)

**Passo 2**


![https://github.com/Geronimonetto/Projeto_API_Filmes/blob/main/02.Camada_Lambda/02.Zipando%20Camada.PNG](https://github.com/Geronimonetto/Projeto_API_Filmes/blob/main/02.Camada_Lambda/02.Zipando%20Camada.PNG)

**Passo 3**

![https://github.com/Geronimonetto/Projeto_API_Filmes/blob/main/02.Camada_Lambda/03.Enviando%20Camada%20para%20AWS%20Lambda.PNG](https://github.com/Geronimonetto/Projeto_API_Filmes/blob/main/02.Camada_Lambda/03.Enviando%20Camada%20para%20AWS%20Lambda.PNG)

**Passo 4**

![https://github.com/Geronimonetto/Projeto_API_Filmes/blob/main/02.Camada_Lambda/04.Adicionando%20Camada%20no%20AWS%20Lambda.PNG](https://github.com/Geronimonetto/Projeto_API_Filmes/blob/main/02.Camada_Lambda/04.Adicionando%20Camada%20no%20AWS%20Lambda.PNG)

Finalizada a criação da camada e importação, devemos então utilizar nosso código em uma função no AWS Lambda.

## Funções no AWS Lambda

Primeiro código para extração dos dados dos filmes da API

```python
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
```

Segundo código para extração dos dados das series da API do TMDB

```python
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

def lambda_handler(event, context):
    process_data(pages_gener())
    return {
        'statusCode': 200,
        'body': 'JSON enviado para o S3'
    }


```

## Transformando os dados no AWS Glue

### Transformando dados dos filmes

```python
import sys
import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, col, expr, year, concat_ws, month, lit
from pyspark.sql.types import StructType, StructField, StringType
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME','S3_INPUT_PATH','S3_TARGET_PATH'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
source_file = args['S3_INPUT_PATH']
target_path = args['S3_TARGET_PATH']

df = glueContext.create_dynamic_frame.from_options(
"s3",{"paths": [source_file]},format='json')

df = df.toDF()
# Print schema to verify if the DataFrame was created

df = df.withColumn('genres', explode('genres'))
df = df.withColumn('id_genero', col('genres').getField('id'))
df = df.withColumn('nome_genero', col('genres').getField('name'))

# # Reorder the columns

df = df.withColumn("credits_cast", explode(col("credits.cast")))
df = df.withColumn("credit_id", col("credits_cast.credit_id"))
df = df.withColumn("id_elenco", col("credits_cast.gender"))
df = df.withColumn("id_ator", col("credits_cast.id"))
df = df.withColumn("profissao", col("credits_cast.known_for_department"))
df = df.withColumn("nome_ator", col("credits_cast.name"))
df = df.withColumn("popularidade_ator", col("credits_cast.popularity"))


df = df.withColumn("id_companhia_producao", explode('production_companies'))
df = df.withColumn("id_companhia", col("id_companhia_producao").getField('id'))
df = df.withColumn("nome_companhia", col("id_companhia_producao").getField('name'))
df = df.withColumn("production_countries", explode(col("production_countries")))
df = df.withColumn("country_code", col("production_countries.iso_3166_1"))
df = df.withColumn("country_name", col("production_countries.name"))
df = df.withColumn("ano_lancamento", year(df["release_date"]))

df = df.withColumnRenamed("id", "id")\
  .withColumnRenamed("popularity", "popularidade")\
  .withColumnRenamed("revenue", "receita")\
  .withColumnRenamed("runtime", "duracao")\
  .withColumnRenamed("title", "titulo")\
  .withColumnRenamed("vote_average", "classificacao_media")\
  .withColumnRenamed("vote_count", "total_votos")\
  .withColumnRenamed("id_genero", "id_genero")\
  .withColumnRenamed("nome_genero", "genero")\
  .withColumnRenamed("credit_id", "id_credito")\
  .withColumnRenamed("id_elenco", "id_elenco")\
  .withColumnRenamed("id_ator", "id_ator")\
  .withColumnRenamed("profissao", "profissao")\
  .withColumnRenamed("nome_ator", "nome_ator")\
  .withColumnRenamed("popularidade", "popularidade_filme")\
  .withColumnRenamed("id_companhia", "id_companhia")\
  .withColumnRenamed("nome_companhia", "nome_companhia")\
  .withColumnRenamed("country_code", "codigo_pais")\
  .withColumnRenamed("country_name", "nome_pais")
# Obter a data de hoje
data_de_hoje = datetime.date.today()

# Extrair o ano, mês e dia da data de hoje
ano_hoje = data_de_hoje.year
mes_hoje = data_de_hoje.month
dia_hoje = data_de_hoje.day

# Adicionar as colunas ao DataFrame
df = df.withColumn("ano_coleta", lit(ano_hoje))
df = df.withColumn("mes_coleta", lit(mes_hoje))
df = df.withColumn("dia_coleta", lit(dia_hoje))

df = df.drop("homepage", "video", "credits_cast", 'status',"genres","imdb_id","adult","backdrop_path","belongs_to_collection","budget","poster_path","overview","tagline",
              'original_language', 'credits', 'production_companies', 'production_countries', 'profile_path',"spoken_languages","tagline",'id_companhia_producao','original_title', 'release_date')
# Criação das dimensões a partir do DataFrame df

# # Dimensão Filme
dim_filme = df.select('id', 'titulo', 'popularidade_filme', 'ano_lancamento', 'receita', 'duracao', 'classificacao_media', 'total_votos')

# # Dimensão Gênero
dim_genero = df.select('id_genero', 'genero')

# # Dimensão Elenco/Ator
dim_elenco = df.select('id_ator', 'profissao', 'nome_ator', 'popularidade_ator')

# # Dimensão Empresa
dim_empresa = df.select('id_companhia', 'nome_companhia')

# # Dimensão País
dim_pais = df.select('codigo_pais', 'nome_pais')

# # Dimensão Data
dim_data = df.select('ano_lancamento')

# #Criando tabela fato
tabela_fato = df.select('id', 'id_genero', 'id_ator', 'id_companhia','codigo_pais', 'ano_lancamento','popularidade_filme', 'receita', 'duracao', 'classificacao_media', 'total_votos')

df.write \
    .option('header', True) \
    .format('parquet') \
    .partitionBy("ano_coleta", "mes_coleta", "dia_coleta") \
    .mode('overwrite') \
    .save(target_path)

# Caminhos alvo para cada dimensão
target_path_genero = "s3://data-lake-desafio-final/Refined/Movies/dim_genero/"
target_path_elenco = "s3://data-lake-desafio-final/Refined/Movies/dim_elenco/"
target_path_empresa = "s3://data-lake-desafio-final/Refined/Movies/dim_empresa/"
target_path_pais = "s3://data-lake-desafio-final/Refined/Movies/dim_pais/"
target_path_data = "s3://data-lake-desafio-final/Refined/Movies/dim_data/"
target_path_filmes = "s3://data-lake-desafio-final/Refined/Movies/dim_filmes/"
target_path_fato = "s3://data-lake-desafio-final/Refined/Movies/tabela_fato/"
# Salvando cada dimensão em seu respectivo caminho alvo
dim_filme.write \
    .option('header', True) \
    .format('parquet') \
    .mode('overwrite') \
    .save(target_path_filmes)
    
dim_genero.write \
    .option('header', True) \
    .format('parquet') \
    .mode('overwrite') \
    .save(target_path_genero)
```

### Transformando dados das Séries

```python
import sys
import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import expr, col, explode, lit
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType, ArrayType, DateType, FloatType, DoubleType
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

s3_path = "s3://data-lake-desafio-final/Raw/TMDB/JSON/Series/2023/10/13/HorrorMystery/"
df = glueContext.create_dynamic_frame_from_options(connection_type= 's3',
                                                               connection_options={"paths": [s3_path]},
                                                                format='json')

df = df.toDF()                                                              
# Para a coluna "created_by"
df = df.withColumn("created_by", explode(col("created_by")))
df = df.withColumn("created_by_id", col("created_by.id"))
df = df.withColumn("created_by_credit_id", col("created_by.credit_id"))
df = df.withColumn("created_by_name", col("created_by.name"))
df = df.withColumn("created_by_gender", col("created_by.gender"))
df = df.withColumn("created_by_profile_path", col("created_by.profile_path"))

# Para a coluna "episode_run_time"
df = df.withColumn("episode_run_time", explode(col("episode_run_time")))

# Para a coluna "genres"
df = df.withColumn("genres", explode(col("genres")))
df = df.withColumn("genre_id", col("genres.id"))
df = df.withColumn("genre_name", col("genres.name"))

# Para a coluna "languages"
df = df.withColumn("languages", explode(col("languages")))

# Para a coluna "networks"
df = df.withColumn("networks", explode(col("networks")))
df = df.withColumn("network_id", col("networks.id"))
df = df.withColumn("network_logo_path", col("networks.logo_path"))
df = df.withColumn("network_name", col("networks.name"))
df = df.withColumn("network_origin_country", col("networks.origin_country"))

# Para a coluna "origin_country"
df = df.withColumn("origin_country", explode(col("origin_country")))

# Para a coluna "production_companies"
df = df.withColumn("production_companies", explode(col("production_companies")))
df = df.withColumn("production_company_id", col("production_companies.id"))
df = df.withColumn("production_company_logo_path", col("production_companies.logo_path"))
df = df.withColumn("production_company_name", col("production_companies.name"))
df = df.withColumn("production_company_origin_country", col("production_companies.origin_country"))

# Para a coluna "production_countries"
df = df.withColumn("production_countries", explode(col("production_countries")))
df = df.withColumn("country_iso_3166_1", col("production_countries.iso_3166_1"))
df = df.withColumn("country_name", col("production_countries.name"))

# Para a coluna "seasons"
df = df.withColumn("seasons", explode(col("seasons")))
df = df.withColumn("season_air_date", col("seasons.air_date"))
df = df.withColumn("season_episode_count", col("seasons.episode_count"))
df = df.withColumn("season_id", col("seasons.id"))
df = df.withColumn("season_name", col("seasons.name"))
df = df.withColumn("season_overview", col("seasons.overview"))
df = df.withColumn("season_poster_path", col("seasons.poster_path"))
df = df.withColumn("season_number", col("seasons.season_number"))
df = df.withColumn("season_vote_average", col("seasons.vote_average"))

# Para a coluna "spoken_languages"
df = df.withColumn("spoken_languages", explode(col("spoken_languages")))
df = df.withColumn("spoken_language_english_name", col("spoken_languages.english_name"))
df = df.withColumn("spoken_language_iso_639_1", col("spoken_languages.iso_639_1"))
df = df.withColumn("spoken_language_name", col("spoken_languages.name"))




df = df.drop(" backdrop_path","backdrop_path","adult","homepage",'in_production,languages','last_episode_to_air', 'networks', 'next_episode_to_air', 'original_language','overview','poster_path','spoken_languages',
             'tagline',"type",'spoken_language_iso_639_1','spoken_language_english_name','spoken_language_english_name','season_overview','country_iso_3166_1','production_company_logo_path','network_name', 'network_logo_path',
             'created_by_profile_path',"season_poster_path","created_by_credit_id","original_name","languages","spoken_language_name","season_name","season_id","season_vote_average","created_by","genres","production_companies","seasons",
             "in_production","production_countries")
df = df.select(
    'id',
    'name',
    'status',
    'popularity',
    'vote_average',
    'vote_count',
    'number_of_episodes',
    'number_of_seasons',
    'origin_country',
    'first_air_date',
    'last_air_date',
    'created_by_id',
    'created_by_name',
    'created_by_gender',
    'genre_id',
    'genre_name',
    'network_id',
    'network_origin_country',
    'production_company_id',
    'production_company_name',
    'production_company_origin_country',
    'country_name',
    'season_air_date',
    'season_episode_count',
    'season_number',
    'episode_run_time'
)
df = df.withColumnRenamed("id", "serie_id")
df = df.withColumnRenamed("name", "nome_serie")
df = df.withColumnRenamed("status", "status_serie")
df = df.withColumnRenamed("popularity", "popularidade")
df = df.withColumnRenamed("vote_average", "avaliacao_media")
df = df.withColumnRenamed("vote_count", "numero_votos")
df = df.withColumnRenamed("number_of_episodes", "numero_episodios")
df = df.withColumnRenamed("number_of_seasons", "numero_temporadas")
df = df.withColumnRenamed("origin_country", "pais_origem")
df = df.withColumnRenamed("first_air_date", "data_estreia")
df = df.withColumnRenamed("last_air_date", "data_ultimo_episodio")
df = df.withColumnRenamed("created_by_id", "criador_id")
df = df.withColumnRenamed("created_by_name", "nome_criador")
df = df.withColumnRenamed("created_by_gender", "genero_criador")
df = df.withColumnRenamed("genre_id", "genero_id")
df = df.withColumnRenamed("genre_name", "nome_genero")
df = df.withColumnRenamed("network_id", "rede_id")
df = df.withColumnRenamed("network_origin_country", "pais_origem_rede")
df = df.withColumnRenamed("production_company_id", "empresa_producao_id")
df = df.withColumnRenamed("production_company_name", "nome_empresa_producao")
df = df.withColumnRenamed("production_company_origin_country", "pais_origem_empresa_producao")
df = df.withColumnRenamed("country_name", "nome_pais")
df = df.withColumnRenamed("season_air_date", "data_estreia_temporada")
df = df.withColumnRenamed("season_episode_count", "numero_episodios_temporada")
df = df.withColumnRenamed("season_number", "numero_temporada")
df = df.withColumnRenamed("episode_run_time", "duracao_media_episodios")

data_de_hoje = datetime.date.today()

# Extrair o ano, mês e dia da data de hoje
ano_hoje = data_de_hoje.year
mes_hoje = data_de_hoje.month
dia_hoje = data_de_hoje.day

# Adicionar as colunas ao DataFrame
df = df.withColumn("ano_coleta", lit(ano_hoje))
df = df.withColumn("mes_coleta", lit(mes_hoje))
df = df.withColumn("dia_coleta", lit(dia_hoje))

# Dimensão Série
dim_serie = df.select('serie_id', 'nome_serie', 'status_serie', 'popularidade',
                      'data_estreia', 'data_ultimo_episodio', 'numero_episodios',
                      'numero_temporadas', 'genero_id', 'nome_genero', 'pais_origem',
                      'duracao_media_episodios')

# Dimensão Criador
dim_criador = df.select('criador_id', 'nome_criador', 'genero_criador')

# Dimensão Rede
dim_rede = df.select('rede_id', 'pais_origem_rede')

# Dimensão Empresa de Produção
dim_empresa_producao = df.select('empresa_producao_id', 'nome_empresa_producao', 'pais_origem_empresa_producao')

# Dimensão País
dim_pais = df.select('pais_origem', 'nome_pais')

# Dimensão Temporada
dim_temporada = df.select('serie_id', 'numero_temporada', 'data_estreia_temporada', 'numero_episodios_temporada')


s3_producao = "s3://data-lake-desafio-final/Refined/Series/dim_empresa_producao/"
s3_serie = "s3://data-lake-desafio-final/Refined/Series/dim_series/"
s3_criador = "s3://data-lake-desafio-final/Refined/Series/dim_criador/"
s3_rede = "s3://data-lake-desafio-final/Refined/Series/dim_rede/"
s3_temporada = "s3://data-lake-desafio-final/Refined/Series/dim_temporada/"
caminho_trusted = "s3://data-lake-desafio-final/Trusted/Series/"

df.write.option('header', True).format('parquet').mode('overwrite').save(caminho_trusted)
dim_serie.write.option('header', True).format('parquet').mode('overwrite').save(s3_serie)
dim_criador.write.option('header', True).format('parquet').mode('overwrite').save(s3_criador)
dim_rede.write.option('header', True).format('parquet').mode('overwrite').save(s3_rede)
dim_empresa_producao.write.option('header', True).format('parquet').mode('overwrite').save(s3_producao)
dim_temporada.write.option('header', True).format('parquet').mode('overwrite').save(s3_temporada)

job.commit()
```

## Modelagem dimensional

Os dados foram tratados e diante disso, devemos então fazer a modelagem dos dados para nosso Data Warehouse

### Modelagem dimensional dos filmes
![](https://github.com/Geronimonetto/Projeto_API_Filmes/blob/main/05.Modelagem_DW/Modelagem_dimensional_filmes.PNG)

### Modelagem dimensional das séries

![](https://github.com/Geronimonetto/Projeto_API_Filmes/blob/main/05.Modelagem_DW/Modelagem_dimensional_series.PNG)

As modelagens dimensionais devem ser feitas para que as execuções sejam de forma rápida, por conta das quantidade dos dados, logo, 
Quando criarem as devemos criar uma tabela fato com informações principais usando o create table e retirando as colunas das outras tabelas, e depois criando nossas dimensões atraves destas colunas

## Dashboards

Por fim e o queridinho de muitos os dashboards de finalização do projeto, sendo eles comparativos entre os gêneros de terror e mistério (Solicitado no projeto)
### Filmes
![](https://github.com/Geronimonetto/Projeto_API_Filmes/blob/main/06.Dashboards/filmes.JPG)


### Series
![](https://github.com/Geronimonetto/Projeto_API_Filmes/blob/main/06.Dashboards/series.JPG)

