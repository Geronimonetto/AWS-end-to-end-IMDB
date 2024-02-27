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