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

