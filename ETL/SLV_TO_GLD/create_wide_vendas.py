import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.types import DecimalType
from pyspark.sql.functions import regexp_replace, col, format_string
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType, DecimalType, FloatType
from pyspark.sql import SparkSession

# Inicializar a SparkSession
spark = SparkSession.builder.appName("CriarTabelaWideVendas").getOrCreate()

# Definir o esquema da tabela
schema = StructType([
    StructField("id_venda", IntegerType(), True),
    StructField("id_cliente", IntegerType(), True),
    StructField("nome_cliente", StringType(), True),
    StructField("sexo_cliente", StringType(), True),
    StructField("dt_nasc", DateType(), True),
    StructField("id_loja", IntegerType(), True),
    StructField("cidade", StringType(), True),
    StructField("id_produto", IntegerType(), True),
    StructField("dt_venda", DateType(), True),
    StructField("produto", StringType(), True),
    StructField("valor", FloatType(), True)  
])

# Criar DataFrame vazio com o esquema definido
wide_vendas_df = spark.createDataFrame([], schema)

# Caminho no S3 onde a tabela será salva
s3_path = "s3://sales-tech-gld/wide_vendas/"

# Salvar a tabela no S3
wide_vendas_df.write.format("parquet") \
    .mode("overwrite") \
    .option("path", s3_path) \
    .save()

# Registrar no Glue Data Catalog (opcional)
database_name = "database_salestech_raw"
table_name = "wide_vendas"

spark.sql(f"""
    CREATE DATABASE IF NOT EXISTS {database_name}
""")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {database_name}.{table_name}
    USING PARQUET
    LOCATION '{s3_path}'
""")

print(f"Tabela {table_name} criada com sucesso no Glue Data Catalog.")

