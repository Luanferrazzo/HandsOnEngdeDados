import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.types import DecimalType
from pyspark.sql.functions import regexp_replace, col, format_string

# Recebe os argumentos
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

# Inicializa o contexto do Glue
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Função para carregar DynamicFrames
def load_dynamic_frame(database, table_name, transformation_ctx):
    return glueContext.create_dynamic_frame.from_catalog(
        database=database,
        table_name=table_name,
        transformation_ctx=transformation_ctx
    )

# Carregar os dados de todas as tabelas
clientes_df = load_dynamic_frame("database_salestech_raw", "clientes", "clientes")
vendas_df = load_dynamic_frame("database_salestech_raw", "vendas", "vendas")
lojas_df = load_dynamic_frame("database_salestech_raw", "lojas", "lojas")
produtos_df = load_dynamic_frame("database_salestech_raw", "produtos", "produtos")


# Converter para DataFrame do Spark para poder trabalhar com as funções
produtos_spark_df = produtos_df.toDF()

# Ajustar a coluna 'valor' para decimal (4,2)
produtos_spark_df = produtos_spark_df.withColumn("valor", 
                                                 (col("valor") / 100).cast(DecimalType(4, 2)))

# Converter de volta para DynamicFrame
produtos_df = DynamicFrame.fromDF(produtos_spark_df, glueContext, "produtos")


# Função para realizar junções
def join_dynamic_frames(left_df, right_df, left_key, right_key, join_type="left"):
    left_spark_df = left_df.toDF()
    right_spark_df = right_df.toDF()
    joined_df = left_spark_df.join(right_spark_df, left_spark_df[left_key] == right_spark_df[right_key], join_type)
    return DynamicFrame.fromDF(joined_df, glueContext, f"joined_{left_key}_{right_key}")

# Realizando as junções
vendas_clientes = join_dynamic_frames(vendas_df, clientes_df, "id_cliente", "id")
vendas_clientes_lojas = join_dynamic_frames(vendas_clientes, lojas_df, "id_loja", "id")
vendas_clientes_lojas_produtos = join_dynamic_frames(vendas_clientes_lojas, produtos_df, "id_produto", "id")

# Alterar o schema para o formato desejado
mapped_data = ApplyMapping.apply(
    frame=vendas_clientes_lojas_produtos,
    mappings=[
        ("id", "int", "id_venda", "int"),
        ("id_cliente", "int", "id_cliente", "int"),
        ("nome", "string", "nome_cliente", "string"),
        ("sexo", "string", "sexo_cliente", "string"),
        ("dt_nasc", "date", "dt_nasc", "date"),
        ("id_loja", "int", "id_loja", "int"),
        ("cidade", "string", "cidade", "string"),
        ("id_produto", "int", "id_produto", "int"),
        ("dt_venda", "date", "dt_venda", "date"),
        ("produto", "string", "produto", "string"),
        ("valor", "decimal", "valor", "decimal")
    ],
    transformation_ctx="ChangeSchema"
)
# Converter para DataFrame do Spark para formatação e escrita
spark_df = mapped_data.toDF()

#Passo 1: Remover todos os caracteres exceto números
spark_df = spark_df.withColumn("valor", regexp_replace(col("valor"), "[^0-9]", ""))


# Converter a coluna 'valor' para DecimalType(4,2)
# Passo 2: Inserir casas decimais (assumindo que os dois últimos dígitos são os centavos)
spark_df = spark_df.withColumn("valor", 
                              (col("valor").cast("bigint") / 100).cast(DecimalType(12, 2)))  # Converte para DecimalType
                               
# # Passo 3: Formatar para o padrão brasileiro com ponto e vírgula
# spark_df = spark_df.withColumn("valor_formatado", 
#                               format_string("%s", col("valor")))


spark_df.write.format("parquet") \
    .mode("overwrite") \
    .option("path", "s3://sales-tech-gld/wide_vendas/") \
    .save()


# Escrever o resultado no Glue Data Catalog
glueContext.write_dynamic_frame.from_catalog(
    frame=DynamicFrame.fromDF(spark_df, glueContext, "final_data"),
    database="database_salestech_raw",
    table_name="wide_vendas",
    transformation_ctx="AWSGlueDataCatalog"
    
)

# Finalizar o trabalho
job.commit()
