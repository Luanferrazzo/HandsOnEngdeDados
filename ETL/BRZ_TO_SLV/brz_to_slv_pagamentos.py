import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame

def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node csv_brz_pagamentos
csv_brz_pagamentos_node1733875335861 = glueContext.create_dynamic_frame.from_options(format_options={"quoteChar": -1, "withHeader": True, "separator": ";", "multiLine": "false", "optimizePerformance": False}, connection_type="s3", format="csv", connection_options={"paths": ["s3://sales-tech-brz/caso_estudo_pagamentos.csv"], "recurse": True}, transformation_ctx="csv_brz_pagamentos_node1733875335861")

# Script generated for node sql_brz_pagamentos
SqlQuery0 = '''
SELECT 
    id, 
    id_venda, 
   to_date(dt_pgto, 'dd/MM/yyyy') as dt_pgto

FROM 
    pagamentos;
'''
sql_brz_pagamentos_node1733881144725 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"pagamentos":csv_brz_pagamentos_node1733875335861}, transformation_ctx = "sql_brz_pagamentos_node1733881144725")

# Script generated for node schema_brz_pagamentos
schema_brz_pagamentos_node1733879279659 = ApplyMapping.apply(frame=sql_brz_pagamentos_node1733881144725, mappings=[("id", "string", "id", "int"), ("id_venda", "string", "id_venda", "int"), ("dt_pgto", "date", "dt_pgto", "date")], transformation_ctx="schema_brz_pagamentos_node1733879279659")

# Script generated for node parquet_slv_pagamentos
parquet_slv_pagamentos_node1733875398569 = glueContext.write_dynamic_frame.from_catalog(frame=schema_brz_pagamentos_node1733879279659, database="database_salestech_raw", table_name="pagamentos", transformation_ctx="parquet_slv_pagamentos_node1733875398569")

job.commit()