import boto3
import time

# Inicialize os clientes do boto3
s3_client = boto3.client('s3')
glue_client = boto3.client('glue', region_name='us-east-2')

# Defina os nomes das tabelas e seus caminhos no S3
tables = {
    'clientes': 's3://sales-tech-slv/slv/clientes/',
    'lojas': 's3://sales-tech-slv/slv/lojas/',
    'pagamentos': 's3://sales-tech-slv/slv/pagamentos/',
    'produtos': 's3://sales-tech-slv/slv/produtos/',
    'promocao': 's3://sales-tech-slv/slv/promocao/',
    'vendas': 's3://sales-tech-slv/slv/vendas/'
}

# Função para deletar os dados no S3
def clear_s3_data(bucket_name, prefix):
    response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
    
    if 'Contents' in response:
        for obj in response['Contents']:
            print(f"Deletando {obj['Key']} do S3.")
            s3_client.delete_object(Bucket=bucket_name, Key=obj['Key'])
        print(f"Todos os arquivos deletados de {prefix}.")
    else:
        print(f"Nenhum arquivo encontrado em {prefix}.")

# Função para executar o job e aguardar a conclusão
def run_job(job_name):
    print(f"Iniciando o Job: {job_name}")
    response = glue_client.start_job_run(JobName=job_name)
    job_run_id = response['JobRunId']
    print(f"Job {job_name} iniciado com o JobRunId: {job_run_id}")
    
    # Esperar até que o job seja concluído
    while True:
        status_response = glue_client.get_job_run(JobName=job_name, RunId=job_run_id)
        job_status = status_response['JobRun']['JobRunState']
        print(f"Status do Job {job_name}: {job_status}")
        
        if job_status in ['SUCCEEDED', 'FAILED', 'STOPPED']:
            print(f"Job {job_name} finalizado com status {job_status}")
            break
        
        time.sleep(30)  # Aguardar 30 segundos antes de verificar novamente

# Limpar os dados no S3
for table, s3_path in tables.items():
    bucket_name = s3_path.split('/')[2]
    prefix = '/'.join(s3_path.split('/')[3:])
    print(f"Limpando dados da tabela {table}...")
    clear_s3_data(bucket_name, prefix)

# Defina os nomes dos jobs que você quer executar
job_names = [
    'brz_to_slv_promocao',
    'brz_to_slv_produtos',
    'brz_to_slv_pagamentos',
    'brz_to_slv_clientes',
    'brz_to_slv_lojas'
]

# Execute todos os jobs na ordem definida
for job_name in job_names:
    run_job(job_name)

print("Todos os jobs foram executados.")
