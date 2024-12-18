# Projeto Sales Tech

## Participantes:
- Augusto Batista – RA: 10444612
- Daniela Alexandra – RA: 10444894
- Luan Ferrazzo – RA: 10397276

---

## Sumário
1. [Objetivo](#objetivo)
2. [Características](#características)
3. [Arquitetura](#arquitetura)
4. [Descrição das Camadas](#descrição-das-camadas)
5. [Código Desenvolvido](#código-desenvolvido)
6. [Resumo dos Serviços Utilizados](#resumo-dos-serviços-utilizados)
7. [Benefícios da Arquitetura](#benefícios-da-arquitetura)

---

## Objetivo
Criar um fluxo que extraia dados transacionais de vendas de produtos de tecnologia e computação e transformá-los para atender às camadas RAW, SILVER e GOLD em uma nuvem privada (AWS), a fim de promover melhorias na escalabilidade, automação e estudos de vendas e promoções. O projeto será com processamento em rotina batch devido a tarefas que não demandam resposta rápida e que envolvem grandes volumes de dados estáticos, como relatórios financeiros, geralmente em intervalos programados (diário, semanal).

---

## Características
- Trabalha com lotes grandes de dados.
- Ideal para tarefas não interativas e que podem ser realizadas em horários fora de pico (ex.: durante a madrugada).
- Alta latência, já que o processamento só começa quando todos os dados estão disponíveis.
- Usado em sistemas como ETL (Extract, Transform, Load) e relatórios gerenciais.

---

## Arquitetura

### Diagrama de Arquitetura
![Arquitetura do Pipeline de Dados](https://github.com/Luanferrazzo/HandsOnEngdeDados/blob/main/Arquitetura/Arquitetura.png)

### Descrição Geral
A arquitetura do pipeline de dados segue a divisão nos seguintes módulos: Fonte de Dados (Source), Ingestão de Dados, Armazenamento de Dados, Serving Layer e Consumo.

---

## Descrição das Camadas

### 1. Fonte de Dados (Source)
- **Batch (CSV):**
  - Arquivos CSV são carregados em lotes de uma fonte externa (ex.: sistemas legados, exportações de banco de dados).
  - Estes arquivos entram no pipeline por meio do AWS Glue.
- **Streaming (API):**
  - Dados contínuos e em tempo real provenientes de APIs ou serviços externos.
  - Estes dados são direcionados ao Amazon SQS para processamento assíncrono.

### 2. Ingestão de Dados
- **AWS Glue:**
  - Para dados em lote, o AWS Glue processa os arquivos CSV carregados.
  - Realiza a extração, transformação e carregamento (ETL), preparando os dados para armazenamento na camada "Bronze" do Amazon S3.
- **Amazon SQS e AWS Lambda:**
  - Para dados em streaming:
    - O Amazon SQS (Simple Queue Service) atua como um buffer, garantindo que as mensagens sejam entregues com resiliência e desacoplamento.
    - O AWS Lambda consome as mensagens da fila e executa transformações ou validações antes de armazenar os dados no Amazon S3.

### 3. Armazenamento de Dados
O armazenamento segue o modelo de Data Lake em camadas no Amazon S3:
- **Bronze:**
  - Contém os dados brutos, diretamente após a ingestão, sem processamento significativo.
  - Utilizado para auditoria ou caso seja necessário reprocessar dados.
- **Silver:**
  - Dados parcialmente processados, com transformações iniciais e validações aplicadas.
  - Esta camada é gerenciada pelo AWS Glue, que executa jobs de ETL.
- **Gold:**
  - Dados altamente processados e prontos para consumo analítico.
  - Otimizado para consultas por ferramentas como Amazon Athena.

### 4. Serving Layer
- **Amazon Athena:**
  - Serviço de consulta que permite acessar os dados armazenados no Amazon S3 usando SQL.
  - É integrado às camadas de dados (Silver e Gold), facilitando a análise sem necessidade de carregar dados para outro ambiente.

### 5. Consumo
- **Amazon QuickSight:**
  - Ferramenta de BI (Business Intelligence) para visualização de dados.
  - Conecta-se ao Amazon Athena para criar relatórios e dashboards interativos.
- **Amazon SageMaker:**
  - Serviço de aprendizado de máquina usado para criar, treinar e implantar modelos de machine learning.
  - Consome dados das camadas Gold (via Athena ou diretamente do S3).

---

## Código Desenvolvido

### 1. Ingestão de Dados

```python
# Script generated for node csv_brz_clientes
csv_brz_clientes_node1733875335861 = glueContext.create_dynamic_frame.from_options(
    format_options={"quoteChar": -1, "withHeader": True, "separator": ";", "multiLine": "false", "optimizePerformance": False}, 
    connection_type="s3", 
    format="csv", 
    connection_options={"paths": ["s3://sales-tech-brz/caso_estudo_clientes.csv"], "recurse": True}, 
    transformation_ctx="csv_brz_clientes_node1733875335861"
)

```
**Descrição**: Este código carrega dados de um arquivo CSV localizado no Amazon S3 para um DynamicFrame, utilizando o AWS Glue


### 2. Transformação de Dados 

```python
# Ajustar a coluna 'valor' para decimal (4,2)
produtos_spark_df = produtos_spark_df.withColumn("valor", 
                                                 (col("valor") / 100).cast(DecimalType(4, 2)))
```
**Descrição**: Este trecho de código ajusta a coluna 'valor' dos dados de produtos, convertendo-a para um tipo decimal com precisão de 4,2.


### 3. Salvando os Dados Transformados 

```python
# Converter para DataFrame do Spark para formatação e escrita
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
```
**Descrição**: Este código salva os dados transformados em formato Parquet no Amazon S3 e atualiza o Glue Data Catalog.


### 4. Links para Repositórios
- [Código Completo no GitHub](https://github.com/Luanferrazzo/HandsOnEngdeDados/tree/main/ETL)

---

## Resumo dos Serviços Utilizados
1. **AWS Glue:**
   - Serviço de ETL gerenciado para descoberta, preparação e transformação de dados.
2. **Amazon SQS:**
   - Serviço de fila de mensagens para comunicação assíncrona.
3. **AWS Lambda:**
   - Serviço de computação serverless para execução de código baseado em eventos.
4. **Amazon S3:**
   - Serviço de armazenamento escalável e durável.
5. **Amazon Athena:**
   - Serviço de consulta SQL diretamente no S3.
6. **Amazon QuickSight:**
   - Serviço de BI para visualização e análise de dados.
7. **Amazon SageMaker:**
   - Plataforma para machine learning, abrangendo desde o treinamento até a implantação de modelos.

---

## Benefícios da Arquitetura
1. **Escalabilidade:** Todos os serviços são altamente escaláveis, permitindo lidar com volumes crescentes de dados.
2. **Custo-Eficiência:** Cobra-se apenas pelo uso real de serviços como Lambda, Athena e S3.
3. **Resiliência:** O uso de SQS e S3 garante alta durabilidade e disponibilidade.
4. **Flexibilidade:** Capacidade de atender a diferentes fontes (batch e streaming) e casos de uso (análises e machine learning).
5. **Integração:** Todos os serviços se integram nativamente, reduzindo complexidade e custos operacionais.
