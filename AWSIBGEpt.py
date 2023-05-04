import requests
import pandas as pd
import urllib.request
import zipfile
import os
from io import BytesIO
import awswrangler as wr
import boto3
from dotenv import load_dotenv

load_dotenv()

def get_file_name_with_prefix(folder_path, prefix):
    for filename in os.listdir(folder_path):
        if filename.startswith(prefix):
            return filename
    return None

caminho_arquivo_excel = get_file_name_with_prefix(os.curdir, 'ipca')

# URL para baixar os dados do IPCA do IBGE
url = 'https://ftp.ibge.gov.br/Precos_Indices_de_Precos_ao_Consumidor/IPCA/Serie_Historica/ipca_SerieHist.zip'
response = requests.get(url)
response.raise_for_status()

zip_file = BytesIO(response.content)

with zipfile.ZipFile(zip_file, 'r') as zip_ref:
    # Especificar o caminho de destino usando barras invertidas simples (válido em Windows)
    zip_ref.extractall()

try:
    df = pd.read_excel(caminho_arquivo_excel)
except FileNotFoundError:
    print("Arquivo Excel não encontrado. Verifique o caminho do arquivo.")

# Ler o arquivo Excel e pular a primeira linha
df = pd.read_excel(caminho_arquivo_excel, skiprows=1)

# Renomear as colunas manualmente
df.columns = ['ANO', 'MÊS', 'ÍNDICE', 'NO MÊS', '3 MESES', '6 MESES', 'NO ANO', '12 MESES']

# Selecionar apenas as colunas desejadas
df = df[['ANO', 'MÊS', 'NO MÊS', '12 MESES']]

# Excluir linhas em que o mês está vazio
df = df.dropna(subset=['MÊS'])

# Preencher células vazias na coluna 'ANO' com o valor do ano anterior
df['ANO'].fillna(method='ffill', inplace=True)

# Utilizar as informações de ano, mês, no mês e 12 meses
ano = df['ANO'].values[0]
mes = df['MÊS'].values[0]
no_mes = df['NO MÊS'].values[0]
doze_meses = df['12 MESES'].values[0]

# Remover linhas em que a coluna 'ANO' contém o valor 'ANO' a partir da segunda linha
df = df[df['ANO'] != 'ANO']

# Configurar as credenciais da AWS
session = boto3.Session(
    aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
    aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
    region_name=os.getenv('REGION_NAME')
)

database_name = os.getenv("DATABASE_NAME")
table_name = os.getenv("TABLE_NAME")
bucket_name = os.getenv("BUCKET_NAME")
#Criar bucket S3
s3 = session.client('s3')
s3.create_bucket(Bucket=bucket_name)
# Criar um banco de dados no AWS Glue
wr.catalog.create_database(name=database_name, exist_ok=True)
# Salvar o dataframe no formato Parquet no S3
path = f"s3://{bucket_name}/ipca_data/"
wr.s3.to_parquet(
    df=df,
    path=path,
    dataset=True,
    database=database_name,
    table=table_name
)

def store_df(df, table_name, database):
    print(f"Storing data on Bucket {bucket_name}")
    try:
        file_path = f"s3://{bucket_name}/source=api/database={database}/{table_name}/"
        wr.s3.delete_objects(file_path)
        wr.s3.to_parquet(
            df=df,
            path=file_path,
            dataset=True,
            database=database,
            table=table_name,
        )
    except Exception as e:
        raise Exception