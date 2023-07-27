import requests
import time
import csv
import json
import logging
import pyarrow
from xml.etree import ElementTree
from datetime import datetime
import pandas as pd
from google.cloud import bigquery
from google.api_core.exceptions import NotFound
import os
os.environ['TZ'] = 'America/Sao_Paulo'
time.tzset()

# Set up logging
logging.basicConfig(level=logging.INFO)

# Set API token
token = os.environ["TINY_ERP_TOKEN"]  # Add Token API from Tiny --> https://erp.tiny.com.br/configuracoes_api_web_services

# URLs
url_pedidos = f"https://api.tiny.com.br/api2/pedidos.pesquisa.php?token={token}&sort=DESC"
url_pdv = f"https://api.tiny.com.br/api2/pdv.pedido.obter.php?token={token}&id={{}}"
url_produto = f"https://api.tiny.com.br/api2/produto.obter.php?token={token}&id={{}}"

table_prefix = "z316-tiny"  # Add your desired table prefix here
dataset_name = "z316_tiny"  # Add your desired dataset name here

# Authenticate your client
os.environ["GOOGLE_APPLICATION_CREDENTIALS"]=os.environ["GOOGLE_APPLICATION_CREDENTIALS"]  # Add here your Google Service Account --> https://console.cloud.google.com/iam-admin/serviceaccounts
client = bigquery.Client()

# File to store the last processed pedido number
last_processed_pedido_number_file = '/opt/scripts/tinyerp-to-bigquery/last_processed_pedido_number.txt'  # Modify this path as needed

# Function to get the last processed pedido number from storage
def get_last_processed_pedido_number():
    if os.path.exists(last_processed_pedido_number_file):
        with open(last_processed_pedido_number_file, 'r') as file:
            return int(file.read().strip())
    else:
        return 0

# Define a function to upload dataframe to BigQuery
def upload_df_to_bigquery(df, dataset_name, table_name, table_schema):
    dataset_ref = client.dataset(dataset_name)

    # Check if the table exists
    table_ref = dataset_ref.table(table_name)
    try:
        client.get_table(table_ref)  # Will raise NotFound if table does not exist
        exists = True
    except NotFound:
        exists = False

    # If the table does not exist, create it
    if not exists:
        table = bigquery.Table(table_ref, schema=table_schema)
        client.create_table(table)

    # Load the dataframe into the table
    job_config = bigquery.LoadJobConfig(schema=table_schema)
    client.load_table_from_dataframe(df, table_ref, job_config=job_config).result()

# Function to store the last processed pedido number to storage
def store_last_processed_pedido_number(number):
    with open(last_processed_pedido_number_file, 'w') as file:
        file.write(str(number))

# Function to send a GET request with retry logic
def get_with_retry(url, max_retries=3):
    retries = 0
    while retries < max_retries:
        try:
            response = requests.get(url)
            response.raise_for_status()  # Raise exception if the request failed
            return response
        except requests.exceptions.RequestException as e:
            logging.error(f"Request failed: {e}. Retrying ({retries + 1}/{max_retries})...")
            retries += 1
            time.sleep(1)  # Wait for a second before retrying
    logging.error(f"Request failed after {max_retries} retries.")
    return None

# Get the last processed pedido number
last_processed_pedido_number = get_last_processed_pedido_number()

# Initialize max_pedido_number
max_pedido_number = 0

# Define the schema for the tables
schema_pedidos = [
    bigquery.SchemaField("timestamp", "TIMESTAMP"),
    bigquery.SchemaField("data_pedido", "DATE"),
    bigquery.SchemaField("id", "INTEGER"),
    bigquery.SchemaField("pedido_number", "INTEGER"),
    bigquery.SchemaField("id_vendedor", "STRING"),
    bigquery.SchemaField("totalProdutos", "FLOAT"),
    bigquery.SchemaField("totalVenda", "FLOAT"),
    bigquery.SchemaField("desconto", "STRING"),
    bigquery.SchemaField("formaPagamento", "STRING"),
]

schema_itens = [
    bigquery.SchemaField("timestamp", "TIMESTAMP"),
    bigquery.SchemaField("data_pedido", "DATE"),
    bigquery.SchemaField("id", "INTEGER"),
    bigquery.SchemaField("pedido_number", "INTEGER"),
    bigquery.SchemaField("id_vendedor", "STRING"),
    bigquery.SchemaField("idProduto", "INTEGER"),
    bigquery.SchemaField("descricao", "STRING"),
    bigquery.SchemaField("quantidade", "FLOAT"),
    bigquery.SchemaField("desconto", "FLOAT"),
    bigquery.SchemaField("valor", "FLOAT"),
    bigquery.SchemaField("preco_custo", "FLOAT"),
    bigquery.SchemaField("categoriaFirst", "STRING"),
    bigquery.SchemaField("categoriaSecond", "STRING"),
]

# Add these lines
with open('pedidos.csv', 'w', newline='') as f_pedidos, open('itens-pedido.csv', 'w', newline='') as f_itens:
    writer_pedidos = csv.writer(f_pedidos)
    writer_itens = csv.writer(f_itens)

    # Write the headers
    writer_pedidos.writerow(['timestamp', 'data_pedido', 'id', 'pedido_number', 'id_vendedor', 'totalProdutos', 'totalVenda', 'desconto', 'formaPagamento'])
    writer_itens.writerow(['timestamp', 'data_pedido', 'id', 'pedido_number', 'id_vendedor', 'idProduto', 'descricao', 'quantidade', 'desconto', 'valor', 'preco_custo', 'categoriaFirst', 'categoriaSecond'])

    # Flag to indicate if all pedidos have been processed
    all_pedidos_processed = False

    # Get the total number of pages
    total_pages = None

    # Send a GET request to the pesquisa.pedidos API
    response_pedidos = get_with_retry(url_pedidos)
    if response_pedidos is not None:
        root_pedidos = ElementTree.fromstring(response_pedidos.content)
        total_pages = int(root_pedidos.find('.//numero_paginas').text)

    # Check if total_pages is not None to avoid TypeError in range function
    if total_pages is not None:
        # Iterate over all pages
        for page in range(1, total_pages + 1):
            if all_pedidos_processed:
                break

            # Send a GET request to the pesquisa.pedidos API for the current page
            response_pedidos = get_with_retry(f"{url_pedidos}&pagina={page}")
            if response_pedidos is not None:
                root_pedidos = ElementTree.fromstring(response_pedidos.content)

                # Iterate over pedidos in the current page
                for pedido in root_pedidos.findall('.//pedido'):
                    pedido_number = int(pedido.find('numero').text)

                    # Update max_pedido_number
                    max_pedido_number = max(max_pedido_number, pedido_number)
                    highest_pedido_number = max(highest_pedido_number, pedido_number)

                    # If this pedido has been processed before, skip it
                    if pedido_number <= last_processed_pedido_number:
                        all_pedidos_processed = True
                        break

                    # Extract required fields
                    pedido_id = pedido.find('id').text
                    pedido_data_pedido = datetime.strptime(pedido.find('data_pedido').text, '%d/%m/%Y').strftime('%Y-%m-%d')  # Convert to YYYY-MM-DD format
                    pedido_id_vendedor = pedido.find('id_vendedor').text

                    try:
                        # Send a GET request to the pdv.pedido API
                        response_pdv = requests.get(url_pdv.format(pedido_id))
                        response_pdv.raise_for_status()  # Raise exception if the request failed
                    except requests.exceptions.RequestException as e:
                        logging.error(f"Failed to fetch data from pdv.pedido API for pedido ID {pedido_id}: {e}")
                        continue

                    pdv = json.loads(response_pdv.content)['retorno']['pedido']

                    # Extract required fields
                    pedido_totalProdutos = pdv['totalProdutos']
                    pedido_totalVenda = pdv['totalVenda']
                    pedido_desconto = pdv['desconto'].replace(',', '.')  # Replace comma with dot
                    pedido_formaPagamento = pdv['formaPagamento']

                    # Append to pedidos data list
                    writer_pedidos.writerow([
                        datetime.now().strftime('%Y-%m-%d %H:%M:%S'),  # Convert to YYYY-MM-DD hh:mm:ss format
                        pedido_data_pedido,
                        pedido_id,
                        pedido_number,
                        pedido_id_vendedor,
                        pedido_totalProdutos,
                        pedido_totalVenda,
                        pedido_desconto,
                        pedido_formaPagamento,
                    ])

                    # Loop over items
                    for j, item in enumerate(pdv['itens']):
                        logging.info(f"Processing item {j+1} of pedido {pedido_number}")
                        try:
                            # Send a GET request to the produto.obter API
                            response_produto = requests.get(url_produto.format(item['idProduto']))
                            response_produto.raise_for_status()  # Raise exception if the request failed
                        except requests.exceptions.RequestException as e:
                            logging.error(f"Failed to fetch data from produto.obter API for item ID {item['idProduto']}: {e}")
                            continue

                        root_produto = ElementTree.fromstring(response_produto.content)
                        produto = root_produto.find('.//produto')

                        # Extract and split category field
                        categoria = produto.find('categoria').text if produto.find('categoria') is not None and produto.find('categoria').text is not None else ''
                        split_index = categoria.find(' >> ')
                        if split_index != -1:
                            produto_categoriaFirst = categoria[:split_index].strip()  # remove any leading or trailing spaces
                            produto_categoriaSecond = categoria[split_index + len(' >> '):].strip()  # remove any leading or trailing spaces
                        else:
                            produto_categoriaFirst = categoria
                            produto_categoriaSecond = ''

                        # Append to itens data list
                        writer_itens.writerow([
                            datetime.now().strftime('%Y-%m-%d %H:%M:%S'),  # Convert to YYYY-MM-DD hh:mm:ss format
                            pedido_data_pedido,
                            pedido_id,
                            pedido_number,
                            pedido_id_vendedor,
                            item['idProduto'],
                            item['descricao'],
                            item['quantidade'],
                            item['desconto'].replace(',', '.'),  # Replace comma with dot
                            item['valor'],
                            produto.find('preco_custo').text,
                            produto_categoriaFirst,
                            produto_categoriaSecond,
                        ])

                        # Throttle API calls
                        time.sleep(1.2)

# Store the last processed pedido number
store_last_processed_pedido_number(highest_pedido_number)

# Load the data into pandas dataframes
df_pedidos = pd.read_csv('pedidos.csv')
df_itens = pd.read_csv('itens-pedido.csv')

# Upload the dataframes to BigQuery
df_pedidos['timestamp'] = pd.to_datetime(df_pedidos['timestamp'])
df_pedidos['data_pedido'] = pd.to_datetime(df_pedidos['data_pedido']).dt.date

df_itens['timestamp'] = pd.to_datetime(df_itens['timestamp'])
df_itens['data_pedido'] = pd.to_datetime(df_itens['data_pedido']).dt.date

df_pedidos[['id_vendedor', 'desconto']] = df_pedidos[['id_vendedor', 'desconto']].astype(str)
df_itens['id_vendedor'] = df_itens['id_vendedor'].astype(str)

df_pedidos[['totalProdutos', 'totalVenda']] = df_pedidos[['totalProdutos', 'totalVenda']].astype(float)
df_itens[['quantidade', 'desconto', 'valor', 'preco_custo']] = df_itens[['quantidade', 'desconto', 'valor', 'preco_custo']].astype(float)

df_itens['categoriaFirst'].fillna('Unknown', inplace=True)
df_itens['categoriaSecond'].fillna('Unknown', inplace=True)

upload_df_to_bigquery(df_pedidos, dataset_name, f'{table_prefix}-pedidos', schema_pedidos)
upload_df_to_bigquery(df_itens, dataset_name, f'{table_prefix}-itens-pedido', schema_itens)
