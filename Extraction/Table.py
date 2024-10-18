!pip install boto3
import boto3
import requests
import pandas as pd
import json
 
#PAYLOAD

table_id = "X"
pipefy_token = 'X'
table_fields = [""]
nome_arquivo = 'X'


url = "https://api.pipefy.com/graphql"

headers = {
    "authorization": f"Bearer {pipefy_token}",
    "content-type": "application/json"
}

df = pd.DataFrame(columns=table_fields)

has_next_page = True
first_query = True

while(has_next_page):

    if first_query:
        payload = {"query": "{  table_records(table_id:\""+table_id+"\") {edges {node {id title record_fields {name value}}} pageInfo {endCursor hasNextPage}}}"}
        first_query = False
    else:
        payload = {"query": "{  table_records(table_id:\""+table_id+"\",after:\""+end_cursor+"\") {edges {node {id title record_fields {name value}}} pageInfo {endCursor hasNextPage}}}"}

    response = requests.request("POST", url, json=payload, headers=headers)

    json_data = json.loads(response.text)
    end_cursor = json_data["data"]["table_records"]["pageInfo"]["endCursor"]
    has_next_page = json_data["data"]["table_records"]["pageInfo"]["hasNextPage"]
    total_records_pg = len(json_data["data"]["table_records"]["edges"])

    for i in range(total_records_pg):
        card_title = json_data["data"]["table_records"]["edges"][i]["node"]["title"]
        card_data_d = json_data["data"]["table_records"]["edges"][i]["node"]["record_fields"]
        card_data = {x['name']:x['value'] for x in card_data_d}
        df = df.append(card_data, ignore_index=True)

#PAYLOAD

#altero o nome das colunas, trocando os espaços com _  e retirando acentos, porque no banco de dados (big query), não aceita nome de colunas com caracteres especiais
df.columns = df.columns.str.replace(' ', '_')
df.columns = df.columns.str.replace('ã', 'a')
df.columns = df.columns.str.replace('Á', 'A')
df.columns = df.columns.str.replace('é', 'e')
df.columns  = df.columns.str.replace('ê', 'e')
df.columns  = df.columns.str.replace('á', 'a')
df.columns  = df.columns.str.replace('ç', 'c')   
df.columns  = df.columns.str.replace('í', 'i')
df.columns  = df.columns.str.replace('ú', 'u') 
df.columns  = df.columns.str.replace('õ', 'o') 
df.columns  = df.columns.str.replace('ó', 'o')
df.columns  = df.columns.str.replace('õ', 'o')
df.columns  = df.columns.str.replace('.', '')

#Enviando para o S3
filename = "/tmp/mydata.csv"
df.to_csv(filename, header=True)
        
#Definir o client da aws
client = boto3.client(
            's3',
            aws_access_key_id= 'X',
            aws_secret_access_key='X'
        )
        
client.upload_file(filename,'X',f'{nome_arquivo}.csv')
