!pip install boto3
import boto3
import requests
import pandas as pd
import json

token = 'X'

url = "https://api.pipefy.com/graphql"

nome_arquivo = 'X'

payload = "{\"query\":\"{ allCards (pipeId:X) { edges { node { id title createdAt current_phase {name} fields { name report_value updated_at value } } } }} \"}"

headers = {
    "authorization": f"Bearer {token}",
    "content-type": "application/json"
}
has_next_page = True
first_query = True
pipe_id = "X"
json_data = {}
records_df = pd.DataFrame()
while(has_next_page):
  if first_query:
    payload = {"query": "{ allCards (pipeId:\""+pipe_id+"\") { edges { node { id title createdAt current_phase {name} fields { name report_value updated_at value } } } pageInfo {endCursor hasNextPage}}}"}
    first_query = False
  else:
    payload = {"query": "{ allCards (pipeId:\""+pipe_id+"\",after:\""+end_cursor+"\") { edges { node { id title createdAt current_phase {name} fields { name report_value updated_at value } } } } pageInfo {endCursor hasNextPage}}}"}
    

  response = requests.request("POST", url, json=payload, headers=headers)
  json_data = json.loads(response.text)
  end_cursor =json_data['data']['allCards']["pageInfo"]["endCursor"] #record é edges
  has_next_page = json_data["data"]["allCards"]["pageInfo"]["hasNextPage"]
  total_records_pg = len(json_data["data"]["allCards"]["edges"])
  for i in range(total_records_pg):
        
        card_title =              json_data["data"]["allCards"]["edges"][i]["node"]["title"]
        card_data_d =             json_data["data"]["allCards"]["edges"][i]["node"]["fields"]
        card_data = {x['name']:x['value'] for x in card_data_d}

        card_current_phase =        json_data['data']['allCards']['edges'][i]['node']["current_phase"]
        card_createdat =            json_data['data']['allCards']['edges'][i]['node']["createdAt"]
        card_id =                   json_data['data']['allCards']['edges'][i]['node']["id"]
        card_data['id']=            card_id
        card_data['createdAt']=     card_createdat
        card_data['current_phase']= card_current_phase
        
        records_df = records_df.append(card_data, ignore_index=True)

records_df.info()

df = records_df

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

df = df.reset_index(drop=True)

#Enviando para o S3
filename = "/tmp/mydata.csv"
df.to_csv(filename, header=True)
        
#Definir o client da aws
client = boto3.client(
            's3',
            aws_access_key_id= 'X',
            aws_secret_access_key='X'
        )
        
client.upload_file(filename,'bowe-bi',f'{nome_arquivo}.csv')
