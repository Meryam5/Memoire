import os
from google.cloud import bigquery
import os

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "credentials.json"

def get_bigquery_client(table_id):
    client = bigquery.Client()
    table = client.get_table(table_id)
    df = client.list_rows(table).to_dataframe()
    return df

table_id = "gcp-data-energy-11459.Conso_realisee_perimetre.Perimetre_Soutirage"
df = get_bigquery_client(table_id)
print(df.head())

table_conso = "gcp-data-energy-11459.Conso_realisee_perimetre.conso-realisee-UPLE"
df_conso = get_bigquery_client(table_conso)
print(df_conso.head())

df_tr = df[df['Cat'] =='TR'].copy()

df_tr_C2 = df_tr[df_tr['Sous profil'] == 'C2'].copy()
df_tr_C4 = df_tr[df_tr['Sous profil'] == 'C4'].copy()

df_tr_C2 = df_tr_C2[['PRM']]
df_tr_C4 = df_tr_C4[['PRM']]

print(df_tr_C2.head())
print(df_tr_C4.head())

df_tr_C2.to_csv('df_tr_C2.csv', index=False)
df_tr_C4.to_csv('df_tr_C4.csv', index=False)