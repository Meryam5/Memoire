# Import des données météo

import pandas as pd
import os
from google.cloud import bigquery
import pytz

# Chemin de sauvegarde
output = r"C:\Users\Meryam GHASSIR\UPLE Dropbox\Achat & Appro\SAUVEGARDE OUTILS\Calcul-Portefeuille\P50\Injection\test-models\Consommation\Output"

# Fonction d'accès à la base données
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "credentials.json"

def get_bigquery_client(table_id):
    client = bigquery.Client()
    table = client.get_table(table_id)
    df = client.list_rows(table).to_dataframe()
    return df

# Application de la fonction
table_meteo = "gcp-data-energy-11459.weather_data.weather_ods"
df_meteo = get_bigquery_client(table_meteo)

# Retraitement température (en Kelvin) à mettre en °C
df_meteo['t'] = df_meteo['t'] - 273.15  
df_meteo.rename(columns={'t': 'temperature', 'ff':'force_vent', 'u':'humidite', 'pres':'pression_atmo'}, inplace=True)

# Ordonner les données dans l'ordre chronologique
df_meteo = df_meteo.sort_values(by='date', ascending=True)

# Mise du datetime en index
df_meteo['date'] = pd.to_datetime(df_meteo['date'], format='%Y-%m-%d %H:%M:%S', utc=True)
df_meteo.set_index('date', inplace=True)
paris_tz = pytz.timezone("Europe/Paris")
df_meteo.index = df_meteo.index.tz_convert(paris_tz)
df_meteo.index = df_meteo.index.tz_localize(None)

print(df_meteo.tail())

df_meteo.to_csv(os.path.join(output,"data_meteo.csv"), index=True, sep=";")
