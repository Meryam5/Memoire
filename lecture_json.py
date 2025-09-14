# Traitement données historiques Enedis

# Packages nécessaires pour l'extraction des données
import pandas as pd
import numpy as np
import glob
import json
from concurrent.futures import ThreadPoolExecutor
import os

# Chemin du dossier contenant les fichiers JSON
dossier = r"C:\Users\Meryam GHASSIR\UPLE Dropbox\Achat & Appro\SAUVEGARDE OUTILS\Calcul-Portefeuille\P50\Injection\test-models\Consommation"

# Récupérer les données Enedis Conso C2
fichiers = glob.glob(dossier + "/Donnees-C2/*.json")

# Lecture de chaque fichier JSON et concaténation des données
def flatten_df(df):
    # On répète la normalisation tant qu'il reste des colonnes avec dict ou list
    while True:
        cols_to_flatten = []
        for col in df.columns:
            if df[col].apply(lambda x: isinstance(x, (dict, list))).any():
                cols_to_flatten.append(col)
        if not cols_to_flatten:
            break
        for col in cols_to_flatten:
            # Si list de dicts, on prend le premier élément
            df[col] = df[col].apply(lambda x: x[0] if isinstance(x, list) and len(x) > 0 else x)
            # Si dict, on normalise
            if df[col].apply(lambda x: isinstance(x, dict)).any():
                norm = pd.json_normalize(df[col])
                norm.columns = [f"{col}.{c}" for c in norm.columns]
                df = pd.concat([df.drop(columns=[col]), norm], axis=1)
    return df

def lire_json(filepath):
    try:
        print(f"Lecture du fichier : {filepath}")
        with open(filepath, encoding="utf-8") as f:
            raw = json.load(f)
        df = pd.DataFrame(raw['mesures'])
             
        df = flatten_df(df)

        mots_cles = ['etapeMetier', 'modeCalcul', 'dateDebut', 'dateFin','grandeurMetier', 'grandeurPhysique', 
                     'unite', 'iv', 'ec']
        cols_to_drop = [col for col in df.columns if any(mot in col for mot in mots_cles)]
        df = df.drop(columns=cols_to_drop, errors='ignore')

        return df
    
    except Exception as e:
        print(f"Erreur lors de la lecture du fichier {filepath}: {e}")
        return None

# Parallélisation de la lecture des fichiers JSON pour réduire le temps de chargement
with ThreadPoolExecutor(max_workers=4) as executor: # Nombre de coeurs disponibles = 8
    dfs = list(executor.map(lire_json, fichiers))

dfs = [df for df in dfs if df is not None]

# Conntôle
print(f"Nombre de fichiers trouvés : {len(fichiers)}")
print(f"Nombre de DataFrames valides : {len([df for df in dfs if df is not None])}")

# Concaténation des DataFrames
data_tr_c2 = pd.concat(dfs, ignore_index=True)

# Sauvegarder le DataFrame dans un fichier CSV
# data_tr_c2.to_csv("data_hist_c2.csv", index=False, sep=";")

# Retraitement à faire : Renommer les colonnes + Supprimer les colonnes 'n' et 'p'

data_tr_c2 = data_tr_c2.rename(columns={
    'idPrm':'PRM',
    'grandeur.points.v':'valeur_conso',
    'grandeur.points.d':'date'
})

data_tr_c2 = data_tr_c2.drop(columns=['grandeur.points.n', 'grandeur.points.p'], errors='ignore')

data_tr_c2['date'] = pd.to_datetime(data_tr_c2['date'], errors='coerce')
data_tr_c2 = data_tr_c2.sort_values(by=['date','PRM'])

data_tr_c2['valeur_conso'] = pd.to_numeric(data_tr_c2['valeur_conso'], errors='coerce')
data_tr_c2['valeur_conso'] = data_tr_c2['valeur_conso'] / 1000  # Conversion en kWh

# 1ere ligne bizarre + mettre dans l'ordre des PRM et chronologique (date + heure)

data_tr_c2.head()

# Récupérer les données Enedis Conso C4
fichiers = glob.glob(dossier + "/Donnees-C4/*.json")
fichiers = [f for f in fichiers if os.path.isfile(f) and f.lower().endswith('.json')]

# Lecture de chaque fichier JSON et concaténation des données
def lire_json(filepath):
    try:
        print(f"Lecture du fichier : {filepath}")
        with open(filepath, encoding="utf-8") as f:
            raw = json.load(f)
        df = pd.DataFrame(raw['mesures'])
        df['periode'] = df['periode'].apply(lambda x: x['dateDebut'] if isinstance(x, dict) and 'dateDebut' in x else x)
        df['periode'] = pd.to_datetime(df['periode'])
        return df
    except Exception as e:
        print(f"Erreur lors de la lecture du fichier {filepath}: {e}")
        return None

# Parallélisation de la lecture des fichiers JSON pour réduire le temps de chargement
with ThreadPoolExecutor(max_workers=4) as executor: # Nombre de coeurs disponibles = 8
    dfs = list(executor.map(lire_json, fichiers))

dfs = [df for df in dfs if df is not None]

# Contrôle
print(f"Nombre de fichiers trouvés : {len(fichiers)}")
print(f"Nombre de DataFrames valides : {len([df for df in dfs if df is not None])}")

data_tr_c4 = pd.concat(dfs, ignore_index=True)

data_tr_c4.head()

# Sauvegarder le DataFrame dans un fichier CSV
data_tr_c4.to_csv("data_hist_c4.csv", index=False, sep=";")