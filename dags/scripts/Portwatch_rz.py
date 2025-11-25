# %%
# ================= LIBS =================
import requests
from datetime import datetime
import pandas as pd
import os
import urllib3
import json
from time import sleep
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def run():
    print("Executando camada RZ...")
    # ================= CONFIG API =================
    URL = "https://services9.arcgis.com/weJ1QsnbMYJlCHdG/arcgis/rest/services/Daily_Chokepoints_Data/FeatureServer/0/query"
    ANOS = [2019, 2020, 2021, 2022, 2023, 2024, 2025]
    PORTOS = ["Suez Canal", "Panama Canal", "Cape of Good Hope", "Strait of Hormuz"]

    BASE_PATH = "/opt/airflow/database"
    LAYER = "RZ" 
    DATA_REF = datetime.today().strftime('%Y-%m-%d')
    BASE_DIR = os.path.join(BASE_PATH, LAYER)
    os.makedirs(BASE_DIR, exist_ok=True)

    # ================= CONSULTA PAGINADA =================

    def fetch_with_pagination(params, max_retries=3): # requisições paginadas
        all_features, offset = [], 0

        while True:
            p = params.copy()
            p.update({
                "resultOffset": offset,         # de onde começar a buscar
                "resultRecordCount": 2000,      # quantos registros trazer por vez
                "orderByFields": "date DESC"    # ordenação por data
            })

            for attempt in range(1, max_retries + 1):
                try:
                    r = requests.get(URL, params=p, verify=False, timeout=60)
                    r.raise_for_status()
                    data = r.json()
                    break
                except Exception as e:
                    if attempt == max_retries:
                        print(f"Erro após {max_retries} tentativas: {e}")
                        return all_features
                    sleep(1.5 * attempt)

            feats = data.get("features", [])
            all_features.extend(feats)

            if not feats or not data.get("exceededTransferLimit"): # Continua paginando enquanto a API indicar que ainda há mais dados
                break
            offset += len(feats)

        return all_features # todos os features concatenados

    # ================= COLETAS =================
    lista_dfs = []

    for ano in ANOS:
        for porto in PORTOS:
            params = {
                'where': f"year={ano} AND portname='{porto}'",
                'outFields': 'date,portname,n_tanker,n_cargo,year,n_total',
                'returnGeometry': 'false',
                'outSR': '4326',
                'f': 'json'
            }

            features = fetch_with_pagination(params) # buscar todos os registros
            if not features:
                print(f"Nenhum dado encontrado para {porto} - {ano}.")
                continue

            records = [it['attributes'] for it in features if 'attributes' in it]
            df = pd.DataFrame(records)  # criação do dataframe

            # Tipagem mínima
            if 'date' in df.columns:
                df['date'] = pd.to_datetime(df['date'], unit='ms', errors='coerce')
            for c in ['n_tanker', 'n_cargo', 'n_total', 'year']:
                if c in df.columns:
                    df[c] = pd.to_numeric(df[c], errors='coerce')

            # Deduplicação, garantir que não ocorra.
            if {'date','portname'}.issubset(df.columns):
                df = df.drop_duplicates(subset=['date','portname'])

            lista_dfs.append(df)
            print(f"Coletado: {porto} - {ano} — {len(df)} registros (total: {sum(len(d) for d in lista_dfs)})")

    # ================= CONSOLIDAÇÃO / SAVE =================
    if lista_dfs:
        df_final = pd.concat(lista_dfs, ignore_index=True)

        df_final = df_final.sort_values(by='date', ascending=False)
        col_order = ['date', 'portname', 'year', 'n_tanker', 'n_cargo', 'n_total']
        df_final = df_final[[c for c in col_order if c in df_final.columns]]
        df_final['data_extracao'] = DATA_REF

        for c in df_final.columns:
            if pd.api.types.is_extension_array_dtype(df_final[c]):
                df_final[c] = df_final[c].astype(object)

        type_map = {
            'portname': 'string',
            'year': 'Int64',
            'n_tanker': 'float64',
            'n_cargo': 'float64',
            'n_total': 'float64',
            'data_extracao': 'string'
        }
        for col, t in type_map.items():
            if col in df_final.columns:
                df_final[col] = df_final[col].astype(t, errors='ignore')

        # ================= FASTPARQUET =================
        nome_arquivo = f"chokepoints_completo_{DATA_REF.replace('-', '')}.parquet"
        caminho_final = os.path.join(BASE_DIR, nome_arquivo)
        df_final.to_parquet(caminho_final, index=False, engine="fastparquet")

        # ================= METADADOS =================
        meta = {
            "data_extracao": DATA_REF,
            "total_registros": len(df_final),
            "portos": PORTOS,
            "anos": ANOS,
            "arquivo": caminho_final,
            "colunas": df_final.columns.tolist(),
            "ultima_data": str(df_final['date'].max().date()) if 'date' in df_final.columns else None
        }
        with open(caminho_final.replace(".parquet", ".json"), "w", encoding="utf-8") as f:
            json.dump(meta, f, indent=2, ensure_ascii=False)

        # ================= LOG =================
        log_dir = "/opt/airflow/logs/portwatch_rz"
        os.makedirs(log_dir, exist_ok=True)

        log_path = os.path.join(log_dir, "exec_rz.log")

        with open(log_path, "a", encoding="utf-8") as log:
            log.write(f"[{datetime.now()}] Extração OK - {len(df_final)} registros - {caminho_final}\n")

        print(f"\nArquivo salvo: {caminho_final}")
        print(f"Total de registros: {len(df_final)}")

    else:
        print("Nenhum dado coletado para salvar.")