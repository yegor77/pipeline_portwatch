# %%
# =====================================================
# TRATAMENTO (GOLD ZONE) - CHOKEPOINTS
# Lê SZ, aplica regras de padronização final e salva CZ
# =====================================================
import os
import glob
import json
from datetime import datetime
import pandas as pd

def run():
    print("Executando camada CZ...")
    # ================= CONFIG. =================
    BASE_DIR = "/opt/airflow/database"
    SZ_DIR = os.path.join(BASE_DIR, "SZ")
    CZ_DIR = os.path.join(BASE_DIR, "CZ")
    os.makedirs(SZ_DIR, exist_ok=True)
    os.makedirs(CZ_DIR, exist_ok=True)

    DATA_PROC = datetime.today().strftime("%Y-%m-%d")
    ARQUIVO_SAIDA = os.path.join(CZ_DIR, f"chokepoints_gold_{DATA_PROC.replace('-', '')}.parquet")
    ARQUIVO_META = ARQUIVO_SAIDA.replace(".parquet", ".json")

    # ================= UTILIDADES =================
    def salvar_parquet(df: pd.DataFrame, path: str):
        try:
            df.to_parquet(path, index=False, engine="pyarrow")
        except Exception as e:
            print(f"⚠️ PyArrow falhou ({e}). Fallback para fastparquet.")
            df.to_parquet(path, index=False, engine="fastparquet")

    # ================= LER SZ =================
    arquivos = sorted(glob.glob(os.path.join(SZ_DIR, "chokepoints_tratado_*.parquet")))
    if not arquivos:
        raise ValueError("Nenhum arquivo encontrado na SZ.")

    arquivo_mais_recente = arquivos[-1]
    df = pd.read_parquet(arquivo_mais_recente)
    print(f"Lido arquivo da SZ: {os.path.basename(arquivo_mais_recente)} | {len(df)} linhas")

    # ================= NORMALIZAÇÃO =================
    df.columns = (
        df.columns
        .str.strip()
        .str.lower()
        .str.replace(" ", "_")
        .str.replace("-", "_")
    )

    rename_map = {
        "date": "dat_referencia",
        "mes_ano": "num_mes_ano",
        "portname": "des_porto",
        "year": "num_ano",
        "n_tanker": "qtd_tankers",
        "n_cargo": "qtd_cargos",
        "n_total": "qtd_total_navios",
        "data_extracao": "dat_extracao",
        "arquivo_origem": "des_arquivo_origem_sz"
    }
    df = df.rename(columns=rename_map)

    # ================= TIPAGEM E FORMATAÇÃO =================
    df["dat_referencia"] = pd.to_datetime(df["dat_referencia"], errors="coerce")
    df["num_ano"] = pd.to_numeric(df["num_ano"], errors="coerce").astype("Int64")

    # ================= ORDENAÇÃO =================
    ordem_final = [
        "dat_referencia", "num_mes_ano", "num_ano", "des_porto",
        "qtd_tankers", "qtd_cargos", "qtd_total_navios",
        "dat_extracao", "des_arquivo_origem_sz"
    ]
    df = df[[c for c in ordem_final if c in df.columns] + [c for c in df.columns if c not in ordem_final]]

    df = df.sort_values(["dat_referencia", "des_porto"]).reset_index(drop=True)

    # ================= METADADES E CONTROLE =================
    qualidade = {
        "linhas": len(df),
        "des_portos": sorted(df["des_porto"].dropna().unique().tolist()) if "des_porto" in df.columns else [],
        "periodo_min": str(df["dat_referencia"].min().date()) if "dat_referencia" in df.columns else None,
        "periodo_max": str(df["dat_referencia"].max().date()) if "dat_referencia" in df.columns else None,
    }

    print("Qualidade (CZ):", json.dumps(qualidade, ensure_ascii=False, indent=2))

    # ================= SALVAR CZ =================
    salvar_parquet(df, ARQUIVO_SAIDA)

    meta = {
        "data_processamento": DATA_PROC,
        "arquivo_saida": ARQUIVO_SAIDA,
        "registros": len(df),
        "colunas": df.columns.tolist(),
        "qualidade": qualidade,
        "origem_sz": arquivo_mais_recente
    }
    with open(ARQUIVO_META, "w", encoding="utf-8") as f:
        json.dump(meta, f, indent=2, ensure_ascii=False)

    print(f"CZ salvo em: {ARQUIVO_SAIDA}")