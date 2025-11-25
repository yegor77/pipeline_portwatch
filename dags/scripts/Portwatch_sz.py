# %%
# ================= CONFIG API =================
import os
import glob
import json
from datetime import datetime
import pandas as pd

def run():
    print("Executando camada SZ...")
    # ================= CONFIG DIRETORIOS =================
    BASE_DIR = "/opt/airflow/database"
    RZ_DIR = os.path.join(BASE_DIR, "RZ")
    SZ_DIR = os.path.join(BASE_DIR, "SZ")
    os.makedirs(RZ_DIR, exist_ok=True)
    os.makedirs(SZ_DIR, exist_ok=True)

    DATA_PROC = datetime.today().strftime("%Y-%m-%d")
    ARQUIVO_SAIDA = os.path.join(SZ_DIR, f"chokepoints_tratado_{DATA_PROC.replace('-', '')}.parquet")
    ARQUIVO_META = ARQUIVO_SAIDA.replace(".parquet", ".json")

    # ================= FASTPARQUET =================
    def salvar_parquet(df: pd.DataFrame, path: str):
            df.to_parquet(path, index=False, engine="fastparquet")

    # ================= LER RZ =================
    arquivos = sorted(glob.glob(os.path.join(RZ_DIR, "chokepoints_completo_*.parquet")))
    if not arquivos:
        raise SystemExit("⚠️ Nenhum arquivo encontrado na RZ.")

    dfs = []
    for f in arquivos:
        df = pd.read_parquet(f)
        df["arquivo_origem"] = os.path.basename(f)
        dfs.append(df)

    df = pd.concat(dfs, ignore_index=True)
    print(f"Lidos {len(arquivos)} arquivos | {len(df)} linhas totais (RZ)")

    # ================= PADRONIZAÇÃO =================
    rename_map = {
        "portname": "portname",
        "year": "year",
        "n_tanker": "n_tanker",
        "n_cargo": "n_cargo",
        "n_total": "n_total",
        "date": "date",
        "data_extracao": "data_extracao"
    }
    # garante apenas colunas esperadas + auxiliares
    colunas_esperadas = ["date","portname","year","n_tanker","n_cargo","n_total","data_extracao","arquivo_origem"]
    df = df[[c for c in df.columns if c in rename_map or c in ["data_extracao","arquivo_origem"]]].rename(columns=rename_map)

    # nomes "snake_case"
    df.columns = [c.strip().lower() for c in df.columns]

    # ================= TIPAGEM =================
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"], errors="coerce")

    for c in ["year", "n_tanker", "n_cargo", "n_total"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    if "data_extracao" in df.columns:
        df["data_extracao"] = pd.to_datetime(df["data_extracao"], errors="coerce").dt.date.astype("string")

    # ================= LIMPEZA E NORMALIZAÇÃO =================
    if "portname" in df.columns:
        df["portname"] = df["portname"].astype("string").str.strip().str.title()
    df = df.dropna(subset=["date", "portname"])
    for c in ["n_tanker", "n_cargo", "n_total"]:
        if c in df.columns:
            df.loc[df[c] < 0, c] = pd.NA

    if set(["n_tanker","n_cargo","n_total"]).issubset(df.columns):
        mask = df["n_tanker"].notna() & df["n_cargo"].notna()
        df.loc[mask, "n_total_calc"] = df.loc[mask, "n_tanker"] + df.loc[mask, "n_cargo"]
        df["n_total"] = df["n_total"].fillna(df["n_total_calc"])
        df["flag_inconsistencia_total"] = (df["n_total"].round(3) != df["n_total_calc"].round(3)).fillna(False)
        df.drop(columns=["n_total_calc"], inplace=True, errors="ignore")
        df = df.sort_values("date").drop_duplicates(subset=["date","portname"], keep="last")

    # ================= DERIVADAS =================
    df["mes_ano"] = df["date"].dt.to_period("M").dt.to_timestamp().dt.strftime("%Y-%m-%d")

    # ================= ORDENAÇÃO =================
    ordem = [
        "date", "mes_ano", "portname", "year",
        "n_tanker", "n_cargo", "n_total",
        "data_extracao", "arquivo_origem"
    ]
    df = df[[c for c in ordem if c in df.columns]]
    df = df[[c for c in ordem if c in df.columns] + [c for c in df.columns if c not in ordem]]

    qualidade = {
        "linhas_totais": len(df),
        "nulos_date": int(df["date"].isna().sum()) if "date" in df.columns else None,
        "nulos_portname": int(df["portname"].isna().sum()) if "portname" in df.columns else None,
        "negativos": {
            c: int((df[c] < 0).sum()) for c in ["n_tanker","n_cargo","n_total"] if c in df.columns
        },
        "inconsistencias_total": int(df.get("flag_inconsistencia_total", pd.Series(False)).sum()),
        "periodo_min": str(df["date"].min().date()) if "date" in df.columns else None,
        "periodo_max": str(df["date"].max().date()) if "date" in df.columns else None,
        "portos_distintos": sorted(df["portname"].dropna().unique().tolist()) if "portname" in df.columns else []
    }

    print("Qualidade (resumo):", json.dumps(qualidade, ensure_ascii=False, indent=2))

    # ================= SALVAMENTO =================
    salvar_parquet(df, ARQUIVO_SAIDA)

    meta = {
        "data_processamento": DATA_PROC,
        "arquivo_saida": ARQUIVO_SAIDA,
        "registros": len(df),
        "colunas": df.columns.tolist(),
        "qualidade": qualidade,
        "origens": arquivos
    }
    with open(ARQUIVO_META, "w", encoding="utf-8") as f:
        json.dump(meta, f, indent=2, ensure_ascii=False)

    print(f"SZ salvo em: {ARQUIVO_SAIDA}")