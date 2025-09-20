from glob import glob
from pathlib import Path
import pandas as pd
import pyarrow as pa
from arquivos_auxiliares import arquivos_auxiliares
import gc

def processar_empresas(caminho, colunas_empresas, dict_njur):

    na = ["", " ", "NA", "N/A", "NULL", "null", "-"]

    print("🔹 Iniciando processamento das EMPRESAS...")

    # Ajuste os caminhos conforme a localização dos seus arquivos
    paths_emp = sorted(glob(
        rf"{caminho}\Empresas*.zip"
    ))
    print(f"🔸 {len(paths_emp)} arquivos de empresas encontrados.")

    dtype_empresas = {
        "CNPJ": pd.ArrowDtype(pa.large_string()),
        "RAZÃO_SOCIAL": pd.ArrowDtype(pa.large_string()),
        "NJUR": "category",
    }

    dfs_emp = []
    for p in paths_emp:
        df = pd.read_csv(
            p, sep=";", encoding="latin1", header=None, names=colunas_empresas,
            na_values=na, keep_default_na=True, engine="pyarrow",
            dtype_backend="pyarrow", dtype=dtype_empresas,
        )
        dfs_emp.append(df)
        del df
        gc.collect()

    dim_empresas = pd.concat(dfs_emp, ignore_index=True)
    del dfs_emp
    gc.collect()

    if "NJUR" in dim_empresas.columns:
        dim_empresas["NATUREZA JURÍDICA"] = dim_empresas["NJUR"].map(dict_njur).astype("category")

    dim_empresas["RAZÃO_SOCIAL"] = dim_empresas["RAZÃO_SOCIAL"].str.replace("|","")

    print(f"📊 Total de registros em empresas: {len(dim_empresas):,}")

    return dim_empresas