from glob import glob
from pathlib import Path
import pandas as pd
import pyarrow as pa
from arquivos_auxiliares import arquivos_auxiliares
import gc
import os
import zipfile
import tempfile
import chardet
import duckdb
import shutil
from compactar_csv import compactar_csv_para_zip

def processar_estabelecimentos(caminho, mapa_sit_cad, df_motivo_situacao, df_cnae, df_municipio, df_natureza_juridica, dim_empresas, estados):

    print("\n🔹 Iniciando processamento dos ESTABELECIMENTOS com DuckDB...")

    paths_estab = sorted(glob(
        rf"{caminho}\Estabelecimentos*.zip"
    ))
    print(f"🔸 {len(paths_estab)} arquivos de estabelecimentos encontrados.")

    # Transforma dicionários de mapeamento em DataFrames para uso no DuckDB
    df_mapa_sit = pd.DataFrame(list(mapa_sit_cad.items()), columns=['SIT_CAD', 'STATUS'])
    df_motivo_situ = df_motivo_situacao.rename(columns={'CD': 'MOT_SIT_CAD', 'Motivo_Situação': 'MOTIVO_SITUACAO'})
    df_cnae = df_cnae.rename(columns={'CNAE': 'CNAE_PRINC', 'Atividade': 'ATIVIDADE_PRINCIPAL'})

    tmp_extract_dir = tempfile.mkdtemp(prefix="rfb_unzipped_")
    conn = None
    
    print(f"📦 Descompactando arquivos .zip para o diretório temporário: {tmp_extract_dir}")
    for zip_path in paths_estab:
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(tmp_extract_dir)

    
    from pathlib import Path
    for arquivo in Path(tmp_extract_dir).iterdir():
        if arquivo.is_file() and arquivo.suffix.upper() == ".ESTABELE":
            novo_nome = arquivo.with_suffix(".csv")
            novo_nome = str(novo_nome).replace(".","_")
            novo_nome = str(novo_nome).replace("_csv",".csv")
            novo_nome = Path(novo_nome)
            arquivo.rename(novo_nome)
            print(f"Renomeado: {arquivo.name} -> {novo_nome.name}")

    # --- CORREÇÃO APLICADA AQUI ---
    # Unificamos os loops de detecção e conversão

    src_dir = Path(tmp_extract_dir)
    dst_dir = Path(rf"{str(tmp_extract_dir)}/csvs_utf8")
    dst_dir.mkdir(parents=True, exist_ok=True)
    
    print("\n🔄 Convertendo arquivos para UTF-8...")
    for src_file in src_dir.glob("*.csv"):
        try:
            # 1. Detecta o encoding do arquivo atual
            with open(src_file, 'rb') as f:
                # Usamos uma amostra maior para mais precisão
                raw_data = f.read(2000000) 
                result = chardet.detect(raw_data)
                # Fallback para ISO-8859-1 se a confiança for baixa ou a detecção falhar
                encoding_csv = result.get('encoding', 'ISO-8859-1') if result['confidence'] > 0.7 else 'ISO-8859-1'
            
            print(f"  - Arquivo: {src_file.name}, Encoding detectado: {encoding_csv}")

            # 2. Converte o arquivo usando o encoding detectado
            dst_file = dst_dir / src_file.name
            with open(src_file, "r", encoding=encoding_csv, errors="replace") as fin, \
                 open(dst_file, "w", encoding="utf-8", newline="") as fout:
                for line in fin:
                    fout.write(line)
            
            # 3. Apaga o arquivo original após a conversão bem-sucedida
            src_file.unlink()

        except Exception as e:
            print(f"  ❌ Erro ao processar o arquivo {src_file.name}: {e}")
            continue # Pula para o próximo arquivo em caso de erro

    print("✅ Transcodificação concluída.")

    tmp_extract_dir = Path(dst_dir)
    
    print(tmp_extract_dir)
    
    csv_glob_path = os.path.join(tmp_extract_dir, "*.csv")
    conn = None

    print("\n🚀 Iniciando processamento e junção com DuckDB...")
    # Conecta a um banco de dados DuckDB em memória
    conn = duckdb.connect(database=':memory:')

    # Disponibiliza os DataFrames do Pandas para o DuckDB como se fossem tabelas
    conn.register('dim_empresas_df', dim_empresas)
    conn.register('mapa_sit_df', df_mapa_sit)
    conn.register('motivo_situ_df', df_motivo_situ)
    conn.register('cnae_df', df_cnae)
    conn.register('municipio_df', df_municipio)
    conn.register('njur_df', df_natureza_juridica)

    uf_list_norm = [u.strip().upper() for u in estados]

    query = rf"""
    WITH estabelecimentos_raw AS (
        SELECT *
        FROM read_csv(
            '{csv_glob_path}',
            header=false,
            columns={{
                'CNPJ': 'VARCHAR', 'ORDEM': 'VARCHAR', 'DV': 'VARCHAR', 'M_F': 'VARCHAR', 
                'NOME_FANTASIA': 'VARCHAR', 'SIT_CAD': 'UBIGINT', 'DT_SIT_CAD': 'VARCHAR', 
                'MOT_SIT_CAD': 'UBIGINT', 'CID_EXTERIOR': 'VARCHAR', 'PAÍS': 'VARCHAR', 
                'DT_INÍCIO': 'VARCHAR', 'CNAE_PRINC': 'BIGINT', 'CNAE_SEC': 'VARCHAR', 
                'TIP_LOG': 'VARCHAR', 'LOGRADOURO': 'VARCHAR', 'NUM': 'VARCHAR', 
                'COMPLEMENTO': 'VARCHAR', 'BAIRRO': 'VARCHAR', 'CEP': 'VARCHAR', 
                'UF': 'VARCHAR', 'MUNICÍPIO': 'VARCHAR', 'DD1': 'VARCHAR', 
                'TEL1': 'VARCHAR', 'DD2': 'VARCHAR', 'TEL2': 'VARCHAR', 'DD3': 'VARCHAR', 
                'FAX': 'VARCHAR', 'EMAIL': 'VARCHAR', 'SIT_ESP': 'VARCHAR', 'DT_SIT_ESP': 'VARCHAR'
            }},
            sep=';',
            auto_detect=false,
            quote='"',
            escape='"',
            strict_mode=false,
            parallel=false
        )
        WHERE TRIM(UPPER(UF)) IN (SELECT * FROM UNNEST(?))
    ),

    -- Monta CNPJ final = BASE(8) + ORDEM(4) + DV(2) e remove colunas brutas
    estabelecimentos AS (
        SELECT
            LPAD(REGEXP_REPLACE(CNPJ,  '[^0-9]', ''), 8, '0') AS CNPJ_BASE,
            LPAD(REGEXP_REPLACE(ORDEM, '[^0-9]', ''), 4, '0') AS ORDEM_NUM,
            LPAD(REGEXP_REPLACE(DV,    '[^0-9]', ''), 2, '0') AS DV_NUM,

            * EXCLUDE (CNPJ, ORDEM, DV),

            -- CNPJ final (14 dígitos)
            (LPAD(REGEXP_REPLACE(CNPJ,  '[^0-9]', ''), 8, '0') ||
            LPAD(REGEXP_REPLACE(ORDEM, '[^0-9]', ''), 4, '0') ||
            LPAD(REGEXP_REPLACE(DV,    '[^0-9]', ''), 2, '0')) AS CNPJ
        FROM estabelecimentos_raw
    ),

    empresas AS (
        SELECT 
            LPAD(REGEXP_REPLACE(CNPJ, '[^0-9]', ''), 8, '0') AS CNPJ_BASE,
            "RAZÃO_SOCIAL",
            "NJUR",
            "NATUREZA JURÍDICA"
        FROM dim_empresas_df
    )

    SELECT
        -- Todas as colunas com CAST para VARCHAR (string)
        CAST(est.CNPJ AS VARCHAR)                                             AS "CNPJ_",
        CAST(emp."RAZÃO_SOCIAL" AS VARCHAR)                                   AS "RAZÃO_SOCIAL",
        CAST(est.M_F AS VARCHAR)                                              AS "M_F",
        CAST(REPLACE(est.NOME_FANTASIA, '|', '') AS VARCHAR) AS "NOME_FANTASIA",
        CAST(est.SIT_CAD AS VARCHAR)                                          AS "SIT_CAD",
        CAST(sit.STATUS AS VARCHAR)                                           AS "STATUS",
        CAST(emp."NJUR" AS VARCHAR) AS "NJUR",
        CAST(emp."NATUREZA JURÍDICA" AS VARCHAR) AS "NATUREZA JURÍDICA",


        -- Datas convertidas para DD/MM/YYYY (já são VARCHAR por STRFTIME)
        CAST(
            CASE
                WHEN LENGTH(REGEXP_REPLACE(est."DT_SIT_CAD", '[^0-9]', '')) = 8
                THEN STRFTIME(TRY_STRPTIME(REGEXP_REPLACE(est."DT_SIT_CAD", '[^0-9]', ''), '%Y%m%d'), '%d/%m/%Y')
                ELSE NULL
            END
        AS VARCHAR)                                                           AS "DT_SIT_CAD",

        CAST(est.MOT_SIT_CAD AS VARCHAR)                                      AS "MOT_SIT_CAD",
        CAST(mot.MOTIVO_SITUACAO AS VARCHAR)                                  AS "MOTIVO DA SITUAÇÃO",
        CAST(est.CID_EXTERIOR AS VARCHAR)                                     AS "CID_EXTERIOR",
        CAST(est."PAÍS" AS VARCHAR)                                           AS "PAÍS",

        CAST(
            CASE
                WHEN LENGTH(REGEXP_REPLACE(est."DT_INÍCIO", '[^0-9]', '')) = 8
                THEN STRFTIME(TRY_STRPTIME(REGEXP_REPLACE(est."DT_INÍCIO", '[^0-9]', ''), '%Y%m%d'), '%d/%m/%Y')
                ELSE NULL
            END
        AS VARCHAR)                                                           AS "DT_INÍCIO",


        CAST(LPAD(CAST(est.CNAE_PRINC AS VARCHAR), 7, '0') AS VARCHAR) AS "CNAE_PRINC",
        CAST(cnae.ATIVIDADE_PRINCIPAL AS VARCHAR) AS "ATIV.PRINCIPAL",


        CAST(est.CEP AS VARCHAR)                                              AS "CEP",
        CAST(est.UF AS VARCHAR)                                               AS "UF",

        -- Nome/descrição do município vindo do DF de municípios (AJUSTE a coluna se necessário)
        CAST(municipio_rfb.Município AS VARCHAR)                                   AS "MUNICÍPIO_RFB",  -- <--- se for NOME_MUNICIPIO, troque aqui

        CAST(est.DD1 AS VARCHAR)                                              AS "DD1",
        CAST(est.TEL1 AS VARCHAR)                                             AS "TEL1",
        CAST(est.DD2 AS VARCHAR)                                              AS "DD2",
        CAST(est.TEL2 AS VARCHAR)                                             AS "TEL2",
        CAST(est.DD3 AS VARCHAR)                                              AS "DD3",
        CAST(est.EMAIL AS VARCHAR)                                            AS "EMAIL",
        CAST(est.SIT_ESP AS VARCHAR)                                          AS "SIT_ESP",
        CAST(est."DT_SIT_ESP" AS VARCHAR)                                     AS "DT_SIT_ESP"

    FROM estabelecimentos AS est
    INNER JOIN empresas         AS emp  ON est.CNPJ_BASE   = emp.CNPJ_BASE
    LEFT  JOIN mapa_sit_df      AS sit  ON est.SIT_CAD     = sit.SIT_CAD
    LEFT  JOIN motivo_situ_df   AS mot  ON est.MOT_SIT_CAD = mot.MOT_SIT_CAD
    LEFT  JOIN cnae_df          AS cnae ON est.CNAE_PRINC  = cnae.CNAE_PRINC

    -- Join com municípios: normalização do código (remove não-dígitos e alinha tipos)
    LEFT  JOIN municipio_df     AS municipio_rfb
        ON REGEXP_REPLACE(est."MUNICÍPIO", '[^0-9]', '') 
            = REGEXP_REPLACE(CAST(municipio_rfb.COD AS VARCHAR), '[^0-9]', '')
    """

    
    
    # caminho do arquivo de saída (CSV)
    if "RJ" in estados:
        output_path = rf"{caminho}/BASE_RFB_CNPJ_COMPLETO.csv"
        output_path_zip = rf"{caminho}/BASE_RFB_CNPJ_COMPLETO.zip"
    else:
        lista_formatada = "_".join(estados)
        output_path = rf"{caminho}/BASE_RFB_CNPJ_{lista_formatada}.csv"
        output_path_zip = rf"{caminho}/BASE_RFB_CNPJ_{lista_formatada}.zip"

    print(f"\n🏁 Executando a consulta e salvando o resultado em: {output_path}")

    # COPY direto para CSV com delimitador '|'
    copy_sql = f"""
    COPY ({query}) TO '{output_path}'
    (FORMAT 'CSV', DELIMITER '|', HEADER TRUE, QUOTE '"', ESCAPE '"');
    """

    conn.execute(copy_sql, [uf_list_norm])

    print("✅ Processo concluído com sucesso!")

    # Exemplo de uso:
    compactar_csv_para_zip(output_path)

    try:
        os.remove(output_path)
    except:
        pass

    dir_to_remove = Path(dst_dir)

    # Verifica se o diretório existe
    if dir_to_remove.exists() and dir_to_remove.is_dir():
        shutil.rmtree(dir_to_remove)
        print(f"✓ Diretório removido: {dir_to_remove}")
    else:
        print(f"✗ Diretório não encontrado: {dir_to_remove}")
    
