import pandas as pd
def arquivos_auxiliares():

    na = ["", " ", "NA", "N/A", "NULL", "null", "-"]
        

    url_natureza_juridica = "https://arquivos.receitafederal.gov.br/dados/cnpj/dados_abertos_cnpj/2025-09/Naturezas.zip"
    url_motivos = "https://arquivos.receitafederal.gov.br/dados/cnpj/dados_abertos_cnpj/2025-09/Motivos.zip"
    url_cnae = "https://arquivos.receitafederal.gov.br/dados/cnpj/dados_abertos_cnpj/2025-09/Cnaes.zip"
    url_municipios = "https://arquivos.receitafederal.gov.br/dados/cnpj/dados_abertos_cnpj/2025-09/Municipios.zip"

    # Se você QUISER manter os códigos como string (evitar cast depois),
    # troque os dtypes de NJUR/CD para 'string' e remova os .astype(int) abaixo.
    df_natureza_juridica = pd.read_csv(url_natureza_juridica, sep=";", encoding="latin1", header=None, names=["NJUR", "Natureza_Jurídica"], na_values=na, keep_default_na=True, dtype={'NJUR': 'Int64', 'Natureza_Jurídica': 'string'})

    df_motivo_situacao = pd.read_csv(url_motivos, sep=';', encoding="latin1", header=None, names=["CD", "Motivo_Situação"],na_values=na, keep_default_na=True, dtype={'CD': 'Int64', 'Motivo_Situação': 'string'})

    df_cnae = pd.read_csv(url_cnae, sep=";", encoding="latin1", header=None, names = ["CNAE", "Atividade"], na_values=na, keep_default_na=True, dtype={'CNAE': 'string', 'Atividade': 'string'})

    df_municipio = pd.read_csv(url_municipios, sep=';', encoding="latin1", header=None, names=["COD", "Município"], na_values=na, keep_default_na=True)

    # Mapa fixo
    mapa_sit_cad = {1: "NULA", 2: "ATIVA", 3: "SUSPENSA", 4: "INAPTA", 8: "BAIXADA"}

    
    # NJUR/CD como int; Natureza/Motivo como string
    # Se preferir chaves como string, troque para .astype('string') nas chaves e remova o .astype(int)
    nj = df_natureza_juridica.dropna(subset=['NJUR']).copy()
    dict_njur = dict(zip(nj['NJUR'].astype(int), nj['Natureza_Jurídica']))

    ms = df_motivo_situacao.dropna(subset=['CD']).copy()
    dict_motivo_situ = dict(zip(ms['CD'].astype(int), ms['Motivo_Situação']))

    # CNAE como string (padrão)
    cnae_clean = df_cnae.dropna(subset=['CNAE']).copy()
    dict_cnae = dict(zip(cnae_clean['CNAE'], cnae_clean['Atividade']))

    # Mapa fixo

    return df_natureza_juridica, df_motivo_situacao, df_cnae, df_municipio, dict_njur, dict_motivo_situ, dict_cnae, mapa_sit_cad