import os
from glob import glob
import sys
import time
import tkinter as tk
from tkinter import filedialog

# --- Constantes ---
UFS_VALIDAS = {
    'AC', 'AL', 'AP', 'AM', 'BA', 'CE', 'DF', 'ES', 'GO', 'MA', 'MT', 'MS',
    'MG', 'PA', 'PB', 'PR', 'PE', 'PI', 'RJ', 'RN', 'RS', 'RO', 'RR', 'SC',
    'SP', 'SE', 'TO'
}

# --- Funções Auxiliares ---

def solicitar_e_validar_pasta_texto():
    """
    Solicita ao usuário que cole o caminho da pasta via terminal.
    Esta é a função de fallback caso o tkinter falhe.
    """
    while True:
        caminho_pasta = input("\n➡️ Por favor, copie e cole o caminho completo para a pasta e pressione Enter: ")
        caminho_pasta = caminho_pasta.strip().strip('"')

        if validar_conteudo_pasta(caminho_pasta):
            return caminho_pasta
        else:
            continue

def selecionar_pasta_com_fallback():
    """
    Tenta abrir uma janela de diálogo com tkinter. Se falhar, usa o input de texto.
    """
    try:
        root = tk.Tk()
        root.withdraw()
        print("\n... Abrindo a janela para seleção de pasta ...")
        caminho_pasta = filedialog.askdirectory(title="Selecione a pasta com os arquivos .zip")
        root.destroy()
        
        if validar_conteudo_pasta(caminho_pasta):
            return caminho_pasta
        else:
            print("\n❌ Nenhuma pasta selecionada. O programa será encerrado.")
            sys.exit()

    except Exception as e:
        print("\n⚠️ A janela de seleção de pasta não pôde ser aberta. Usando o método de texto.")
        print(f"(Erro: {e})")
        return solicitar_e_validar_pasta_texto()

def validar_conteudo_pasta(caminho_pasta):
    """
    Verifica se um caminho de pasta é válido e contém os arquivos necessários.
    """
    if not caminho_pasta or not os.path.isdir(caminho_pasta):
        print(f"\n❌ ERRO: O caminho fornecido não é uma pasta válida.")
        return False

    print(f"\n✅ Pasta selecionada: {caminho_pasta}")
    print("Verificando a presença dos arquivos .zip necessários...")

    arquivos_empresas = glob(os.path.join(caminho_pasta, "Empresas*.zip"))
    arquivos_estabelecimentos = glob(os.path.join(caminho_pasta, "Estabelecimentos*.zip"))

    if not arquivos_empresas or not arquivos_estabelecimentos:
        print("\n❌ ERRO: A pasta selecionada não contém os arquivos necessários.")
        print("   Certifique-se de que os arquivos 'Empresas*.zip' e 'Estabelecimentos*.zip' estão presentes.")
        return False
        
    print(f"  - Encontrados {len(arquivos_empresas)} arquivos de Empresas.")
    print(f"  - Encontrados {len(arquivos_estabelecimentos)} arquivos de Estabelecimentos.")
    print("✅ Verificação concluída com sucesso!")
    return True

def solicitar_ufs():
    """
    Solicita ao usuário que insira as UFs para filtrar os dados.
    Valida a entrada e retorna uma lista de UFs.
    """
    while True:
        print("\n" + "-" * 50)
        print("FILTRO POR ESTADO (OPCIONAL)")
        entrada_ufs = input(
            "➡️ Digite as siglas dos estados que deseja extrair, separadas por vírgula (ex: SP, RJ, BA).\n"
            "   Deixe em branco e pressione Enter para extrair TODOS os estados: "
        )

        if not entrada_ufs.strip():
            print("\n✅ Nenhum filtro aplicado. Todos os estados serão processados.")
            return []

        ufs_lista = [uf.strip().upper() for uf in entrada_ufs.split(',')]
        ufs_invalidas = [uf for uf in ufs_lista if uf not in UFS_VALIDAS]

        if ufs_invalidas:
            print(f"\n❌ ERRO: As seguintes siglas de UF são inválidas: {', '.join(ufs_invalidas)}")
            print("   Por favor, verifique e tente novamente.")
            continue
        
        print(f"\n✅ Filtro aplicado. Serão processados os estados: {', '.join(sorted(ufs_lista))}")
        return sorted(ufs_lista)

# --- Início do Programa Principal ---

print("="*50)
print("     Seja bem-vindo ao Extrator de Bases CNPJ")
print("="*50)
print("""
Este assistente irá guiá-lo para carregar os dados de Empresas
e Estabelecimentos da Receita Federal em um banco de dados
de alta performance para suas análises.
""")

opcao = input("""
Escolha como você fornecerá os dados:
[1] A partir de uma pasta local com os arquivos .zip já baixados
[2] A partir de um download da web (em desenvolvimento)

Digite 1 ou 2 para escolher a opção: """)

while opcao not in ['1', '2']:
    opcao = input("Opção inválida. Por favor, digite 1 ou 2: ")

if opcao == '2':
    print("\n🚧 Esta funcionalidade ainda está em construção.")
    print("Por favor, baixe os arquivos manualmente e use a opção [1].")
    sys.exit()

else:
    print("\nÓtimo! Vamos carregar os arquivos a partir do seu computador.")
    
    caminho_selecionado = selecionar_pasta_com_fallback()
    ufs_selecionadas = solicitar_ufs()
    
    print("\n" + "="*50)
    print("🚀 INICIANDO PROCESSAMENTO COMPLETO DOS DADOS")
    print("   Isso pode levar vários minutos, dependendo do seu computador.")
    print("="*50)
    start_time = time.time()

    # Supondo que essas funções existem em outros arquivos .py
    from arquivos_auxiliares import arquivos_auxiliares
    from processamento_empresa import processar_empresas
    from processamento_estabelecimentos import processar_estabelecimentos

    # Definição das colunas (mantido como no seu código)
    colunas_empresas = [
        "CNPJ", "RAZÃO_SOCIAL", "NJUR", "QUALIFIC", "CAPITAL_SOCIAL", "PORTE", "ENTE_FED"
    ]
    
    print("\n(1/4) Processando arquivos auxiliares...")
    df_natureza_juridica, df_motivo_situacao, df_cnae, df_municipio, dict_njur, dict_motivo_situ, dict_cnae, mapa_sit_cad = arquivos_auxiliares()
    print("✅ Arquivos auxiliares carregados.")

    print("\n(2/4) Processando dados das Empresas...")
    dim_empresas = processar_empresas(caminho_selecionado, colunas_empresas, dict_njur)
    print("✅ Dados das Empresas processados.")

    print("\n(3/4) Processando dados dos Estabelecimentos...")
    processar_estabelecimentos(caminho_selecionado, mapa_sit_cad, df_motivo_situacao, df_cnae, df_municipio, df_natureza_juridica, dim_empresas, ufs_selecionadas)
    print("✅ Dados dos Estabelecimentos processados.")

    end_time = time.time()
    total_time = end_time - start_time
    
    print("\n" + "="*50)
    print("🎉 PROCESSAMENTO FINALIZADO COM SUCESSO! 🎉")
    print(f"Tempo total de execução: {total_time / 60:.2f} minutos.")
    print("Seus dados foram processados e estão prontos para análise.")
    print("="*50)

