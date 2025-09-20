# 🚀 Pipeline de ETL para Dados Públicos CNPJ

## 📖 Descrição

Este projeto é uma ferramenta de ETL (Extração, Transformação e Carga) desenvolvida em Python para processar os gigantescos volumes de dados públicos de CNPJ disponibilizados pela Receita Federal do Brasil.

O objetivo principal é transformar os múltiplos arquivos CSV, que somam dezenas de gigabytes, em um formato estruturado e otimizado para análises, utilizando um banco de dados de alta performance como o DuckDB. A ferramenta guia o usuário através de um assistente de linha de comando para facilitar todo o processo.

### ✨ Principais Funcionalidades

* **Assistente Interativo (CLI):** Guia o usuário passo a passo, desde a seleção dos arquivos até a aplicação de filtros.
* **Processamento Local Eficiente:** Extrai dados diretamente dos arquivos `.zip` baixados, sem a necessidade de descompactação manual prévia.
* **Conversão Inteligente de Encoding:** Detecta e converte automaticamente arquivos com codificações de texto problemáticas (`ISO-8859-1`, `ASCII`) para o padrão UTF-8.
* **Filtro por Estado (UF):** Permite ao usuário processar dados de apenas alguns estados, reduzindo drasticamente o tempo e os recursos necessários para análises regionais.
* **Arquitetura Modular:** O código é dividido em módulos de responsabilidade única, facilitando a manutenção e a expansão.

## ⚙️ Como Começar

Siga estas instruções para configurar e executar o projeto em seu ambiente local.

### Pré-requisitos

* Python 3.8 ou superior.
* Git para clonar o repositório.

### 1. Instalação

Primeiro, clone o repositório para a sua máquina local e navegue para dentro da pasta do projeto.

git clone https://github.com/seu-usuario/seu-repositorio.git
cd seu-repositorio

É altamente recomendado criar e ativar um ambiente virtual (`venv`) para isolar as dependências do projeto.

**No Windows:**

python -m venv venv
.\venv\Scripts\activate


**No macOS / Linux:**

python -m venv venv
source venv/bin/activate


Com o ambiente virtual ativado, instale todas as dependências necessárias executando:

pip install -r requirements.txt


### 2. Estrutura de Arquivos

Para que o script funcione, você precisa criar uma pasta para armazenar os dados brutos. Crie uma pasta chamada `dados` e coloque todos os arquivos `.zip` da Receita Federal dentro dela.

```
seu-projeto/
│
├── dados/  <-- COLOQUE OS ARQUIVOS .ZIP AQUI (exemplo)
│   ├── Empresas0.zip
│   ├── Empresas1.zip
│   ├── ...
│   ├── Estabelecimentos0.zip
│   ├── Estabelecimentos1.zip
│   └── ...
│
├── main.py             <-- SCRIPT PRINCIPAL
├── arquivos_auxiliares.py
├── processamento_empresa.py
├── processamento_estabelecimentos.py
├── requirements.txt
└── README.md

```

### 3. Execução

Após baixar os dados e instalar as dependências, execute o script principal a partir do seu terminal:

python extrator_cnpj.py


O assistente interativo irá guiá-lo pelo resto do processo, solicitando o caminho para a pasta `dados` e os filtros de estado que desejar.

## 🛣️ Próximos Passos (Roadmap)

Este projeto está em desenvolvimento ativo. A funcionalidade mais aguardada para futuras atualizações é:

* **[ ] Extração via Web:** Implementar a capacidade de baixar e processar os arquivos diretamente da web, eliminando a necessidade de download manual por parte do usuário.

## 💻 Tecnologias Utilizadas

* **Python**
* **DuckDB**
* **Pandas**
* **Chardet**

## 📄 Licença
Este projeto é distribuído sob a licença MIT. Veja o arquivo `LICENSE` para mais detalhes.