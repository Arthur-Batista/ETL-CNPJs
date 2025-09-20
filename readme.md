# Extrator e Processador de Dados Públicos CNPJ

## 📖 Descrição

Este projeto é uma ferramenta de ETL (Extração, Transformação e Carga) desenvolvida em Python para processar os gigantescos volumes de dados públicos de CNPJ disponibilizados pela Receita Federal do Brasil.

O objetivo principal é transformar os múltiplos arquivos CSV, que somam dezenas de gigabytes, em um formato estruturado e otimizado para análises, utilizando um banco de dados de alta performance como o DuckDB. A ferramenta guia o usuário através de um assistente de linha de comando para facilitar todo o processo.

### Principais Funcionalidades

* **Assistente Interativo (CLI):** Guia o usuário passo a passo, desde a seleção dos arquivos até a aplicação de filtros.

* **Processamento Local:** Extrai dados diretamente dos arquivos `.zip` baixados, sem a necessidade de descompactação manual.

* **Filtro por Estado (UF):** Permite ao usuário processar dados de apenas alguns estados, reduzindo drasticamente o tempo e os recursos necessários para análises regionais.

* **Arquitetura Modular:** O código é dividido em módulos de responsabilidade única (arquivos auxiliares, processamento de empresas, processamento de estabelecimentos), facilitando a manutenção e expansão.

## 🚀 Como Começar

Siga estas instruções para configurar e executar o projeto em seu ambiente local.

### Pré-requisitos

* Python 3.8 ou superior

* Espaço em disco suficiente para armazenar os dados processados (recomenda-se no mínimo 50 GB livres).

### Instalação

1. **Clone o repositório** (ou baixe os arquivos do projeto para uma pasta local).

2. **Crie e ative um ambiente virtual** (recomendado):