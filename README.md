# <img src="https://raw.githubusercontent.com/PokeAPI/sprites/master/sprites/items/poke-ball.png" width="30"> Pokedex Insights: Data Pipeline

Pokedex Insights é um projeto que tem como objetivo a construção de uma infraestrutura de dados end-to-end, implementando um pipeline ELT de ingestão e transformação sob o modelo de Data Lakehouse local, utilizando como fonte de dados a PokéAPI.

## 🏗️ Arquitetura do Pipeline de Dados

<p align="center">
  <img src="images/diagrama-pokedex-insights.gif" alt="Arquitetura do Pipeline de Dados" width="700"/>
</p>

#### 1. Data Sources & Ingestion
- **API:** Fonte de dados open-source disponível em [PokéAPI](https://pokeapi.co/).
- **Python:** Script de extração que consome a API e carrega no MongoDB (Camada Raw).
- **MongoDB:** Banco de dados NoSQL utilizado para armazenar os documentos JSON provenientes da API.

#### 2. Data Pipeline (Azure & Databricks)
- **Orquestração:** Todo o fluxo é gerenciado pelo Apache Airflow rodando no Docker.
- **Bronze Layer:** Ingestão dos dados brutos da PokéAPI via Python + requests, armazenando os documentos JSON no MongoDB.
- **Silver Layer:** Leitura dos documentos brutos do MongoDB, flatten dos campos aninhados via Python + Pandas e persistência em arquivos .parquet.
- **Gold Layer:** Consultas analíticas sobre os arquivos .parquet via DuckDB, gerando as tabelas agregadas prontas para consumo.

#### 2. DataViz
- **Streamlit:** Uma aplicação Python que consome os dados da camada Gold para exibir análises de forma interativa.


