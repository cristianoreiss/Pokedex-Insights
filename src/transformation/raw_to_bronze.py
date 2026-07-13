import json
import requests
from pathlib import Path
import polars as pl

raiz_projeto = Path(__file__).resolve().parents[2]
caminho_dados_raw = raiz_projeto / "data" / "raw"
caminho_dados_bronze = raiz_projeto / "data" / "bronze"
caminho_dados_bronze.mkdir(parents=True,exist_ok=True)

# criação das listas que irão virar linhas das tabelas
lista_pokemon = []
lista_stats = []
lista_types = []
lista_abilities = []

# gerar as linhas
for caminho in caminho_dados_raw.glob('*.json'):
    with open (caminho, "r") as arquivo:
        pokemon = json.load(arquivo)
    propriedades_pokemon = {
        "id":pokemon["id"],
        "name":pokemon["name"],
        "height":pokemon["height"],
        "weight":pokemon["weight"],
        "base_experience":pokemon["base_experience"]
        }
    lista_pokemon.append(propriedades_pokemon)

    for item_stat in pokemon["stats"]:
        stats_pokemon = {
            "id":pokemon["id"],
            "stat_name":item_stat["stat"]["name"],
            "base_stat":item_stat["base_stat"],
            "effort":item_stat["effort"]
        }
        lista_stats.append(stats_pokemon)

    for item_type in pokemon["types"]:
        types_pokemon = {
            "id":pokemon["id"],
            "slot":item_type["slot"],
            "type_name": item_type["type"]["name"]
        }
        lista_types.append(types_pokemon)

    for item_abilities in pokemon["abilities"]:
        abilities_pokemon = {
            "id":pokemon["id"],
            "slot":item_abilities["slot"],
            "ability_name":item_abilities["ability"]["name"],
            "is_hidden":item_abilities["is_hidden"]
        }
        lista_abilities.append(abilities_pokemon)

## transformar as linhas em dataframes
df_pokemons = pl.DataFrame(lista_pokemon)
df_stats = pl.DataFrame(lista_stats)
df_types = pl.DataFrame(lista_types)
df_abilities = pl.DataFrame(lista_abilities)

## trasformar os dataframes em parquet
df_pokemons.write_parquet(caminho_dados_bronze / "pokemon_bronze.parquet")
df_stats.write_parquet(caminho_dados_bronze / "pokemon_stats_bronze.parquet")
df_types.write_parquet(caminho_dados_bronze / "pokemon_types_bronze.parquet")
df_abilities.write_parquet(caminho_dados_bronze / "pokemon_abilities_bronze.parquet")
  
