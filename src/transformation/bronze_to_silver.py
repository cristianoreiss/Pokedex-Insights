import polars as pl
from pathlib import Path

raiz_projeto = Path(__file__).resolve().parents[2]
caminho_dados_bronze = raiz_projeto / "data" / "bronze"
caminho_dados_silver = raiz_projeto / "data" / "silver"
caminho_dados_silver.mkdir(parents=True,exist_ok=True)

df_pokemon = pl.read_parquet(caminho_dados_bronze / "pokemon.parquet")
df_pokemon_abilities = pl.read_parquet(caminho_dados_bronze / "pokemon_abilities.parquet")
df_pokemon_stats = pl.read_parquet(caminho_dados_bronze / "pokemon_stats.parquet")
df_pokemon_type = pl.read_parquet(caminho_dados_bronze / "pokemon_types.parquet")

df_pokemon = df_pokemon.with_columns(
    weight=pl.when(pl.col("weight") == 0).then(pl.lit(None)).otherwise(pl.col("weight"))
)

df_pokemon = df_pokemon.with_columns(
    name=df_pokemon["name"].str.replace_all("-"," ").str.to_titlecase()
)

df_pokemon_abilities = df_pokemon_abilities.with_columns(
    ability_name=df_pokemon_abilities["ability_name"].str.replace_all("-"," ").str.to_titlecase()
)

df_pokemon_type = df_pokemon_type.with_columns(
    type_name=df_pokemon_type["type_name"].str.to_titlecase()
)

df_pokemon.write_parquet(caminho_dados_silver / "pokemon_silver.parquet")
df_pokemon_stats.write_parquet(caminho_dados_silver / "pokemon_stats_silver.parquet")
df_pokemon_type.write_parquet(caminho_dados_silver / "pokemon_types_silver.parquet")
df_pokemon_abilities.write_parquet(caminho_dados_silver / "pokemon_abilities_silver.parquet")