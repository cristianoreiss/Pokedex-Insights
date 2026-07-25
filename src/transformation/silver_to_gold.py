import polars as pl
from pathlib import Path

raiz_projeto = Path(__file__).resolve().parents[2]
caminho_dados_silver = raiz_projeto / "data" / "silver"
caminho_dados_gold = raiz_projeto / "data" / "gold"
caminho_dados_gold.mkdir(parents=True,exist_ok=True)

pokemon = pl.read_parquet(caminho_dados_silver / "pokemon_silver.parquet")
pokemon_stats = pl.read_parquet(caminho_dados_silver / "pokemon_stats_silver.parquet")
pokemon_type = pl.read_parquet(caminho_dados_silver / "pokemon_types_silver.parquet")

##print(pokemon)
##print(pokemon_stats)

pokemon_table = pokemon.join(pokemon_stats,on="id",how="left")

pokemon_table = pokemon_table.pivot(
    index=["id", "name", "height", "weight", "base_experience"],
    on="stat_name",
    values="base_stat"
)
print(pokemon_table)