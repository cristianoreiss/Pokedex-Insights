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

pokemon_table = (
    pokemon
    .join(pokemon_stats,on="id",how="left")
    .pivot(
        index=["id", "name", "height", "weight", "base_experience"],
        on="stat_name",
        values="base_stat"
    )
)

print(pokemon_table)

most_10_hp_pokemon = (
    pokemon_table
    .select(["id","name","hp"])
    .sort("hp",descending=True)
    .head(10)
)

most_10_attack_pokemon = (
    pokemon_table
    .select(["id","name","attack"])
    .sort("attack",descending=True)
    .head(10)
)

most_10_defense_pokemom = (
    pokemon_table
    .select(["id","name","defense"])
    .sort("defense",descending=True)
    .head(10)

)

most_10_defense_pokemom.write_parquet(caminho_dados_gold / "top_defense")
most_10_attack_pokemon.write_parquet(caminho_dados_gold / "top_attack")
most_10_hp_pokemon.write_parquet(caminho_dados_gold / "top_hp")