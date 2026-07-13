import polars as pl
from pathlib import Path

raiz_projeto = Path(__file__).resolve().parents[2]
caminho_dados_silver = raiz_projeto / "data" / "silver"
caminho_dados_gold = raiz_projeto / "data" / "gold"
caminho_dados_gold.mkdir(parents=True,exist_ok=True)