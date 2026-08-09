import streamlit as st
from pathlib import Path
import polars as pl

raiz_projeto = Path(__file__).resolve().parents[1]
caminho_dados_gold = raiz_projeto / "data" / "gold"

## Dados
top_attack = pl.read_parquet(caminho_dados_gold / "top_attack.parquet")
top_defense = pl.read_parquet(caminho_dados_gold / "top_defense.parquet")
top_hp = pl.read_parquet(caminho_dados_gold / "top_hp.parquet")

def escolher_stat(option_stat):
    if option_stat == "hp":
        return top_hp
    elif option_stat == "attack":
        return top_attack
    elif option_stat == "defense":
        return top_defense

st.set_page_config(
    page_title="Pokedex Insights",
    page_icon=raiz_projeto / "images" / "pokemon.png",
    layout="wide"
)

st.title("Pokedex Insights")

filtros_laterais, tela_1 = st.columns([1, 4])

with filtros_laterais:
    with st.container():
        option_stat = st.selectbox(
            "Stat",
            ["hp", "attack", "defense"],
            placeholder="Choose a stat",
            accept_new_options=False
        )
        st.write(f"You selected:", option_stat)
        opcao_stat = escolher_stat(option_stat)

        #option_type = st.multiselect(
        #    "Type",
        #    ["fire", "water", "earth", "plant", "fly"],
        #    default=None
        #)

with tela_1:
    with st.container():

        top_1 = opcao_stat.head(1)
        metrica1,metrica2 = st.columns(2)
        metrica1.metric("Nome do Pokemon", top_1["Nome"].item())
        metrica2.metric("Valor", top_1["Valor"].item())

        tabela,grafico = st.columns(2)
        tabela.dataframe(opcao_stat)
        grafico.bar_chart(opcao_stat,x="Nome",y="Valor",sort="-Valor")