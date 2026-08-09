import streamlit as st
from pathlib import Path
import pandas as pd

raiz_projeto = Path(__file__).resolve().parents[1]
caminho_dados_gold = raiz_projeto / "data" / "gold"


st.set_page_config(
    page_title="Pokedex Insights",
    page_icon=raiz_projeto / "images" / "pokemon.png",
    layout="wide"
)

st.title("Pokedex Insights")

filtros_laterais, tela_1 = st.columns([1, 4])

with filtros_laterais:
    with st.container(border=True):
        option = st.selectbox(
            "Stat",
            ["hp", "attack", "defense"],
            placeholder="Choose a stat",
            accept_new_options=False
        )
        st.write(f"You selected:", option)

        option_type = st.multiselect(
            "Type",
            ["fire", "water", "earth", "plant", "fly"],
            default=None
        )

with tela_1:
    with st.container():

        metrica1,metrica2,metrica3 = st.columns(3)
        metrica1.metric("Nome do Pokemon", "Blissey")
        metrica2.metric("Tipo", "Comum")
        metrica3.metric("Valor", "255")

        tabela,grafico = st.columns(2)
        tabela.dataframe(dataframe_teste)
        grafico.bar_chart(dataframe_teste,x="Nome",y="Valor")