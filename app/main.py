import streamlit as st
from pathlib import Path

raiz_projeto = Path(__file__).resolve().parents[1]


st.set_page_config(
    page_title="Pokedex Insights",
    page_icon=raiz_projeto / "images" / "pokemon.png",
    layout="centered"

)

st.title("Pokedex Insights")

filtros_laterais, tela_1 = st.columns([1, 3])
with filtros_laterais:
    option = st.selectbox(
        "Stat",
        ["hp","attack","defense"],
        placeholder="Choose a stat",
        accept_new_options=False
    )
    st.write(f"You selected:", option)

    option_type = st.multiselect(
        "Type",
        ["fire","water","earth","plant","fly"],
        default=None
    )
    
with tela_1:
    st.write('Primeira Tela')