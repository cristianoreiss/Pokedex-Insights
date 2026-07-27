import streamlit as st
from pathlib import Path

raiz_projeto = Path(__file__).resolve().parents[1]


st.set_page_config(
    page_title="Pokemon",
    page_icon=raiz_projeto / "images" / "pokemon.png",
    layout="centered"

)

st.title("Meu Primeiro App com Streamlit")
st.write("Olá, mundo!")

