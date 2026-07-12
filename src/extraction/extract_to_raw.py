import json
import requests
from pathlib import Path

raiz_projeto = Path(__file__).resolve().parents[2]

caminho_pasta_dados = raiz_projeto / "data" / "raw"
caminho_pasta_dados.mkdir(parents=True, exist_ok=True)

url_paginacao = "https://pokeapi.co/api/v2/pokemon/"
sessao = requests.Session()

while(url_paginacao):
    response = sessao.get(url_paginacao)
    pagina = response.json()

    prox_pagina = pagina['next']
    dados_brutos = pagina['results']

    for item in dados_brutos:
        nome_pokemon = item['name']
        url_pokemon = item['url']
        caminho_arquivo = caminho_pasta_dados / f"{nome_pokemon}.json"
        detalhes_pokemon = sessao.get(url_pokemon).json()
        
        with open(caminho_arquivo,"w") as arquivo:
            json.dump(detalhes_pokemon,arquivo, indent=4)
    url_paginacao = prox_pagina

