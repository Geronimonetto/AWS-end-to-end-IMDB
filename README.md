# Projeto API de filmes

O projeto é baseado em serviços de cloud e tem como principal objetivo verificar os filmes contidos em um arquivo csv, que faltam informações dentro dele, 
sendo assim necessário utilização da API do TMDB para consultas dos IDs dos filmes e obter essas informações adicionais.

Requisitos mínimos: 
- Criar API no TMDB - https://developer.themoviedb.org/reference/intro/getting-started
- Conhecimento em AWS

### Criando API no TMDB

![image](https://github.com/Geronimonetto/Projeto_API_Filmes/blob/main/01.API/01.Criando_chave_de_autentica%C3%A7%C3%A3o_api.PNG)

Contudo após a criação da API podemos verificar a documentação da API no site

### Testando API no VsCode

```python
import requests
import pandas as pd
import json
from IPython.display import display

api_key = "ea33ae145ce37250f8ab09b9583b7a7f"
url = f"https://api.themoviedb.org/3/movie/top_rated?api_key={api_key}&language=pt-BR"


class BuscaFilmes:

    def __init__(self, url: str) -> None:
        self.url = url
        self.response = requests.get(self.url)
        self.data = self.response.json()
        self.filmes = []

    def pegando_dados(self):
        for movie in self.data['results']:
            df = {'Titulo': movie['title'],
                  'Data de lançamento': movie['release_date'],
                  'Visão geral': movie['overview'],
                  'Votos': movie['vote_count'],
                  'Média de votos:': movie['vote_average']}
            self.filmes.append(df)
            with open("filmes_gerais.json", "w") as arquivo:
                json.dump(self.data, arquivo, indent=4)

    def imprimindo_dados(self):
        df = pd.DataFrame(self.filmes)
        pd.set_option('display.max_columns', None)
        pd.set_option('display.expand_frame_repr', False)
        display(df)

    def run(self):
        self.pegando_dados()
        self.imprimindo_dados()


if __name__ == "__main__":
    buscador_filmes = Busca_Filmes(url)
    buscador_filmes.run()

```

Através do teste sabemos que a API está retornando dados, e a partir disso podemos obter informações para nossa consulta e análises, 
faremos isto através do serviço da AWS Web Services o AWS Lambda, serviço que executa códigos em nuvem

Porém para executar funções no AWS Lambda precisamos importar camadas que servem como libs

Passo a Passo da criação da camada pandas

![https://github.com/Geronimonetto/Projeto_API_Filmes/blob/main/02.Camada_Lambda/01.Criando%20Camada%20para%20AWS%20Lambda.PNG](https://github.com/Geronimonetto/Projeto_API_Filmes/blob/main/02.Camada_Lambda/01.Criando%20Camada%20para%20AWS%20Lambda.PNG)

