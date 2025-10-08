# category_discovery.py
import requests
from bs4 import BeautifulSoup
import pandas as pd
from sqlalchemy import create_engine
import time

# A URL principal da página de "Mais Vendidos" da Amazon Brasil
BESTSELLERS_HOME_URL = "https://www.amazon.com.br/gp/bestsellers"
BASE_URL = "https://www.amazon.com.br"

def discover_categories(url: str):
    """
    Acessa a página principal dos mais vendidos e extrai as URLs de cada categoria.
    """
    print(f"Buscando categorias em: {url}")
    try:
        # --- HEADERS MELHORADOS ---
        # Este é o novo conjunto de cabeçalhos para parecer mais com um navegador real.
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'Accept-Language': 'pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7',
            'Accept-Encoding': 'gzip, deflate, br',
            'DNT': '1', # Do Not Track
            'Upgrade-Insecure-Requests': '1',
        }
        
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status() # Lança um erro para status ruins (como 503)

        soup = BeautifulSoup(response.text, 'html.parser')

        # Seletor para encontrar os links das categorias na barra lateral
        category_list_items = soup.select("li._p13n-zg-nav-tree-all_style_zg-browse-item__1rdKf")

        if not category_list_items:
            print("AVISO: Nenhum item de categoria encontrado. O seletor CSS pode ter mudado.")
            return None

        categories_data = []
        for item in category_list_items:
            link_tag = item.find('a')
            if link_tag and link_tag.has_attr('href'):
                category_name = link_tag.text.strip()
                relative_url = link_tag['href']
                full_url = BASE_URL + relative_url
                
                categories_data.append({
                    "category_name": category_name,
                    "category_url": full_url
                })
        
        return pd.DataFrame(categories_data)

    except requests.exceptions.RequestException as e:
        print(f"ERRO: Falha ao buscar a página de categorias. Detalhes: {e}")
        return None

def save_categories_to_db(categories_df: pd.DataFrame):
    """Salva o DataFrame de categorias em uma nova tabela no banco de dados."""
    if categories_df is None or categories_df.empty:
        print("Nenhuma categoria para salvar.")
        return

    # Adiciona colunas de controle
    categories_df['status'] = 'pending'
    categories_df['discovered_at'] = pd.to_datetime('now', utc=True)
    
    engine = create_engine('sqlite:///reviews.db')
    
    # Salva os dados em uma nova tabela chamada 'bestseller_categories'
    categories_df.to_sql('bestseller_categories', con=engine, if_exists='replace', index=False)
    
    print(f"Sucesso! {len(categories_df)} categorias foram salvas na tabela 'bestseller_categories'.")
    print("\nExemplo das primeiras 5 categorias encontradas:")
    print(categories_df.head())


if __name__ == '__main__':
    # Orquestra a execução
    df_categories = discover_categories(BESTSELLERS_HOME_URL)
    save_categories_to_db(df_categories)