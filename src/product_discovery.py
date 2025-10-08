# product_discovery.py (Versão 5 - Final e Otimizada)
import asyncio
from playwright.async_api import async_playwright
import pandas as pd
from sqlalchemy import create_engine, text
import time
import random
import re

DB_ENGINE = create_engine('sqlite:///reviews.db')
BASE_URL = "https://www.amazon.com.br"

def get_pending_category():
    """Busca uma categoria pendente para processar."""
    with DB_ENGINE.connect() as connection:
        query = text("SELECT category_url, category_name FROM bestseller_categories WHERE status = 'pending' LIMIT 1")
        result = connection.execute(query).fetchone()
        if result:
            return {"url": result[0], "name": result[1]}
    return None

def update_category_status(url: str, status: str):
    """Atualiza o status da categoria no banco de dados."""
    with DB_ENGINE.connect() as connection:
        stmt = text("UPDATE bestseller_categories SET status = :status WHERE category_url = :url")
        connection.execute(stmt, {"status": status, "url": url})
        connection.commit()

def save_products_to_db(products_list: list):
    """Salva a lista de produtos extraídos na tabela 'product_details'."""
    if not products_list:
        print("Nenhum produto novo para salvar.")
        return
    df = pd.DataFrame(products_list)
    df['extracted_at'] = pd.to_datetime('now', utc=True)
    df.to_sql('product_details', con=DB_ENGINE, if_exists='append', index=False)
    print(f"--- Sucesso! {len(df)} produtos foram salvos na tabela 'product_details'.")


async def scrape_bestseller_page(page, category_url: str, category_name: str):
    """
    Visita uma página de categoria e extrai NOME, PREÇO, NOTA e AVALIAÇÕES de todos os produtos.
    """
    print(f"Buscando produtos em: {category_url}")
    all_products_data = []
    
    try:
        await page.goto(category_url, wait_until='domcontentloaded', timeout=90000)
        await page.wait_for_selector('div#gridItemRoot', timeout=30000)

        # Pega todos os "cards" de produtos da página
        product_cards = await page.locator('div#gridItemRoot').all()
        print(f"Encontrado {len(product_cards)} cards de produtos na página.")

        for card in product_cards:
            product_data = {'category': category_name}

            # --- Extração de cada dado DENTRO do card ---
            
            # NOME e URL
            link_locator = card.locator('a.a-link-normal[href*="/dp/"]')
            try:
                name_div = await link_locator.locator('div._cDEzb_p13n-sc-css-line-clamp-3_g3dy1, div._cDEzb_p13n-sc-css-line-clamp-4_2q2cc, div._cDEzb_p13n-sc-css-line-clamp-2_EWgCb').first.inner_text()
                product_data['name'] = name_div.strip()
                href = await link_locator.first.get_attribute('href')
                product_data['url'] = BASE_URL + href.split('/ref=')[0] if href else None
            except Exception:
                product_data['name'] = None
                product_data['url'] = None

            # NOTA MÉDIA
            try:
                rating_text = await card.locator('.a-icon-alt').first.inner_text()
                product_data['rating_avg'] = float(rating_text.split()[0].replace(',', '.'))
            except Exception:
                product_data['rating_avg'] = None

            # NÚMERO DE AVALIAÇÕES
            try:
                reviews_text = await card.locator('.a-size-small').first.inner_text()
                product_data['reviews_count'] = int(reviews_text.replace('.', ''))
            except Exception:
                product_data['reviews_count'] = None

            # PREÇO
            try:
                price_text = await card.locator('._cDEzb_p13n-sc-price_3mJ9Z').first.inner_text()
                price_match = re.search(r'[\d,.]+', price_text)
                product_data['price_brl'] = float(price_match.group(0).replace('.','').replace(',', '.')) if price_match else None
            except Exception:
                product_data['price_brl'] = None

            # Adiciona à lista apenas se o nome do produto foi encontrado
            if product_data.get('name'):
                all_products_data.append(product_data)

        return all_products_data

    except Exception as e:
        print(f"ERRO GERAL ao processar a categoria {category_name}. Detalhes: {e}")
        return []


async def main():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()

        while True:
            category = get_pending_category()
            if category:
                print(f"\n--- Processando categoria: {category['name']} ---")
                
                # Chama a função principal de scraping
                products = await scrape_bestseller_page(page, category['url'], category['name'])
                
                if products:
                    save_products_to_db(products)
                    update_category_status(category['url'], 'processed')
                else:
                    update_category_status(category['url'], 'failed')
                
                sleep_duration = random.uniform(5, 10)
                print(f"Pausa de {sleep_duration:.2f} segundos...")
                await asyncio.sleep(sleep_duration)
            else:
                print("\n--- Nenhuma categoria pendente. Processo concluído. ---")
                break
        
        await browser.close()

if __name__ == '__main__':
    # Limpa as tabelas para um novo começo
    try:
        with DB_ENGINE.connect() as connection:
            connection.execute(text("DROP TABLE IF EXISTS product_details"))
            connection.execute(text("DROP TABLE IF EXISTS product_urls"))
            # Reseta o status das categorias para 'pending'
            connection.execute(text("UPDATE bestseller_categories SET status = 'pending'"))
            connection.commit()
        print("Tabelas 'product_details' e 'product_urls' antigas removidas. Status das categorias resetado.")
    except Exception:
        pass # Tabelas podem não existir, sem problemas.

    asyncio.run(main())