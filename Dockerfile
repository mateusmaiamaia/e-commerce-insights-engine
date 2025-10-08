# Dockerfile (Versão 2 - Atualizado para a estrutura com 'src')

# 1. Imagem Base: Começamos com uma imagem oficial que já vem com Python e Playwright.
FROM mcr.microsoft.com/playwright/python:v1.44.0-jammy

# 2. Diretório de Trabalho: Define o diretório onde nossa aplicação vai viver dentro do contêiner.
WORKDIR /app

# 3. Copiar Dependências: Copia o arquivo de requisitos para o contêiner.
COPY requirements.txt .

# 4. Instalar Dependências: Instala as bibliotecas Python.
RUN pip install --no-cache-dir -r requirements.txt

# 5. Copiar o Código da Aplicação: AQUI ESTÁ A MUDANÇA!
# Copiamos o conteúdo da nossa pasta local 'src' para o diretório de trabalho '/app' no contêiner.
COPY ./src .

# 6. Comando de Execução: Define o comando para rodar o scraper quando o contêiner iniciar.
CMD ["python3", "product_discovery.py"]