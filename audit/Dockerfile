# Usa a imagem oficial do RAPIDS (Vem com CUDA, Python 3.11 e o cuML para o UMAP em GPU)
FROM rapidsai/base:24.04-cuda12.2-py3.11

# Define a pasta de trabalho dentro do contêiner
WORKDIR /app

# Copia o arquivo de dependências para dentro
COPY NLP_requirements.txt .

# O RAPIDS usa um ambiente Conda chamado 'base'. 
# 1º: Instalamos o PyTorch compilado para CUDA 12.1 (compatível com a base 12.2)
RUN pip install torch torchvision torchaudio --index-url https://download.pytorch.org/whl/cu121

# 2º: Instalamos o resto do seu ecossistema (BERTopic, LIWC, Wordcloud, etc.)
RUN pip install -r NLP_requirements.txt

# Copia todo o código do seu projeto para dentro do contêiner
COPY . .

# Comando que mantém o contêiner vivo para podermos interagir ou rodar o script
CMD ["bash"]