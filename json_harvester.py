import requests
import time
import random
import os
import json

# Configurações Globais do Módulo
HEADERS = {'User-Agent': 'UFOP_Research_Gentle_Harvesting_With_Jitter (contact: jonas.kretli@aluno.ufop.edu.br)'}
BASE_PATH = "./json_dumps/"

# ______________________________________________________________________________________________
# Elemento mais básico: Extrai JSON de um response da página web

def get_json(url):
    try:
        response = requests.get(url, headers=HEADERS)
        if response.status_code == 200:
            return response.json()
        elif response.status_code == 429:
            print("Opa! 429 (Too Many Requests). Hora de uma soneca longa...")
            time.sleep(60) # Back-off de 1 minuto
            return None
    except Exception as e:
        print(f"Erro na requisição: {e}")
    return None

# ______________________________________________________________________________________________
# Segundo elemento básico, salva JSON dos posts em json_dumps

def save_post(data, base_path=BASE_PATH):
    try:
        sub_name = data[0]['data']['children'][0]['data']['subreddit']
        post_id = data[0]['data']['children'][0]['data']['id']
        
        target_dir = os.path.join(base_path, sub_name)
        os.makedirs(target_dir, exist_ok=True)
        
        filepath = os.path.join(target_dir, f"{post_id}.json")
        
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=4)
            
        print(f"   [OK] Post {post_id} salvo em: {target_dir}")
        
    except (KeyError, IndexError) as e:
        print(f"   [ERRO] Falha ao parsear estrutura do JSON: {e}")

# ______________________________________________________________________________________________
# Passo 1: Realiza a colheita de posts a partir do subreddit escolhido

def harvest_subreddit(subreddit_name, category, limit=25):
    """
    Função principal do módulo. Inicia a colheita em um subreddit específico.
    """
    print(f"\n[HARVESTER] Iniciando colheita no r/{subreddit_name} | Alvo: {limit} posts")
    sub_url = f"https://www.reddit.com/r/{subreddit_name}/{category}/.json?limit={limit}"
    data = get_json(sub_url)

    if not data:
        print(f"[HARVESTER] [!] Falha ao obter dados base de r/{subreddit_name}.")
        return

    posts = data['data']['children']
    for post in posts:
        post_data = post['data']
        title = post_data['title']
        permalink = post_data['permalink']
        
        print(f"\n--- Lendo Post: {title[:50]}... ---")
        
        comment_url = f"https://www.reddit.com{permalink}.json"
        comment_data = get_json(comment_url)
        
        if comment_data:
            save_post(comment_data)
            
            # Print rápido dos comentários para acompanhamento visual
            comments = comment_data[1]['data']['children']
            for c in comments[:3]: # Reduzido para 3 para não poluir o terminal
                if c['kind'] == 't1': 
                    body = c['data'].get('body', '')
                    score = c['data'].get('score', 0)
                    print(f"   [{score}] {body[:40]}...")
        
        # Jitter entre posts
        wait_time = random.uniform(3.0, 7.0)
        print(f"Aguardando {wait_time:.2f}s para o próximo...")
        time.sleep(wait_time)
        
    print(f"\n[HARVESTER] Colheita finalizada para r/{subreddit_name}.")

# ______________________________________________________________________________________________
# Passo 3: Realiza a colheita de usuários encontrados nos posts

def harvest_user(subreddit_name, limit_users=20, base_user_path="./user_dumps"):
    """
    Lê o ranking de um subreddit, seleciona os top usuários e coleta
    o histórico unificado deles, fazendo merge se o arquivo já existir.
    Se limit_users for 0, processa a lista inteira.
    """
    ranking_file = f"./aggregates/top_scorers_{subreddit_name.lower()}.json"
    os.makedirs(base_user_path, exist_ok=True)

    if not os.path.exists(ranking_file):
        print(f"[!] ERRO: Arquivo de ranking não encontrado -> {ranking_file}")
        print("    Rode o 'generate_rankings' primeiro!")
        return

    # 1. Carrega a lista de alvos
    with open(ranking_file, 'r', encoding='utf-8') as f:
        ranking_data = json.load(f)
    
    users = ranking_data.get("rankings", [])
    
    # Lógica de "Bypass" para pegar a pasta inteira
    if limit_users == 0:
        total_alvos = len(users)
        print(f"\n[USER HARVESTER] Modo de colheita TOTAL ativado.")
    else:
        total_alvos = min(limit_users, len(users))
        print(f"\n[USER HARVESTER] Modo de colheita LIMITADO ativado.")
    
    print(f"Alvos: {total_alvos} usuários do r/{subreddit_name}")

    # 2. Inicia a colheita individual
    for i, user_info in enumerate(users[:total_alvos]):
        username = user_info['user']
        user_filepath = os.path.join(base_user_path, f"{username}.json")
        
        print(f"\n   -> [{i+1}/{total_alvos}] Colhendo histórico de u/{username}...")
        
        # Endpoint de histórico do usuário
        user_url = f"https://www.reddit.com/user/{username}/.json?limit=100"
        new_data = get_json(user_url)
        
        if not new_data or 'data' not in new_data:
            print(f"      [!] Falha ou conta inacessível. Pulando.")
            continue 

        # 3. Lógica de Merge
        if os.path.exists(user_filepath):
            with open(user_filepath, 'r', encoding='utf-8') as f:
                existing_data = json.load(f)
            
            existing_ids = {item['data']['id'] for item in existing_data['data']['children']}
            
            new_items_count = 0
            for item in new_data['data']['children']:
                if item['data']['id'] not in existing_ids:
                    existing_data['data']['children'].append(item)
                    new_items_count += 1
            
            final_data = existing_data
            print(f"      [MERGE] +{new_items_count} novas interações.")
        else:
            final_data = new_data
            print(f"      [NOVO] Perfil criado com {len(final_data['data']['children'])} itens.")

        # 4. Salva o perfil atualizado
        with open(user_filepath, 'w', encoding='utf-8') as f:
            json.dump(final_data, f, ensure_ascii=False, indent=4)
            
        wait_time = random.uniform(3.0, 7.0)
        print(f"      Aguardando {wait_time:.2f}s...")
        time.sleep(wait_time)

    print(f"\n[USER HARVESTER] Colheita finalizada para {subreddit_name}.")