import os
import json
from pathlib import Path
from collections import defaultdict

def extract_author_data(node, karma_map):
    """
    Função recursiva para navegar na árvore de comentários do Reddit.
    Extrai autor e score de cada comentário e suas respostas.
    """
    if not node or not isinstance(node, dict):
        return

    # Se for um comentário (kind 't1') ou post (kind 't3')
    data = node.get('data', {})
    author = data.get('author')
    score = data.get('score', 0)

    # Filtros de validade do autor
    if author and author not in ["[deleted]", "[removed]", "None"]:
        karma_map[author]['comment_karma'] += score
        karma_map[author]['total_karma'] += score

    # Recursão para as respostas (replies)
    replies = data.get('replies')
    if isinstance(replies, dict):
        children = replies.get('data', {}).get('children', [])
        for child in children:
            extract_author_data(child, karma_map)

# ______________________________________________________________________________________________
# Passo 2: Realiza a análise de quais usuários foram encontrados nos posts

def generate_rankings(sub_name, base_path="./json_dumps"):
    # Pathlib lida melhor com Windows/Linux
    sub_dir = Path(base_path) / sub_name
    
    if not sub_dir.exists():
        print(f"[ERRO] Diretório não encontrado: {sub_dir}")
        return

    # Estrutura: { 'user': {'post_karma': 0, 'comment_karma': 0, 'total_karma': 0} }
    stats = defaultdict(lambda: {'post_karma': 0, 'comment_karma': 0, 'total_karma': 0})
    
    files = list(sub_dir.glob("*.json"))
    total_files = len(files)
    print(f"[*] Iniciando processamento de {total_files} arquivos em {sub_name}...")

    for i, file_path in enumerate(files, 1):
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                content = json.load(f)
                
                # O Reddit JSON é uma lista: [PostListing, CommentListing]
                if not isinstance(content, list) or len(content) < 2:
                    continue

                # 1. Processar o Post (data[0])
                post_listing = content[0].get('data', {}).get('children', [])
                if post_listing:
                    p_data = post_listing[0].get('data', {})
                    p_author = p_data.get('author')
                    p_score = p_data.get('score', 0)
                    
                    if p_author and p_author not in ["[deleted]", "[removed]"]:
                        stats[p_author]['post_karma'] += p_score
                        stats[p_author]['total_karma'] += p_score

                # 2. Processar Comentários e Respostas (data[1])
                comment_children = content[1].get('data', {}).get('children', [])
                for child in comment_children:
                    extract_author_data(child, stats)

            if i % 10 == 0:
                print(f"    > Processados {i}/{total_files} arquivos...")

        except Exception as e:
            print(f"    [!] Falha no arquivo {file_path.name}: {e}")

# 3. Ordenação e Transformação
    # Primeiro: Criamos a lista ordenada (O Pylance precisa ver essa linha antes)
    sorted_users = sorted(
        stats.items(), 
        key=lambda x: x[1]['total_karma'], 
        reverse=True
    )

    # Segundo: Montamos o dicionário de output usando a lista já criada
    ranking_data = {
        "metadata": {
            "subreddit": sub_name,
            "timestamp_analysis": Path(sub_dir).stat().st_mtime,
            "total_users_found": len(sorted_users)
        },
        "rankings": [
            {
                "user": user,
                "post_karma": data['post_karma'],
                "comment_karma": data['comment_karma'],
                "total_karma": data['total_karma']
            }
            for user, data in sorted_users
        ]
    }

    # 4. Escrita do Output (Certifique-se que o nome bate aqui)
    output_filename = f"./aggregates/top_scorers_{sub_name.lower()}.json"
    with open(output_filename, 'w', encoding='utf-8') as f:
        json.dump(ranking_data, f, ensure_ascii=False, indent=4)

    print(f"\n[SUCESSO] Ranking finalizado em {output_filename}")