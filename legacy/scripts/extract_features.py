"""
extract_features.py

Transforma 1.1 milhão de nós em uma Matriz de Características (Feature Vector) por Subreddit.
Calcula métricas topológicas, transições de sentimento e volume absoluto.
Salva em CSV para a etapa de Taxonomia e Plotagem de Quadrantes.
"""

import json
import os
import pandas as pd
from collections import defaultdict, Counter

# Caminho para o seu dataset final, pós-passagem da Robertinha
DATASET_PATH = "DATA/4-inferred/INFERRED_MULTIMODAL_FINAL.jsonl"
OUTPUT_CSV = "subreddit_features_matrix.csv"

VALID_SENTIMENTS = {'POSITIVE', 'NEGATIVE', 'NEUTRAL'}
MODERATION_LABELS = {'REMOVED_BY_MOD', 'USER_DELETED', 'AUTOMOD_WARNING'}

def calculate_tree_virality(edges, num_nodes):
    """Calcula a Distância Média da cascata em O(N)"""
    if num_nodes <= 1: return 0.0
    adj = defaultdict(list)
    for u, v in edges:
        adj[u].append(v)
        adj[v].append(u)
        
    if not adj: return 0.0
        
    start_node = next(iter(adj.keys()))
    bfs_order, queue = [], [start_node]
    parent_map = {start_node: None}
    
    while queue:
        curr = queue.pop(0)
        bfs_order.append(curr)
        for neighbor in adj[curr]:
            if neighbor != parent_map[curr]:
                parent_map[neighbor] = curr
                queue.append(neighbor)
                
    subtree_size = {}
    total_paths_sum = 0
    
    for node in reversed(bfs_order):
        size = 1
        for neighbor in adj[node]:
            if neighbor != parent_map[node]:
                size += subtree_size[neighbor]
        subtree_size[node] = size
        
        if parent_map[node] is not None:
            S = size
            total_paths_sum += (S * (num_nodes - S))
            
    return total_paths_sum / ((num_nodes * (num_nodes - 1)) / 2)

def main():
    if not os.path.exists(DATASET_PATH):
        print(f"[-] Erro: Arquivo não encontrado em {DATASET_PATH}")
        return

    print(f"[*] Escaneando {DATASET_PATH} e particionando por subreddit...")
    
    # Estruturas de dados separadas por subreddit
    sub_nodes = defaultdict(dict)
    sub_children = defaultdict(lambda: defaultdict(list))
    sub_roots = defaultdict(list)
    sub_volume = Counter()
    
    with open(DATASET_PATH, 'r', encoding='utf-8') as f:
        for line in f:
            try:
                record = json.loads(line)
                if record.get('type') == 'metadata_footer': continue
                
                sub = record.get('subreddit')
                if not sub: continue
                
                sub_volume[sub] += 1
                
                n_id = record['id']
                p_id = record.get('parent_id')
                depth = record.get('depth', 0)
                label = record.get('ai_analysis', {}).get('label', 'UNKNOWN')
                
                sub_nodes[sub][n_id] = {'parent_id': p_id, 'depth': depth, 'label': label}
                
                if p_id: 
                    sub_children[sub][p_id].append(n_id)
                if depth == 1: 
                    sub_roots[sub].append(n_id)
            except: continue

    print(f"[+] Foram encontrados {len(sub_nodes)} subreddits diferentes.")
    print("[*] Extraindo os Vetores de Características...")

    features = []

    for sub in sub_nodes.keys():
        nodes = sub_nodes[sub]
        children_map = sub_children[sub]
        
        # 1. Volume Total
        total_vol = sub_volume[sub]
        if total_vol < 1000: 
            continue # Filtro: ignora subs quase vazios ou com erro de extração
            
        total_valid_sentiments = 0
        total_negative = 0
        mod_interventions = 0
        
        transitions = defaultdict(lambda: Counter())
        cascades_virality = []
        cascades_depths = []

        # Calculando IA e Moderação
        for n_id, data in nodes.items():
            lbl = data['label']
            if lbl in VALID_SENTIMENTS:
                total_valid_sentiments += 1
                if lbl == 'NEGATIVE': total_negative += 1
            if lbl in MODERATION_LABELS:
                mod_interventions += 1
                
            p_id = data['parent_id']
            if p_id and p_id in nodes:
                p_lbl = nodes[p_id]['label']
                if p_lbl in VALID_SENTIMENTS and lbl in VALID_SENTIMENTS:
                    transitions[p_lbl][lbl] += 1

        # Processando Topologia (BFS pelas raízes)
        for root_id in sub_roots[sub]:
            queue = [root_id]
            edges = []
            num_nodes = 0
            max_depth = 0
            root_depth = nodes[root_id].get('depth', 1)
            
            while queue:
                curr = queue.pop(0)
                num_nodes += 1
                rel_depth = nodes[curr].get('depth', 1) - root_depth
                if rel_depth > max_depth: max_depth = rel_depth
                
                for child_id in children_map.get(curr, []):
                    edges.append((curr, child_id))
                    queue.append(child_id)
                    
            # Filtro de Ignição: Ignora micro-interações que não viraram cascatas
            if num_nodes >= 5: 
                cascades_virality.append(calculate_tree_virality(edges, num_nodes))
                cascades_depths.append(max_depth)

        # 2. Viralidade Estrutural Representativa (Percentil 90)
        # Pega a assinatura das conversas que realmente decolaram no subreddit
        median_virality = pd.Series(cascades_virality).quantile(0.90) if cascades_virality else 0
        
        # 3. Densidade de Profundidade (% de cascatas que passam do Nível 5)
        deep_cascades = sum(1 for d in cascades_depths if d >= 5)
        depth_density = (deep_cascades / len(cascades_depths)) if cascades_depths else 0
        
        # 4. Toxicidade Global / Índice de Conflito (% Negativa)
        global_tox = (total_negative / total_valid_sentiments) if total_valid_sentiments > 0 else 0
        
        # 5. Homofilia Negativa (Pai Negativo -> Filho Negativo)
        total_neg_parents = sum(transitions['NEGATIVE'].values())
        neg_homophily = (transitions['NEGATIVE']['NEGATIVE'] / total_neg_parents) if total_neg_parents > 0 else 0
        
        # 6. Atrito Positivo (Pai Positivo -> Filho Negativo)
        total_pos_parents = sum(transitions['POSITIVE'].values())
        pos_friction = (transitions['POSITIVE']['NEGATIVE'] / total_pos_parents) if total_pos_parents > 0 else 0
        
        # 7. Taxa de Moderação
        mod_rate = mod_interventions / total_vol

        features.append({
            'Subreddit': sub,
            'Total_Volume': total_vol,
            'Median_Virality': median_virality,
            'Depth_Density': depth_density,
            'Global_Toxicity': global_tox,
            'Negative_Homophily': neg_homophily,
            'Positive_Friction': pos_friction,
            'Moderation_Rate': mod_rate
        })

    # Exportando a matriz
    df = pd.DataFrame(features)
    df.to_csv(OUTPUT_CSV, index=False)
    
    print("\n" + "="*50)
    print(" 📊 MATRIZ DE CARACTERÍSTICAS (FEATURE VECTOR) GERADA ")
    print("="*50)
    print(f"[+] Total de Subreddits válidos mapeados: {len(df)}")
    print(f"[+] Arquivo salvo: {OUTPUT_CSV}")

if __name__ == "__main__":
    main()