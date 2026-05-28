"""
plot_ccdf_sentiments.py

Gera Análise CCDF (Complementary Cumulative Distribution Function) 
dos tamanhos das cascatas, separadas por Sentimento (Positivo, Neutro, Negativo) 
e facetadas por Quadrante Taxonômico (BCC Framework).
Prova estatisticamente as caudas pesadas (Power Laws) de engajamento.
"""

import json
import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from collections import defaultdict

DATASET_PATH = "DATA/4-inferred/INFERRED_MULTIMODAL_FINAL.jsonl"
FEATURES_CSV = "audit/subreddit_features_matrix.csv"
OUTPUT_IMG = "CCDF_Sentiment_by_Taxonomy.png"

# Cores consistentes para os sentimentos
COLOR_MAP = {
    'POSITIVE': '#2ecc71', # Verde
    'NEUTRAL':  '#95a5a6', # Cinza
    'NEGATIVE': '#e74c3c'  # Vermelho
}

def assign_taxonomy(row, x_mid, y_mid, x_margin, y_margin):
    x, y = row['Median_Virality'], row['Global_Toxicity'] * 100
    if (x_mid - x_margin <= x <= x_mid + x_margin) and (y_mid - y_margin <= y <= y_mid + y_margin):
        return 'Comunidades de Ágora'
    if x > x_mid and y > y_mid: return 'Conflito Crônico'
    elif x > x_mid and y <= y_mid: return 'Debate Resiliente'
    elif x <= x_mid and y > y_mid: return 'Câmaras de Eco'
    else: return 'Praças de Consumo'

def compute_ccdf(data):
    """Calcula a CCDF P(X >= x) para um array de dados"""
    if not data: return [], []
    sorted_data = np.sort(data)
    # yvals é a proporção de itens maiores ou iguais a x
    yvals = 1. - np.arange(len(sorted_data)) / float(len(sorted_data))
    return sorted_data, yvals

def main():
    print("\n" + "="*60)
    print(" 📈 GERADOR DE CCDF: CAUDAS PESADAS E SENTIMENTO ")
    print("="*60)

    # 1. Reconstruir o mapa de Taxonomia
    print("[*] A mapear Taxonomias dos Subreddits...")
    df = pd.read_csv(FEATURES_CSV)
    x = df['Median_Virality']
    y = df['Global_Toxicity'] * 100 
    x_mid, y_mid = x.median(), y.median()
    x_margin, y_margin = (x.max() - x.min()) * 0.10, (y.max() - y.min()) * 0.10
    
    sub_to_tax = {}
    for _, row in df.iterrows():
        tax = assign_taxonomy(row, x_mid, y_mid, x_margin, y_margin)
        sub_to_tax[row['Subreddit']] = tax

    # 2. Reconstruir as Cascatas do Dataset Multimodal
    print("[*] A extrair árvores de conversação do Dataset Multimodal...")
    sub_nodes = defaultdict(dict)
    sub_children = defaultdict(lambda: defaultdict(list))
    sub_roots = defaultdict(list)
    
    with open(DATASET_PATH, 'r', encoding='utf-8') as f:
        for line in f:
            if not line.strip(): continue
            try:
                record = json.loads(line)
                if record.get('type') == 'metadata_footer': continue
                
                sub = record.get('subreddit')
                if not sub or sub not in sub_to_tax: continue
                
                n_id = record['id']
                p_id = record.get('parent_id')
                depth = record.get('depth', 0)
                label = record.get('ai_analysis', {}).get('label', 'UNKNOWN')
                
                sub_nodes[sub][n_id] = {'parent_id': p_id, 'depth': depth, 'label': label}
                
                if p_id: sub_children[sub][p_id].append(n_id)
                if depth == 1: sub_roots[sub].append(n_id)
            except: continue

    # Estrutura para guardar os tamanhos das cascatas: tax_data[taxonomy][sentiment] = [size, size, ...]
    tax_data = defaultdict(lambda: defaultdict(list))

    print("[*] A calcular tamanhos das cascatas por sentimento raiz...")
    for sub, taxonomy in sub_to_tax.items():
        nodes = sub_nodes[sub]
        children_map = sub_children[sub]
        
        for root_id in sub_roots[sub]:
            root_node = nodes.get(root_id)
            if not root_node: continue
            
            # O sentimento do comentário que engatilhou a cascata
            root_sentiment = root_node['label']
            if root_sentiment not in ['POSITIVE', 'NEGATIVE', 'NEUTRAL']:
                continue
                
            # BFS para contar o tamanho da cascata
            queue = [root_id]
            size = 0
            while queue:
                curr = queue.pop(0)
                size += 1
                for child_id in children_map.get(curr, []):
                    queue.append(child_id)
                    
            if size >= 1: # Mantemos mesmo as de tamanho 1 para ver a mortalidade precoce
                tax_data[taxonomy][root_sentiment].append(size)

    # 3. Plotagem da Grade CCDF (Log-Log)
    print("[*] A gerar visualizações Log-Log CCDF...")
    sns.set_theme(style="ticks")
    
    taxonomies = ['Conflito Crônico', 'Debate Resiliente', 'Câmaras de Eco', 'Praças de Consumo', 'Comunidades de Ágora']
    fig, axes = plt.subplots(2, 3, figsize=(18, 12))
    axes = axes.flatten()

    for idx, tax in enumerate(taxonomies):
        ax = axes[idx]
        
        has_data = False
        for sentiment in ['POSITIVE', 'NEUTRAL', 'NEGATIVE']:
            sizes = tax_data[tax].get(sentiment, [])
            if len(sizes) > 10: # Só plota se tiver uma amostra estatística mínima
                has_data = True
                x_vals, y_vals = compute_ccdf(sizes)
                
                # Plot Log-Log
                ax.plot(x_vals, y_vals, label=sentiment, color=COLOR_MAP[sentiment], linewidth=2.5, alpha=0.8)

        if has_data:
            ax.set_xscale('log')
            ax.set_yscale('log')
            ax.set_title(tax, fontsize=14, fontweight='bold', pad=10)
            ax.set_xlabel('Tamanho da Cascata (Nós)', fontsize=12)
            ax.set_ylabel('CCDF P(Tamanho >= x)', fontsize=12)
            ax.grid(True, which="both", ls="--", alpha=0.3)
            ax.legend(title="Sentimento Raiz", loc='lower left')
            
            # Ajuste de eixos para estética
            ax.set_ylim(bottom=1e-5, top=1.2)
        else:
            ax.text(0.5, 0.5, 'Dados Insuficientes', ha='center', va='center')

    # Esconder o 6º gráfico (vazio, já que temos 5 quadrantes)
    fig.delaxes(axes[5])

    plt.suptitle('CCDF: Distribuição de Tamanho das Cascatas por Sentimento Raiz e Taxonomia', 
                 fontsize=18, fontweight='bold', y=1.02)
    plt.tight_layout()
    plt.savefig(OUTPUT_IMG, dpi=300, bbox_inches='tight')
    print(f"\n[+] Sucesso! Gráfico de Power Laws guardado como: {OUTPUT_IMG}")

if __name__ == "__main__":
    main()