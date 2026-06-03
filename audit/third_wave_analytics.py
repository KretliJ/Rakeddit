import json
import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import matplotlib.ticker as mtick
from scipy.stats import kruskal, sem
from collections import defaultdict
import networkx as nx

class ThirdWaveAnalytics:
    def __init__(self):
        self.MULTIMODAL_PATH = "DATA/4-inferred/INFERRED_MULTIMODAL_FINAL.jsonl"
        self.RESULTS_DIR = "results_third_wave"
        os.makedirs(self.RESULTS_DIR, exist_ok=True)
        
        self.VALID_SENTIMENTS = {'POSITIVE', 'NEUTRAL', 'NEGATIVE'}
        self.CATEGORIES = ['PUBLIC ARENAS', 'HUMOR', 'SOCIOCULTURAL', 'HOBBIES']
        self.CATEGORY_MAP = {
            'brasil': 'PUBLIC ARENAS', 'brasilivre': 'PUBLIC ARENAS', 'brasildob': 'PUBLIC ARENAS', 'debatesbr': 'PUBLIC ARENAS', 'noticiasbr': 'PUBLIC ARENAS',
            'botecodoreddit': 'HUMOR', 'farialimabets': 'HUMOR', 'memesbr': 'HUMOR', 'shitpostbr': 'HUMOR',
            'antitrampo': 'SOCIOCULTURAL', 'opiniaoburra': 'SOCIOCULTURAL', 'opiniaoimpopular': 'SOCIOCULTURAL', 'filosofiabar': 'SOCIOCULTURAL', 'infernosocial': 'SOCIOCULTURAL',
            'futebol': 'HOBBIES', 'gamesecultura': 'HOBBIES', 'videogamesbrasil': 'HOBBIES', 'carros': 'HOBBIES', 'computadores': 'HOBBIES', 'saopaulo': 'HOBBIES'
        }
        
        self.node_memory = {}
        self.sub_roots = defaultdict(list)
        self.sub_children = defaultdict(lambda: defaultdict(list))
        self.cascade_stats = []

    def extract_data(self):
        print("[*] Lendo dataset e construindo Grafos de Usuários...")
        with open(self.MULTIMODAL_PATH, 'r', encoding='utf-8') as f:
            for line in f:
                try:
                    record = json.loads(line)
                    sub = record.get('subreddit', '').lower()
                    if sub not in self.CATEGORY_MAP: continue
                    
                    n_id = record['id']
                    p_id = record.get('parent_id')
                    depth = record.get('depth', 0)
                    label = record.get('ai_analysis', {}).get('label', 'UNKNOWN')
                    author = record.get('author', 'deleted')
                    
                    self.node_memory[n_id] = {'p_id': p_id, 'label': label, 'sub': sub, 'author': author}
                    if p_id: self.sub_children[sub][p_id].append(n_id)
                    if depth == 1 or p_id is None: self.sub_roots[sub].append(n_id)
                except: continue

        print("[*] Calculando Motifs e Sentimentos por Cascata...")
        for sub, cat in self.CATEGORY_MAP.items():
            if sub not in self.sub_roots: continue
            
            for root_id in self.sub_roots[sub]:
                queue = [root_id]
                sentiments = {'POSITIVE': 0, 'NEUTRAL': 0, 'NEGATIVE': 0}
                total_valid = 0
                
                # Grafo Direcionado de Usuários (A -> B significa A respondeu a B)
                G = nx.DiGraph() 
                
                while queue:
                    curr = queue.pop(0)
                    c_data = self.node_memory[curr]
                    lbl = c_data['label']
                    curr_author = c_data['author']
                    
                    if lbl in self.VALID_SENTIMENTS:
                        sentiments[lbl] += 1
                        total_valid += 1
                        
                    p_id = c_data['p_id']
                    if p_id and p_id in self.node_memory:
                        parent_author = self.node_memory[p_id]['author']
                        # Só adiciona aresta se não for o próprio utilizador a responder-se a si mesmo
                        if curr_author != 'deleted' and parent_author != 'deleted' and curr_author != parent_author:
                            G.add_edge(curr_author, parent_author)

                    for child_id in self.sub_children[sub].get(curr, []):
                        queue.append(child_id)

                if total_valid >= 3 and G.number_of_nodes() >= 2:
                    pct_neg = sentiments['NEGATIVE'] / total_valid
                    pct_neu = sentiments['NEUTRAL'] / total_valid
                    pct_pos = sentiments['POSITIVE'] / total_valid
                    
                    # 1. Contagem Direta de Díades
                    dyads = sum(1 for u, v in G.edges() if not G.has_edge(v, u))
                    mutual_dyads = sum(1 for u, v in G.edges() if G.has_edge(v, u)) / 2
                    
                    # 2. Censo Triádico do NetworkX (Identificação Rápida de Isomorfismos)
                    triads = nx.triadic_census(G) if G.number_of_nodes() >= 3 else defaultdict(int)
                    
                    # Mapeamento com a Tabela 1
                    motifs = {
                        'Dyad': dyads,
                        'Mutual Dyad': mutual_dyads,
                        'Chain': triads.get('021C', 0), # A->B->C
                        'Fan-In': triads.get('021D', 0), # B->A, C->A
                        'Fan-Out': triads.get('021U', 0), # A->B, A->C
                        'Triangle': triads.get('030T', 0), # A->B, B->C, A->C
                        'Recip. Triangle': triads.get('300', 0) # A<->B<->C<->A
                    }
                    
                    total_motifs = sum(motifs.values())
                    
                    self.cascade_stats.append({
                        'cascade_id': root_id,
                        'category': cat,
                        'pct_neg': pct_neg * 100,
                        'pct_neu': pct_neu * 100,
                        'pct_pos': pct_pos * 100,
                        'total_motifs': total_motifs,
                        **motifs
                    })

    # ==========================================================
    # Figura 2.5: Heatmap Média +- Erro Padrão de Sentimento
    # ==========================================================
    def plot_sentiment_mean_se_heatmap(self):
        df = pd.DataFrame(self.cascade_stats)
        
        # Agrupar por Categoria e calcular Média e Erro Padrão (SEM)
        metrics = ['pct_neg', 'pct_neu', 'pct_pos']
        agg_mean = df.groupby('category')[metrics].mean()
        agg_se = df.groupby('category')[metrics].apply(lambda x: x.sem())
        
        # Formatação Customizada das Células para o Heatmap "Média ± SE"
        labels = np.asarray([
            [f"{agg_mean.loc[cat, m]:.1f}\n±{agg_se.loc[cat, m]:.1f}" for m in metrics]
            for cat in self.CATEGORIES
        ])
        
        fig, ax = plt.subplots(figsize=(10, 6))
        sns.heatmap(agg_mean, annot=labels, fmt="", cmap="magma_r", cbar=True, ax=ax, vmin=0, vmax=100, annot_kws={'size': 16, 'weight': 'bold'})
        
        ax.set_ylabel("SUBREDDIT CATEGORY", fontsize=16, fontweight='bold')
        ax.set_xlabel("MESSAGE POLARITY", fontsize=16, fontweight='bold')
        ax.set_xticklabels(['NEGATIVE', 'NEUTRAL', 'POSITIVE'])
        ax.tick_params(labelsize=14)
        plt.yticks(rotation=0)
        
        plt.tight_layout()
        plt.savefig(os.path.join(self.RESULTS_DIR, "Fig2_Sentiment_Mean_SE.pdf"), dpi=300)
        plt.close()

    # ==========================================================
    # Análise 3.3.1: Heatmap de Motifs por Categoria
    # ==========================================================
    def plot_motifs_heatmap(self):
        df = pd.DataFrame(self.cascade_stats)
        df = df[df['total_motifs'] > 0].copy()
        
        motif_cols = ['Dyad', 'Mutual Dyad', 'Chain', 'Fan-In', 'Fan-Out', 'Triangle', 'Recip. Triangle']
        for m in motif_cols:
            df[m + '_pct'] = (df[m] / df['total_motifs']) * 100
            
        agg_mean = df.groupby('category')[[m + '_pct' for m in motif_cols]].mean()
        agg_se = df.groupby('category')[[m + '_pct' for m in motif_cols]].apply(lambda x: x.sem())
        
        labels = np.asarray([
            [f"{agg_mean.loc[cat, m+'_pct']:.1f}\n±{agg_se.loc[cat, m+'_pct']:.1f}" for m in motif_cols]
            for cat in self.CATEGORIES
        ])
        
        fig, ax = plt.subplots(figsize=(16, 6))
        sns.heatmap(agg_mean, annot=labels, fmt="", cmap="viridis_r", cbar=True, ax=ax, annot_kws={'size': 14, 'weight': 'bold'})
        
        ax.set_ylabel("SUBREDDIT CATEGORY", fontsize=16, fontweight='bold')
        ax.set_xlabel("USER INTERACTION MOTIFS", fontsize=16, fontweight='bold')
        ax.set_xticklabels(motif_cols, rotation=15)
        ax.tick_params(labelsize=14)
        plt.yticks(rotation=0)
        
        plt.tight_layout()
        plt.savefig(os.path.join(self.RESULTS_DIR, "Fig3_Motifs_Categories.pdf"), dpi=300)
        plt.close()

    # ==========================================================
    # Análise 3.3.2: Tabelas Quartil Negatividade
    # ==========================================================
    def export_negativity_quartiles(self):
        df = pd.DataFrame(self.cascade_stats)
        df = df[df['total_motifs'] > 0].copy()
        
        # Dividir em Quartis baseado na % de Negatividade
        df['neg_quartile'] = pd.qcut(df['pct_neg'], 4, labels=['Q1 (Low Negativity)', 'Q2', 'Q3', 'Q4 (High Negativity)'], duplicates='drop')
        
        motif_cols = ['Dyad', 'Mutual Dyad', 'Chain', 'Fan-In', 'Fan-Out', 'Triangle', 'Recip. Triangle']
        for m in motif_cols:
            df[m + '_pct'] = (df[m] / df['total_motifs']) * 100
            
        q1_means = df[df['neg_quartile'] == 'Q1 (Low Negativity)'][[(m + '_pct') for m in motif_cols]].mean().round(2)
        q4_means = df[df['neg_quartile'] == 'Q4 (High Negativity)'][[(m + '_pct') for m in motif_cols]].mean().round(2)
        
        q1_df = pd.DataFrame({'Motif': motif_cols, 'Percentage (%)': q1_means.values})
        q4_df = pd.DataFrame({'Motif': motif_cols, 'Percentage (%)': q4_means.values})
        
        q1_df.to_csv(os.path.join(self.RESULTS_DIR, "Table_Q1_Low_Negativity_Motifs.csv"), index=False)
        q4_df.to_csv(os.path.join(self.RESULTS_DIR, "Table_Q4_High_Negativity_Motifs.csv"), index=False)

if __name__ == "__main__":
    analyser = ThirdWaveAnalytics()
    analyser.extract_data()
    analyser.plot_sentiment_mean_se_heatmap()
    analyser.plot_motifs_heatmap()
    analyser.export_negativity_quartiles()