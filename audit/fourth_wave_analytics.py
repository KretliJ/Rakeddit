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

class FourthWaveAnalytics:
    def __init__(self):
        self.MULTIMODAL_PATH = "DATA/4-inferred/INFERRED_MULTIMODAL_FINAL.jsonl"
        self.RESULTS_DIR = "results_fourth_wave"
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

    def extract_and_analyze(self):
        print("[*] Parsing network structures and user interactions...")
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

        for sub, cat in self.CATEGORY_MAP.items():
            if sub not in self.sub_roots: continue
            for root_id in self.sub_roots[sub]:
                queue = [root_id]
                sentiments = {'POSITIVE': 0, 'NEUTRAL': 0, 'NEGATIVE': 0}
                total_valid = 0
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
                        if curr_author != 'deleted' and parent_author != 'deleted' and curr_author != parent_author:
                            G.add_edge(curr_author, parent_author)

                    for child_id in self.sub_children[sub].get(curr, []):
                        queue.append(child_id)

                if total_valid >= 3 and G.number_of_nodes() >= 2:
                    pct_neg = (sentiments['NEGATIVE'] / total_valid) * 100
                    pct_neu = (sentiments['NEUTRAL'] / total_valid) * 100
                    pct_pos = (sentiments['POSITIVE'] / total_valid) * 100
                    
                    dyads = sum(1 for u, v in G.edges() if not G.has_edge(v, u))
                    mutual_dyads = sum(1 for u, v in G.edges() if G.has_edge(v, u)) / 2
                    triads = nx.triadic_census(G) if G.number_of_nodes() >= 3 else defaultdict(int)
                    
                    motifs = {
                        'Dyad': dyads, 'Mutual Dyad': mutual_dyads,
                        'Chain': triads.get('021C', 0), 'Fan-In': triads.get('021D', 0),
                        'Fan-Out': triads.get('021U', 0), 'Triangle': triads.get('030T', 0),
                        'Recip. Triangle': triads.get('300', 0)
                    }
                    total_motifs = sum(motifs.values())
                    
                    self.cascade_stats.append({
                        'cascade_id': root_id, 'category': cat,
                        'pct_neg': pct_neg, 'pct_neu': pct_neu, 'pct_pos': pct_pos,
                        'total_motifs': total_motifs, **motifs
                    })
        print(f"[+] Processed {len(self.cascade_stats)} valid network cascades.")

    def run_kruskal_tests(self):
        df = pd.DataFrame(self.cascade_stats)
        print("\n=== ITEM 1: Kruskal-Wallis (Figura 1 CCDF Properties) ===")
        # Nota: Substitua com seus vetores reais da fig 1 se rodar no mesmo script
        metrics_f1 = ['pct_neg', 'pct_neu', 'pct_pos'] 
        for m in metrics_f1:
            groups = [df[df['category'] == cat][m].values for cat in self.CATEGORIES]
            stat, p = kruskal(*groups)
            print(f"Metric {m} -> H-Stat: {stat:.4f}, p-value: {p:.4e}")

    def plot_sentiment_heatmap(self):
        print("\n=== ITEM 2: Heatmap de Sentimentos Mean +- SE por Cascata ===")
        df = pd.DataFrame(self.cascade_stats)
        metrics = ['pct_neg', 'pct_neu', 'pct_pos']
        
        agg_mean = df.groupby('category')[metrics].mean()
        agg_se = df.groupby('category')[metrics].apply(lambda x: x.sem())
        
        # Ajuste para garantir soma de 100% rigorosa nas médias das linhas
        agg_mean = agg_mean.round(1)
        for cat in agg_mean.index:
            diff = round(100.0 - agg_mean.loc[cat].sum(), 1)
            if diff != 0:
                max_col = agg_mean.loc[cat].idxmax()
                agg_mean.loc[cat, max_col] = round(agg_mean.loc[cat, max_col] + diff, 1)

        labels = np.asarray([[f"{agg_mean.loc[cat, m]:.1f}%\n±{agg_se.loc[cat, m]:.2f}%" for m in metrics] for cat in self.CATEGORIES])
        
        fig, ax = plt.subplots(figsize=(10, 6))
        sns.heatmap(agg_mean, annot=labels, fmt="", cmap="magma_r", cbar=True, ax=ax, vmin=0, vmax=100, annot_kws={'size': 14, 'weight': 'bold'})
        ax.set_ylabel("SUBREDDIT CATEGORY", fontsize=14, fontweight='bold')
        ax.set_xlabel("CASCADE POLARITY DISTRIBUTION", fontsize=14, fontweight='bold')
        ax.set_xticklabels(['NEGATIVE', 'NEUTRAL', 'POSITIVE'])
        plt.yticks(rotation=0)
        plt.tight_layout()
        plt.savefig(os.path.join(self.RESULTS_DIR, "Fig2_Sentiment_Cascades_Heatmap.pdf"), dpi=300)
        plt.close()

    def plot_motifs_heatmap(self):
        print("\n=== ITEM 3: Heatmap de Motifs por Categoria + Kruskal ===")
        df = pd.DataFrame(self.cascade_stats)
        df = df[df['total_motifs'] > 0].copy()
        motif_cols = ['Dyad', 'Mutual Dyad', 'Chain', 'Fan-In', 'Fan-Out', 'Triangle', 'Recip. Triangle']
        
        for m in motif_cols:
            df[m + '_pct'] = (df[m] / df['total_motifs']) * 100
            
        agg_mean = df.groupby('category')[[m + '_pct' for m in motif_cols]].mean()
        agg_se = df.groupby('category')[[m + '_pct' for m in motif_cols]].apply(lambda x: x.sem())
        
        print("Kruskal-Wallis para diferenças de Motifs entre as Categorias:")
        for m in motif_cols:
            groups = [df[df['category'] == cat][m + '_pct'].values for cat in self.CATEGORIES]
            stat, p = kruskal(*groups)
            print(f"Motif {m:15} -> H-Stat: {stat:8.2f} | p-value: {p:.4e}")

        labels = np.asarray([[f"{agg_mean.loc[cat, m+'_pct']:.1f}%\n±{agg_se.loc[cat, m+'_pct']:.2f}%" for m in motif_cols] for cat in self.CATEGORIES])
        
        fig, ax = plt.subplots(figsize=(15, 6))
        sns.heatmap(agg_mean, annot=labels, fmt="", cmap="magma_r", cbar=True, ax=ax, annot_kws={'size': 11, 'weight': 'bold'})
        ax.set_ylabel("SUBREDDIT CATEGORY", fontsize=14, fontweight='bold')
        ax.set_xlabel("USER INTERACTION MOTIFS PROPORION", fontsize=14, fontweight='bold')
        ax.set_xticklabels(motif_cols)
        plt.yticks(rotation=0)
        plt.tight_layout()
        plt.savefig(os.path.join(self.RESULTS_DIR, "Fig3_Motifs_Ecosystem_Heatmap.pdf"), dpi=300)
        plt.close()

    def export_quartile_tables(self):
        print("\n=== ITEM 4: Separando Quartis de Negatividade (Q1 vs Q4) ===")
        df = pd.DataFrame(self.cascade_stats)
        df = df[df['total_motifs'] > 0].copy()
        motif_cols = ['Dyad', 'Mutual Dyad', 'Chain', 'Fan-In', 'Fan-Out', 'Triangle', 'Recip. Triangle']
        
        for m in motif_cols:
            df[m + '_pct'] = (df[m] / df['total_motifs']) * 100
            
        df['quartile'] = pd.qcut(df['pct_neg'], 4, labels=['Q1', 'Q2', 'Q3', 'Q4'])
        
        q1_df = df[df['quartile'] == 'Q1'][[m + '_pct' for m in motif_cols]].mean().round(2)
        q4_df = df[df['quartile'] == 'Q4'][[m + '_pct' for m in motif_cols]].mean().round(2)
        
        print("\nTabela Q1 - Low Negativity:")
        print(q1_df)
        print("\nTabela Q4 - High Negativity:")
        print(q4_df)

if __name__ == "__main__":
    wave4 = FourthWaveAnalytics()
    wave4.extract_and_analyze()
    wave4.run_kruskal_tests()
    wave4.plot_sentiment_heatmap()
    wave4.plot_motifs_heatmap()
    wave4.export_quartile_tables()