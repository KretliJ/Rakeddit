import json
import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt # type: ignore
import seaborn as sns # type: ignore
import matplotlib.ticker as mtick # type: ignore
from scipy.stats import kruskal, sem
from collections import defaultdict
import networkx as nx

class FourthWaveAnalytics:
    def __init__(self):
        self.MULTIMODAL_PATH = "DATA/4-inferred/INFERRED_MULTIMODAL_FINAL.jsonl"
        self.RESULTS_DIR = "results/4-results_fourth_wave"
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
        import pandas as pd
        
        sub_roots = defaultdict(list)
        sub_children = defaultdict(lambda: defaultdict(list))
        
        with open(self.MULTIMODAL_PATH, 'r', encoding='utf-8') as f:
            for line in f:
                try:
                    record = json.loads(line)
                    sub = record.get('subreddit', '').lower()
                    if sub not in self.CATEGORY_MAP: continue
                    
                    n_id_raw = record.get('id', '')
                    p_id_raw = record.get('parent_id')
                    depth = record.get('depth', 0)
                    
                    # Higienização de IDs para NetworkX
                    n_id = str(n_id_raw).split('_')[-1]
                    
                    if p_id_raw and pd.notna(p_id_raw):
                        p_id = str(p_id_raw).split('_')[-1]
                        is_post_reply = str(p_id_raw).startswith('t3_')
                    else:
                        p_id = None
                        is_post_reply = False
                        
                    label = record.get('ai_analysis', {}).get('label', 'UNKNOWN')
                    author = record.get('author', '[deleted]')
                    
                    self.node_memory[n_id] = {'p_id': p_id, 'label': label, 'sub': sub, 'author': author}
                    
                    # Definição: Raízes são nível 1
                    if depth == 1 or is_post_reply:
                        sub_roots[sub].append(n_id)
                    elif p_id:
                        sub_children[sub][p_id].append(n_id)
                except: continue

        for sub, cat in self.CATEGORY_MAP.items():
            if sub not in sub_roots: continue
            
            for root_id in sub_roots[sub]:
                queue = [(root_id, None)]
                sentiments = {'POSITIVE': 0, 'NEUTRAL': 0, 'NEGATIVE': 0}
                total_valid = 0
                
                # Grafo DIRECIONADO DE USUÁRIOS (User A -> User B)
                G = nx.DiGraph() 
                
                while queue:
                    curr, actual_parent = queue.pop(0)
                    
                    c_data = self.node_memory.get(curr, {})
                    lbl = c_data.get('label', 'UNKNOWN')
                    curr_author = c_data.get('author', '[deleted]')
                    
                    if lbl in self.VALID_SENTIMENTS:
                        sentiments[lbl] += 1
                        total_valid += 1
                        
                    # CRIAÇÃO DA ARESTA ENTRE USUÁRIOS (Ignorando quem responde a si mesmo)
                    if actual_parent is not None:
                        parent_author = self.node_memory.get(actual_parent, {}).get('author', '[deleted]')
                        if curr_author not in ['[deleted]', 'deleted'] and parent_author not in ['[deleted]', 'deleted']:
                            if curr_author != parent_author:
                                G.add_edge(curr_author, parent_author)

                    for child_id in sub_children[sub].get(curr, []):
                        queue.append((child_id, curr))

                # Condição de viabilidade: Pelo menos 3 mensagens válidas E 2 usuários distintos (para ter aresta)
                if total_valid >= 3 and G.number_of_nodes() >= 2:
                    pct_neg = (sentiments['NEGATIVE'] / total_valid) * 100
                    pct_neu = (sentiments['NEUTRAL'] / total_valid) * 100
                    pct_pos = (sentiments['POSITIVE'] / total_valid) * 100
                    
                    # Censo Diádico
                    dyads = sum(1 for u, v in G.edges() if not G.has_edge(v, u))
                    mutual_dyads = sum(1 for u, v in G.edges() if G.has_edge(v, u)) / 2
                    
                    # Censo Triádico (Requer >= 3 nós no grafo)
                    triads = nx.triadic_census(G) if G.number_of_nodes() >= 3 else defaultdict(int)
                    
                    motifs = {
                        'Dyad': dyads, 
                        'Mutual Dyad': mutual_dyads,
                        'Chain': triads.get('021C', 0), 
                        'Fan-In': triads.get('021D', 0),
                        'Fan-Out': triads.get('021U', 0), 
                        'Triangle': triads.get('030T', 0),
                        'Recip. Triangle': triads.get('300', 0)
                    }
                    
                    total_motifs = sum(motifs.values())
                    
                    self.cascade_stats.append({
                        'cascade_id': root_id, 'category': cat,
                        'pct_neg': pct_neg, 'pct_neu': pct_neu, 'pct_pos': pct_pos,
                        'total_motifs': total_motifs, **motifs
                    })
                    
        print(f"[+] Processed {len(self.cascade_stats)} valid network cascades for motifs.")

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
        ax.set_xlabel("USER INTERACTION MOTIFS PROPORTION", fontsize=14, fontweight='bold')
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