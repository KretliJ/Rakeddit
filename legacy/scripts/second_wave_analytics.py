import json
import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt # type: ignore
import seaborn as sns # type: ignore
import matplotlib.ticker as mtick # type: ignore
from collections import defaultdict, Counter

class SecondWaveAnalytics:
    def __init__(self):
        self.MULTIMODAL_PATH = "DATA/4-inferred/INFERRED_MULTIMODAL_FINAL.jsonl"
        self.RESULTS_DIR = "results/2-results_second_wave"
        os.makedirs(self.RESULTS_DIR, exist_ok=True)
        
        self.VALID_SENTIMENTS = {'POSITIVE', 'NEUTRAL', 'NEGATIVE'}
        
        self.CATEGORIES = ['PUBLIC ARENAS', 'HUMOR', 'SOCIOCULTURAL', 'HOBBIES']
        self.CATEGORY_MAP = {
            'brasil': 'PUBLIC ARENAS', 'brasilivre': 'PUBLIC ARENAS', 'brasildob': 'PUBLIC ARENAS', 'debatesbr': 'PUBLIC ARENAS', 'noticiasbr': 'PUBLIC ARENAS',
            'botecodoreddit': 'HUMOR', 'farialimabets': 'HUMOR', 'memesbr': 'HUMOR', 'shitpostbr': 'HUMOR',
            'antitrampo': 'SOCIOCULTURAL', 'opiniaoburra': 'SOCIOCULTURAL', 'opiniaoimpopular': 'SOCIOCULTURAL', 'filosofiabar': 'SOCIOCULTURAL', 'infernosocial': 'SOCIOCULTURAL',
            'futebol': 'HOBBIES', 'gamesecultura': 'HOBBIES', 'videogamesbrasil': 'HOBBIES', 'carros': 'HOBBIES', 'computadores': 'HOBBIES', 'saopaulo': 'HOBBIES'
        }
        
        # Estruturas de Memória
        self.node_memory = {}
        self.sub_children = defaultdict(lambda: defaultdict(list))
        self.sub_roots = defaultdict(list)
        
        # Dados Globais Extraídos
        self.sentiment_counts = {cat: {s: 0 for s in self.VALID_SENTIMENTS} for cat in self.CATEGORIES}
        self.triad_patterns = {cat: {'Persistence': 0, 'Convergence': 0, 'Shift': 0, 'Oscillation': 0, 'Mixed': 0} for cat in self.CATEGORIES}
        self.specific_triads = {cat: Counter() for cat in self.CATEGORIES}
        self.global_specific_triads = Counter()
        self.cascade_data = []

    def classify_triad(self, s1, s2, s3):
        """Classifica uma sequência de 3 sentimentos num dos 5 padrões do TCC."""
        if s1 == s2 and s2 == s3: return 'Persistence'
        if s1 != s2 and s2 == s3: return 'Convergence'
        if s1 == s2 and s2 != s3: return 'Shift'
        if s1 != s2 and s2 != s3 and s1 == s3: return 'Oscillation'
        return 'Mixed'

    def extract_and_process(self):
        print("[*] Lendo dataset e construindo árvores em memória...")
        with open(self.MULTIMODAL_PATH, 'r', encoding='utf-8') as f:
            for line in f:
                try:
                    record = json.loads(line)
                    if record.get('type') == 'metadata_footer': continue
                    sub_raw = record.get('subreddit')
                    if not sub_raw: continue
                    sub = sub_raw.lower()
                    if sub not in self.CATEGORY_MAP: continue
                    
                    n_id = record['id']
                    p_id = record.get('parent_id')
                    depth = record.get('depth', 0)
                    label = record.get('ai_analysis', {}).get('label', 'UNKNOWN')
                    
                    self.node_memory[n_id] = {'p_id': p_id, 'label': label, 'sub': sub}
                    
                    if p_id: self.sub_children[sub][p_id].append(n_id)
                    if depth == 1 or p_id is None: self.sub_roots[sub].append(n_id)
                except: continue

        print("[*] Processando Topologia, Sentimentos e Triádicas por Cascata...")
        for sub, cat in self.CATEGORY_MAP.items():
            if sub not in self.sub_roots: continue
            children_map = self.sub_children[sub]
            
            for root_id in self.sub_roots[sub]:
                # Fila para BFS modificado que carrega o histórico de sentimentos do caminho (path)
                # Cada item na fila: (node_id, [lista_de_sentimentos_ate_aqui])
                root_label = self.node_memory[root_id]['label']
                queue = [(root_id, [root_label] if root_label in self.VALID_SENTIMENTS else [])]
                
                c_total_nodes = 0
                edges = []
                cascade_triad_counts = {'Persistence': 0, 'Convergence': 0, 'Shift': 0, 'Oscillation': 0, 'Mixed': 0}
                
                while queue:
                    curr, path_labels = queue.pop(0)
                    c_total_nodes += 1
                    curr_label = self.node_memory[curr]['label']
                    
                    if curr_label in self.VALID_SENTIMENTS:
                        self.sentiment_counts[cat][curr_label] += 1
                        
                        # Extrair Relações Triádicas (Janela de 3)
                        if len(path_labels) >= 3:
                            s1, s2, s3 = path_labels[-3], path_labels[-2], path_labels[-1]
                            pattern = self.classify_triad(s1, s2, s3)
                            triad_str = f"{s1[:3]}-{s2[:3]}-{s3[:3]}" # Ex: POS-NEG-POS
                            
                            # Registos Globais e de Categoria
                            self.triad_patterns[cat][pattern] += 1
                            self.specific_triads[cat][triad_str] += 1
                            self.global_specific_triads[triad_str] += 1
                            
                            # Registos da Cascata (Para correlação com Viralidade)
                            cascade_triad_counts[pattern] += 1

                    # Adicionar filhos à fila
                    for child_id in children_map.get(curr, []):
                        edges.append((curr, child_id))
                        child_label = self.node_memory[child_id]['label']
                        new_path = path_labels.copy()
                        if child_label in self.VALID_SENTIMENTS:
                            new_path.append(child_label)
                        queue.append((child_id, new_path))
                
                # Calcular Viralidade Estrutural se a cascata for grande o suficiente
                if c_total_nodes >= 5:
                    adj = defaultdict(list)
                    for u, v in edges: adj[u].append(v); adj[v].append(u)
                    if adj:
                        start_node = next(iter(adj.keys()))
                        bfs_order, q, parent_map = [], [start_node], {start_node: None}
                        while q:
                            curr_n = q.pop(0); bfs_order.append(curr_n)
                            for neighbor in adj[curr_n]:
                                if neighbor != parent_map[curr_n]: 
                                    parent_map[neighbor] = curr_n; q.append(neighbor)
                        subtree_size, total_paths = {}, 0
                        for node in reversed(bfs_order):
                            size = 1
                            for neighbor in adj[node]:
                                if neighbor != parent_map[node]: size += subtree_size[neighbor]
                            subtree_size[node] = size
                            if parent_map[node] is not None: total_paths += (size * (c_total_nodes - size))
                        
                        virality = total_paths / ((c_total_nodes * (c_total_nodes - 1)) / 2)
                        
                        self.cascade_data.append({
                            'cascade_id': root_id,
                            'category': cat,
                            'virality': virality,
                            **cascade_triad_counts
                        })
        print("[+] Extração concluída.")

    # ==========================================================
    # 3.2 Sentiment Analysis (Heatmap Base) - SOMA 100% RIGOROSA
    # ==========================================================
    def plot_3_2_sentiment_heatmap(self):
        print("[*] Gerando 3.2 Sentiment Analysis Heatmap (Forçando soma 100%)...")
        df = pd.DataFrame(self.sentiment_counts).T
        df_pct = df.div(df.sum(axis=1), axis=0) * 100
        df_pct = df_pct[['NEGATIVE', 'NEUTRAL', 'POSITIVE']] # Ordem lógica
        
        # TRUQUE DE ARREDONDAMENTO PARA CRAVAR 100.0%
        df_pct = df_pct.round(1)
        for idx in df_pct.index:
            total = df_pct.loc[idx].sum()
            diff = round(100.0 - total, 1)
            if diff != 0:
                # Joga a diferença (ex: 0.1 ou -0.1) na coluna com o maior valor daquela linha
                max_col = df_pct.loc[idx].idxmax()
                df_pct.loc[idx, max_col] = round(df_pct.loc[idx, max_col] + diff, 1)
        
        fig, ax = plt.subplots(figsize=(10, 6))
        sns.heatmap(df_pct, annot=True, fmt=".1f", cmap="magma_r", cbar=True, ax=ax, vmin=0, vmax=100,
                    annot_kws={'size': 18, 'weight': 'bold'})
        
        ax.set_ylabel("SUBREDDIT CATEGORY", fontsize=16, fontweight='bold')
        ax.set_xlabel("MESSAGE POLARITY", fontsize=16, fontweight='bold')
        
        # Eixos e Rotação
        ax.tick_params(labelsize=14)
        plt.yticks(rotation=0) 
        
        plt.tight_layout()
        plt.savefig(os.path.join(self.RESULTS_DIR, "3.2_Sentiment_Distribution.pdf"), dpi=300)
        plt.close()

    # ==========================================================
    # 3.3 Triadic Sentiment Patterns (Padrões e Top 10)
    # ==========================================================
    def plot_3_3_patterns_heatmap(self):
        print("[*] Gerando 3.3 Triadic Patterns Heatmap...")
        df = pd.DataFrame(self.triad_patterns).T
        df_pct = df.div(df.sum(axis=1), axis=0) * 100
        
        # Ordenar colunas lógicamente
        cols_order = ['Persistence', 'Convergence', 'Shift', 'Oscillation', 'Mixed']
        df_pct = df_pct[cols_order]
        
        fig, ax = plt.subplots(figsize=(12, 6))
        sns.heatmap(df_pct, annot=True, fmt=".1f", cmap="magma_r", cbar=True, ax=ax,
                    annot_kws={'size': 16, 'weight': 'bold'})
        
        ax.set_ylabel("SUBREDDIT CATEGORY", fontsize=16, fontweight='bold')
        ax.set_xlabel("TRIADIC PATTERN (GRANDFATHER -> FATHER -> SON)", fontsize=16, fontweight='bold')
        
        # Eixos e Rotação
        ax.tick_params(labelsize=14)
        plt.yticks(rotation=0) # <-- FORÇA A LABEL DO EIXO Y A FICAR HORIZONTAL
        
        plt.tight_layout()
        plt.savefig(os.path.join(self.RESULTS_DIR, "3.3_Triadic_Patterns_Aggregated.pdf"), dpi=300)
        plt.close()

    def plot_3_3_top10_specific_heatmap(self):
        print("[*] Gerando 3.3 Top 10 Specific Triads Heatmap...")
        top_10_triads = [t[0] for t in self.global_specific_triads.most_common(10)]
        
        data = {cat: [] for cat in self.CATEGORIES}
        for triad in top_10_triads:
            for cat in self.CATEGORIES:
                data[cat].append(self.specific_triads[cat].get(triad, 0))
                
        df = pd.DataFrame(data, index=top_10_triads)
        
        total_triads_cat = {cat: sum(self.specific_triads[cat].values()) for cat in self.CATEGORIES}
        for cat in self.CATEGORIES:
            if total_triads_cat[cat] > 0:
                df[cat] = (df[cat] / total_triads_cat[cat]) * 100
            else:
                df[cat] = 0.0

        fig, ax = plt.subplots(figsize=(10, 8))
        sns.heatmap(df, annot=True, fmt=".1f", cmap="magma_r", cbar=True, ax=ax,
                    annot_kws={'size': 16, 'weight': 'bold'})
        
        ax.set_ylabel("TOP 10 SPECIFIC TRIADIC RELATIONS", fontsize=16, fontweight='bold')
        ax.set_xlabel("SUBREDDIT CATEGORY", fontsize=16, fontweight='bold')
        
        # Eixos e Rotação
        ax.tick_params(labelsize=14)
        plt.yticks(rotation=0) # <-- FORÇA A LABEL DO EIXO Y A FICAR HORIZONTAL
        
        plt.tight_layout()
        plt.savefig(os.path.join(self.RESULTS_DIR, "3.3_Top10_Specific_Triads.pdf"), dpi=300)
        plt.close()

    # ==========================================================
    # 3.4 Comparing Triadic Patterns with Structural Virality
    # ==========================================================
    def plot_3_4_virality_quartiles(self):
        print("[*] Gerando 3.4 Virality vs Triadic Patterns Heatmap...")
        df = pd.DataFrame(self.cascade_data)
        
        df['Total_Triads'] = df[['Persistence', 'Convergence', 'Shift', 'Oscillation', 'Mixed']].sum(axis=1)
        df = df[df['Total_Triads'] > 0].copy()
        
        df = df.sort_values(by='virality').reset_index(drop=True)
        df['Quartile'] = pd.qcut(df['virality'], 4, labels=['Q1 (Least Viral)', 'Q2', 'Q3', 'Q4 (Most Viral)'])
        
        patterns = ['Persistence', 'Convergence', 'Shift', 'Oscillation', 'Mixed']
        for p in patterns:
            df[p+'_Rate'] = df[p] / df['Total_Triads']
            
        q_means = df.groupby('Quartile')[[p+'_Rate' for p in patterns]].mean().T
        q_means.index = patterns 
        
        q_means_pct = q_means.div(q_means.sum(axis=1), axis=0) * 100

        fig, ax = plt.subplots(figsize=(10, 6))
        sns.heatmap(q_means_pct, annot=True, fmt=".1f", cmap="magma_r", cbar=True, ax=ax,
                    annot_kws={'size': 18, 'weight': 'bold'})
        
        ax.set_ylabel("TRIADIC PATTERN", fontsize=16, fontweight='bold')
        ax.set_xlabel("STRUCTURAL VIRALITY QUARTILES", fontsize=16, fontweight='bold')
        
        # Eixos e Rotação
        ax.tick_params(labelsize=14)
        plt.yticks(rotation=0) # <-- FORÇA A LABEL DO EIXO Y A FICAR HORIZONTAL
        
        plt.tight_layout()
        plt.savefig(os.path.join(self.RESULTS_DIR, "3.4_Triads_vs_Virality_Quartiles.pdf"), dpi=300)
        plt.close()

if __name__ == "__main__":
    analyzer = SecondWaveAnalytics()
    analyzer.extract_and_process()
    analyzer.plot_3_2_sentiment_heatmap()
    analyzer.plot_3_3_patterns_heatmap()
    analyzer.plot_3_3_top10_specific_heatmap()
    analyzer.plot_3_4_virality_quartiles()
    print("\n👋 Análise de Segunda Vaga concluída. Ficheiros guardados em 'results_second_wave/'.")