import json
import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt # type: ignore
import matplotlib.ticker as mtick # type: ignore
import seaborn as sns # type: ignore
import networkx as nx # type: ignore
from collections import defaultdict

class SixthWaveAnalyticsOrchestrator:
    def __init__(self):
        # Base Paths
        self.MULTIMODAL_PATH = "DATA/4-inferred/INFERRED_MULTIMODAL_FINAL.jsonl"
        
        # Output Directories
        self.RESULTS_DIR = "results/6-sixth_wave"
        os.makedirs(self.RESULTS_DIR, exist_ok=True)
        
        # Constants
        self.VALID_SENTIMENTS = {'POSITIVE', 'NEGATIVE', 'NEUTRAL'}
        
        # Estética global do Seaborn
        sns.set_theme(style="ticks", rc={"axes.grid": True, "grid.alpha": 0.5, "grid.linestyle": "--"})
        self.magma_hex = sns.color_palette("magma", 4).as_hex()
        
        # DataFrame de Resultados (Memória)
        self.df_cascades = None
        
        # Armazenamento Agregado
        self.global_motifs = defaultdict(int)
        self.global_triads = defaultdict(int)
        self.global_sentiments = {'POSITIVE': 0, 'NEGATIVE': 0, 'NEUTRAL': 0}

    # =========================================================================
    # 1. LEITURA, CONSTRUÇÃO DE GRAFOS E EXTRAÇÃO DE FEATURES
    # =========================================================================
    def extract_and_compute_all(self):
        print("[*] Iniciando a Sexta Onda: Leitura e Extração Massiva...")
        if not os.path.exists(self.MULTIMODAL_PATH):
            print(f"[-] Erro: Dataset não encontrado em {self.MULTIMODAL_PATH}")
            return False

        node_memory = {}
        sub_children = defaultdict(list)
        sub_roots = []

        print("   -> Fase 1/3: Mapeando nós e reconstruindo a floresta (RAM)...")
        with open(self.MULTIMODAL_PATH, 'r', encoding='utf-8') as f:
            for line in f:
                try:
                    record = json.loads(line)
                    if record.get('type') == 'metadata_footer': continue
                    
                    n_id_raw = record.get('id', '')
                    p_id_raw = record.get('parent_id')
                    depth = record.get('depth', 0)
                    
                    n_id = str(n_id_raw).split('_')[-1]
                    
                    if p_id_raw and pd.notna(p_id_raw):
                        p_id = str(p_id_raw).split('_')[-1]
                        is_post_reply = str(p_id_raw).startswith('t3_')
                    else:
                        p_id = None
                        is_post_reply = False
                        
                    author = record.get('author', '[deleted]')
                    
                    ts = record.get('created_utc') or record.get('timestamp') or record.get('ai_analysis', {}).get('created_utc')
                    lbl = record.get('ai_analysis', {}).get('label', 'UNKNOWN')
                    
                    node_memory[n_id] = {
                        'author': author, 
                        'timestamp': float(ts) if ts else None,
                        'label': lbl
                    }
                    
                    if depth == 1 or is_post_reply:
                        sub_roots.append(n_id)
                    elif p_id:
                        sub_children[p_id].append(n_id)
                        
                except Exception:
                    continue

        print("   -> Fase 2/3: Computando Grafos, Motifs e Tríades via NetworkX...")
        cascades_data = []
        
        for root_id in sub_roots:
            queue = [(root_id, None)]
            G_struct = nx.DiGraph() # Grafo Estrutural (Nó -> Nó)
            G_users = nx.DiGraph()  # Grafo de Interação (User -> User)
            timestamps = []
            
            c_pos, c_neu, c_neg, c_valid = 0, 0, 0, 0
            
            while queue:
                curr, actual_parent = queue.pop(0)
                G_struct.add_node(curr)
                
                nd = node_memory.get(curr, {})
                ts = nd.get('timestamp')
                author = nd.get('author', '[deleted]')
                lbl = nd.get('label', 'UNKNOWN')
                
                if ts: timestamps.append(ts)
                
                if lbl in self.VALID_SENTIMENTS:
                    c_valid += 1
                    self.global_sentiments[lbl] += 1
                    if lbl == 'POSITIVE': c_pos += 1
                    elif lbl == 'NEUTRAL': c_neu += 1
                    elif lbl == 'NEGATIVE': c_neg += 1
                
                if actual_parent is not None:
                    G_struct.add_edge(actual_parent, curr)
                    parent_author = node_memory.get(actual_parent, {}).get('author', '[deleted]')
                    # User Motif edge: actual_parent_author respondeu para author
                    if author != '[deleted]' and parent_author != '[deleted]':
                        if G_users.has_edge(author, parent_author):
                            G_users[author][parent_author]['weight'] += 1
                        else:
                            G_users.add_edge(author, parent_author, weight=1)
                    
                for child_id in sub_children.get(curr, []):
                    queue.append((child_id, curr))
            
            num_messages = G_struct.number_of_nodes()
            if num_messages < 3: 
                continue
            
            # --- Métricas Estruturais ---
            G_un = G_struct.to_undirected()
            virality = nx.wiener_index(G_un) / (num_messages * (num_messages - 1)) if num_messages > 1 else 1.0
            
            lengths = nx.single_source_shortest_path_length(G_struct, root_id)
            max_depth = max(lengths.values()) if lengths else 1
            
            level_counts = defaultdict(int)
            for dist in lengths.values(): level_counts[dist] += 1
            max_breadth = max(level_counts.values()) if level_counts else 1
            
            duration_minutes = 0.0
            if len(timestamps) >= 2:
                sorted_ts = np.sort(timestamps)
                duration_minutes = (sorted_ts[-1] - sorted_ts[0]) / 60.0
            
            unique_users = max(len(G_users.nodes()), 1)
            
            perc_neg = (c_neg / c_valid * 100) if c_valid > 0 else 0.0

            # --- Extração de Motifs (User Graph) ---
            # Dyad: A->B (Qualquer aresta)
            # Mutual: A->B e B->A
            # Chain: A->B->C
            # Fan-In: B->A e C->A (In-degree >= 2)
            # Fan-Out: A->B e A->C (Out-degree >= 2)
            
            for u, v in G_users.edges():
                self.global_motifs['Dyad'] += 1
                if G_users.has_edge(v, u):
                    self.global_motifs['Mutual Dyad'] += 0.5 # Divide por 2 para não duplicar

            for node in G_users.nodes():
                in_deg = G_users.in_degree(node)
                out_deg = G_users.out_degree(node)
                if in_deg >= 2: self.global_motifs['Fan-In'] += 1
                if out_deg >= 2: self.global_motifs['Fan-Out'] += 1
                
                # Chain (A -> node -> B)
                preds = list(G_users.predecessors(node))
                succs = list(G_users.successors(node))
                for p in preds:
                    for s in succs:
                        if p != s: self.global_motifs['Chain'] += 1
            
            # --- Extração de Tríades de Sentimento (DFS Paths) ---
            # Precisamos de caminhos diretos de tamanho 3 (Parent -> Child -> Grandchild)
            paths = []
            def dfs_paths(node, current_path):
                current_path.append(node)
                if len(current_path) >= 3:
                    # Avalia os últimos 3 nós do caminho
                    n1, n2, n3 = current_path[-3], current_path[-2], current_path[-1]
                    s1 = node_memory.get(n1, {}).get('label')
                    s2 = node_memory.get(n2, {}).get('label')
                    s3 = node_memory.get(n3, {}).get('label')
                    
                    if s1 in self.VALID_SENTIMENTS and s2 in self.VALID_SENTIMENTS and s3 in self.VALID_SENTIMENTS:
                        if s1 == s2 == s3:
                            self.global_triads['Persistence'] += 1
                        elif s1 != s2 and s2 == s3:
                            self.global_triads['Convergence'] += 1
                        elif s1 == s2 and s2 != s3:
                            self.global_triads['Shift'] += 1
                        elif s1 == s3 and s1 != s2:
                            self.global_triads['Oscillation'] += 1
                        else:
                            self.global_triads['Mixed Transition'] += 1
                
                for neighbor in G_struct.successors(node):
                    dfs_paths(neighbor, current_path.copy())

            dfs_paths(root_id, [])

            # --- Salva no DataFrame da Memória ---
            cascades_data.append({
                'max_depth': max_depth,
                'max_breadth': max_breadth,
                'structural_virality': virality,
                'cascade_size': num_messages,
                'duration_minutes': duration_minutes,
                'unique_users': unique_users,
                'perc_negative': perc_neg
            })

        self.df_cascades = pd.DataFrame(cascades_data)
        print("   -> Fase 3/3: Matriz de Features Finalizada.")
        return True

    # =========================================================================
    # RQ1: ANÁLISES ESTRUTURAIS E MOTIFS
    # =========================================================================
    def plot_rq1(self):
        print("\n[*] Executando RQ1 (Estrutura e Motifs)...")
        df = self.df_cascades

        def plot_ccdf(column, x_label, filename):
            data = df[column].dropna().values
            if len(data) == 0: return
            sorted_data = np.sort(data)
            y = (1.0 - np.arange(len(sorted_data)) / len(sorted_data)) * 100
            
            fig, ax = plt.subplots(figsize=(8, 6))
            ax.plot(sorted_data, y, color=self.magma_hex[0], linewidth=3)
            if 'CCDF' not in x_label: ax.set_xscale('log')
            ax.set_xlabel(x_label, fontsize=16, fontweight='bold')
            ax.set_ylabel('CCDF (% OF CASCADES)', fontsize=16, fontweight='bold')
            ax.yaxis.set_major_formatter(mtick.PercentFormatter(decimals=0))
            ax.tick_params(labelsize=14)
            sns.despine()
            plt.savefig(os.path.join(self.RESULTS_DIR, filename), dpi=300, bbox_inches='tight')
            plt.close()

        # CCDFs
        plot_ccdf('max_depth', 'MAX CASCADE DEPTH', 'RQ1_Fig1_CCDF_Depth.pdf')
        plot_ccdf('max_breadth', 'MAX CASCADE BREADTH', 'RQ1_Fig1_CCDF_Breadth.pdf')
        plot_ccdf('structural_virality', 'STRUCTURAL VIRALITY (WIENER)', 'RQ1_Fig1_CCDF_Virality.pdf')

        # Gráficos de Tendência Agregados (Bivariados)
        def plot_bivariate_trend(x_col, y_col, x_label, y_label, filename):
            # Arredonda o eixo X para agrupar as médias de Y adequadamente
            trend_df = df.copy()
            trend_df[x_col] = trend_df[x_col].apply(lambda v: int(v) if v < 50 else (int(v)//10)*10)
            grouped = trend_df.groupby(x_col)[y_col].mean().reset_index()
            
            fig, ax = plt.subplots(figsize=(8, 6))
            sns.lineplot(data=grouped, x=x_col, y=y_col, color=self.magma_hex[1], linewidth=3, marker='o', markersize=8, ax=ax)
            ax.set_xlabel(x_label, fontsize=16, fontweight='bold')
            ax.set_ylabel(y_label, fontsize=16, fontweight='bold')
            ax.tick_params(labelsize=14)
            sns.despine()
            plt.savefig(os.path.join(self.RESULTS_DIR, filename), dpi=300, bbox_inches='tight')
            plt.close()

        plot_bivariate_trend('max_depth', 'duration_minutes', 'MAX DEPTH', 'MEAN DURATION (MINUTES)', 'RQ1_Trend_Depth_vs_Minutes.pdf')
        plot_bivariate_trend('max_depth', 'max_breadth', 'MAX DEPTH', 'MEAN MAX BREADTH', 'RQ1_Trend_Depth_vs_Breadth.pdf')
        plot_bivariate_trend('max_depth', 'unique_users', 'MAX DEPTH', 'MEAN UNIQUE USERS', 'RQ1_Trend_Depth_vs_Users.pdf')
        plot_bivariate_trend('unique_users', 'duration_minutes', 'UNIQUE USERS', 'MEAN DURATION (MINUTES)', 'RQ1_Trend_Users_vs_Minutes.pdf')

        # Figura 5: Motifs
        fig, ax = plt.subplots(figsize=(10, 6))
        motifs_s = pd.Series(self.global_motifs)
        motifs_s = (motifs_s / motifs_s.sum()) * 100
        motifs_s = motifs_s.sort_values(ascending=False)
        sns.barplot(x=motifs_s.values, y=motifs_s.index, color=self.magma_hex[2], ax=ax)
        ax.set_xlabel('PROPORTION OF MOTIFS (%)', fontsize=16, fontweight='bold')
        ax.set_ylabel('USER INTERACTION MOTIF', fontsize=16, fontweight='bold')
        ax.xaxis.set_major_formatter(mtick.PercentFormatter(decimals=0))
        ax.tick_params(labelsize=14)
        sns.despine()
        plt.savefig(os.path.join(self.RESULTS_DIR, 'RQ1_Fig5_Motifs_Base_Inteira.pdf'), dpi=300, bbox_inches='tight')
        plt.close()

    # =========================================================================
    # RQ2: SENTIMENTOS E TRÍADES
    # =========================================================================
    def plot_rq2(self):
        print("[*] Executando RQ2 (Sentimentos e Padrões Triádicos)...")
        
        # Figura 2: Sentimentos Globais
        fig, ax = plt.subplots(figsize=(8, 6))
        sent_s = pd.Series(self.global_sentiments)
        sent_s = (sent_s / sent_s.sum()) * 100
        colors = ['#3B0F70', '#CA3E72', '#FECF92'] 
        sns.barplot(x=sent_s.index, y=sent_s.values, palette=colors, ax=ax)
        ax.set_ylabel('GLOBAL MESSAGE VOLUME (%)', fontsize=16, fontweight='bold')
        ax.yaxis.set_major_formatter(mtick.PercentFormatter(decimals=0))
        ax.tick_params(labelsize=14)
        sns.despine()
        plt.savefig(os.path.join(self.RESULTS_DIR, 'RQ2_Fig2_Sentimentos_Base_Inteira.pdf'), dpi=300, bbox_inches='tight')
        plt.close()

        # Figura 9: Tríades
        fig, ax = plt.subplots(figsize=(10, 6))
        triads_s = pd.Series(self.global_triads)
        if triads_s.sum() > 0:
            triads_s = (triads_s / triads_s.sum()) * 100
            triads_s = triads_s.sort_values(ascending=False)
            sns.barplot(x=triads_s.values, y=triads_s.index, color=self.magma_hex[0], ax=ax)
            ax.set_xlabel('DISTRIBUTION OF TRIADIC PATTERNS (%)', fontsize=16, fontweight='bold')
            ax.set_ylabel('TRIADIC EMOTIONAL EVOLUTION', fontsize=16, fontweight='bold')
            ax.xaxis.set_major_formatter(mtick.PercentFormatter(decimals=0))
            ax.tick_params(labelsize=14)
            sns.despine()
            plt.savefig(os.path.join(self.RESULTS_DIR, 'RQ2_Fig9_Triades_Base_Inteira.pdf'), dpi=300, bbox_inches='tight')
            plt.close()

    # =========================================================================
    # RQ3: INTERSECÇÕES ESTRUTURA VS SENTIMENTO
    # =========================================================================
    def plot_rq3(self):
        print("[*] Executando RQ3 (Intersecção Estrutura x Sentimento Negativo)...")
        df = self.df_cascades
        
        scatter_params = [
            ('max_depth', 'perc_negative', 'MAXIMUM CASCADE DEPTH', 'NEGATIVE SENTIMENT (%)', 'RQ3_Scatter_Depth_vs_Neg.pdf'),
            ('structural_virality', 'perc_negative', 'STRUCTURAL VIRALITY', 'NEGATIVE SENTIMENT (%)', 'RQ3_Scatter_Virality_vs_Neg.pdf'),
            ('duration_minutes', 'perc_negative', 'CASCADE DURATION (MINUTES)', 'NEGATIVE SENTIMENT (%)', 'RQ3_Scatter_Duration_vs_Neg.pdf')
        ]
        
        for x_col, y_col, xlabel, ylabel, filename in scatter_params:
            fig, ax = plt.subplots(figsize=(10, 8))
            
            # 1. Plotamos OS PONTOS rasterizados (leve para o PDF). 
            # Adicionei edgecolor=None, vital para milhões de pontos não virarem um borrão escuro
            sns.scatterplot(data=df, x=x_col, y=y_col, 
                            alpha=0.05, s=15, color='gray', edgecolor=None,
                            ax=ax, rasterized=True)
            
            # 2. Plotamos APENAS A LINHA de regressão vetorizada por cima
            sns.regplot(data=df, x=x_col, y=y_col, 
                        scatter=False, 
                        line_kws={'color': '#d62728', 'linewidth': 3}, 
                        ax=ax)
            
            ax.set_xlabel(xlabel, fontsize=16, fontweight='bold')
            ax.set_ylabel(ylabel, fontsize=16, fontweight='bold')
            ax.yaxis.set_major_formatter(mtick.PercentFormatter(decimals=0))
            ax.tick_params(labelsize=14)
            
            if x_col in ['duration_minutes', 'max_depth']:
                ax.set_xscale('log')
                
            sns.despine()
            
            # O dpi=300 dita a resolução da camada rasterizada no PDF
            plt.savefig(os.path.join(self.RESULTS_DIR, filename), dpi=300, bbox_inches='tight')
            plt.close()
if __name__ == "__main__":
    app = SixthWaveAnalyticsOrchestrator()
    if app.extract_and_compute_all():
        app.plot_rq1()
        app.plot_rq2()
        app.plot_rq3()
        print("\n[SUCCESS] Pipeline da Sexta Onda concluído. Gráficos em 'results/6-sixth_wave/'.")