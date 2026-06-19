import json
import os
import pandas as pd
import numpy as np
import matplotlib # type: ignore
matplotlib.use('Agg')
import matplotlib.pyplot as plt # type: ignore
import matplotlib.ticker as mtick # type: ignore
import matplotlib.patheffects as pe # type: ignore
import seaborn as sns # type: ignore
import networkx as nx
from collections import defaultdict
from scipy import stats
from scipy.stats import linregress, kruskal

try:
    from adjustText import adjust_text # type: ignore
except ImportError:
    adjust_text = None

from Utilities import Config

class AnalyticsEngine:
    def __init__(self):
        Config.setup_directories()
        self.colors = Config.get_colors()
        self.df_cascades = None
        self.node_memory = {}
        self.sub_roots = defaultdict(list)
        self.sub_children = defaultdict(lambda: defaultdict(list))
        self.triad_counts = {cat: defaultdict(int) for cat in Config.CATEGORIES}

    def load_or_extract_data(self):
        triads_cache_path = Config.CACHE_PATH.replace('.parquet', '_triads.json')
        homophily_cache_path = Config.CACHE_PATH.replace('.parquet', '_homophily.json')
        
        if os.path.exists(Config.CACHE_PATH) and os.path.exists(triads_cache_path) and os.path.exists(homophily_cache_path):
            print(f"[*] Cache encontrado. A carregar DataFrame de {Config.CACHE_PATH}...")
            self.df_cascades = pd.read_parquet(Config.CACHE_PATH)
            
            with open(triads_cache_path, 'r', encoding='utf-8') as f:
                self.triad_counts = json.load(f)
                
            # Carrega os dados de homofilia do cache
            with open(homophily_cache_path, 'r', encoding='utf-8') as f:
                homophily_data = json.load(f)
                self.global_sentiments = homophily_data.get('global_sentiments', {})
                self.user_sentiments = defaultdict(lambda: {'total': 0, 'negative': 0}, homophily_data.get('user_sentiments', {}))
                # Converte listas de volta para tuplas para o processamento de arestas
                self.global_user_edges = [tuple(x) for x in homophily_data.get('global_user_edges', [])]
                
            print("   -> DataFrame, Tríades e Homofilia carregados instantaneamente para a RAM.")
            return True
        else:
            print("[*] Cache não encontrado ou desatualizado. A iniciar extração profunda dos grafos...")
            sucesso = self.extract_and_compute_all()
            
            if sucesso and self.df_cascades is not None and not self.df_cascades.empty:
                self.df_cascades.to_parquet(Config.CACHE_PATH, index=False)
                
                with open(triads_cache_path, 'w', encoding='utf-8') as f:
                    json.dump(self.triad_counts, f)
                    
                # Guarda os novos dados globais num cache separado
                with open(homophily_cache_path, 'w', encoding='utf-8') as f:
                    json.dump({
                        'global_sentiments': self.global_sentiments,
                        'user_sentiments': dict(self.user_sentiments),
                        'global_user_edges': self.global_user_edges
                    }, f)
                    
                print(f"   -> Cache guardado com sucesso (Parquet + JSONs).")
            return sucesso

    def extract_and_compute_all(self):
        print("[*] Initiating Single-Pass Extraction (Unified Orchestrator)...")
        
        # NOVOS RASTREADORES GLOBAIS
        self.global_sentiments = {'POSITIVE': 0, 'NEUTRAL': 0, 'NEGATIVE': 0}
        self.user_sentiments = defaultdict(lambda: {'total': 0, 'negative': 0})
        self.global_user_edges = [] # Para a rede de homofilia (source, target)

        with open(Config.MULTIMODAL_PATH, 'r', encoding='utf-8') as f:
            for line in f:
                try:
                    record = json.loads(line)
                    sub = record.get('subreddit', '').lower()
                    if sub not in Config.CATEGORY_MAP: continue
                    
                    n_id_raw = record.get('id', '')
                    p_id_raw = record.get('parent_id')
                    depth = record.get('depth', 0)
                    score = record.get('metadata_score', 0)
                    
                    n_id = str(n_id_raw).split('_')[-1]
                    p_id = str(p_id_raw).split('_')[-1] if p_id_raw and pd.notna(p_id_raw) else None
                    is_post_reply = str(p_id_raw).startswith('t3_') if p_id_raw else False
                    
                    label = record.get('ai_analysis', {}).get('label', 'UNKNOWN')
                    author = record.get('author', '[deleted]')
                    ts = record.get('created_utc') or record.get('timestamp') or record.get('ai_analysis', {}).get('created_utc')
                    
                    self.node_memory[n_id] = {
                        'p_id': p_id, 'label': label, 'sub': sub, 
                        'author': author, 'timestamp': float(ts) if ts else None,
                        'metadata_score': score
                    }
                    
                    # Rastreio Global de Sentimentos da Base
                    if label in Config.VALID_SENTIMENTS:
                        self.global_sentiments[label] += 1
                        # FILTRO MÍNIMO: Remove apenas o vácuo dos deletados e a moderação automatizada do Reddit
                        # Bots informativos (como robôs de dicionário ou links) passam livremente e são contabilizados
                        if author not in ['[deleted]', 'deleted', 'automoderator', 'redditcaresresources']:
                            self.user_sentiments[author]['total'] += 1
                            if label == 'NEGATIVE':
                                self.user_sentiments[author]['negative'] += 1

                    if depth == 1 or is_post_reply:
                        self.sub_roots[sub].append(n_id)
                    elif p_id:
                        self.sub_children[sub][p_id].append(n_id)
                        
                except Exception:
                    continue

        cascades_data = []
        
        for sub, cat in Config.CATEGORY_MAP.items():
            if sub not in self.sub_roots: continue
            
            for root_id in self.sub_roots[sub]:
                queue = [(root_id, None)]
                G_struct = nx.DiGraph()
                G_users = nx.DiGraph()
                timestamps = []
                scores = []
                authors = set() 
                sentiments = {'POSITIVE': 0, 'NEUTRAL': 0, 'NEGATIVE': 0}
                total_valid = 0
                
                # Para o longest_negative_run
                node_labels = {}
                
                while queue:
                    curr, actual_parent = queue.pop(0)
                    G_struct.add_node(curr)
                    
                    nd = self.node_memory.get(curr, {})
                    ts = nd.get('timestamp')
                    author = nd.get('author', '[deleted]')
                    lbl = nd.get('label', 'UNKNOWN')
                    score = nd.get('metadata_score', 0)
                    
                    node_labels[curr] = lbl
                    
                    if ts: timestamps.append(ts)
                    if author not in ['[deleted]', 'deleted']: authors.add(author)
                    scores.append(score)
                    
                    if lbl in Config.VALID_SENTIMENTS:
                        sentiments[lbl] += 1
                        total_valid += 1
                    
                    if actual_parent is not None:
                        G_struct.add_edge(actual_parent, curr)
                        parent_author = self.node_memory.get(actual_parent, {}).get('author', '[deleted]')
                        
                        if author not in ['[deleted]', 'deleted'] and parent_author not in ['[deleted]', 'deleted']:
                            self.global_user_edges.append((author, parent_author)) # Salva para homofilia
                            if author != parent_author:
                                G_users.add_edge(author, parent_author)
                    
                    for child_id in self.sub_children[sub].get(curr, []):
                        queue.append((child_id, curr))

                num_nodes = G_struct.number_of_nodes()
                if num_nodes < 3 or total_valid == 0: continue
                
                # DP Simples para achar o longest_negative_run
                dp_neg = {}
                try:
                    for n in nx.topological_sort(G_struct):
                        if node_labels.get(n) == 'NEGATIVE':
                            preds = list(G_struct.predecessors(n))
                            dp_neg[n] = (dp_neg[preds[0]] + 1) if preds and preds[0] in dp_neg else 1
                        else:
                            dp_neg[n] = 0
                    longest_neg_run = max(dp_neg.values()) if dp_neg else 0
                except nx.NetworkXUnfeasible:
                    longest_neg_run = 0

                ratio_neg_run = (longest_neg_run / sentiments['NEGATIVE']) if sentiments['NEGATIVE'] > 0 else 0.0

                G_un = G_struct.to_undirected()
                virality = nx.wiener_index(G_un) / (num_nodes * (num_nodes - 1)) if num_nodes > 1 else 1.0
                
                try:
                    lengths = nx.single_source_shortest_path_length(G_struct, root_id)
                    max_depth = max(lengths.values()) if lengths else 1
                    level_counts = defaultdict(int)
                    for dist in lengths.values(): level_counts[dist] += 1
                    max_breadth = max(level_counts.values()) if level_counts else 1
                except:
                    max_depth, max_breadth = 1, 1
                    
                unique_users = max(len(authors), 1)
                duration_minutes = (np.sort(timestamps)[-1] - np.sort(timestamps)[0])/60.0 if len(timestamps) >= 2 else 0.0
                duration_hours = duration_minutes / 60.0
                
                # Cálculo real dos Motifs estruturais
                dyads = sum(1 for u, v in G_users.edges() if not G_users.has_edge(v, u))
                mutual_dyads = sum(1 for u, v in G_users.edges() if G_users.has_edge(v, u)) / 2
                triads = nx.triadic_census(G_users) if G_users.number_of_nodes() >= 3 else defaultdict(int)
                
                motifs = {
                    'Dyad': dyads, 'Mutual Dyad': mutual_dyads, 'Chain': triads.get('021C', 0),
                    'Fan-In': triads.get('021U', 0), 'Fan-Out': triads.get('021D', 0),
                    'Triangle': triads.get('030T', 0), 'Recip. Triangle': triads.get('300', 0)
                }
                cascades_data.append({
                    'Cascade_ID': root_id, 'Subreddit': sub, 'Category': cat,
                    'Structural_Virality': virality, 'Cascade_Size': num_nodes,
                    'Duration_Minutes': duration_minutes,
                    'Duration_Hours': duration_hours,
                    'Max_Depth': max_depth,
                    'Max_Breadth': max_breadth,
                    'Unique_Users': unique_users,
                    'Average_Score': np.mean(scores) if scores else 0.0,
                    'Perc_Negative': (sentiments['NEGATIVE'] / total_valid) * 100,
                    'Longest_Neg_Run_Ratio': ratio_neg_run,
                    'Dominant_Sentiment': max(sentiments, key=sentiments.get),
                    'Total_Motifs': sum(motifs.values()),
                    **motifs
                })

        self.df_cascades = pd.DataFrame(cascades_data)
        print(f"   -> Phase 3 Complete. {len(self.df_cascades)} cascades loaded.")
        return True

    def print_dataset_overview(self):
        """Gera os dados brutos solicitados para as Tabelas 1, 2 e 3 do TCC e salva num arquivo TXT."""
        import os
        import pandas as pd
        from Utilities import Config
        
        df_cascades = self._prepare_quartiles(interactive_only=False)
        output_file = os.path.join(Config.RESULTS_DIR, "Detailed_Dataset_Stats.txt")
        
        with open(output_file, "w", encoding="utf-8") as f:
            def log_and_print(msg):
                print(msg)
                f.write(msg + "\n")

            log_and_print("\n" + "="*70)
            log_and_print(" RELATÓRIO DE INFORMAÇÕES GLOBAIS DA BASE (PARA TABELAS LATEX)")
            log_and_print("="*70)
            
            # TABELA 3: Sentimentos
            log_and_print("\n[TABELA 3] Quantidade Total de Comentários Válidos por Sentimento:")
            total_comments = sum(self.global_sentiments.values())
            for k, v in self.global_sentiments.items():
                pct = (v / total_comments * 100) if total_comments > 0 else 0
                log_and_print(f"   - {k}: {v:,} ({pct:.2f}%)")
                
            # TABELA 2: Cascatas
            log_and_print("\n[TABELA 2] Limites e Contagem dos Quartis de Negatividade das CASCATAS:")
            for q in ['Q1', 'Q2', 'Q3', 'Q4']:
                subset = df_cascades[df_cascades['neg_quartile'] == q]['Perc_Negative']
                count = len(subset)
                if count > 0:
                    log_and_print(f"   - {q}: {count:,} cascatas | Inicia em: {subset.min():.2f}% -> Termina em: {subset.max():.2f}%")

            # TABELA 1: Usuários
            log_and_print("\n[TABELA 1] Limites dos Quartis de Negatividade dos USUÁRIOS (Homofilia):")
            user_data = []
            for author, counts in self.user_sentiments.items():
                if counts['total'] > 0:
                    pct_neg = (counts['negative'] / counts['total']) * 100
                    user_data.append({'author': author, 'perc_negative': pct_neg})
            
            if user_data:
                df_users = pd.DataFrame(user_data)
                
                bins = [-1.0, 25.00, 50.00, 75.00, 100.00]
                df_users['user_type'] = pd.cut(df_users['perc_negative'], bins=bins, labels=['UQ1', 'UQ2', 'UQ3', 'UQ4'])
                
                for q in ['UQ1', 'UQ2', 'UQ3', 'UQ4']:
                    subset = df_users[df_users['user_type'] == q]['perc_negative']
                    count = len(subset)
                    if count > 0:
                        log_and_print(f"   - {q}: {count:,} usuários | Inicia em: {subset.min():.2f}% -> Termina em: {subset.max():.2f}%")

            log_and_print("="*70 + "\n")
            
        # O último print é para garantir que você saiba onde o arquivo foi parar
        print(f"[*] Relatório completo das Tabelas salvo com sucesso em: {output_file}")

    def _prepare_quartiles(self, interactive_only=False):
        df = self.df_cascades.copy()
        
        if interactive_only:
            df = df[df['Total_Motifs'] > 0].copy()
            
        neg_col = 'Perc_Negative' if 'Perc_Negative' in df.columns else 'perc_negative'
        
        # CATEGORIZAÇÃO ABSOLUTA: Força a divisão exata de 25% em 25% de negatividade.
        # Usa pd.cut com limites imutáveis para espelhar a classificação dos usuários (UQ).
        bins = [-1.0, 25.0, 50.0, 75.0, 100.0]
        df['neg_quartile'] = pd.cut(df[neg_col], bins=bins, labels=['Q1', 'Q2', 'Q3', 'Q4'])
        
        return df

    def _get_grouping_config(self, grouping, df, interactive_only=False):
        """Helper to dynamically fetch target columns, labels, specific color palettes, and output directory."""
        folder_suffix = "_Interactive_Cascades" if interactive_only else ""
        output_dir = os.path.join(Config.RESULTS_DIR, f"{grouping}{folder_suffix}")
        os.makedirs(output_dir, exist_ok=True)

        if grouping == 'Categories':
            return 'Category', Config.CATEGORIES, self.colors['COLOR_SCHEME'], output_dir
        elif grouping == 'Quartiles':
            return 'neg_quartile', df['neg_quartile'].cat.categories, self.colors['COLOR_SCHEME'], output_dir
        elif grouping == 'Sentiments':
            colors = [self.colors['SENTIMENTS']['POSITIVE'], self.colors['SENTIMENTS']['NEUTRAL'], self.colors['SENTIMENTS']['NEGATIVE']]
            return 'Dominant_Sentiment', ['POSITIVE', 'NEUTRAL', 'NEGATIVE'], colors, output_dir
        
        return 'Category', Config.CATEGORIES, self.colors['COLOR_SCHEME'], output_dir

    # =========================================================
    # FIGURA 1: TRENDLINES E CCDFs ESTRUTURAIS
    # =========================================================
    def plot_structural_ccdfs(self, grouping="Categories", interactive_only=False):
        print(f"[*] Generating Figure 1 Structural CCDFs and Trendlines by {grouping}...")
        Config.set_sns_theme()
        df = self._prepare_quartiles(interactive_only)
        group_col, groups_list, current_colors, output_dir = self._get_grouping_config(grouping, df, interactive_only)

        metrics = [
            ('Structural_Virality', 'STRUCTURAL VIRALITY', 'Fig1_CCDF_Structural_Virality.pdf'),
            ('Max_Depth', 'MAXIMUM DEPTH', 'Fig1_CCDF_Max_Depth.pdf'),
            ('Max_Breadth', 'MAXIMUM BREADTH', 'Fig1_CCDF_Max_Breadth.pdf'),
            ('Cascade_Size', 'SIZE (NUMBER OF MESSAGES)', 'Fig1_CCDF_Number_Messages.pdf'),
            ('Duration_Hours', 'CASCADE DURATION (HOURS)', 'Fig1_CCDF_Duration_Hours.pdf'),
            ('Longest_Neg_Run_Ratio', 'LONGEST NEGATIVE RUN / TOTAL NEGATIVE', 'Fig1_CCDF_Longest_Negative_Run.pdf')
        ]
        
        for col, xlabel, filename in metrics:
            if col not in df.columns: continue
            fig, ax = plt.subplots(figsize=(10, 7))
            
            global_max_val = df[col].max()
            
            for i, cat in enumerate(groups_list):
                data = df[df[group_col] == cat][col].dropna().values
                if len(data) == 0: continue
                mean_val, std_val = np.mean(data), np.std(data)
                label_text = f"{cat} (μ={mean_val:.2f}, σ={std_val:.2f})"
                
                sorted_data = np.sort(data)
                y = (1.0 - np.arange(len(sorted_data)) / len(sorted_data)) * 100
                ax.plot(sorted_data, y, color=current_colors[i], 
                        linestyle=self.colors['LINESTYLES'][i % len(self.colors['LINESTYLES'])], 
                        linewidth=3.5, label=label_text)

            ax.set_xlabel(xlabel, fontsize=18, fontweight='bold')
            ax.set_ylabel('CCDF (% OF CASCADES)', fontsize=18, fontweight='bold')
            ax.yaxis.set_major_formatter(mtick.PercentFormatter(decimals=0))
            
            ax.legend(fontsize=20, loc='upper right', framealpha=0.9, edgecolor='black')
            ax.tick_params(labelsize=16)
            sns.despine()
            
            # 1. Margem invisível minúscula (2%) apenas para o número final não raspar na borda do PDF
            ax.set_xlim(left=0, right=global_max_val * 1.02)
            
            # 2. Deixa o Matplotlib usar o comportamento padrão dele ("AutoLocator" para números bonitos)
            # Exceto para os que obrigatoriamente precisam ser inteiros
            if col == 'Structural_Virality':
                ax.xaxis.set_major_locator(mtick.MaxNLocator(integer=True))
            elif col == 'Max_Depth':
                ax.set_xticks(np.arange(1, global_max_val + 2, 2))
            else:
                ax.xaxis.set_major_locator(mtick.AutoLocator())

            # Força a renderização para podermos extrair o que o AutoLocator decidiu fazer
            fig.canvas.draw()
            current_ticks = ax.get_xticks()

            # 3. Mantém apenas os números onde a distância para o máximo é segura (maior que 8% do total)
            safe_distance = global_max_val * 0.08
            clean_ticks = [t for t in current_ticks if t >= 0 and (global_max_val - t) > safe_distance]
            
            # 4. Adiciona o nosso máximo absoluto cravado no final
            if col in ['Structural_Virality', 'Max_Depth', 'Max_Breadth', 'Cascade_Size']:
                clean_ticks.append(int(global_max_val))
            else:
                clean_ticks.append(global_max_val)
                
            ax.set_xticks(clean_ticks)

            # Roda as legendas em 45 graus para leitura perfeita
            # plt.setp(ax.get_xticklabels(), rotation=45, ha="right", rotation_mode="anchor")

            out_file = os.path.join(output_dir, filename)
            plt.savefig(out_file, dpi=300, bbox_inches='tight')
            plt.close()

    # =========================================================
    # FIGURA 2: MOTIFS HEATMAP
    # =========================================================
    def run_motif_analysis(self, grouping="Categories", interactive_only=False):
        print(f"[*] Generating Figure 2 Motifs Heatmap + Kruskal-Wallis Tests by {grouping}...")
        df = self._prepare_quartiles(interactive_only)
        df = df[df['Total_Motifs'] > 0].copy()
        
        group_col, groups_list, _, output_dir = self._get_grouping_config(grouping, df, interactive_only)
        motif_cols = ['Dyad', 'Mutual Dyad', 'Chain', 'Fan-In', 'Fan-Out', 'Triangle', 'Recip. Triangle']
        
        for m in motif_cols:
            df[m + '_pct'] = (df[m] / df['Total_Motifs']) * 100
            
        agg_mean = df.groupby(group_col, observed=False)[[m + '_pct' for m in motif_cols]].mean()
        agg_se = df.groupby(group_col, observed=False)[[m + '_pct' for m in motif_cols]].apply(lambda x: x.sem())
        
        labels = np.asarray([[f"{agg_mean.loc[cat, m+'_pct']:.1f}%\n±{agg_se.loc[cat, m+'_pct']:.2f}%" for m in motif_cols] for cat in groups_list])
        
        fig, ax = plt.subplots(figsize=(15, 6))
        sns.heatmap(agg_mean, annot=labels, fmt="", cmap=self.colors['CMAP'], cbar=True, ax=ax, annot_kws={'size': 14, 'weight': 'bold'})
        
        ax.set_ylabel(grouping.upper(), fontsize=16, fontweight='bold')
        ax.set_xlabel("USER INTERACTION MOTIFS PROPORTION", fontsize=16, fontweight='bold')
        ax.set_xticklabels(motif_cols)
        ax.set_yticklabels(groups_list, rotation=0)
        
        out_file = os.path.join(output_dir, "Fig2_Motifs_Ecosystem_Heatmap.pdf")
        plt.tight_layout()
        plt.savefig(out_file, dpi=300)
        plt.close()
        print(f"   -> Saved: Fig2_Motifs_Ecosystem_Heatmap.pdf")

    # =========================================================
    # FIGURA 3: AVERAGE SCORE CCDF
    # =========================================================
    def run_figure3_average_score(self, grouping="Categories", interactive_only=False):
        print(f"[*] Generating Figure 3 Average Score CCDF by {grouping}...")
        Config.set_sns_theme()
        df = self._prepare_quartiles(interactive_only)
        
        group_col, groups_list, current_colors, output_dir = self._get_grouping_config(grouping, df, interactive_only)

        fig, ax = plt.subplots(figsize=(10, 7))
        
        for j, cat in enumerate(groups_list):
            data = df[df[group_col] == cat]['Average_Score'].dropna().values
            if len(data) == 0: continue
            
            mean_val, std_val = np.mean(data), np.std(data)
            label_text = f"{cat} (μ={mean_val:.2f}, σ={std_val:.2f})"
            
            sorted_data = np.sort(data)
            y = (1.0 - np.arange(len(sorted_data)) / len(sorted_data)) * 100
            
            ax.plot(sorted_data, y, color=current_colors[j], 
                    linestyle=self.colors['LINESTYLES'][j % len(self.colors['LINESTYLES'])], 
                    linewidth=3.5, label=label_text)

        ax.set_xlabel('AVERAGE CASCADE SCORE (UPVOTES - DOWNVOTES)', fontsize=18, fontweight='bold')
        ax.set_ylabel('CCDF (% OF CASCADES)', fontsize=18, fontweight='bold')
        ax.yaxis.set_major_formatter(mtick.PercentFormatter(decimals=0))
        ax.legend(fontsize=14, loc='upper right', framealpha=0.9, edgecolor='black')
        ax.tick_params(labelsize=14)
        sns.despine()
        
        out_file = os.path.join(output_dir, "Fig3_CCDF_Average_Score.pdf")
        plt.tight_layout()
        plt.savefig(out_file, dpi=300, bbox_inches='tight')
        plt.close()
        print(f"   -> Saved: Fig3_CCDF_Average_Score.pdf")

    def run_statistical_reports(self, grouping="Categories", interactive_only=False):
        import itertools
        self.print_dataset_overview()
        self.generate_cascade_diagram()
        print(f"[*] Exporting Statistical Reports (PDF & TXT) by {grouping}...")
        df = self._prepare_quartiles(interactive_only)
        
        group_col, groups_list, _, output_dir = self._get_grouping_config(grouping, df, interactive_only)

        # ==========================================
        # PARTE 1: COMPUTAÇÃO DOS TESTES INFERENCIAIS
        # ==========================================
        stats_results = []
        # 'Longest_Neg_Run_Ratio' adicionado com sucesso para rodar KW e KS automaticamente!
        metrics = ['Structural_Virality', 'Max_Depth', 'Max_Breadth', 'Unique_Users', 'Cascade_Size', 'Duration_Minutes', 'Average_Score', 'Longest_Neg_Run_Ratio']
        
        for col in metrics:
            if col not in df.columns: continue
            
            groups = [df[df[group_col] == cat][col].dropna().values for cat in groups_list if len(df[df[group_col] == cat][col].dropna()) > 0]
            
            if len(groups) >= 2:
                kw_stat, kw_p = stats.kruskal(*groups)
                q1, q4 = groups[0], groups[-1]
                
                ks_stat, ks_p = stats.ks_2samp(q1, q4) if len(q1)>0 and len(q4)>0 else (float('nan'), float('nan'))
                
                if len(q1)>0 and len(q4)>0:
                    u, _ = stats.mannwhitneyu(q1, q4, alternative='two-sided')
                    c_delta = (2 * u) / (len(q1) * len(q4)) - 1
                else: 
                    c_delta = float('nan')
                
                stats_results.append({
                    'Metric': col, 
                    'Kruskal H': f"{kw_stat:.2f}", 'Kruskal p': f"{kw_p:.2e}",
                    'KS D-value': f"{ks_stat:.4f}", 'KS p': f"{ks_p:.2e}", 
                    "Cliff's Delta (First v Last)": f"{c_delta:.4f}"
                })

        if 'Perc_Negative' in df.columns and 'Structural_Virality' in df.columns:
            clean_df = df[['Perc_Negative', 'Structural_Virality']].dropna()
            corr, p_value = stats.spearmanr(clean_df['Perc_Negative'], clean_df['Structural_Virality'])
            stats_results.append({
                'Metric': 'Spearman (Neg x Virality)',
                'Kruskal H': "-", 'Kruskal p': f"{p_value:.2e}",
                'KS D-value': f"{corr:.4f} (Rho)", 'KS p': "-",
                "Cliff's Delta (First v Last)": "-"
            })

        # ==========================================
        # PARTE 2: MONTAGEM DO CENSO GLOBAL DO DATASET
        # ==========================================
        total_cascades = len(df)
        total_messages = sum(self.global_sentiments.values())
        
        global_summary_data = [
            ['Total Cascades', f"{total_cascades:,}", '-'],
            ['Total Valid Messages (No Headers/Footers)', f"{total_messages:,}", '100.00%'],
            ['   - POSITIVE Sentiment', f"{self.global_sentiments.get('POSITIVE', 0):,}", f"{(self.global_sentiments.get('POSITIVE', 0)/total_messages*100 if total_messages > 0 else 0):.2f}%"],
            ['   - NEUTRAL Sentiment', f"{self.global_sentiments.get('NEUTRAL', 0):,}", f"{(self.global_sentiments.get('NEUTRAL', 0)/total_messages*100 if total_messages > 0 else 0):.2f}%"],
            ['   - NEGATIVE Sentiment', f"{self.global_sentiments.get('NEGATIVE', 0):,}", f"{(self.global_sentiments.get('NEGATIVE', 0)/total_messages*100 if total_messages > 0 else 0):.2f}%"]
        ]
        df_global = pd.DataFrame(global_summary_data, columns=['Metric / Feature', 'Absolute Volume', 'Proportion (%)'])

        # ==========================================
        # PARTE 3: PLOTAGEM DUPLA NO PDF
        # ==========================================
        fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(14, 10))
        ax1.axis('off')
        ax2.axis('off')
        
        # Tabela Superior: Resumo Descritivo Global
        t1 = ax1.table(cellText=df_global.values, colLabels=df_global.columns,
                       cellLoc='center', loc='center', colColours=['#e6e6e6']*len(df_global.columns))
        t1.auto_set_font_size(False)
        t1.set_fontsize(10)
        t1.scale(1.0, 1.6)
        ax1.set_title("Global Dataset Overview Summary (Censo da Base)", fontsize=12, fontweight='bold', pad=15)

        # Tabela Inferior: Validação Estatística H e D
        if stats_results:
            df_stats = pd.DataFrame(stats_results)
            t2 = ax2.table(cellText=df_stats.values, colLabels=df_stats.columns, 
                           cellLoc='center', loc='center', colColours=['#f2f2f2']*len(df_stats.columns))
            t2.auto_set_font_size(False)
            t2.set_fontsize(10)
            t2.scale(1.0, 1.6)
            ax2.set_title(f"Statistical Validation Report ({grouping})", fontsize=12, fontweight='bold', pad=15)
            
        out_file = os.path.join(output_dir, "Statistical_Report_Summary.pdf")
        plt.tight_layout()
        plt.savefig(out_file, dpi=300, bbox_inches='tight')
        plt.close()
        print(f"   -> Guardado com sucesso: Statistical_Report_Summary.pdf")

        # ==========================================
        # PARTE 4: GERA O TXT DETALHADO COMBINATÓRIO
        # ==========================================
        txt_metrics = ['Structural_Virality', 'Max_Depth', 'Max_Breadth', 'Cascade_Size', 'Duration_Hours', 'Longest_Neg_Run_Ratio', 'Average_Score']
        txt_file_path = os.path.join(output_dir, "Detailed_Pairwise_Stats.txt")
        
        with open(txt_file_path, "w") as f:
            for col in txt_metrics:
                if col not in df.columns: continue
                f.write(f"\n{'='*40}\nMETRIC: {col}\n{'='*40}\n")
                
                groups_dict = {cat: df[df[group_col] == cat][col].dropna().values for cat in groups_list}
                
                # Kruskal Geral
                all_vals = [v for v in groups_dict.values() if len(v) > 0]
                if len(all_vals) >= 2:
                    h, p = stats.kruskal(*all_vals)
                    f.write(f"Global Kruskal-Wallis: H={h:.2f}, p={p:.2e}\n\n")
                
                # KS Test Combinatório para todas as duplas de quartis/categorias
                pairs = list(itertools.combinations(groups_list, 2))
                for g1, g2 in pairs:
                    arr1, arr2 = groups_dict[g1], groups_dict[g2]
                    if len(arr1) > 0 and len(arr2) > 0:
                        ks_stat, ks_p = stats.ks_2samp(arr1, arr2)
                        f.write(f"KS Test ({g1} vs {g2}): D={ks_stat:.4f}, p={ks_p:.2e}\n")
                        
        print(f"   -> Guardado com sucesso: Detailed_Pairwise_Stats.txt")
    
    def run_rq3_analysis(self, grouping="Categories", interactive_only=False):
        print(f"[*] Generating RQ3: Taxonomy Cascades Trendlines by {grouping}...")
        Config.set_sns_theme()
        df = self._prepare_quartiles(interactive_only)
        
        group_col, groups_list, current_colors, output_dir = self._get_grouping_config(grouping, df, interactive_only)
        palette_dict = dict(zip(groups_list, current_colors))
        
        sentiments = ['POSITIVE', 'NEUTRAL', 'NEGATIVE']
        fig, axes = plt.subplots(1, 3, figsize=(24, 7), sharey=True)
        
        global_max_virality = df['Structural_Virality'].max()
        global_max_y = df['Perc_Negative'].max()
        
        for i, sentiment in enumerate(sentiments):
            ax = axes[i]
            df_sub = df[df['Dominant_Sentiment'] == sentiment]
            if df_sub.empty: continue
            
            sns.scatterplot(data=df_sub, x='Structural_Virality', y='Perc_Negative', hue=group_col, palette=palette_dict,
                            alpha=0.5, s=30, edgecolor='none', ax=ax, zorder=2, rasterized=True, legend=False)
            
            sns.regplot(x=df_sub['Structural_Virality'], y=df_sub['Perc_Negative'], scatter=False, color='black', 
                        ci=95, line_kws={'linestyle':'-', 'linewidth':3.5, 'zorder': 4}, ax=ax)
            
            slope, intercept, r_value, p_value, std_err = linregress(df_sub['Structural_Virality'], df_sub['Perc_Negative'])
            stats_text = f"Trendline ($R^2={r_value**2:.3f}$)\n$p={p_value:.1e}$"
            ax.text(0.05, 0.95, stats_text, transform=ax.transAxes, fontsize=16, fontweight='bold',
                    verticalalignment='top', bbox=dict(facecolor='white', alpha=0.9, edgecolor='black', boxstyle='round,pad=0.5'))
            
            ax.set_title(f"{sentiment} DOMINANT CASCADES", fontsize=20, fontweight='bold', color=self.colors['SENTIMENTS'][sentiment], pad=15)
            ax.set_xlabel('STRUCTURAL VIRALITY', fontsize=18, fontweight='bold')
            
            if i == 0: ax.set_ylabel('% SENTIMENT', fontsize=18, fontweight='bold')
            ax.yaxis.set_major_formatter(mtick.PercentFormatter(decimals=0))
            
            ax.set_xlim(left=0, right=global_max_virality * 1.05)
            ax.set_ylim(bottom=-2, top=max(102, global_max_y * 1.05))

            # ==========================================
            # 1. AUMENTO DA FONTE DOS TICKS (EIXOS X e Y)
            # ==========================================
            ax.tick_params(axis='both', which='major', labelsize=16)

        from matplotlib.lines import Line2D # type: ignore
        custom_handles = [
            Line2D([0], [0], marker='o', color='w', markerfacecolor=current_colors[idx], 
                   markersize=16, label=cat) for idx, cat in enumerate(groups_list)
        ]
        
        axes[1].legend(handles=custom_handles, title=grouping.upper(), loc='upper center', 
                       bbox_to_anchor=(0.5, -0.15), ncol=len(groups_list), fontsize=16, 
                       title_fontsize=18, frameon=True, edgecolor='black')

        out_file = os.path.join(output_dir, "RQ3_Taxonomy_Trendlines.pdf")
        
        plt.tight_layout()
        
        # ==========================================
        # 2. APROXIMAÇÃO DOS GRÁFICOS (wspace)
        # ==========================================
        # wspace=0.05 reduz drasticamente a margem em branco horizontal entre os 3 painéis
        plt.subplots_adjust(bottom=0.2, wspace=0.05) 
        
        plt.savefig(out_file, dpi=300, bbox_inches='tight')
        plt.close()
        print(f"   -> Saved: RQ3_Taxonomy_Trendlines.pdf")

    def run_taxonomy_analysis(self, grouping="Categories", interactive_only=False):
        if grouping in ["Quartiles", "Sentiments"]:
            print(f"  [!] Aviso: Taxonomy Analysis agrega Subreddits e é baseada estritamente em Categorias. A forçar 'Categories'...")
            grouping = "Categories"

        print("[*] Generating BCC Taxonomy Trendline Plot...")
        Config.set_sns_theme()
        
        # O filtro interactive_only aqui precisa ser passado manual se formos criar a pasta certa
        df = self.df_cascades.copy()
        if interactive_only:
            df = df[df['Total_Motifs'] > 0].copy()
            
        _, _, _, output_dir = self._get_grouping_config(grouping, df, interactive_only)
        
        sub_stats = df.groupby(['Subreddit', 'Category']).agg(
            Median_Virality=('Structural_Virality', 'median'),
            Global_Toxicity=('Perc_Negative', 'mean')
        ).reset_index()
        
        fig, ax = plt.subplots(figsize=(16, 12))
        sns.set_style("white") 
        ax.grid(False)
        
        for cat in Config.CATEGORIES:
            df_cat = sub_stats[sub_stats['Category'] == cat]
            if df_cat.empty: continue
            ax.scatter(df_cat['Median_Virality'], df_cat['Global_Toxicity'], 
                       s=250, c=self.colors['CATEGORIES'][cat], label=cat, edgecolors='black', linewidth=1.2, zorder=3)
        
        sns.regplot(x=sub_stats['Median_Virality'], y=sub_stats['Global_Toxicity'], scatter=False, 
                    color='black', line_kws={'linestyle':'--', 'linewidth':2.5, 'zorder': 2}, ax=ax)
        
        slope, intercept, r_value, p_value, std_err = linregress(sub_stats['Median_Virality'], sub_stats['Global_Toxicity'])
        stats_text = f"Linear Regression\n$R^2 = {r_value**2:.4f}$\n$p = {p_value:.4e}$"
        ax.text(0.02, 0.98, stats_text, transform=ax.transAxes, fontsize=16, fontweight='bold',
                verticalalignment='top', bbox=dict(facecolor='#f9f9f9', alpha=0.9, edgecolor='black', boxstyle='round,pad=0.5', lw=1.5), zorder=6)

        texts = []
        for _, row in sub_stats.iterrows():
            pos_x, pos_y = row['Median_Virality'], row['Global_Toxicity']
            label_text = f"r/{row['Subreddit']}\n({pos_x:.2f}, {pos_y:.1f}%)"
            texts.append(ax.text(pos_x, pos_y, label_text, fontsize=14, fontweight='bold', zorder=4))
        
        if adjust_text:
            adjust_text(texts, expand_points=(1.5, 1.5), arrowprops=dict(arrowstyle='-', color='gray', lw=0.5, shrinkA=5, shrinkB=5))

        ax.legend(title="CATEGORIES", fontsize=14, loc='lower right', framealpha=0.9, edgecolor='black')
        ax.set_xlabel('MEDIAN STRUCTURAL VIRALITY', fontsize=18, fontweight='bold')
        ax.set_ylabel('CONFLICT INDEX (% NEGATIVE)', fontsize=18, fontweight='bold')
        
        out_file = os.path.join(output_dir, "BCC_Taxonomy_Trendline.pdf")
        plt.tight_layout()
        plt.savefig(out_file, dpi=300, bbox_inches='tight')
        plt.close()
        print(f"   -> Saved: BCC_Taxonomy_Trendline.pdf")

    def run_triadic_analysis(self, grouping="Categories", interactive_only=False):
        if grouping in ["Quartiles", "Sentiments"]:
            print(f"  [!] Aviso: Triadic Analysis é extraída estruturalmente por Categorias no Parser. A forçar 'Categories'...")
            grouping = "Categories"

        print("[*] Generating Normalized Triadic Heatmap (RQ2)...")
        
        # Interactive Only tem pouco impacto aqui já que a tríade por definição exige interação (avo->pai->filho)
        # Mas atualizamos a pasta
        _, _, _, output_dir = self._get_grouping_config(grouping, self.df_cascades, interactive_only)

        df_counts = pd.DataFrame(self.triad_counts).fillna(0)
        
        for t in Config.ORDERED_TRIADS:
            if t not in df_counts.index: df_counts.loc[t] = 0.0
                
        df_counts = df_counts.reindex(Config.ORDERED_TRIADS, columns=Config.CATEGORIES)
        Z = df_counts.to_numpy().sum()
        
        if Z == 0:
            print("  [-] Aviso: A soma de todas as tríades é zero. O Heatmap foi ignorado.")
            return

        df_normalized = (df_counts / Z) * 100
        df_normalized = df_normalized.fillna(0)

        fig, ax = plt.subplots(figsize=(10, 8))

        sns.heatmap(df_normalized, annot=True, fmt=".2f", cmap=self.colors['CMAP'], 
                    cbar_kws={'label': 'Global Concentration (A / Z) %'}, 
                    linewidths=1, ax=ax, annot_kws={'size': 16, 'weight': 'bold'})
        
        ax.set_ylabel("TRIADIC SENTIMENT RELATIONS (T1 → T2 → T3)", fontsize=16, fontweight='bold')
        ax.set_xlabel("SUBREDDIT CATEGORY", fontsize=16, fontweight='bold')
        plt.xticks(rotation=0)
        ax.set_yticklabels([f"{t}" for t in Config.ORDERED_TRIADS], rotation=0)

        out_file = os.path.join(output_dir, "RQ2_Triadic_Sentiment_Motifs.pdf")
        plt.tight_layout()
        plt.savefig(out_file, dpi=300)
        plt.close()
        print(f"   -> Saved: RQ2_Triadic_Sentiment_Motifs.pdf")

        print(f"[*] Generating RQ3: Taxonomy Cascades Trendlines by {grouping}...")

        Config.set_sns_theme()
        df = self._prepare_quartiles(interactive_only)
        
        group_col, groups_list, current_colors, output_dir = self._get_grouping_config(grouping, df, interactive_only)
        palette_dict = dict(zip(groups_list, current_colors))
        
        sentiments = ['POSITIVE', 'NEUTRAL', 'NEGATIVE']
        fig, axes = plt.subplots(1, 3, figsize=(24, 7), sharey=True)
        
        for i, sentiment in enumerate(sentiments):
            ax = axes[i]
            df_sub = df[df['Dominant_Sentiment'] == sentiment]
            if df_sub.empty: continue
            
            sns.scatterplot(data=df_sub, x='Structural_Virality', y='Perc_Negative', hue=group_col, palette=palette_dict,
                            alpha=0.5, s=30, edgecolor='none', ax=ax, zorder=2, rasterized=True, legend=False)
            
            sns.regplot(x=df_sub['Structural_Virality'], y=df_sub['Perc_Negative'], scatter=False, color='black', 
                        ci=95, line_kws={'linestyle':'-', 'linewidth':3.5, 'zorder': 4}, ax=ax)
            
            slope, intercept, r_value, p_value, std_err = linregress(df_sub['Structural_Virality'], df_sub['Perc_Negative'])
            stats_text = f"Trendline ($R^2={r_value**2:.3f}$)\n$p={p_value:.1e}$"
            ax.text(0.05, 0.95, stats_text, transform=ax.transAxes, fontsize=16, fontweight='bold',
                    verticalalignment='top', bbox=dict(facecolor='white', alpha=0.9, edgecolor='black', boxstyle='round,pad=0.5'))
            
            ax.set_title(f"{sentiment} DOMINANT CASCADES", fontsize=20, fontweight='bold', color=self.colors['SENTIMENTS'][sentiment], pad=15)
            ax.set_xlabel('STRUCTURAL VIRALITY', fontsize=18, fontweight='bold')
            if i == 0: ax.set_ylabel('NEGATIVE SENTIMENT (%)', fontsize=18, fontweight='bold')
            ax.yaxis.set_major_formatter(mtick.PercentFormatter(decimals=0))

        from matplotlib.lines import Line2D # type: ignore
        custom_handles = [
            Line2D([0], [0], marker='o', color='w', markerfacecolor=current_colors[idx], 
                   markersize=16, label=cat) for idx, cat in enumerate(groups_list)
        ]
        
        axes[1].legend(handles=custom_handles, title=grouping.upper(), loc='upper center', 
                       bbox_to_anchor=(0.5, -0.15), ncol=len(groups_list), fontsize=16, 
                       title_fontsize=18, frameon=True, edgecolor='black')

        out_file = os.path.join(output_dir, "RQ3_Taxonomy_Trendlines.pdf")
        
        plt.tight_layout()
        plt.subplots_adjust(bottom=0.2) 
        plt.savefig(out_file, dpi=300, bbox_inches='tight')
        plt.close()
        print(f"   -> Saved: RQ3_Taxonomy_Trendlines.pdf")

    def run_user_homophily_analysis(self, grouping="Categories", interactive_only=False):
        import matplotlib.pyplot as plt # type: ignore
        import seaborn as sns # type: ignore
        import pandas as pd
        from collections import defaultdict
        import os
        from Utilities import Config
        
        print("[*] Generating User Homophily Analysis (UQ1-UQ4)...")
        folder_suffix = "_Interactive_Cascades" if interactive_only else ""
        output_dir = os.path.join(Config.RESULTS_DIR, f"Homophily_Analysis{folder_suffix}")
        os.makedirs(output_dir, exist_ok=True)

        user_data = []
        for author, counts in self.user_sentiments.items():
            if counts['total'] > 0:
                user_data.append({'author': author, 'perc_negative': (counts['negative'] / counts['total']) * 100})
                
        df_users = pd.DataFrame(user_data)
        bins = [-1.0, 25.00, 50.00, 75.00, 100.00]
        df_users['user_type'] = pd.cut(df_users['perc_negative'], bins=bins, labels=['UQ1', 'UQ2', 'UQ3', 'UQ4'])
        user_type_map = dict(zip(df_users['author'], df_users['user_type']))

        # NOVO: Contagem total de usuários por UQ (Para a Legenda)
        uq_counts = df_users['user_type'].value_counts()

        edge_data, user_homophily_stats = [], defaultdict(lambda: {'s_i': 0, 'd_i': 0, 'type': None})
        for source, target in self.global_user_edges:
            s_type, t_type = user_type_map.get(source), user_type_map.get(target)
            if s_type and t_type:
                edge_data.append({'Source_Type': s_type, 'Target_Type': t_type})
                user_homophily_stats[source]['type'] = s_type
                if s_type == t_type: user_homophily_stats[source]['s_i'] += 1
                else: user_homophily_stats[source]['d_i'] += 1

        homophily_records = []
        for author, stats_dict in user_homophily_stats.items():
            s_i, d_i = stats_dict['s_i'], stats_dict['d_i']
            if (s_i + d_i) > 0:
                homophily_records.append({'author': author, 'user_type': stats_dict['type'], 'H_i': s_i / (s_i + d_i)})
                
        df_h, df_edges = pd.DataFrame(homophily_records), pd.DataFrame(edge_data)
        order = ['UQ1', 'UQ2', 'UQ3', 'UQ4']

        # PLOT 1: Barplot
        fig, ax = plt.subplots(figsize=(8, 6))
        ax.yaxis.grid(True, linestyle='--', alpha=0.7, zorder=0)
        ax.set_axisbelow(True)

        # CORREÇÃO: Cria um dicionário estrito para o Seaborn não embaralhar as cores
        strict_palette = dict(zip(order, self.colors['COLOR_SCHEME']))

        sns.barplot(data=df_h, x='user_type', y='H_i', hue='user_type', legend=False, 
                     order=order, errorbar='se', capsize=.1, 
                     palette=strict_palette, ax=ax, edgecolor='black', linewidth=1.5, zorder=3)

        # Legenda com N, Média e SD
        import matplotlib.patches as mpatches #type:ignore
        legend_handles = []
        for i, q in enumerate(order):
            mean_val = df_h[df_h['user_type'] == q]['H_i'].mean()
            std_val = df_h[df_h['user_type'] == q]['H_i'].std()
            count = uq_counts[q] # Puxa o total de usuários deste quartil
            
            color = self.colors['COLOR_SCHEME'][i]
            # Adiciona o N na string da legenda
            legend_handles.append(mpatches.Patch(color=color, label=f'{q} (N={count:,}): μ={mean_val:.2f} ± {std_val:.2f}'))
            
        ax.legend(handles=legend_handles, loc='upper right', fontsize=11, framealpha=0.9, edgecolor='black')
        ax.set_xlabel('USER TYPE (TOXICITY QUARTILES)', fontsize=14, fontweight='bold')
        ax.set_ylabel('HOMOPHILY INDEX ($H_i$)', fontsize=14, fontweight='bold')
        ax.set_ylim(0, 1)
        ax.tick_params(labelsize=14)
        sns.despine()
        plt.savefig(os.path.join(output_dir, "Homophily_Barplot_SE.pdf"), dpi=300, bbox_inches='tight')
        plt.close()

        # PLOT 2: Heatmap
        crosstab = pd.crosstab(df_edges['Source_Type'], df_edges['Target_Type'], normalize='index') * 100
        crosstab = crosstab.reindex(index=order, columns=order, fill_value=0)
        
        fig, ax = plt.subplots(figsize=(8, 6))
        sns.heatmap(crosstab, annot=True, fmt=".1f", cmap=self.colors['CMAP'], 
                    vmin=0, vmax=100, 
                    cbar_kws={'orientation': 'horizontal', 'location': 'top', 'label': '% of Replies', 'pad': 0.05}, 
                    annot_kws={'size': 14, 'weight': 'bold'}, ax=ax)
        
        ax.set_xlabel('REPLIED TO (TARGET TYPE)', fontsize=14, fontweight='bold')
        ax.set_ylabel('REPLIED BY (SOURCE TYPE)', fontsize=14, fontweight='bold')
        plt.yticks(rotation=0) 
        ax.tick_params(labelsize=14) 
        
        cbar = ax.collections[0].colorbar
        cbar.ax.tick_params(labelsize=12)
        cbar.set_label('% of Replies', size=14, weight='bold')
        
        plt.savefig(os.path.join(output_dir, "Homophily_Replies_Heatmap.pdf"), dpi=300, bbox_inches='tight')
        plt.close()
        print("   -> Saved: Homophily Analyses (Barplot and Heatmap)")

    def generate_cascade_diagram(self, *args, **kwargs):
        import networkx as nx
        import matplotlib.pyplot as plt # type: ignore
        import os
        from Utilities import Config
        
        output_dir = os.path.join(Config.RESULTS_DIR)
        os.makedirs(output_dir, exist_ok=True)
        
        G = nx.DiGraph()
        # Adicionando os nós (Post = 0, Respostas = 1 a 6)
        edges = [(0, 1), (0, 2), (1, 3), (1, 4), (2, 5), (4, 6)]
        G.add_edges_from(edges)
        
        # Layout hierárquico
        pos = {
            0: (0.5, 1.0),
            1: (0.3, 0.66), 2: (0.7, 0.66),
            3: (0.15, 0.33), 4: (0.45, 0.33), 5: (0.7, 0.33),
            6: (0.45, 0.0)
        }

        # Aumentei levemente a largura da figura para caber o texto lateral
        fig, ax = plt.subplots(figsize=(7, 5))
        
        # Desenhando as arestas (Replies)
        nx.draw_networkx_edges(G, pos, arrowstyle='-|>', arrowsize=20, width=2, edge_color='gray', ax=ax)
        
        # Cores dos nós para simular Sentimentos/Quartis
        # colors = ['#2c3e50', '#e74c3c', '#3498db', '#e74c3c', '#e74c3c', "#ccbc2e", '#e74c3c']
        
        # Índices alinhados de 0 a 6 para dar match com os nós do grafo
        labels = {0: "Root", 1: "Reply 1", 2: "Reply 5", 3: "Reply 2", 4: "Reply 3", 5: "Reply 6", 6: "Reply 4"}
        
        nx.draw_networkx_nodes(G, pos, node_size=2000, node_color='#3498db', edgecolors='black', ax=ax)
        nx.draw_networkx_labels(G, pos, labels=labels, font_size=10, font_color='white', font_weight='bold')
        
        # ==========================================
        # Indicadores de Profundidade (Eixo Y)
        # ==========================================
        # CORREÇÃO: Começando do Depth 1
        depths = {1: 1.0, 2: 0.66, 3: 0.33, 4: 0.0}
        for depth_level, y_coord in depths.items():
            # Escreve o texto da profundidade à esquerda
            ax.text(-0.05, y_coord, f"Depth {depth_level}", 
                    fontsize=12, fontweight='bold', color='#555555', 
                    verticalalignment='center', horizontalalignment='right')
            # Desenha uma linha guia tracejada
            ax.axhline(y=y_coord, color='gray', linestyle='--', alpha=0.3, xmin=0.15, xmax=0.95)
            
        # Ajusta os limites do eixo X para que o texto não fique cortado
        ax.set_xlim(-0.25, 0.95)
        
        plt.axis('off')
        plt.tight_layout()
        
        plt.savefig(os.path.join(output_dir,"Cascade_Graph_Example.pdf"), dpi=300, bbox_inches='tight')
        plt.close()
        print("   -> Salvo: Cascade_Graph_Example.pdf")
    
    def run_ablation_matrix_analysis(self, *args, **kwargs):
        import json
        import os
        import pandas as pd
        import matplotlib.pyplot as plt # type: ignore
        import seaborn as sns # type: ignore
        from sklearn.metrics import confusion_matrix
        from Utilities import Config

        print("[*] Running Multimodal vs. Blind Ablation Matrix Analysis...")
        output_dir = os.path.join(Config.RESULTS_DIR)
        os.makedirs(output_dir, exist_ok=True)

        blind_path = os.path.join(Config.BLIND_PATH) 
        multi_path = os.path.join(Config.MULTIMODAL_PATH) 

        def extract_label(data_dict):
            """Extracts the label from the specific 'ai_analysis' schema."""
            ai_data = data_dict.get("ai_analysis")
            if isinstance(ai_data, dict):
                label = ai_data.get("label")
                if label:
                    return label
                    
            # Fallback for flat structures
            for key in ['sentiment', 'label', 'roberta_sentiment', 'qwen_sentiment']:
                val = data_dict.get(key)
                if isinstance(val, dict):
                    return val.get('label', val.get('sentiment', None))
                if isinstance(val, str):
                    return val
            return None

        print("   -> Loading Blind Dataset into memory...")
        blind_data = {}
        with open(blind_path, 'r', encoding='utf-8') as f:
            for line in f:
                try:
                    obj = json.loads(line)
                    c_id = obj.get('id')
                    label = extract_label(obj)
                    if c_id and label:
                        blind_data[c_id] = label.upper()
                except Exception:
                    continue
                    
        print("   -> Loading Multimodal Dataset into memory...")
        multi_data = {}
        with open(multi_path, 'r', encoding='utf-8') as f:
            for line in f:
                try:
                    obj = json.loads(line)
                    c_id = obj.get('id')
                    label = extract_label(obj)
                    if c_id and label:
                        multi_data[c_id] = label.upper()
                except Exception:
                    continue

        # Intersect IDs (Fast O(N) lookup)
        common_ids = set(blind_data.keys()).intersection(set(multi_data.keys()))
        print(f"[*] Found {len(common_ids):,} intersecting records between both datasets.")

        if not common_ids:
            print("[!] Error: No matching IDs found between the two datasets. Check your file paths and JSON structure.")
            return

        y_blind = [blind_data[cid] for cid in common_ids]
        y_multi = [multi_data[cid] for cid in common_ids]
        
        labels_order = ["NEGATIVE", "NEUTRAL", "POSITIVE"]
        
        # 1. Confusion Matrix Calculation
        cm = confusion_matrix(y_multi, y_blind, labels=labels_order)
        df_cm = pd.DataFrame(cm, index=labels_order, columns=labels_order)

        # 2. Plot Heatmap
        fig, ax = plt.subplots(figsize=(8, 6))
        sns.heatmap(df_cm, annot=True, fmt='d', cmap='Blues', cbar=False,
                    annot_kws={"size": 18, "weight": "bold"}, ax=ax,
                    linewidths=1, linecolor='black')
        
        ax.set_ylabel('Ground Truth (Qwen3-VL + XLM-R)', fontsize=18, fontweight='bold')
        ax.set_xlabel('Predicted (Blind XLM-RoBERTa)', fontsize=18, fontweight='bold')
        ax.tick_params(labelsize=16)
        
        heatmap_path = os.path.join(output_dir, "Ablation_Confusion_Matrix.pdf")
        plt.savefig(heatmap_path, dpi=300, bbox_inches='tight')
        plt.close()
        
        # 3. Export LaTeX Table
        latex_table = f"""\\begin{{table}}[htbp]
    \\centering
    \\caption{{Confusion matrix comparing textual-only (Blind) vs. multimodal inference. The multimodal labels are considered the ground truth for this ablation study.}}
    \\label{{tab:ablation_confusion_matrix}}
    \\begin{{tabular}}{{l|ccc}}
        \\toprule
        & \\multicolumn{{3}}{{c}}{{\\textbf{{Blind Model Inference}}}} \\\\
        \\textbf{{Multimodal Ground Truth}} & \\textbf{{Negative}} & \\textbf{{Neutral}} & \\textbf{{Positive}} \\\\
        \\midrule
        \\textbf{{Negative}} & {df_cm.loc['NEGATIVE', 'NEGATIVE']:,} & {df_cm.loc['NEGATIVE', 'NEUTRAL']:,} & {df_cm.loc['NEGATIVE', 'POSITIVE']:,} \\\\
        \\textbf{{Neutral}}  & {df_cm.loc['NEUTRAL', 'NEGATIVE']:,} & {df_cm.loc['NEUTRAL', 'NEUTRAL']:,} & {df_cm.loc['NEUTRAL', 'POSITIVE']:,} \\\\
        \\textbf{{Positive}} & {df_cm.loc['POSITIVE', 'NEGATIVE']:,} & {df_cm.loc['POSITIVE', 'NEUTRAL']:,} & {df_cm.loc['POSITIVE', 'POSITIVE']:,} \\\\
        \\bottomrule
    \\end{{tabular}}
\\end{{table}}"""

        txt_path = os.path.join(output_dir, "Ablation_Matrix_LaTeX.txt")
        with open(txt_path, "w", encoding="utf-8") as f:
            f.write(latex_table)

        print(f"   -> Success! PDF and LaTeX table saved in: {output_dir}")



