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
        
        if os.path.exists(Config.CACHE_PATH) and os.path.exists(triads_cache_path):
            print(f"[*] Cache encontrado. A carregar DataFrame de {Config.CACHE_PATH}...")
            self.df_cascades = pd.read_parquet(Config.CACHE_PATH)
            
            with open(triads_cache_path, 'r', encoding='utf-8') as f:
                self.triad_counts = json.load(f)
                
            print("   -> DataFrame e Tríades carregados instantaneamente para a RAM.")
            return True
        else:
            print("[*] Cache não encontrado ou incompleto. A iniciar extração profunda dos grafos...")
            sucesso = self.extract_and_compute_all()
            if sucesso and self.df_cascades is not None and not self.df_cascades.empty:
                self.df_cascades.to_parquet(Config.CACHE_PATH, index=False)
                with open(triads_cache_path, 'w', encoding='utf-8') as f:
                    json.dump(self.triad_counts, f)
                print(f"   -> Cache guardado com sucesso (Parquet + JSON).")
            return sucesso

    def extract_and_compute_all(self):
        print("[*] Initiating Single-Pass Extraction (Unified Orchestrator)...")
        
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
                
                while queue:
                    curr, actual_parent = queue.pop(0)
                    G_struct.add_node(curr)
                    
                    nd = self.node_memory.get(curr, {})
                    ts = nd.get('timestamp')
                    author = nd.get('author', '[deleted]')
                    lbl = nd.get('label', 'UNKNOWN')
                    score = nd.get('metadata_score', 0)
                    
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
                            if author != parent_author:
                                G_users.add_edge(author, parent_author)
                    
                    for child_id in self.sub_children[sub].get(curr, []):
                        queue.append((child_id, curr))

                num_nodes = G_struct.number_of_nodes()
                if num_nodes < 3 or total_valid == 0: continue
                
                G_un = G_struct.to_undirected()
                virality = nx.wiener_index(G_un) / (num_nodes * (num_nodes - 1)) if num_nodes > 1 else 1.0
                
                try:
                    lengths = nx.single_source_shortest_path_length(G_struct, root_id)
                    max_depth = max(lengths.values()) if lengths else 1
                    level_counts = defaultdict(int)
                    for dist in lengths.values(): level_counts[dist] += 1
                    max_breadth = max(level_counts.values()) if level_counts else 1
                except:
                    max_depth = 1
                    max_breadth = 1
                    
                unique_users = max(len(authors), 1)
                average_score = np.mean(scores) if scores else 0.0
                
                dyads = sum(1 for u, v in G_users.edges() if not G_users.has_edge(v, u))
                mutual_dyads = sum(1 for u, v in G_users.edges() if G_users.has_edge(v, u)) / 2
                triads = nx.triadic_census(G_users) if G_users.number_of_nodes() >= 3 else defaultdict(int)
                
                motifs = {
                    'Dyad': dyads, 'Mutual Dyad': mutual_dyads, 'Chain': triads.get('021C', 0),
                    'Fan-In': triads.get('021D', 0), 'Fan-Out': triads.get('021U', 0),
                    'Triangle': triads.get('030T', 0), 'Recip. Triangle': triads.get('300', 0)
                }
                
                def dfs_paths(node, current_path):
                    current_path.append(node)
                    if len(current_path) >= 3:
                        s1 = self.node_memory.get(current_path[-3], {}).get('label')
                        s2 = self.node_memory.get(current_path[-2], {}).get('label')
                        s3 = self.node_memory.get(current_path[-1], {}).get('label')
                        if all(s in Config.VALID_SENTIMENTS for s in [s1, s2, s3]):
                            t_name = Config.TRIAD_MAPPING.get((s1, s2, s3))
                            if t_name: self.triad_counts[cat][t_name] += 1
                    for neighbor in G_struct.successors(node):
                        dfs_paths(neighbor, current_path.copy())
                        
                dfs_paths(root_id, [])
                
                duration_minutes = (np.sort(timestamps)[-1] - np.sort(timestamps)[0])/60.0 if len(timestamps) >= 2 else 0.0
                
                cascades_data.append({
                    'Cascade_ID': root_id, 'Subreddit': sub, 'Category': cat,
                    'Structural_Virality': virality, 'Cascade_Size': num_nodes,
                    'Duration_Minutes': duration_minutes,
                    'Max_Depth': max_depth,
                    'Max_Breadth': max_breadth,
                    'Unique_Users': unique_users,
                    'Average_Score': average_score,
                    'Perc_Negative': (sentiments['NEGATIVE'] / total_valid) * 100,
                    'Perc_Neutral': (sentiments['NEUTRAL'] / total_valid) * 100,
                    'Perc_Positive': (sentiments['POSITIVE'] / total_valid) * 100,
                    'Dominant_Sentiment': max(sentiments, key=sentiments.get),
                    'Total_Motifs': sum(motifs.values()),
                    **motifs
                })

        self.df_cascades = pd.DataFrame(cascades_data)
        print(f"   -> Phase 3: Extraction Complete. {len(self.df_cascades)} cascades loaded.")
        return True

    def _prepare_quartiles(self, interactive_only=False):
        df = self.df_cascades.copy()
        
        # Filtro de Reciprocidade (Ruído de difusão isolado)
        if interactive_only:
            df = df[df['Total_Motifs'] > 0].copy()
            
        neg_col = 'Perc_Negative' if 'Perc_Negative' in df.columns else 'perc_negative'
        try:
            df['neg_quartile'] = pd.qcut(df[neg_col], q=4, labels=['Q1', 'Q2', 'Q3', 'Q4'])
        except ValueError:
            df['neg_quartile'], bins = pd.qcut(df[neg_col], q=4, retbins=True, duplicates='drop')
            df['neg_quartile'] = pd.cut(df[neg_col], bins=bins, 
                                        labels=[f"Q{i+1}" for i in range(len(bins)-1)], 
                                        include_lowest=True)
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
            ('Structural_Virality', 'STRUCTURAL VIRALITY (WIENER)', 'Fig1_CCDF_Structural_Virality.pdf'),
            ('Max_Depth', 'MAX CASCADE DEPTH', 'Fig1_CCDF_Max_Depth.pdf'),
            ('Max_Breadth', 'MAX CASCADE BREADTH', 'Fig1_CCDF_Max_Breadth.pdf'),
            ('Unique_Users', 'UNIQUE PARTICIPATING USERS', 'Fig1_CCDF_Unique_Users.pdf'),
            ('Cascade_Size', 'TOTAL VOLUME OF MESSAGES', 'Fig1_CCDF_Number_Messages.pdf')
        ]
        
        for col, xlabel, filename in metrics:
            if col not in df.columns: continue
            fig, ax = plt.subplots(figsize=(10, 7))
            
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

            ax.set_xlabel(xlabel, fontsize=16, fontweight='bold')
            ax.set_ylabel('CCDF (% OF CASCADES)', fontsize=16, fontweight='bold')
            ax.yaxis.set_major_formatter(mtick.PercentFormatter(decimals=0))
            ax.legend(fontsize=12, loc='upper right', framealpha=0.9, edgecolor='black')
            ax.tick_params(labelsize=14)
            sns.despine()
            
            out_file = os.path.join(output_dir, filename)
            plt.savefig(out_file, dpi=300, bbox_inches='tight')
            plt.close()
            print(f"  -> Guardado: {filename}")

        trendlines = [
            ('Max_Depth', 'CASCADE DEPTH', 'Fig1_Trendline_Depth_vs_Time.pdf'),
            ('Unique_Users', 'UNIQUE PARTICIPATING USERS', 'Fig1_Trendline_Users_vs_Time.pdf')
        ]
        
        df_clean = df[df['Duration_Minutes'] > 0].copy()
        palette_dict = dict(zip(groups_list, current_colors))

        for x_col, xlabel, filename in trendlines:
            fig, ax = plt.subplots(figsize=(10, 7))
            
            sns.lineplot(data=df_clean, x=x_col, y='Duration_Minutes', hue=group_col,
                         palette=palette_dict, marker="o", markersize=8, 
                         linewidth=3.5, estimator='mean', errorbar=None, ax=ax)
            
            ax.set_xlabel(xlabel, fontsize=16, fontweight='bold')
            ax.set_ylabel('AVERAGE DURATION (MINUTES)', fontsize=16, fontweight='bold')
            ax.tick_params(labelsize=14)
            ax.legend(title=grouping.upper(), fontsize=12, loc='upper left', framealpha=0.9, edgecolor='black')
            sns.despine()
            
            out_file = os.path.join(output_dir, filename)
            plt.savefig(out_file, dpi=300, bbox_inches='tight')
            plt.close()
            print(f"  -> Guardado: {filename}")

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
        sns.heatmap(agg_mean, annot=labels, fmt="", cmap=self.colors['CMAP'], cbar=True, ax=ax, annot_kws={'size': 11, 'weight': 'bold'})
        ax.set_ylabel(grouping.upper(), fontsize=14, fontweight='bold')
        ax.set_xlabel("USER INTERACTION MOTIFS PROPORTION", fontsize=14, fontweight='bold')
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

        ax.set_xlabel('AVERAGE CASCADE SCORE (UPVOTES - DOWNVOTES)', fontsize=16, fontweight='bold')
        ax.set_ylabel('CCDF (% OF CASCADES)', fontsize=16, fontweight='bold')
        ax.yaxis.set_major_formatter(mtick.PercentFormatter(decimals=0))
        ax.legend(fontsize=12, loc='upper right', framealpha=0.9, edgecolor='black')
        ax.tick_params(labelsize=14)
        sns.despine()
        
        out_file = os.path.join(output_dir, "Fig3_CCDF_Average_Score.pdf")
        plt.tight_layout()
        plt.savefig(out_file, dpi=300, bbox_inches='tight')
        plt.close()
        print(f"   -> Saved: Fig3_CCDF_Average_Score.pdf")

    # =========================================================
    # OUTRAS ANÁLISES (RQ2, RQ3, Stats, Taxonomy)
    # =========================================================
    def run_statistical_reports(self, grouping="Categories", interactive_only=False):
        print(f"[*] Exporting Rigorous Statistical Report PDF by {grouping}...")
        df = self._prepare_quartiles(interactive_only)
        
        group_col, groups_list, _, output_dir = self._get_grouping_config(grouping, df, interactive_only)

        stats_results = []
        metrics = ['Structural_Virality', 'Max_Depth', 'Max_Breadth', 'Unique_Users', 'Cascade_Size', 'Duration_Minutes', 'Average_Score']
        
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

        df_stats = pd.DataFrame(stats_results)
        fig, ax = plt.subplots(figsize=(14, 4))
        ax.axis('off')
        
        table = ax.table(cellText=df_stats.values, colLabels=df_stats.columns, 
                         cellLoc='center', loc='center', colColours=['#f2f2f2']*len(df_stats.columns))
        table.auto_set_font_size(False)
        table.set_fontsize(10)
        table.scale(1.2, 1.8)
        
        plt.title(f"Statistical Validation Report ({grouping})", 
                  fontsize=14, fontweight='bold', pad=20)
        
        out_file = os.path.join(output_dir, "Statistical_Report_Summary.pdf")
        plt.savefig(out_file, dpi=300, bbox_inches='tight')
        plt.close()
        print(f"   -> Guardado: Statistical_Report_Summary.pdf")

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
        ax.text(0.02, 0.98, stats_text, transform=ax.transAxes, fontsize=14, fontweight='bold',
                verticalalignment='top', bbox=dict(facecolor='#f9f9f9', alpha=0.9, edgecolor='black', boxstyle='round,pad=0.5', lw=1.5), zorder=6)

        texts = []
        for _, row in sub_stats.iterrows():
            pos_x, pos_y = row['Median_Virality'], row['Global_Toxicity']
            label_text = f"r/{row['Subreddit']}\n({pos_x:.2f}, {pos_y:.1f}%)"
            texts.append(ax.text(pos_x, pos_y, label_text, fontsize=12, fontweight='bold', zorder=4))
        
        if adjust_text:
            adjust_text(texts, expand_points=(1.5, 1.5), arrowprops=dict(arrowstyle='-', color='gray', lw=0.5, shrinkA=5, shrinkB=5))

        ax.legend(title="CATEGORIES", fontsize=12, loc='lower right', framealpha=0.9, edgecolor='black')
        ax.set_xlabel('MEDIAN STRUCTURAL VIRALITY', fontsize=16, fontweight='bold')
        ax.set_ylabel('CONFLICT INDEX (% NEGATIVE)', fontsize=16, fontweight='bold')
        
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
                    linewidths=1, ax=ax, annot_kws={'size': 14, 'weight': 'bold'})
        
        ax.set_ylabel("TRIADIC SENTIMENT RELATIONS (T1 → T2 → T3)", fontsize=14, fontweight='bold')
        ax.set_xlabel("SUBREDDIT CATEGORY", fontsize=14, fontweight='bold')
        plt.xticks(rotation=0)
        ax.set_yticklabels([f"{t}" for t in Config.ORDERED_TRIADS], rotation=0)

        out_file = os.path.join(output_dir, "RQ2_Triadic_Sentiment_Motifs.pdf")
        plt.tight_layout()
        plt.savefig(out_file, dpi=300)
        plt.close()
        print(f"   -> Saved: RQ2_Triadic_Sentiment_Motifs.pdf")

    def run_rq3_analysis(self, grouping="Categories", interactive_only=False):
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
            ax.text(0.05, 0.95, stats_text, transform=ax.transAxes, fontsize=14, fontweight='bold',
                    verticalalignment='top', bbox=dict(facecolor='white', alpha=0.9, edgecolor='black', boxstyle='round,pad=0.5'))
            
            ax.set_title(f"{sentiment} DOMINANT CASCADES", fontsize=18, fontweight='bold', color=self.colors['SENTIMENTS'][sentiment], pad=15)
            ax.set_xlabel('STRUCTURAL VIRALITY', fontsize=16, fontweight='bold')
            if i == 0: ax.set_ylabel('TOXICITY (%)', fontsize=16, fontweight='bold')
            ax.yaxis.set_major_formatter(mtick.PercentFormatter(decimals=0))

        from matplotlib.lines import Line2D # type: ignore
        custom_handles = [
            Line2D([0], [0], marker='o', color='w', markerfacecolor=current_colors[idx], 
                   markersize=14, label=cat) for idx, cat in enumerate(groups_list)
        ]
        
        axes[1].legend(handles=custom_handles, title=grouping.upper(), loc='upper center', 
                       bbox_to_anchor=(0.5, -0.15), ncol=len(groups_list), fontsize=14, 
                       title_fontsize=16, frameon=True, edgecolor='black')

        out_file = os.path.join(output_dir, "RQ3_Taxonomy_Trendlines.pdf")
        
        plt.tight_layout()
        plt.subplots_adjust(bottom=0.2) 
        plt.savefig(out_file, dpi=300, bbox_inches='tight')
        plt.close()
        print(f"   -> Saved: RQ3_Taxonomy_Trendlines.pdf")