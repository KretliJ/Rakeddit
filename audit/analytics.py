import json
import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt # type: ignore
import matplotlib.patheffects as path_effects # type: ignore
import matplotlib.ticker as mtick # type: ignore
import matplotlib.patheffects as pe # type: ignore
import seaborn as sns # type: ignore
from collections import defaultdict, Counter
from scipy.stats import kruskal, sem
import scikit_posthocs as sp # type: ignore
from scipy.stats import kruskal, sem, linregress

try:
    from adjustText import adjust_text # type: ignore
except ImportError:
    adjust_text = None
    print("[!] Warning: 'adjustText' not installed. Labels might overlap. (pip install adjustText)")

class RakedditAnalyticsOrchestrator:
    def __init__(self):
        # Base Paths
        self.MULTIMODAL_PATH = "DATA/4-inferred/INFERRED_MULTIMODAL_FINAL.jsonl"
        
        # Output Directories
        self.RESULTS_DIR = "results/first_wave"
        os.makedirs(self.RESULTS_DIR, exist_ok=True)
        
        # Constants & Binning
        self.VALID_SENTIMENTS = {'POSITIVE', 'NEGATIVE', 'NEUTRAL'}
        # FIX: Linha reinserida para alimentar o gráfico de Moderation Friction
        self.MODERATION_LABELS = {'REMOVED_BY_MOD', 'USER_DELETED', 'AUTOMOD_WARNING'}
        
        self.CATEGORIES = ['PUBLIC ARENAS', 'HUMOR', 'SOCIOCULTURAL', 'HOBBIES']
        self.CATEGORY_MAP = {
            'brasil': 'PUBLIC ARENAS', 'brasilivre': 'PUBLIC ARENAS', 'brasildob': 'PUBLIC ARENAS', 'debatesbr': 'PUBLIC ARENAS', 'noticiasbr': 'PUBLIC ARENAS',
            'botecodoreddit': 'HUMOR', 'farialimabets': 'HUMOR', 'memesbr': 'HUMOR', 'shitpostbr': 'HUMOR',
            'antitrampo': 'SOCIOCULTURAL', 'opiniaoburra': 'SOCIOCULTURAL', 'opiniaoimpopular': 'SOCIOCULTURAL', 'filosofiabar': 'SOCIOCULTURAL', 'infernosocial': 'SOCIOCULTURAL',
            'futebol': 'HOBBIES', 'gamesecultura': 'HOBBIES', 'videogamesbrasil': 'HOBBIES', 'carros': 'HOBBIES', 'computadores': 'HOBBIES', 'saopaulo': 'HOBBIES'
        }
        
        # In-Memory Data
        self.df_features = None
        self.x_mid = 0.0
        self.y_mid = 0.0
        
        # Distributions and Streaks
        self.confidences = {cat: {s: [] for s in self.VALID_SENTIMENTS} for cat in self.CATEGORIES}
        self.max_streaks = {s: 0 for s in self.VALID_SENTIMENTS}
        self.node_memory = {}

    # =========================================================================
    # I. EXTRACTION & MAPPING
    # =========================================================================
    def extract_and_assign_taxonomy(self):
        if not os.path.exists(self.MULTIMODAL_PATH):
            print(f"[-] Error: Dataset not found at {self.MULTIMODAL_PATH}")
            return False

        print(f"\n[*] Building Feature Matrix and computing streaks in RAM...")
        
        sub_volume = Counter()
        sub_children = defaultdict(lambda: defaultdict(list))
        sub_roots = defaultdict(list)
        
        # Step A: Load Nodes
        with open(self.MULTIMODAL_PATH, 'r', encoding='utf-8') as f:
            for line in f:
                try:
                    record = json.loads(line)
                    if record.get('type') == 'metadata_footer': continue
                    sub_raw = record.get('subreddit')
                    if not sub_raw: continue
                    sub = sub_raw.lower()
                    if sub not in self.CATEGORY_MAP: continue
                    
                    sub_volume[sub] += 1
                    n_id = record['id']
                    p_id = record.get('parent_id')
                    depth = record.get('depth', 0)
                    ai_data = record.get('ai_analysis', {})
                    label = ai_data.get('label', 'UNKNOWN')
                    conf = ai_data.get('confidence', 0.0)
                    
                    cat = self.CATEGORY_MAP[sub]
                    if label in self.VALID_SENTIMENTS:
                        self.confidences[cat][label].append(conf)
                    
                    self.node_memory[n_id] = {'p_id': p_id, 'label': label, 'sub': sub, 'streak': 1}
                    
                    if p_id: sub_children[sub][p_id].append(n_id)
                    if depth == 1: sub_roots[sub].append(n_id)
                except: continue

        # Step B: Compute DFS for Structural Virality and Maximum Streaks
        features = []
        for sub in sub_volume.keys():
            children_map = sub_children[sub]
            total_vol = sub_volume[sub]
            
            total_valid = 0
            total_negative = 0
            cascades_virality = []
            
            for root_id in sub_roots[sub]:
                queue = [root_id]
                edges = []
                num_nodes = 0
                
                while queue:
                    curr = queue.pop(0)
                    num_nodes += 1
                    curr_node = self.node_memory[curr]
                    lbl = curr_node['label']
                    
                    if lbl in self.VALID_SENTIMENTS:
                        total_valid += 1
                        if lbl == 'NEGATIVE': total_negative += 1
                        
                        if curr_node['streak'] > self.max_streaks[lbl]:
                            self.max_streaks[lbl] = curr_node['streak']
                    
                    for child_id in children_map.get(curr, []):
                        edges.append((curr, child_id))
                        c_node = self.node_memory[child_id]
                        if c_node['label'] == lbl and lbl in self.VALID_SENTIMENTS:
                            c_node['streak'] = curr_node['streak'] + 1
                        queue.append(child_id)
                        
                if num_nodes >= 5: 
                    adj = defaultdict(list)
                    for u, v in edges:
                        adj[u].append(v); adj[v].append(u)
                    start_node = next(iter(adj.keys()))
                    bfs_order, q = [], [start_node]
                    parent_map = {start_node: None}
                    while q:
                        curr = q.pop(0)
                        bfs_order.append(curr)
                        for neighbor in adj[curr]:
                            if neighbor != parent_map[curr]:
                                parent_map[neighbor] = curr
                                q.append(neighbor)
                    subtree_size = {}
                    total_paths = 0
                    for node in reversed(bfs_order):
                        size = 1
                        for neighbor in adj[node]:
                            if neighbor != parent_map[node]: size += subtree_size[neighbor]
                        subtree_size[node] = size
                        if parent_map[node] is not None:
                            total_paths += (size * (num_nodes - size))
                    virality = total_paths / ((num_nodes * (num_nodes - 1)) / 2)
                    cascades_virality.append(virality)

            median_virality = pd.Series(cascades_virality).quantile(0.90) if cascades_virality else 0
            global_tox = (total_negative / total_valid) if total_valid > 0 else 0

            features.append({
                'Subreddit': sub,
                'Category': self.CATEGORY_MAP[sub],
                'Total_Volume': total_vol,
                'Median_Virality': median_virality,
                'Global_Toxicity': global_tox
            })

        self.df_features = pd.DataFrame(features)
        self.x_mid = self.df_features['Median_Virality'].median()
        self.y_mid = (self.df_features['Global_Toxicity'] * 100).median()
        
        print(f"[+] Extraction complete. Max streak found -> POS: {self.max_streaks['POSITIVE']} | NEU: {self.max_streaks['NEUTRAL']} | NEG: {self.max_streaks['NEGATIVE']}")
        return True

    # =========================================================================
    # II & IV. TABLES (Database Summary and Means with SE)
    # =========================================================================
    def generate_tables(self):
        print("\n[*] Generating Tables I and IV...")
        
        sub_msgs = defaultdict(int)
        sub_users = defaultdict(set)
        
        with open(self.MULTIMODAL_PATH, 'r', encoding='utf-8') as f:
            for line in f:
                try:
                    rec = json.loads(line)
                    sub_raw = rec.get('subreddit')
                    if not sub_raw: continue
                    sub = sub_raw.lower()
                    if sub not in self.CATEGORY_MAP: continue
                    sub_msgs[sub] += 1
                    author = rec.get('author')
                    if author and author != '[deleted]': sub_users[sub].add(author)
                except: continue

        rows_base = []
        for cat in self.CATEGORIES:
            subs_in_cat = [s for s, c in self.CATEGORY_MAP.items() if c == cat]
            total_msgs = sum(sub_msgs[s] for s in subs_in_cat)
            total_users = set().union(*(sub_users[s] for s in subs_in_cat))
            rows_base.append([cat, len(subs_in_cat), f"{total_msgs:,}", f"{len(total_users):,}"])

        stats = self.df_features.groupby('Category').agg(
            Tox_mean=('Global_Toxicity', lambda x: np.mean(x)*100),
            Tox_se=('Global_Toxicity', lambda x: sem(x)*100),
            Vir_mean=('Median_Virality', 'mean'),
            Vir_se=('Median_Virality', sem)
        ).reset_index()
        
        rows_means = []
        for _, r in stats.iterrows():
            rows_means.append([
                r['Category'], 
                f"{r['Tox_mean']:.2f}% ± {r['Tox_se']:.2f}", 
                f"{r['Vir_mean']:.2f} ± {r['Vir_se']:.2f}"
            ])

        def render_table(data, cols, filename, col_widths):
            fig, ax = plt.subplots(figsize=(10, 3))
            ax.axis('tight')
            ax.axis('off')
            table = ax.table(cellText=data, colLabels=cols, cellLoc='center', loc='center', colWidths=col_widths)
            table.auto_set_font_size(False)
            table.set_fontsize(12)
            table.scale(1.2, 2.0)
            for (i, j), cell in table.get_celld().items():
                if i == 0: 
                    cell.set_text_props(weight='bold')
                    cell.set_facecolor('#f0f0f0')
            plt.savefig(os.path.join(self.RESULTS_DIR, filename), dpi=300, bbox_inches='tight')
            plt.close()

        render_table(rows_base, ['Category', '# Subreddits', '# Messages', '# Unique Users'], "Tab_II_Database_Summary.png", [0.3, 0.2, 0.25, 0.25])
        render_table(rows_means, ['Category', 'Mean Negativity (± SE)', 'Mean Virality (± SE)'], "Tab_IV_Category_Means.png", [0.3, 0.35, 0.35])
        print(f"[SUCCESS] Tables saved in {self.RESULTS_DIR}/")

    # =========================================================================
    # III. TAXONOMY PLOT (Magma, Resultados Centralizados, Estrelas Corrigidas)
    # =========================================================================
    def plot_bcc_taxonomy(self):
        print("\n[*] Generating Taxonomy Plot (Manual Offsets & Path Effects)...")
        out_file = os.path.join(self.RESULTS_DIR, "BCC_Taxonomy_Plot.pdf")
        
        x = self.df_features['Median_Virality']
        y = self.df_features['Global_Toxicity'] * 100 

        # DEBUG: Imprime no terminal quantos e quais subreddits estão no DataFrame do plot
        print(f"\n[DEBUG] Total de subreddits encontrados para o plot: {len(self.df_features)}")
        print(f"[DEBUG] Subreddits na lista: {self.df_features['Subreddit'].tolist()}")
        
        fig, ax = plt.subplots(figsize=(16, 12))
        sns.set_style("white") 
        ax.grid(False)
        ax.set_title('') 
        
        main_scatter = ax.scatter(x, y, s=175, c='gray', alpha=0.5, edgecolors='black', linewidth=1.2, zorder=3)
        
        centroids = self.df_features.groupby('Category')[['Median_Virality', 'Global_Toxicity']].mean()
        
        magma_hex = sns.color_palette("magma", 4).as_hex()
        colors_cat = {
            'PUBLIC ARENAS': magma_hex[0], 
            'HUMOR': magma_hex[1], 
            'SOCIOCULTURAL': magma_hex[2], 
            'HOBBIES': magma_hex[3]
        }
        
        star_handles = []
        for cat, c_row in centroids.iterrows():
            cx, cy = c_row['Median_Virality'], c_row['Global_Toxicity'] * 100
            # Estrela com zorder baixo para ficar atrás dos textos
            star = ax.scatter(cx, cy, marker='*', s=800, c=colors_cat[cat], edgecolors='black', zorder=2, label=cat)
            star_handles.append(star)

        ax.axvline(self.x_mid, color='black', linestyle='--', alpha=0.8, zorder=1)
        ax.axhline(self.y_mid, color='black', linestyle='--', alpha=0.8, zorder=1)

        # ------------------------------
        # DICIONÁRIO DE OFFSETS MANUAIS 
        # ------------------------------
        manual_offsets = {
            'saopaulo': (-80, -50),
            'opiniaoimpopular': (-10, 30),
            'futebol': (-50, -50),
            'memesbr': (-40, -50),
        }
        
        display_names = {
            'brasildob': 'BrasildoB',
            'debatesbr': 'DebatesBr',
            'noticiasbr': 'NoticiasBR',
            'opiniaoburra': 'OpiniaoBurra',
            'filosofiabar': 'FilosofiaBAR',
            'shitpostbr': 'ShitpostBR',
            'memesbr': 'MemesBR',
            'farialimabets': 'FariaLimaBets',
            'botecodoreddit': 'BotecoDoReddit',
            'opiniaoimpopular': 'OpiniaoImpopular',
            'infernosocial': 'InfernoSocial',
            'gamesecultura': 'GamesECultura',
            'videogamesbrasil': 'VideoGamesBrasil'
        }

        texts = []         # Textos que o adjust_text vai tentar arrumar
        manual_texts = []  # Textos que nós fixamos (vão servir de obstáculo)
        
        for _, row in self.df_features.iterrows():
            pos_x, pos_y = row['Median_Virality'], row['Global_Toxicity'] * 100
            sub_name = row['Subreddit']
            
            # Pega o nome bonito e capitalizado para o gráfico
            display_name = display_names.get(sub_name, sub_name)
            label_text = f"r/{display_name}\n({pos_x:.2f}, {pos_y:.1f}%)"
            
            # SE O SUBREDDIT ESTIVER NO NOSSO DICIONÁRIO MANUAL:
            if sub_name in manual_offsets:
                offset_x, offset_y = manual_offsets[sub_name]
                t = ax.annotate(
                    label_text, 
                    xy=(pos_x, pos_y),              
                    xytext=(offset_x, offset_y),    
                    textcoords='offset points',     
                    fontsize=16, fontweight='bold', color='black', zorder=10,
                    arrowprops=dict(arrowstyle='-', color='#666666', lw=1.5, alpha=0.7, shrinkA=2, shrinkB=2)
                )
                t.set_path_effects([pe.withStroke(linewidth=4, foreground='white')])
                manual_texts.append(t)
            
            # O RESTO VAI NO AUTOMÁTICO:
            else:
                t = ax.text(pos_x, pos_y, label_text, fontsize=16, fontweight='bold', color='black', zorder=10)
                t.set_path_effects([pe.withStroke(linewidth=4, foreground='white')])
                texts.append(t)
        
        if adjust_text:
            # Lista de obstáculos = Estrelas + Os textos desenhados à mão
            obstacles = star_handles.copy() + manual_texts
                
            adjust_text(
                texts,
                add_objects=obstacles,          
                expand_points=(1.2, 1.2),         
                expand_text=(1.1, 1.1),
                expand_objects=(1.5, 1.5),         
                arrowprops=dict(
                    arrowstyle='-',
                    color='#666666',
                    lw=1.5,
                    alpha=0.7,
                    shrinkA=2,
                    shrinkB=2
                )
            )

        # Legenda no topo
        ax.legend(handles=star_handles, title="CATEGORIES (CENTROIDS)", fontsize=14, title_fontsize=15, 
                  loc='lower center', bbox_to_anchor=(0.5, 1.02), ncol=4, framealpha=1.0, edgecolor='black')

        x_min, x_max = ax.get_xlim()
        y_min, y_max = ax.get_ylim()
        
        q_font = {'fontsize': 45, 'fontweight': 'bold', 'alpha': 0.1, 'color': 'black'}
        ax.text(x_max, y_max, 'Q1', ha='right', va='top', **q_font)
        ax.text(x_min, y_max, 'Q2', ha='left', va='top', **q_font)
        ax.text(x_min, y_min, 'Q3', ha='left', va='bottom', **q_font)
        ax.text(x_max, y_min, 'Q4', ha='right', va='bottom', **q_font)

        ax.set_xticks([x_min, self.x_mid, x_max])
        ax.set_yticks([y_min, self.y_mid, y_max])
        ax.xaxis.set_major_formatter(mtick.FormatStrFormatter('%.2f'))
        ax.yaxis.set_major_formatter(mtick.PercentFormatter(decimals=1))
        
        ax.set_xlabel('MEDIAN STRUCTURAL VIRALITY', fontsize=16, fontweight='bold')
        ax.set_ylabel('CONFLICT INDEX (% NEGATIVE)', fontsize=16, fontweight='bold')
        ax.tick_params(labelsize=14)

        plt.tight_layout()
        plt.savefig(out_file, dpi=300, bbox_inches='tight')
        plt.close()
        print(f"[SUCCESS] Plot saved in '{out_file}'")

    # =========================================================================
    # III-B. TAXONOMY PLOT WITH TRENDLINE (Subreddits Only)
    # =========================================================================
    def plot_bcc_taxonomy_trendline(self):
        print("\n[*] Generating Taxonomy Plot (Subreddits + Trendline)...")
        out_file = os.path.join(self.RESULTS_DIR, "BCC_Taxonomy_Trendline.pdf")
        
        x = self.df_features['Median_Virality']
        y = self.df_features['Global_Toxicity'] * 100 
        
        fig, ax = plt.subplots(figsize=(16, 12))
        sns.set_style("white") 
        ax.grid(False)
        ax.set_title('') 
        
        magma_hex = sns.color_palette("magma", 4).as_hex()
        colors_cat = {
            'PUBLIC ARENAS': magma_hex[0], 
            'HUMOR': magma_hex[1], 
            'SOCIOCULTURAL': magma_hex[2], 
            'HOBBIES': magma_hex[3]
        }
        
        # Desenha os Subreddits coloridos por categoria
        for cat in self.CATEGORIES:
            df_cat = self.df_features[self.df_features['Category'] == cat]
            if df_cat.empty: continue
            ax.scatter(df_cat['Median_Virality'], df_cat['Global_Toxicity'] * 100, 
                       s=250, c=colors_cat[cat], label=cat, edgecolors='black', linewidth=1.2, zorder=3)
        
        # Traça a Trendline (Regressão Linear)
        sns.regplot(x=x, y=y, scatter=False, color='black', line_kws={'linestyle':'--', 'linewidth':2.5, 'alpha':0.8, 'zorder': 2}, ax=ax)
        
        # Calcula as estatísticas da Trendline
        slope, intercept, r_value, p_value, std_err = linregress(x, y)
        r_squared = r_value**2
        
        # Caixa flutuante com a estatística da Regressão
        stats_text = f"Linear Regression\n$R^2 = {r_squared:.4f}$\n$p = {p_value:.4e}$"
        ax.text(0.02, 0.98, stats_text, transform=ax.transAxes, fontsize=14, fontweight='bold',
                verticalalignment='top', bbox=dict(facecolor='#f9f9f9', alpha=0.9, edgecolor='black', boxstyle='round,pad=0.5', lw=1.5), zorder=6)

        texts = []
        for _, row in self.df_features.iterrows():
            pos_x, pos_y = row['Median_Virality'], row['Global_Toxicity'] * 100
            label_text = f"r/{row['Subreddit']}\n({pos_x:.2f}, {pos_y:.1f}%)"
            texts.append(ax.text(pos_x, pos_y, label_text, fontsize=12, fontweight='bold', color='black', zorder=4))
        
        if adjust_text:
            adjust_text(
                texts, 
                expand_points=(1.5, 1.5),
                arrowprops=dict(arrowstyle='-', color='gray', lw=0.5, alpha=0.6, shrinkA=8, shrinkB=5)
            )

        ax.legend(title="CATEGORIES", fontsize=12, title_fontsize=13, loc='lower right', framealpha=0.9, edgecolor='black')

        ax.xaxis.set_major_formatter(mtick.FormatStrFormatter('%.2f'))
        ax.yaxis.set_major_formatter(mtick.PercentFormatter(decimals=1))
        
        ax.set_xlabel('MEDIAN STRUCTURAL VIRALITY', fontsize=16, fontweight='bold')
        ax.set_ylabel('CONFLICT INDEX (% NEGATIVE)', fontsize=16, fontweight='bold')
        ax.tick_params(labelsize=14)

        plt.tight_layout()
        plt.savefig(out_file, dpi=300, bbox_inches='tight')
        plt.close()
        print(f"[SUCCESS] Trendline Plot saved in '{out_file}'")
    
    # =========================================================================
    # V. VALIDAÇÃO DO MODELO DE NLP (Centralizado em RESULTS_DIR)
    # =========================================================================
    def plot_nlp_validation_ccdf(self):
        print("\n[*] Generating NLP Validation (Confidence CCDF)...")
        out_file = os.path.join(self.RESULTS_DIR, "NLP-Validation-CCDF.png") # Caminho atualizado
        
        sns.set_theme(style="ticks")
        fig, axes = plt.subplots(1, 3, figsize=(22, 7), sharey=True)
        fig.suptitle('') 
        
        linestyles = ['-', '--', '-.', ':']
        handles, labels = [], []
        
        for i, sentiment in enumerate(['POSITIVE', 'NEUTRAL', 'NEGATIVE']):
            ax = axes[i]
            ax.set_title(sentiment, fontsize=18, fontweight='bold', pad=25)
            
            for j, cat in enumerate(self.CATEGORIES):
                data = np.array(self.confidences[cat][sentiment])
                if len(data) == 0: continue
                
                sorted_data = np.sort(data)
                y = 1.0 - np.arange(len(sorted_data)) / len(sorted_data)
                sorted_data = np.append(sorted_data, sorted_data[-1])
                y = np.append(y, 0)
                
                line, = ax.step(sorted_data, y, where='post', linestyle=linestyles[j], linewidth=3)
                
                if i == 0: 
                    handles.append(line)
                    labels.append(f"{cat}")

            ax.set_xlim(0, 1)
            ax.set_ylim(0, 1)
            ax.set_xlabel('AI CONFIDENCE SCORE [0 - 1]', fontsize=14, fontweight='bold')
            if i == 0: ax.set_ylabel('PROBABILITY P(X >= x)', fontsize=14, fontweight='bold')
            ax.tick_params(labelsize=12)

        fig.legend(handles, labels, loc='upper center', bbox_to_anchor=(0.5, 1.05), ncol=4, fontsize=14, frameon=False)

        sns.despine(fig)
        plt.tight_layout()
        plt.subplots_adjust(top=0.85) 
        plt.savefig(out_file, dpi=300, bbox_inches='tight')
        plt.close()
        print(f"[SUCCESS] NLP Validation saved in '{out_file}'")

    # =========================================================================
    # VI. DESCOBERTA COMPORTAMENTAL ESTENDIDA (8 CURVAS INDIVIDUAIS)
    # =========================================================================
    def plot_behavioral_ccdf(self):
        print("\n[*] A processar Cascatas Individuais para Subfigures (8 arquivos separados)...")
        
        # Estruturas para rastrear métricas a nível de CASCATA (Root_ID)
        cascade_metrics = {cat: defaultdict(list) for cat in self.CATEGORIES}
        
        sub_children = defaultdict(lambda: defaultdict(list))
        sub_roots = defaultdict(list)

        # 1. Reconstrói a topologia a partir da memória
        for n_id, data in self.node_memory.items():
            sub = data['sub']
            p_id = data['p_id']
            if p_id: sub_children[sub][p_id].append(n_id)
            if data.get('p_id') is None or data.get('depth') == 1: 
                sub_roots[sub].append(n_id)

        # 2. Analisa cada árvore de discussão individualmente
        for sub, cat in self.CATEGORY_MAP.items():
            if sub not in sub_roots: continue
            children_map = sub_children[sub]
            
            for root_id in sub_roots[sub]:
                queue = [root_id]
                c_total, c_valid, c_pos, c_neu, c_neg, c_mod = 0, 0, 0, 0, 0, 0
                c_shifts, c_links, c_max_streak = 0, 0, 0
                edges = []
                
                while queue:
                    curr = queue.pop(0)
                    c_total += 1
                    
                    if curr in self.node_memory:
                        c_data = self.node_memory[curr]
                        lbl = c_data['label']
                        p_id = c_data['p_id']
                        streak = c_data.get('streak', 0)
                        
                        if streak > c_max_streak: c_max_streak = streak
                        
                        if lbl in self.MODERATION_LABELS: c_mod += 1
                        if lbl in self.VALID_SENTIMENTS:
                            c_valid += 1
                            if lbl == 'POSITIVE': c_pos += 1
                            elif lbl == 'NEUTRAL': c_neu += 1
                            elif lbl == 'NEGATIVE': c_neg += 1
                            
                            if p_id and p_id in self.node_memory:
                                p_lbl = self.node_memory[p_id]['label']
                                if p_lbl in self.VALID_SENTIMENTS:
                                    c_links += 1
                                    if p_lbl != lbl: c_shifts += 1

                    for child_id in children_map.get(curr, []):
                        edges.append((curr, child_id))
                        queue.append(child_id)

                if c_total >= 3:
                    cascade_metrics[cat]['Max_Streak'].append(c_max_streak)
                    cascade_metrics[cat]['Mod_Rate'].append(c_mod / c_total)
                    if c_valid > 0:
                        cascade_metrics[cat]['Global_Toxicity'].append(c_neg / c_valid)
                        cascade_metrics[cat]['Positive_Ratio'].append(c_pos / c_valid)
                        cascade_metrics[cat]['Neutral_Ratio'].append(c_neu / c_valid)
                    if c_links > 0:
                        cascade_metrics[cat]['Sentiment_Friction'].append(c_shifts / c_links)
                    if (c_pos + c_neg) > 0:
                        cascade_metrics[cat]['Pos_Dominance'].append(c_pos / (c_pos + c_neg))

                    if c_total >= 5:
                        adj = defaultdict(list)
                        for u, v in edges: adj[u].append(v); adj[v].append(u)
                        if adj:
                            start_node = next(iter(adj.keys()))
                            bfs_order, q = [], [start_node]
                            parent_map = {start_node: None}
                            while q:
                                curr = q.pop(0)
                                bfs_order.append(curr)
                                for neighbor in adj[curr]:
                                    if neighbor != parent_map[curr]:
                                        parent_map[neighbor] = curr
                                        q.append(neighbor)
                            subtree_size = {}
                            total_paths = 0
                            for node in reversed(bfs_order):
                                size = 1
                                for neighbor in adj[node]:
                                    if neighbor != parent_map[node]: size += subtree_size[neighbor]
                                subtree_size[node] = size
                                if parent_map[node] is not None:
                                    total_paths += (size * (c_total - size))
                            virality = total_paths / ((c_total * (c_total - 1)) / 2)
                            cascade_metrics[cat]['Median_Virality'].append(virality)
        
        # =========================================================================
        # GERAÇÃO DOS 8 GRÁFICOS CCDF (Sem legenda individual)
        # =========================================================================
        magma_hex = sns.color_palette("magma", 4).as_hex()
        linestyles = ['-', '--', '-.', ':']
        
        # Agora a lista recebe: (Nome da Coluna, É porcentagem?, Rótulo do Eixo X)
        metrics = [
            ('Median_Virality', False, 'STRUCTURAL VIRALITY'), 
            ('Max_Streak', False, 'ECHO DEPTH (NODES)'), 
            ('Mod_Rate', True, 'MODERATION ATTRITION (%)'), 
            ('Global_Toxicity', True, 'NEGATIVE SENTIMENT (%)'),
            ('Positive_Ratio', True, 'POSITIVE SENTIMENT (%)'), 
            ('Neutral_Ratio', True, 'NEUTRAL (%)'), 
            ('Sentiment_Friction', True, 'SENTIMENT FRICTION (%)'), 
            ('Pos_Dominance', True, 'POSITIVE DOMINANCE (%)')
        ]
        
        for i, (col, is_pct, x_label) in enumerate(metrics):
            fig, ax = plt.subplots(figsize=(8, 6))
            ax.grid(True, alpha=0.5, linestyle="--", zorder=0)
            ax.set_title('') 
            
            for j, cat in enumerate(self.CATEGORIES):
                data = np.array(cascade_metrics[cat][col])
                if len(data) == 0: continue
                if is_pct: data = data * 100 
                
                sorted_data = np.sort(data)
                y = (1.0 - np.arange(len(sorted_data)) / len(sorted_data)) * 100
                ax.plot(sorted_data, y, color=magma_hex[j], linestyle=linestyles[j], linewidth=4.0, zorder=3)

            # EIXO X AGORA RECEBE O NOME ESPECÍFICO
            ax.set_xlabel(x_label, fontsize=22, fontweight='bold')
            
            # Eixo Y apenas na primeira coluna
            if i % 2 == 0:
                ax.set_ylabel('CCDF (% OF CASCADES)', fontsize=22, fontweight='bold')
            else:
                ax.set_ylabel('')
                
            ax.yaxis.set_major_formatter(mtick.PercentFormatter(decimals=0))
            if is_pct: 
                ax.xaxis.set_major_formatter(mtick.PercentFormatter(decimals=0))
            
            ax.tick_params(labelsize=18)
            ax.set_ylim(-2, 105) 
            if not is_pct: ax.set_xlim(left=0)

            sns.despine()
            plt.tight_layout()
            out_file = os.path.join(self.RESULTS_DIR, f"Behavioral_CCDF_{col}.pdf")
            plt.savefig(out_file, dpi=300, bbox_inches='tight')
            plt.close()

            # =========================================================================
            # GERAÇÃO DA LEGENDA MESTRE (Topo da Página)
            # =========================================================================
            print("[*] Gerando Legenda Mestre Horizontal...")
            # Cria uma figura bem larga e fininha (10 de largura, 0.5 de altura)
            fig_leg, ax_leg = plt.subplots(figsize=(10, 0.5)) 
            ax_leg.axis('off') # Esconde todo o gráfico fantasma
            
            dummy_handles = []
            for j, cat in enumerate(self.CATEGORIES):
                line, = ax_leg.plot([], [], color=magma_hex[j], linestyle=linestyles[j], linewidth=4.0, label=cat)
                dummy_handles.append(line)
                
            # Desenha apenas a legenda, com 4 colunas, centralizada
            ax_leg.legend(handles=dummy_handles, loc='center', ncol=4, 
                        fontsize=16, framealpha=1.0, edgecolor='black', 
                        handlelength=4.0, handletextpad=1.0)
            
            out_legend = os.path.join(self.RESULTS_DIR, "Behavioral_CCDF_Top_Legend.pdf")
            plt.savefig(out_legend, dpi=300, bbox_inches='tight')
            plt.close()
            print(f"   -> Salvo (Sem Labels): {out_file}")

        # 2. GERAÇÃO DA FIGURA EXTRA (Com Margens Fantasmas Simétricas)
        print("[*] Gerando Figura Extra de Rótulos e Legenda (Margens Idênticas)...")
        fig_leg, ax_leg = plt.subplots(figsize=(8, 6))
        sns.set_theme(style="ticks", rc={"axes.grid": False}) # Sem grid para o fundo ficar limpo
        
        # TRUQUE DE MARGEM: Criamos um eixo falso com as mesmas dimensões dos outros
        ax_leg.set_xlim(0, 100)
        ax_leg.set_ylim(-2, 105)
        ax_leg.yaxis.set_major_formatter(mtick.PercentFormatter(decimals=0))
        ax_leg.xaxis.set_major_formatter(mtick.PercentFormatter(decimals=0))
        ax_leg.tick_params(labelsize=18)
        
        # Pintamos a borda e os números de BRANCO para guardarem o espaço da margem sem aparecerem
        ax_leg.tick_params(axis='x', colors='white')
        ax_leg.tick_params(axis='y', colors='white')
        for spine in ax_leg.spines.values():
            spine.set_color('white')
        
        dummy_handles = []
        for j, cat in enumerate(self.CATEGORIES):
            line, = ax_leg.plot([], [], color=magma_hex[j], linestyle=linestyles[j], linewidth=6.0, label=cat)
            dummy_handles.append(line)
            
        # Textos ajustados (mais altos para não cortarem) e centralizados
        ax_leg.text(0.5, 0.85, "AXIS LABELS REFERENCE", ha='center', va='center', fontsize=22, fontweight='bold', color='#333333', transform=ax_leg.transAxes)
        ax_leg.text(0.5, 0.62, "Vertical Axis (Y):\nCCDF (% OF CASCADES)", ha='center', va='center', fontsize=18, fontweight='bold', transform=ax_leg.transAxes)
        ax_leg.text(0.5, 0.39, "Horizontal Axis (X):\nMETRIC VALUE", ha='center', va='center', fontsize=18, fontweight='bold', transform=ax_leg.transAxes)
        
        # Legenda com ncol=2 para o quadrinho ficar mais largo
        ax_leg.legend(handles=dummy_handles, loc='lower center', bbox_to_anchor=(0.5, 0.05), 
                      ncol=2, fontsize=16, framealpha=0.9, edgecolor='black', borderpad=1.2)
        
        out_legend = os.path.join(self.RESULTS_DIR, "Behavioral_CCDF_Legend_Master.pdf")
        
        # O tight_layout agora vai calcular a margem baseando-se nos números invisíveis!
        plt.tight_layout()
        plt.savefig(out_legend, dpi=300, bbox_inches='tight')
        plt.close()
        print(f"[SUCCESS] Figura de Legenda Master salva em: {out_legend}")

        print("\n=== VALORES REAIS: KRUSKAL-WALLIS PARA A TABELA 2 ===")
        metrics = [
            'Median_Virality', 'Max_Streak', 'Mod_Rate', 'Global_Toxicity', 
            'Positive_Ratio', 'Neutral_Ratio', 'Sentiment_Friction', 'Pos_Dominance'
        ]
        
        for m in metrics:
            # Extrai as listas de valores de cada categoria para a métrica atual
            groups = [cascade_metrics[cat][m] for cat in self.CATEGORIES if len(cascade_metrics[cat][m]) > 0]
            
            if len(groups) == 4: # Garante que temos as 4 categorias para comparar
                stat, p = kruskal(*groups)
                print(f"Métrica: {m:20} | H-Statistic: {stat:8.2f} | p-value: {p:.4e}")

    # =========================================================================
    # VII. RELATÓRIO PDF DE AUDITORIA ESTATÍSTICA (Centralizado em RESULTS_DIR)
    # =========================================================================
    def generate_statistical_report(self):
        print("\n[*] Compiling statistical report PDF...")
        out_file = os.path.join(self.RESULTS_DIR, "Statistical_Audit_Report.pdf") # Caminho atualizado
        
        report_lines = []
        report_lines.append("=" * 70)
        report_lines.append(" STATISTICAL AUDIT REPORT: KRUSKAL-WALLIS & DUNN'S POST-HOC ")
        report_lines.append("=" * 70 + "\n")
        
        for sentiment in ['POSITIVE', 'NEUTRAL', 'NEGATIVE']:
            groups_data = []
            active_cats = []
            for cat in self.CATEGORIES:
                data = np.array(self.confidences[cat][sentiment])
                if len(data) > 0:
                    groups_data.append(data)
                    active_cats.append(cat)
                    
            if len(groups_data) > 1:
                stat, p = kruskal(*groups_data)
                report_lines.append(f"--- P-SCORE: NLP CONFIDENCE ({sentiment}) ---")
                report_lines.append(f"Kruskal-Wallis H-statistic: {stat:.4f}")
                report_lines.append(f"Kruskal-Wallis p-value: {p:.4e}")
                
                if p < 0.05:
                    dunn = sp.posthoc_dunn(groups_data, p_adjust='bonferroni')
                    dunn.columns = dunn.index = active_cats
                    report_lines.append("\nDunn's Post-Hoc p-values (<0.05 is significant):")
                    dunn_str = dunn.map(lambda x: f"{x:.4f}*" if x < 0.05 else f"{x:.4f} ").to_string()
                    report_lines.append(dunn_str)
                report_lines.append("\n" + "-" * 70 + "\n")

        fig = plt.figure(figsize=(10, 14))
        fig.clf()
        plt.axis('off')
        
        full_text = "\n".join(report_lines)
        plt.text(0.05, 0.95, full_text, transform=fig.transFigure, fontsize=10, 
                 verticalalignment='top', fontfamily='monospace')
        
        plt.savefig(out_file, dpi=300, bbox_inches='tight')
        plt.close()
        print(f"[SUCCESS] Statistical PDF generated in '{out_file}'")
        
    # =========================================================================
    # VIII. MARKOV HEATMAPS (Inverted, Categories, Uppercase, No Title)
    # =========================================================================
    def plot_markov(self):
        print("\n[*] Generating Markov Heatmaps...")
        out_file = os.path.join(self.RESULTS_DIR, "Markov_Heatmaps_Categories.png")
        
        transitions = {cat: {p: {c: 0 for c in self.VALID_SENTIMENTS} for p in self.VALID_SENTIMENTS} for cat in self.CATEGORIES}

        for data in self.node_memory.values():
            p_id = data['p_id']
            if p_id and p_id in self.node_memory:
                parent_label = self.node_memory[p_id]['label']
                child_label = data['label']
                
                if parent_label in self.VALID_SENTIMENTS and child_label in self.VALID_SENTIMENTS:
                    transitions[self.CATEGORY_MAP[data['sub']]][parent_label][child_label] += 1

        states = ['NEGATIVE', 'NEUTRAL', 'POSITIVE']
        
        fig, axes = plt.subplots(1, 4, figsize=(24, 6))
        fig.suptitle('') 
        
        for idx, cat in enumerate(self.CATEGORIES):
            ax = axes[idx]
            df_trans = pd.DataFrame(transitions[cat]).T
            df_trans = df_trans.div(df_trans.sum(axis=1), axis=0).fillna(0) * 100
            df_trans = df_trans.reindex(index=states, columns=states)
            
            sns.heatmap(df_trans, annot=True, fmt=".1f", cmap="viridis_r", cbar=(idx == 3), ax=ax, vmin=0, vmax=100, 
                        annot_kws={'size': 16, 'weight': 'bold'}, xticklabels=states, yticklabels=states)
            
            ax.text(0.5, 1.05, cat, ha='center', va='bottom', transform=ax.transAxes, fontsize=18, fontweight='bold')
            
            if idx == 0: ax.set_ylabel("PARENT SENTIMENT", fontsize=14, fontweight='bold')
            ax.set_xlabel("CHILD SENTIMENT", fontsize=14, fontweight='bold')
            ax.tick_params(labelsize=12)

        plt.tight_layout()
        plt.savefig(out_file, dpi=300, bbox_inches='tight')
        plt.close()
        print(f"[SUCCESS] Heatmaps saved in '{out_file}'")

    # =========================================================================
    # IX. CURVAS MICRO-TOPOLÓGICAS (Depth, Size, Virality - N=1.1M)
    # =========================================================================
    def plot_micro_topology_ccdf(self):
        print("\n[*] A processar 1.1M de nós para as Curvas Micro-Topológicas...")
        out_file = os.path.join(self.RESULTS_DIR, "Micro_Topology_CCDF.pdf")

        cat_depths = {cat: [] for cat in self.CATEGORIES}
        cat_sizes = {cat: [] for cat in self.CATEGORIES}
        cat_virality = {cat: [] for cat in self.CATEGORIES}

        sub_children = defaultdict(lambda: defaultdict(list))
        sub_roots = defaultdict(list)
        sub_cats = {}

        with open(self.MULTIMODAL_PATH, 'r', encoding='utf-8') as f:
            for line in f:
                try:
                    record = json.loads(line)
                    if record.get('type') == 'metadata_footer': continue
                    sub_raw = record.get('subreddit')
                    if not sub_raw: continue
                    sub = sub_raw.lower()
                    if sub not in self.CATEGORY_MAP: continue

                    cat = self.CATEGORY_MAP[sub]
                    sub_cats[sub] = cat

                    depth = record.get('depth', 0)
                    cat_depths[cat].append(depth)

                    n_id = record['id']
                    p_id = record.get('parent_id')
                    if p_id: sub_children[sub][p_id].append(n_id)
                    if depth == 1: sub_roots[sub].append(n_id)
                except: continue

        print("   -> A calcular tamanhos e viralidade das cascatas...")
        
        for sub, cat in sub_cats.items():
            children_map = sub_children[sub]
            for root_id in sub_roots[sub]:
                queue = [root_id]
                edges = []
                num_nodes = 0

                while queue:
                    curr = queue.pop(0)
                    num_nodes += 1
                    for child_id in children_map.get(curr, []):
                        edges.append((curr, child_id))
                        queue.append(child_id)

                if num_nodes > 0:
                    cat_sizes[cat].append(num_nodes)

                if num_nodes >= 5:
                    adj = defaultdict(list)
                    for u, v in edges:
                        adj[u].append(v); adj[v].append(u)
                    start_node = next(iter(adj.keys()))
                    bfs_order, q = [], [start_node]
                    parent_map = {start_node: None}
                    while q:
                        curr = q.pop(0)
                        bfs_order.append(curr)
                        for neighbor in adj[curr]:
                            if neighbor != parent_map[curr]:
                                    parent_map[neighbor] = curr
                                    q.append(neighbor)
                    subtree_size = {}
                    total_paths = 0
                    for node in reversed(bfs_order):
                        size = 1
                        for neighbor in adj[node]:
                            if neighbor != parent_map[node]: size += subtree_size[neighbor]
                        subtree_size[node] = size
                        if parent_map[node] is not None:
                            total_paths += (size * (num_nodes - size))
                    virality = total_paths / ((num_nodes * (num_nodes - 1)) / 2)
                    cat_virality[cat].append(virality)

        sns.set_theme(style="ticks", rc={"axes.grid": True, "grid.alpha": 0.5, "grid.linestyle": "--"})
        fig, axes = plt.subplots(1, 3, figsize=(24, 8))

        # Magma Aplicado
        magma_hex = sns.color_palette("magma", 4).as_hex()
        linestyles = ['-', '--', '-.', ':']

        plot_configs = [
            (cat_depths, 'NODE DEPTH DISTRIBUTION', True, 'DEPTH LEVEL'),
            (cat_sizes, 'CASCADE SIZE DISTRIBUTION', True, 'TOTAL NODES IN CASCADE'),
            (cat_virality, 'RAW STRUCTURAL VIRALITY', False, 'WIENER INDEX (VIRALITY)')
        ]

        handles, labels_leg = [], []

        for i, (data_dict, title, is_log_x, x_label) in enumerate(plot_configs):
            ax = axes[i]
            ax.set_title(title, fontsize=18, fontweight='bold', pad=20)

            for j, cat in enumerate(self.CATEGORIES):
                data = np.array(data_dict[cat])
                if len(data) == 0: continue

                sorted_data = np.sort(data)
                # Escala Y para Porcentagem Direta
                y = (1.0 - np.arange(len(sorted_data)) / len(sorted_data)) * 100
                
                sorted_data = sorted_data[:-1]
                y = y[:-1]
                
                if len(sorted_data) == 0: continue

                line, = ax.plot(sorted_data, y, color=magma_hex[j], linestyle=linestyles[j], linewidth=3.5)

                if i == 0:
                    handles.append(line)
                    labels_leg.append(f"{cat}")

            # Removido o ax.set_yscale('log')
            ax.set_ylim(-2, 105)
            ax.yaxis.set_major_formatter(mtick.PercentFormatter(decimals=0))
            
            if is_log_x:
                ax.set_xscale('log')

            ax.set_xlabel(x_label, fontsize=16, fontweight='bold')
            if i == 0:
                ax.set_ylabel('CCDF (% OF CASCADES)', fontsize=16, fontweight='bold')

            ax.tick_params(labelsize=14)

        fig.legend(handles, labels_leg, loc='upper center', bbox_to_anchor=(0.5, 1.10), ncol=4, fontsize=16, frameon=False)

        sns.despine(fig)
        plt.tight_layout()
        plt.savefig(out_file, dpi=300, bbox_inches='tight')
        plt.close()
        print(f"[SUCCESS] Curvas Micro-Topológicas (N=1.1M) salvas em '{out_file}'")

    # =========================================================================
    # X. CCDFS RQ1
    # =========================================================================
    def calculate_structural_and_plot_rq1(self):
        print("[*] Iniciando a Quarta Onda: Processamento Estrutural e Temporal RQ1...")
        import pandas as pd
        import numpy as np
        import matplotlib.pyplot as plt
        import matplotlib.ticker as mtick
        import seaborn as sns
        import networkx as nx
        from collections import defaultdict
        import json
        import os

        magma_hex = sns.color_palette("magma", 4).as_hex()
        colors_cat = {
            'PUBLIC ARENAS': magma_hex[0], 
            'HUMOR': magma_hex[1], 
            'SOCIOCULTURAL': magma_hex[2], 
            'HOBBIES': magma_hex[3]
        }
        linestyles = ['-', '--', '-.', ':']
        cascade_metrics = {cat: defaultdict(list) for cat in self.CATEGORIES}
        sub_children = defaultdict(lambda: defaultdict(list))
        node_memory = {}
        sub_roots = defaultdict(list)

        print("   -> Fase 1: Mapeando nós (Definição Kretli et al. 2026: Floresta de Cascatas)...")
        with open(self.MULTIMODAL_PATH, 'r', encoding='utf-8') as f:
            for line in f:
                try:
                    record = json.loads(line)
                    sub = record.get('subreddit', '').lower()
                    if sub not in self.CATEGORY_MAP: continue
                    
                    n_id_raw = record.get('id', '')
                    p_id_raw = record.get('parent_id')
                    depth = record.get('depth', 0)
                    
                    # Higienização de IDs para que o NetworkX consiga ligar a árvore
                    n_id = str(n_id_raw).split('_')[-1]
                    
                    if p_id_raw and pd.notna(p_id_raw):
                        p_id = str(p_id_raw).split('_')[-1]
                        # Se o parent_id começa com 't3_', é o post original. Logo, é nó de nível 1.
                        is_post_reply = str(p_id_raw).startswith('t3_')
                    else:
                        p_id = None
                        is_post_reply = False
                        
                    author = record.get('author', '[deleted]')
                    
                    ts = record.get('created_utc')
                    if not ts: ts = record.get('timestamp')
                    if not ts: ts = record.get('ai_analysis', {}).get('created_utc')
                    
                    node_memory[n_id] = {
                        'author': author, 
                        'timestamp': float(ts) if ts else None
                    }
                    
                    # APLICAÇÃO DA DEFINIÇÃO DO TCC:
                    # A raiz é EXCLUSIVAMENTE um nó de profundidade 1 (resposta direta ao post)
                    if depth == 1 or is_post_reply:
                        sub_roots[sub].append(n_id)
                    elif p_id:
                        # Se não é raiz, é galho/folha. Salva quem é o pai para a Fase 2 conectar.
                        sub_children[sub][p_id].append(n_id)
                        
                except Exception:
                    continue

        print("   -> Fase 2: Computando Grafos Temporais Exclusivos via NetworkX...")
        for sub, cat in self.CATEGORY_MAP.items():
            if sub not in sub_roots: continue
            
            cascatas_salvas = 0
            for root_id in sub_roots[sub]:
                # Como definimos pelas raízes do nível 1, iniciamos a fila por elas
                queue = [(root_id, None)]
                G = nx.DiGraph()
                timestamps = []
                authors = set()
                
                while queue:
                    curr, actual_parent = queue.pop(0)
                    G.add_node(curr)
                    if actual_parent is not None:
                        G.add_edge(actual_parent, curr)
                        
                    nd = node_memory.get(curr, {})
                    ts = nd.get('timestamp')
                    author = nd.get('author', '[deleted]')
                    
                    if ts: timestamps.append(ts)
                    if author not in ['[deleted]', 'deleted']: authors.add(author)
                        
                    for child_id in sub_children[sub].get(curr, []):
                        queue.append((child_id, curr))
                
                num_messages = G.number_of_nodes()
                if num_messages < 3: 
                    continue
                
                cascatas_salvas += 1
                
                # 1. Structural Virality
                G_un = G.to_undirected()
                virality = nx.wiener_index(G_un) / (num_messages * (num_messages - 1)) if num_messages > 1 else 1.0

                # 2. Max Depth e Breadth
                try:
                    lengths = nx.single_source_shortest_path_length(G, root_id)
                    max_depth = max(lengths.values()) if lengths else 1
                    level_counts = defaultdict(int)
                    for dist in lengths.values(): level_counts[dist] += 1
                    max_breadth = max(level_counts.values()) if level_counts else 1
                except:
                    max_depth = 1
                    max_breadth = 1

                # 4. Métricas Temporais
                if len(timestamps) >= 2:
                    sorted_ts = np.sort(timestamps)
                    duration_hours = (sorted_ts[-1] - sorted_ts[0]) / 3600.0
                    iats = np.diff(sorted_ts) / 60.0
                    mean_iat_min = np.mean(iats) if len(iats) > 0 else 0.0
                else:
                    duration_hours = 0.0
                    mean_iat_min = 0.0

                cascade_metrics[cat]['Structural_Virality'].append(virality)
                cascade_metrics[cat]['Max_Depth'].append(max_depth)
                cascade_metrics[cat]['Max_Breadth'].append(max_breadth)
                cascade_metrics[cat]['Unique_Users'].append(max(len(authors), 1))
                cascade_metrics[cat]['Number_Messages'].append(num_messages)
                cascade_metrics[cat]['Duration_Hours'].append(duration_hours)
                cascade_metrics[cat]['Mean_IAT_Min'].append(mean_iat_min)

            if cascatas_salvas > 0:
                print(f"      [OK] {cat} ({sub}): {cascatas_salvas} n-ary trees processadas.")

        print("   -> Fase 3: Renderizando os novos CCDFs com eixos específicos...")
        metrics_rq1 = [
            ('Structural_Virality', False, 'STRUCTURAL VIRALITY (WIENER INDEX)'),
            ('Max_Depth', False, 'MAXIMUM CASCADE DEPTH'),
            ('Max_Breadth', True, 'MAXIMUM CASCADE BREADTH'),
            ('Unique_Users', True, 'UNIQUE PARTICIPATING USERS'),
            ('Number_Messages', True, 'NUMBER OF MESSAGES (SIZE)'),
            ('Duration_Hours', True, 'CASCADE DURATION (HOURS)'),
            ('Mean_IAT_Min', True, 'MEAN INTER-ARRIVAL TIME (MINUTES)')
        ]

        # Configuração estética do Seaborn
        sns.set_theme(style="ticks", rc={"axes.grid": True, "grid.alpha": 0.5, "grid.linestyle": "--"})

        for i, (col, is_log_x, x_label) in enumerate(metrics_rq1):
            fig, ax = plt.subplots(figsize=(8, 6))
            # Garante o grid de forma explícita em todos
            ax.grid(True, alpha=0.5, linestyle="--", zorder=0)
            
            for j, cat in enumerate(self.CATEGORIES):
                data = np.array(cascade_metrics[cat][col])
                if len(data) == 0: 
                    print(f"   [AVISO] Sem dados para renderizar: {cat} -> {col}")
                    continue
                
                sorted_data = np.sort(data)
                y = (1.0 - np.arange(len(sorted_data)) / len(sorted_data)) * 100
                ax.plot(sorted_data, y, color=magma_hex[j], linestyle=linestyles[j], linewidth=4.0, zorder=3)

            # Rótulos customizados conforme diretriz
            ax.set_xlabel(x_label, fontsize=18, fontweight='bold')
            
            if i % 2 == 0:
                ax.set_ylabel('CCDF (% OF CASCADES)', fontsize=18, fontweight='bold')
            else:
                ax.set_ylabel('')
                
            ax.yaxis.set_major_formatter(mtick.PercentFormatter(decimals=0))
            ax.tick_params(labelsize=14)
            ax.set_ylim(-2, 105) 
            
            if is_log_x:
                ax.set_xscale('log')
            else:
                ax.set_xlim(left=0)

            sns.despine()
            plt.tight_layout()
            out_file = os.path.join(self.RESULTS_DIR, f"Structural_RQ1_CCDF_{col}.pdf")
            plt.savefig(out_file, dpi=300, bbox_inches='tight')
            plt.close()
            print(f"   [SUCESSO] Salvo: {out_file}")

        # =========================================================================
        # GERAÇÃO DA LEGENDA MESTRE HORIZONTAL UNIVERSAL NO TOPO
        # =========================================================================
        print("[*] Gerando Barra de Legenda Mestre Superior...")
        fig_leg, ax_leg = plt.subplots(figsize=(10, 0.5)) 
        ax_leg.axis('off')
        dummy_handles = []
        for j, cat in enumerate(self.CATEGORIES):
            line, = ax_leg.plot([], [], color=magma_hex[j], linestyle=linestyles[j], linewidth=4.0, label=cat)
            dummy_handles.append(line)
            
        ax_leg.legend(handles=dummy_handles, loc='center', ncol=4, 
                      fontsize=14, framealpha=1.0, edgecolor='black', 
                      handlelength=4.0, handletextpad=1.0)
        
        plt.savefig(os.path.join(self.RESULTS_DIR, "Structural_RQ1_Top_Legend.pdf"), dpi=300, bbox_inches='tight')
        plt.close()
        print("[SUCCESS] Pipeline de Quarta Onda concluído sem lacunas.")

if __name__ == "__main__":
    app = RakedditAnalyticsOrchestrator()
    if app.extract_and_assign_taxonomy():
        # Tabelas e Quadrantes
        app.generate_tables()
        app.plot_bcc_taxonomy()
        app.plot_bcc_taxonomy_trendline()
        
        # Validação NLP
        app.plot_nlp_validation_ccdf()
        app.generate_statistical_report()
        
        # Descobertas Comportamentais (Os degraus)
        app.plot_behavioral_ccdf()
        
        # O novo Megazord Topológico (As curvas suaves de 1M de nós)
        app.plot_micro_topology_ccdf()
        
        app.calculate_structural_and_plot_rq1()
        # Transições
        app.plot_markov()
        print("\n👋 Processamento analítico completamente finalizado.")