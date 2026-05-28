import json
import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.patheffects as path_effects
import matplotlib.ticker as mtick
import seaborn as sns
from collections import defaultdict, Counter
from scipy.stats import kruskal
import scikit_posthocs as sp

try:
    from adjustText import adjust_text
except ImportError:
    adjust_text = None
    print("[!] Warning: 'adjustText' not installed. Labels on scatter plot might overlap. (pip install adjustText)")


class RakedditAnalyticsOrchestrator:
    def __init__(self):
        # Base Paths
        self.MULTIMODAL_PATH = "DATA/4-inferred/INFERRED_MULTIMODAL_FINAL.jsonl"
        self.BLIND_PATH = "DATA/4-inferred/INFERRED_BLIND_DATASET.jsonl"
        
        # Output Directories
        self.RESULTS_DIR = "results"
        self.AUDIT_DIR = "audit"
        os.makedirs(self.RESULTS_DIR, exist_ok=True)
        os.makedirs(self.AUDIT_DIR, exist_ok=True)
        
        # Constants
        self.VALID_SENTIMENTS = {'POSITIVE', 'NEGATIVE', 'NEUTRAL'}
        self.MODERATION_LABELS = {'REMOVED_BY_MOD', 'USER_DELETED', 'AUTOMOD_WARNING'}
        self.TAXONOMIES = ['Hostile Echoes', 'Chronic Conflict', 'Passive Consumption', 'Constructive Deliberation']
        self.VIRIDIS_COLORS = sns.color_palette("viridis", 4)
        
        # In-Memory Data
        self.df_features = None
        self.sub_to_tax = {}
        self.x_mid = 0.0
        self.y_mid = 0.0

    # =========================================================================
    # 1. CORE ENGINE: FEATURE EXTRACTION & TAXONOMY (IN-MEMORY)
    # =========================================================================
    def extract_and_assign_taxonomy(self):
        """Scans the dataset, computes topological features, and assigns BCC Taxonomy."""
        if not os.path.exists(self.MULTIMODAL_PATH):
            print(f"[-] Error: Dataset not found at {self.MULTIMODAL_PATH}")
            return False

        print(f"\n[*] Scanning {os.path.basename(self.MULTIMODAL_PATH)} to build Feature Matrix in RAM...")
        
        sub_nodes = defaultdict(dict)
        sub_children = defaultdict(lambda: defaultdict(list))
        sub_roots = defaultdict(list)
        sub_volume = Counter()
        
        with open(self.MULTIMODAL_PATH, 'r', encoding='utf-8') as f:
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
                    if p_id: sub_children[sub][p_id].append(n_id)
                    if depth == 1: sub_roots[sub].append(n_id)
                except: continue

        features = []
        for sub in sub_nodes.keys():
            nodes = sub_nodes[sub]
            children_map = sub_children[sub]
            total_vol = sub_volume[sub]
            if total_vol < 1000: continue # Filter micro-subs
                
            total_valid = 0
            total_negative = 0
            mod_interventions = 0
            cascades_virality = []
            
            for n_id, data in nodes.items():
                lbl = data['label']
                if lbl in self.VALID_SENTIMENTS:
                    total_valid += 1
                    if lbl == 'NEGATIVE': total_negative += 1
                if lbl in self.MODERATION_LABELS:
                    mod_interventions += 1

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
                        
                if num_nodes >= 5: 
                    # Quick O(N) Virality calc inline
                    if num_nodes <= 1: virality = 0.0
                    else:
                        adj = defaultdict(list)
                        for u, v in edges:
                            adj[u].append(v)
                            adj[v].append(u)
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
                'Total_Volume': total_vol,
                'Median_Virality': median_virality,
                'Global_Toxicity': global_tox
            })

        self.df_features = pd.DataFrame(features)
        
        # Calculate Cut-offs and Assign Taxonomy
        self.x_mid = self.df_features['Median_Virality'].median()
        self.y_mid = (self.df_features['Global_Toxicity'] * 100).median()
        
        def assign(row):
            x, y = row['Median_Virality'], row['Global_Toxicity'] * 100
            if x > self.x_mid and y > self.y_mid: return 'Chronic Conflict', self.VIRIDIS_COLORS[3]
            elif x > self.x_mid and y <= self.y_mid: return 'Constructive Deliberation', self.VIRIDIS_COLORS[2]
            elif x <= self.x_mid and y > self.y_mid: return 'Hostile Echoes', self.VIRIDIS_COLORS[0]
            else: return 'Passive Consumption', self.VIRIDIS_COLORS[1]

        self.df_features[['Taxonomy', 'Color']] = self.df_features.apply(lambda row: pd.Series(assign(row)), axis=1)
        self.sub_to_tax = dict(zip(self.df_features['Subreddit'], self.df_features['Taxonomy']))
        
        print(f"[+] Feature extraction complete. Mapped {len(self.df_features)} subreddits.")
        return True

    # =========================================================================
    # 2. ANALYTICAL PLOTS
    # =========================================================================
    def plot_bcc_taxonomy(self):
        """Generates the BCC Framework Quadrant Plot as a PDF."""
        print("\n[*] Generating BCC Taxonomy Quadrant Plot...")
        out_file = os.path.join(self.AUDIT_DIR, "BCC_Taxonomy_English.pdf")
        
        x = self.df_features['Median_Virality']
        y = self.df_features['Global_Toxicity'] * 100 
        
        fig, ax = plt.subplots(figsize=(14, 10))
        sns.set_style("white") 
        ax.grid(False) 
        
        ax.scatter(x, y, s=150, c=self.df_features['Color'], alpha=0.9, edgecolors='black', linewidth=1.2, zorder=3)
        ax.axvline(self.x_mid, color='black', linestyle='--', alpha=0.6, zorder=1)
        ax.axhline(self.y_mid, color='black', linestyle='--', alpha=0.6, zorder=1)
        
        ax.plot(self.x_mid, self.y_mid, marker='o', color='red', markersize=6, zorder=4)
        ax.text(self.x_mid + 0.02, self.y_mid + 0.5, f'Cut-off Median: ({self.x_mid:.2f}, {self.y_mid:.2f}%)', 
                color='red', fontsize=11, fontweight='bold', zorder=5,
                path_effects=[path_effects.withStroke(linewidth=3, foreground='white')])

        texts = []
        for _, row in self.df_features.iterrows():
            vol_str = f"{row['Total_Volume']/1000:.1f}k" if row['Total_Volume'] >= 1000 else str(row['Total_Volume'])
            texts.append(ax.text(row['Median_Virality'], row['Global_Toxicity']*100, f"r/{row['Subreddit']} ({vol_str})", 
                                  fontsize=10, fontweight='bold', color='#2c3e50', zorder=4))
        
        if adjust_text:
            adjust_text(texts, arrowprops=dict(arrowstyle='-', color='gray', lw=0.5))

        ax.set_title('Behavioral Taxonomy of Cascades', fontsize=20, fontweight='bold', pad=20)
        ax.set_xlabel('Structural Axis: Median Virality (Deliberative Complexity)', fontsize=14, fontweight='bold')
        ax.set_ylabel('Semantic Axis: Conflict Index (% of Negative Interactions)', fontsize=14, fontweight='bold')
        ax.yaxis.set_major_formatter(mtick.PercentFormatter(decimals=0))

        plt.tight_layout()
        plt.savefig(out_file, dpi=300, bbox_inches='tight')
        plt.close()
        print(f"[SUCCESS] Plot saved as '{out_file}'")

    def generate_summary_table(self):
        """Generates the taxonomy summary table purely as an image (No CSV)."""
        print("\n[*] Generating Summary Table Image...")
        out_file = os.path.join(self.RESULTS_DIR, "taxonomy_summary_table.png")
        
        sub_msgs = defaultdict(int)
        sub_users = defaultdict(set)
        
        with open(self.MULTIMODAL_PATH, 'r', encoding='utf-8') as f:
            for line in f:
                try:
                    rec = json.loads(line)
                    sub = rec.get('subreddit')
                    if not sub or sub not in self.sub_to_tax: continue
                    sub_msgs[sub] += 1
                    author = rec.get('author')
                    if author and author != '[deleted]': sub_users[sub].add(author)
                except: continue

        rows = []
        for tax in self.TAXONOMIES:
            subs_in_tax = [s for s, t in self.sub_to_tax.items() if t == tax]
            total_msgs = sum(sub_msgs[s] for s in subs_in_tax)
            total_users = set().union(*(sub_users[s] for s in subs_in_tax))
            rows.append([tax, len(subs_in_tax), f"{total_msgs:,}", f"{len(total_users):,}"])

        rows.append(['TOTAL', len(self.sub_to_tax), 
                     f"{sum(sub_msgs.values()):,}", 
                     f"{len(set().union(*sub_users.values())):,}"])

        fig, ax = plt.subplots(figsize=(10, 3))
        ax.axis('tight')
        ax.axis('off')
        
        table = ax.table(cellText=rows, colLabels=['Taxonomy Quadrant', '# Subreddits', '# Messages', '# Unique Users'],
                         cellLoc='center', loc='center', colWidths=[0.3, 0.15, 0.25, 0.25])
        table.auto_set_font_size(False)
        table.set_fontsize(11)
        table.scale(1.2, 1.8)
        
        for (i, j), cell in table.get_celld().items():
            if i == 0 or i == len(rows): cell.set_text_props(weight='bold')
            if i == 0: cell.set_facecolor('#f0f0f0')
                
        plt.title("Table 1: Taxonomy Quadrant Summary", fontsize=14, fontweight='bold', pad=10)
        plt.tight_layout()
        plt.savefig(out_file, dpi=300, bbox_inches='tight')
        plt.close()
        print(f"[SUCCESS] Table saved to '{out_file}'")

    def plot_sentiment_distributions(self):
        """Generates Bar Chart with Standard Error (SEM) bars."""
        print("\n[*] Generating SEM Sentiment Distributions...")
        out_file = os.path.join(self.RESULTS_DIR, "OBJ-I_Sentiment_Distribution_with_Errors.png")
        
        sub_counts = defaultdict(lambda: {'POSITIVE': 0, 'NEUTRAL': 0, 'NEGATIVE': 0, 'TOTAL': 0})
        
        with open(self.MULTIMODAL_PATH, 'r', encoding='utf-8') as f:
            for line in f:
                try:
                    rec = json.loads(line)
                    sub = rec.get('subreddit')
                    label = rec.get('ai_analysis', {}).get('label')
                    if sub in self.sub_to_tax and label in self.VALID_SENTIMENTS:
                        sub_counts[sub][label] += 1
                        sub_counts[sub]['TOTAL'] += 1
                except: continue

        rows = []
        for sub, counts in sub_counts.items():
            total = counts['TOTAL']
            if total > 0:
                for state in self.VALID_SENTIMENTS:
                    rows.append({'Taxonomy': self.sub_to_tax[sub], 'Sentiment': state, 'Percentage': (counts[state]/total)*100})

        df_plot = pd.DataFrame(rows)
        
        plt.figure(figsize=(14, 8))
        sns.set_theme(style="whitegrid")
        ax = sns.barplot(
            data=df_plot, x='Taxonomy', y='Percentage', hue='Sentiment',
            hue_order=['POSITIVE', 'NEUTRAL', 'NEGATIVE'], order=self.TAXONOMIES,
            palette={'POSITIVE': '#fde725', 'NEUTRAL': '#21918c', 'NEGATIVE': '#440154'},
            errorbar='se', capsize=0.05, err_kws={'linewidth': 1.5, 'color': 'black'}, edgecolor='black'
        )
        
        plt.title('Mean Sentiment Distribution by Taxonomy\nwith Standard Error of the Mean (SEM)', fontsize=16, fontweight='bold', pad=20)
        plt.ylabel('Mean Proportion (%)', fontsize=12, fontweight='bold')
        ax.yaxis.set_major_formatter(mtick.PercentFormatter(decimals=0))
        plt.tight_layout()
        plt.savefig(out_file, dpi=300, bbox_inches='tight')
        plt.close()
        print(f"[SUCCESS] Chart saved to '{out_file}'")

    def plot_ccdf_stats(self):
        """Generates Log-scale CCDF with Kruskal-Wallis & Dunn's Post-Hoc Test."""
        print("\n[*] Generating CCDF & Executing Dunn's Test...")
        out_file = os.path.join(self.RESULTS_DIR, "OBJ-I_Sentiment_CCDF_Stats.png")
        
        # We reuse the logic to gather proportions
        sub_counts = defaultdict(lambda: {'POSITIVE': 0, 'NEUTRAL': 0, 'NEGATIVE': 0, 'TOTAL': 0})
        with open(self.MULTIMODAL_PATH, 'r', encoding='utf-8') as f:
            for line in f:
                try:
                    rec = json.loads(line)
                    sub = rec.get('subreddit')
                    label = rec.get('ai_analysis', {}).get('label')
                    if sub in self.sub_to_tax and label in self.VALID_SENTIMENTS:
                        sub_counts[sub][label] += 1
                        sub_counts[sub]['TOTAL'] += 1
                except: continue

        rows = []
        for sub, counts in sub_counts.items():
            if counts['TOTAL'] > 0:
                for state in ['POSITIVE', 'NEUTRAL', 'NEGATIVE']:
                    rows.append({'Taxonomy': self.sub_to_tax[sub], 'Sentiment': state, 'Percentage': (counts[state]/counts['TOTAL'])*100})
        df_plot = pd.DataFrame(rows)

        sns.set_theme(style="ticks", rc={"axes.grid": True, "grid.alpha": 0.5, "grid.linestyle": "--"})
        fig, axes = plt.subplots(1, 3, figsize=(20, 7), sharey=True)
        fig.suptitle("Sentiment Distribution CCDF by BCC Taxonomy", fontsize=18, fontweight='bold', y=1.05)
        color_map = dict(zip(self.TAXONOMIES, self.VIRIDIS_COLORS))

        for i, sentiment in enumerate(['POSITIVE', 'NEUTRAL', 'NEGATIVE']):
            ax = axes[i]
            groups_data, active_tax = [], []
            
            for tax in self.TAXONOMIES:
                data = df_plot[(df_plot['Taxonomy'] == tax) & (df_plot['Sentiment'] == sentiment)]['Percentage'].values
                data = data[data > 0]
                if len(data) == 0: continue
                groups_data.append(data)
                active_tax.append(tax)
                
                label_str = rf"{tax} ($\mu={np.mean(data):.1f}\%$, $\sigma={np.std(data):.1f}\%$)"
                sorted_data = np.sort(data)
                y = 100 * (1 - np.arange(1, len(sorted_data) + 1) / len(sorted_data))
                ax.step(np.append(sorted_data, sorted_data[-1]), np.append(y, 0), where='post', color=color_map[tax], label=label_str, linewidth=3)

            # Terminal Stats Logging
            if len(groups_data) > 1:
                stat, p = kruskal(*groups_data)
                print(f"\n--- {sentiment} STATS ---")
                print(f"Kruskal-Wallis p-value: {p:.4e}")
                if p < 0.05:
                    dunn = sp.posthoc_dunn(groups_data, p_adjust='bonferroni')
                    dunn.columns = dunn.index = active_tax
                    print(dunn.map(lambda x: f"{x:.4f}*" if x < 0.05 else f"{x:.4f} "))

            ax.set_title(f"{sentiment} Sentiment", fontsize=15, fontweight='600')
            ax.set_xscale('log')
            ax.set_xlim(0.1, 100)
            ax.set_ylim(-2, 102)
            ax.xaxis.set_major_formatter(mtick.PercentFormatter(xmax=100, decimals=0))
            if i == 0: ax.set_ylabel(rf"CCDF (% of Subreddits $\geq x$)", fontsize=13)
            ax.legend(title='Taxonomy', fontsize=11, loc='upper right')

        sns.despine(fig)
        plt.tight_layout()
        plt.savefig(out_file, dpi=300, bbox_inches='tight')
        plt.close()
        print(f"[SUCCESS] CCDF saved to '{out_file}'")

    def plot_markov(self):
        """Generates Markov Transition Heatmaps."""
        print("\n[*] Generating Markov Heatmaps...")
        out_file = os.path.join(self.RESULTS_DIR, "Markov_Heatmaps.png")
        
        node_memory = {}
        with open(self.MULTIMODAL_PATH, 'r', encoding='utf-8') as f:
            for line in f:
                try:
                    rec = json.loads(line)
                    sub = rec.get('subreddit')
                    label = rec.get('ai_analysis', {}).get('label')
                    if sub in self.sub_to_tax and label in self.VALID_SENTIMENTS:
                        node_memory[rec['id']] = {'label': label, 'parent': rec.get('parent_id'), 'tax': self.sub_to_tax[sub]}
                except: continue

        states = ['NEGATIVE', 'NEUTRAL', 'POSITIVE']
        transitions = {tax: {p: {c: 0 for c in states} for p in states} for tax in self.TAXONOMIES}

        for data in node_memory.values():
            p_id = data['parent']
            if p_id in node_memory:
                transitions[data['tax']][node_memory[p_id]['label']][data['label']] += 1

        fig, axes = plt.subplots(1, 4, figsize=(20, 5))
        for idx, tax in enumerate(self.TAXONOMIES):
            ax = axes[idx]
            df_trans = pd.DataFrame(transitions[tax]).T
            df_trans = df_trans.div(df_trans.sum(axis=1), axis=0).fillna(0) * 100
            df_trans = df_trans.reindex(index=states, columns=states)
            
            sns.heatmap(df_trans, annot=True, fmt=".1f", cmap="viridis", cbar=(idx == 3), ax=ax, vmin=0, vmax=100, 
                        annot_kws={'size': 14, 'weight': 'bold'}, xticklabels=['NEG', 'NEU', 'POS'], yticklabels=['NEG', 'NEU', 'POS'])
            ax.set_title(tax.upper(), fontsize=13, fontweight='bold', pad=15)
            if idx == 0: ax.set_ylabel("PARENT SENTIMENT", fontsize=12, fontweight='bold')
            ax.set_xlabel("CHILD SENTIMENT", fontsize=12, fontweight='bold')

        plt.suptitle("Markov Transition Probabilities: Parent → Child Sentiment", fontsize=16, fontweight='bold', y=1.05)
        plt.tight_layout()
        plt.savefig(out_file, dpi=300, bbox_inches='tight')
        plt.close()
        print(f"[SUCCESS] Heatmaps saved to '{out_file}'")

    # =========================================================================
    # 3. VISION DELTA (CLIMAX)
    # =========================================================================
    def analyze_vision_delta(self):
        """Cross-references Multimodal vs Blind datasets and outputs an image table."""
        print("\n" + "="*60)
        print(" 👁️ vs 🙈 CÁLCULO DO DELTA DE COMPREENSÃO MULTIMODAL ")
        print("="*60)

        if not os.path.exists(self.MULTIMODAL_PATH) or not os.path.exists(self.BLIND_PATH):
            print("[-] Error: Missing Blind or Multimodal datasets.")
            return

        print(f"[*] Loading Blind Inference: {os.path.basename(self.BLIND_PATH)}")
        blind_memory = {}
        with open(self.BLIND_PATH, 'r', encoding='utf-8') as f:
            for line in f:
                try:
                    rec = json.loads(line)
                    if "ai_analysis" in rec: blind_memory[rec["id"]] = rec["ai_analysis"].get("label")
                except: continue

        print(f"[*] Cross-referencing with Ground Truth: {os.path.basename(self.MULTIMODAL_PATH)}")
        total_compared, total_divergence = 0, 0
        sub_totals, sub_divergences = Counter(), Counter()

        with open(self.MULTIMODAL_PATH, 'r', encoding='utf-8') as f:
            for line in f:
                try:
                    rec = json.loads(line)
                    n_id, sub = rec.get("id"), rec.get("subreddit")
                    mm_label = rec.get("ai_analysis", {}).get("label")
                    blind_label = blind_memory.get(n_id)
                    
                    if blind_label in self.VALID_SENTIMENTS and mm_label in self.VALID_SENTIMENTS:
                        total_compared += 1
                        sub_totals[sub] += 1
                        if mm_label != blind_label:
                            total_divergence += 1
                            sub_divergences[sub] += 1
                except: continue

        if total_compared == 0:
            print("[-] Error: No matching nodes found.")
            return

        # Prepare Table for Image Rendering
        sub_data = []
        for sub, total in sub_totals.items():
            delta_pct = (sub_divergences[sub] / total) * 100
            sub_data.append([f"r/{sub}", f"{total:,}", f"{sub_divergences[sub]:,}", f"{delta_pct:.2f}%"])
            
        sub_data.sort(key=lambda x: float(x[3][:-1]), reverse=True)
        sub_data.insert(0, ["GLOBAL", f"{total_compared:,}", f"{total_divergence:,}", f"{(total_divergence/total_compared)*100:.2f}%"])

        # Render Table to PNG
        out_file = os.path.join(self.RESULTS_DIR, "Vision_Impact_Delta_Table.png")
        fig, ax = plt.subplots(figsize=(10, max(4, len(sub_data)*0.3)))
        ax.axis('tight')
        ax.axis('off')
        
        table = ax.table(cellText=sub_data, colLabels=['Subreddit', 'Matched Nodes', 'Divergences (AI Changed Mind)', 'Vision Impact (Delta)'],
                         cellLoc='center', loc='center', colWidths=[0.25, 0.2, 0.3, 0.25])
        table.auto_set_font_size(False)
        table.set_fontsize(10)
        table.scale(1.2, 1.5)
        
        for (i, j), cell in table.get_celld().items():
            if i == 0 or i == 1: cell.set_text_props(weight='bold')
            if i == 0: cell.set_facecolor('#d9ead3')
            if i == 1: cell.set_facecolor('#fce5cd') # Highlight Global Row
                
        plt.title("Multimodal vs Blind: AI Vision Dependency Rate", fontsize=14, fontweight='bold', pad=15)
        plt.tight_layout()
        plt.savefig(out_file, dpi=300, bbox_inches='tight')
        plt.close()
        
        print(f"\n[+] Global Vision Impact Delta: {(total_divergence/total_compared)*100:.2f}%")
        print(f"[SUCCESS] Image Table saved to: {out_file}")

# =========================================================================
# 4. CLI MENU
# =========================================================================
if __name__ == "__main__":
    app = RakedditAnalyticsOrchestrator()
    
    print("\n" + "="*50)
    print(" 📊 RAKEDDIT ANALYTICS SUITE ")
    print("="*50)
    print(" [1] Run ALL Analytics & Plots")
    print(" [2] Generate Taxonomy Plot (BCC Framework)")
    print(" [3] Generate Summary Table")
    print(" [4] Generate Sentiment Distributions (SEM Bars)")
    print(" [5] Generate CCDF & Dunn's Statistics")
    print(" [6] Generate Markov Transition Heatmaps")
    print(" [7] Calculate Vision Delta (Multimodal vs Blind)")
    print("="*50)
    
    choice = input("Select an option (1-7): ").strip()
    
    if choice in ['1', '2', '3', '4', '5', '6']:
        if app.extract_and_assign_taxonomy():
            if choice == '1' or choice == '2': app.plot_bcc_taxonomy()
            if choice == '1' or choice == '3': app.generate_summary_table()
            if choice == '1' or choice == '4': app.plot_sentiment_distributions()
            if choice == '1' or choice == '5': app.plot_ccdf_stats()
            if choice == '1' or choice == '6': app.plot_markov()
    
    if choice == '1' or choice == '7':
        app.analyze_vision_delta()
        
    print("\n👋 Analytics Session Complete.")