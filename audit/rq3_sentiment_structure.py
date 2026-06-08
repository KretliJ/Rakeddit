import json
import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt # type: ignore
import seaborn as sns # type: ignore
import matplotlib.ticker as mtick # type: ignore
import networkx as nx
from collections import defaultdict
from scipy.stats import linregress, kruskal

class RQ3SentimentStructure:
    def __init__(self):
        self.MULTIMODAL_PATH = "DATA/4-inferred/INFERRED_MULTIMODAL_FINAL.jsonl"
        self.RESULTS_DIR = "results/RQ3"
        os.makedirs(self.RESULTS_DIR, exist_ok=True)
        
        self.VALID_SENTIMENTS = {'POSITIVE', 'NEUTRAL', 'NEGATIVE'}
        self.CATEGORIES = ['PUBLIC ARENAS', 'HUMOR', 'SOCIOCULTURAL', 'HOBBIES']
        self.CATEGORY_MAP = {
            'brasil': 'PUBLIC ARENAS', 'brasilivre': 'PUBLIC ARENAS', 'brasildob': 'PUBLIC ARENAS', 'debatesbr': 'PUBLIC ARENAS', 'noticiasbr': 'PUBLIC ARENAS',
            'botecodoreddit': 'HUMOR', 'farialimabets': 'HUMOR', 'memesbr': 'HUMOR', 'shitpostbr': 'HUMOR',
            'antitrampo': 'SOCIOCULTURAL', 'opiniaoburra': 'SOCIOCULTURAL', 'opiniaoimpopular': 'SOCIOCULTURAL', 'filosofiabar': 'SOCIOCULTURAL', 'infernosocial': 'SOCIOCULTURAL',
            'futebol': 'HOBBIES', 'gamesecultura': 'HOBBIES', 'videogamesbrasil': 'HOBBIES', 'carros': 'HOBBIES', 'computadores': 'HOBBIES', 'saopaulo': 'HOBBIES'
        }
        
        # Cores padronizadas para Sentimentos
        self.SENTIMENT_COLORS = {
            'POSITIVE': '#3B0F70',
            'NEUTRAL': '#CA3E72', 
            'NEGATIVE': '#FECF92'
        }
        
        self.cascade_data = []

    def extract_cascades_kretli_2026(self):
        print("[*] RQ3: Mapeando Floresta de Cascatas (Sentimento x Estrutura)...")
        node_memory = {}
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
                    
                    n_id = str(n_id_raw).split('_')[-1]
                    if p_id_raw and pd.notna(p_id_raw):
                        p_id = str(p_id_raw).split('_')[-1]
                        is_post_reply = str(p_id_raw).startswith('t3_')
                    else:
                        p_id = None
                        is_post_reply = False
                        
                    label = record.get('ai_analysis', {}).get('label', 'UNKNOWN')
                    node_memory[n_id] = {'label': label}
                    
                    # Regra Kretli 2026: Raízes são Depth 1
                    if depth == 1 or is_post_reply:
                        sub_roots[sub].append(n_id)
                    elif p_id:
                        sub_children[sub][p_id].append(n_id)
                except: continue

        print("   -> Computando Propriedades Estruturais e Polaridade Dominante...")
        for sub, cat in self.CATEGORY_MAP.items():
            if sub not in sub_roots: continue
            
            for root_id in sub_roots[sub]:
                queue = [(root_id, None)]
                G = nx.DiGraph()
                sentiments = {'POSITIVE': 0, 'NEUTRAL': 0, 'NEGATIVE': 0}
                total_valid = 0
                
                while queue:
                    curr, actual_parent = queue.pop(0)
                    G.add_node(curr)
                    if actual_parent is not None:
                        G.add_edge(actual_parent, curr)
                        
                    lbl = node_memory.get(curr, {}).get('label', 'UNKNOWN')
                    if lbl in self.VALID_SENTIMENTS:
                        sentiments[lbl] += 1
                        total_valid += 1
                        
                    for child_id in sub_children[sub].get(curr, []):
                        queue.append((child_id, curr))
                
                num_nodes = G.number_of_nodes()
                if num_nodes < 3 or total_valid == 0: continue
                
                # Polaridade Dominante da Cascata
                dominant_sentiment = max(sentiments, key=sentiments.get)
                toxicity_ratio = sentiments['NEGATIVE'] / total_valid
                
                # Virality
                G_un = G.to_undirected()
                virality = nx.wiener_index(G_un) / (num_nodes * (num_nodes - 1)) if num_nodes > 1 else 1.0

                # Max Depth
                try:
                    lengths = nx.single_source_shortest_path_length(G, root_id)
                    max_depth = max(lengths.values()) if lengths else 1
                except:
                    max_depth = 1

                self.cascade_data.append({
                    'Category': cat,
                    'Dominant_Sentiment': dominant_sentiment,
                    'Virality': virality,
                    'Max_Depth': max_depth,
                    'Toxicity': toxicity_ratio
                })
        
        print(f"[+] Extraídas {len(self.cascade_data)} cascatas válidas para RQ3.")

    def plot_ccdfs_by_sentiment(self):
        print("[*] Gerando Figura 4: CCDFs de Estrutura por Sentimento (Linhas Distintas)...")
        df = pd.DataFrame(self.cascade_data)
        
        # Mapeamento de estilos para garantir legibilidade
        sentiment_styles = {
            'POSITIVE': '-',    # Sólida
            'NEUTRAL': '--',    # Tracejado
            'NEGATIVE': '-.'    # Ponto-traço
        }
        
        sns.set_theme(style="ticks", rc={"axes.grid": True, "grid.alpha": 0.5, "grid.linestyle": "--"})
        
        configs = [
            ('Max_Depth', 'max_depth_by_sentiment', 'ECHO DEPTH (MAXIMUM CASCADE DEPTH)'),
            ('Virality', 'virality_by_sentiment', 'STRUCTURAL VIRALITY (WIENER INDEX)')
        ]
        
        for col, filename, x_label in configs:
            fig, ax = plt.subplots(figsize=(8, 6))
            ax.grid(True, alpha=0.5, linestyle="--", zorder=0)
            
            handles = []
            labels = []
            
            for sentiment in ['POSITIVE', 'NEUTRAL', 'NEGATIVE']:
                data = df[df['Dominant_Sentiment'] == sentiment][col].values
                if len(data) == 0: continue
                
                sorted_data = np.sort(data)
                y = (1.0 - np.arange(len(sorted_data)) / len(sorted_data)) * 100
                
                # Aplica o linestyle específico do sentimento
                line, = ax.plot(sorted_data, y, color=self.SENTIMENT_COLORS[sentiment], 
                                linestyle=sentiment_styles[sentiment], linewidth=4.0, zorder=3)
                handles.append(line)
                labels.append(f"{sentiment} CASCADES (N={len(data)})")

            ax.set_xlabel(x_label, fontsize=18, fontweight='bold')
            ax.set_ylabel('CCDF (% OF CASCADES)', fontsize=18, fontweight='bold')
            ax.yaxis.set_major_formatter(mtick.PercentFormatter(decimals=0))
            ax.tick_params(labelsize=14)
            ax.set_ylim(-2, 105)
            
            if col == 'Virality':
                ax.set_xlim(left=0)

            ax.legend(handles, labels, fontsize=12, framealpha=1.0, edgecolor='black', loc='upper right')
            sns.despine()
            plt.tight_layout()
            
            out_file = os.path.join(self.RESULTS_DIR, f"RQ3_CCDF_{filename}.pdf")
            plt.savefig(out_file, dpi=300, bbox_inches='tight')
            plt.close()
            print(f"   -> Salvo: {filename}.pdf")

    def plot_cascade_taxonomy_trendlines(self):
        print("[*] Gerando Figura 8: Taxonomy Trendline Individual de Cascatas...")
        df = pd.DataFrame(self.cascade_data)
        
        magma_hex = sns.color_palette("magma", 4).as_hex()
        cat_palette = {cat: color for cat, color in zip(self.CATEGORIES, magma_hex)}
        
        sentiments = ['POSITIVE', 'NEUTRAL', 'NEGATIVE']
        fig, axes = plt.subplots(1, 3, figsize=(24, 7), sharey=True)
        
        for i, sentiment in enumerate(sentiments):
            ax = axes[i]
            df_sub = df[df['Dominant_Sentiment'] == sentiment]
            
            if df_sub.empty: continue
            
            x = df_sub['Virality']
            y = df_sub['Toxicity'] * 100 # Em porcentagem
            
            # Scatter Plot (Rasterizado para o PDF não travar)
            sns.scatterplot(data=df_sub, x='Virality', y=y, hue='Category', palette=cat_palette,
                            alpha=0.5, s=30, edgecolor='none', ax=ax, zorder=2, rasterized=True, legend=False)
            # Trendline com Intervalo de Confiança (CI)
            sns.regplot(x=df_sub['Virality'], y=df_sub['Toxicity']*100, scatter=False, color='black', 
                        ci=95, line_kws={'linestyle':'-', 'linewidth':3.5, 'zorder': 4}, ax=ax)
            
            # Regressão Linear Estatísticas
            slope, intercept, r_value, p_value, std_err = linregress(x, y)
            r_squared = r_value**2
            p_str = f"p < 0.001" if p_value < 0.001 else f"p = {p_value:.3f}"
            
            slope, intercept, r_value, p_value, std_err = linregress(df_sub['Virality'], df_sub['Toxicity']*100)
            stats_text = f"Trendline ($R^2={r_value**2:.3f}$)\n$p={p_value:.1e}$"
            ax.text(0.05, 0.95, stats_text, transform=ax.transAxes, fontsize=14, fontweight='bold',
                    verticalalignment='top', bbox=dict(facecolor='white', alpha=0.9, edgecolor='black', boxstyle='round,pad=0.5'))
            
            # Borda do Sentimento
            for spine in ax.spines.values():
                spine.set_edgecolor(self.SENTIMENT_COLORS[sentiment])
                spine.set_linewidth(3.5)
            
            ax.set_title(f"{sentiment} DOMINANT CASCADES", fontsize=18, fontweight='bold', color=self.SENTIMENT_COLORS[sentiment], pad=15)
            ax.set_xlabel('STRUCTURAL VIRALITY', fontsize=16, fontweight='bold')
            if i == 0:
                ax.set_ylabel('SENTIMENT vs TOTAL (%)', fontsize=16, fontweight='bold')
            else:
                ax.set_ylabel('')
                
            ax.yaxis.set_major_formatter(mtick.PercentFormatter(decimals=0))
            ax.tick_params(labelsize=14)
            
        # FORA DO LOOP: Criamos a legenda UMA ÚNICA VEZ para toda a figura
        # Usamos 'lines' manuais apenas para a legenda, evitando conflito com Line2D
        from matplotlib.lines import Line2D # type: ignore
        
        custom_handles = [
            Line2D([0], [0], marker='o', color='w', markerfacecolor=cat_palette[cat], 
                   markersize=14, label=cat) for cat in self.CATEGORIES
        ]
        
        axes[1].legend(handles=custom_handles, title="CATEGORIES", loc='upper center', 
                       bbox_to_anchor=(0.5, -0.15), ncol=4, fontsize=14, 
                       title_fontsize=16, frameon=True, edgecolor='black')

        plt.tight_layout()
        out_file = os.path.join(self.RESULTS_DIR, "RQ3_Taxonomy_Cascades_Trendlines.pdf")
        plt.savefig(out_file, dpi=300, bbox_inches='tight')
        plt.close()
        print(f"[SUCCESS] Figura 8 gerada sem erros de atributo.")
        
        # Teste Kruskal-Wallis Global
        print("\n=== ESTATÍSTICA: KRUSKAL-WALLIS (VIRALITY VS SENTIMENT) ===")
        groups = [df[df['Dominant_Sentiment'] == s]['Virality'].values for s in sentiments if not df[df['Dominant_Sentiment'] == s].empty]
        if len(groups) == 3:
            stat, p = kruskal(*groups)
            print(f"H-Statistic: {stat:.2f} | p-value: {p:.4e}")

if __name__ == "__main__":
    app = RQ3SentimentStructure()
    app.extract_cascades_kretli_2026()
    app.plot_ccdfs_by_sentiment()
    app.plot_cascade_taxonomy_trendlines()
    print("[SUCCESS] RQ3 Concluída.")