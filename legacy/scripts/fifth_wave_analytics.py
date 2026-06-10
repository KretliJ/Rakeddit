import json
import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import networkx as nx
from collections import defaultdict

class FifthWaveAnalytics:
    def __init__(self):
        self.MULTIMODAL_PATH = "DATA/4-inferred/INFERRED_MULTIMODAL_FINAL.jsonl"
        self.RESULTS_DIR = "results/5-results_fifth_wave" # Salvando direto na pasta correta
        os.makedirs(self.RESULTS_DIR, exist_ok=True)
        
        self.CATEGORIES = ['PUBLIC ARENAS', 'HUMOR', 'SOCIOCULTURAL', 'HOBBIES']
        self.CATEGORY_MAP = {
            'brasil': 'PUBLIC ARENAS', 'brasilivre': 'PUBLIC ARENAS', 'brasildob': 'PUBLIC ARENAS', 'debatesbr': 'PUBLIC ARENAS', 'noticiasbr': 'PUBLIC ARENAS',
            'botecodoreddit': 'HUMOR', 'farialimabets': 'HUMOR', 'memesbr': 'HUMOR', 'shitpostbr': 'HUMOR',
            'antitrampo': 'SOCIOCULTURAL', 'opiniaoburra': 'SOCIOCULTURAL', 'opiniaoimpopular': 'SOCIOCULTURAL', 'filosofiabar': 'SOCIOCULTURAL', 'infernosocial': 'SOCIOCULTURAL',
            'futebol': 'HOBBIES', 'gamesecultura': 'HOBBIES', 'videogamesbrasil': 'HOBBIES', 'carros': 'HOBBIES', 'computadores': 'HOBBIES', 'saopaulo': 'HOBBIES'
        }
        
        self.cascade_data = []

    def extract_dimensions(self):
        print("[*] Quinta Onda: Extraindo Dimensões Estruturais...")
        
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
                        
                    author = record.get('author', '[deleted]')
                    
                    ts = record.get('created_utc')
                    if not ts: ts = record.get('timestamp')
                    if not ts: ts = record.get('ai_analysis', {}).get('created_utc')
                    
                    node_memory[n_id] = {'author': author, 'timestamp': float(ts) if ts else None}
                    
                    if depth == 1 or is_post_reply:
                        sub_roots[sub].append(n_id)
                    elif p_id:
                        sub_children[sub][p_id].append(n_id)
                except: continue

        for sub, cat in self.CATEGORY_MAP.items():
            if sub not in sub_roots: continue
            
            for root_id in sub_roots[sub]:
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
                if num_messages < 3: continue
                
                try:
                    lengths = nx.single_source_shortest_path_length(G, root_id)
                    max_depth = max(lengths.values()) if lengths else 1
                    level_counts = defaultdict(int)
                    for dist in lengths.values(): level_counts[dist] += 1
                    max_breadth = max(level_counts.values()) if level_counts else 1
                except:
                    max_depth, max_breadth = 1, 1

                if len(timestamps) >= 2:
                    sorted_ts = np.sort(timestamps)
                    duration_min = (sorted_ts[-1] - sorted_ts[0]) / 60.0 # Em minutos!
                else:
                    duration_min = 0.0

                self.cascade_data.append({
                    'Category': cat,
                    'Depth': max_depth,
                    'Unique_Users': max(len(authors), 1),
                    'Max_Breadth': max_breadth,
                    'Time_Minutes': duration_min
                })
        
        print(f"[+] Processadas {len(self.cascade_data)} cascatas para análise dimensional.")

    def plot_correlations(self):
        print("[*] Gerando os 4 Gráficos Relacionais de Dimensão (Versão Clean e Filtrada)...")
        df = pd.DataFrame(self.cascade_data)
        
        # 1. Filtro de ruído (remove cascatas instantâneas)
        df = df[df['Time_Minutes'] > 0].copy()
        
        # 2. CRIA a coluna de Binning PRIMEIRO
        df['Unique_Users_Binned'] = (df['Unique_Users'] // 3) * 3
        df['Unique_Users_Binned'] = df['Unique_Users_Binned'].replace(0, 1)
        
        # 3. APLICA o Filtro de Significância (Corta o final caótico da cauda longa)
        # Só mantemos na visualização agrupamentos com pelo menos 5 cascatas
        df = df.groupby('Depth').filter(lambda x: len(x) >= 5)
        df = df.groupby('Unique_Users_Binned').filter(lambda x: len(x) >= 5)
        
        sns.set_theme(style="ticks", rc={"axes.grid": True, "grid.alpha": 0.4, "grid.linestyle": "--"})
        magma_hex = sns.color_palette("magma", 4).as_hex()
        palette_dict = {cat: color for cat, color in zip(self.CATEGORIES, magma_hex)}
        
        # Estrutura: (Arquivo, X_Col, Y_Col, X_Label, Y_Label, Log_X, Log_Y)
        plots_config = [
            ("Structural_Correlation_Depth_vs_Time", 'Depth', 'Time_Minutes', "CASCADE DEPTH", "MEDIAN DURATION (MINUTES)", False, True),
            ("Structural_Correlation_Users_vs_Time", 'Unique_Users_Binned', 'Time_Minutes', "UNIQUE USERS IN CASCADE", "MEDIAN DURATION (MINUTES)", True, True),
            ("Structural_Correlation_Depth_vs_Users", 'Depth', 'Unique_Users', "CASCADE DEPTH", "MEDIAN UNIQUE USERS", False, True),
            ("Structural_Correlation_Depth_vs_Breadth", 'Depth', 'Max_Breadth', "CASCADE DEPTH", "MEDIAN MAX BREADTH", False, True)
        ]
        
        for filename, x_col, y_col, x_label, y_label, log_x, log_y in plots_config:
            fig, ax = plt.subplots(figsize=(8, 6))
            
            # MAGIA DA LIMPEZA: errorbar=None tira a bagunça sombreada
            # estimator='median' ignora os outliers bizarros e traça uma linha suave
            sns.lineplot(data=df, x=x_col, y=y_col, hue='Category', palette=palette_dict, 
                         marker="o", markersize=7, linewidth=3.5, 
                         estimator='median', errorbar=None, ax=ax, zorder=3)
            
            ax.set_xlabel(x_label, fontsize=16, fontweight='bold')
            ax.set_ylabel(y_label, fontsize=16, fontweight='bold')
            ax.tick_params(labelsize=14)
            
            if log_x: ax.set_xscale('log')
            if log_y: ax.set_yscale('log')
                
            ax.legend().remove()
            sns.despine()
            plt.tight_layout()
            
            out_file = os.path.join(self.RESULTS_DIR, f"{filename}.pdf")
            plt.savefig(out_file, dpi=300, bbox_inches='tight')
            plt.close()
            print(f"   -> Salvo: {filename}.pdf")

if __name__ == "__main__":
    app = FifthWaveAnalytics()
    app.extract_dimensions()
    app.plot_correlations()
    print("[SUCCESS] Relacionamentos topológicos concluídos.")