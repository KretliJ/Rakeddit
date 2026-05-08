import json
import os
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from collections import defaultdict
from glob import glob
from modules.config_loader import config

def generate_cascade_stats(jsonl_path):
    # Read dataset and calculate structural metrics per subreddit
  
    data = []
    with open(jsonl_path, 'r', encoding='utf-8') as f:
        for line in f:
            record = json.loads(line)
            # Ignoramos o footer de metadados para não quebrar o log
            if record.get('type') in ['post_header', 'comment']:
                data.append(record)
    
    df = pd.DataFrame(data)
    
    # Filter for valid text
    df_valid = df[df['is_valid_text'] == True].copy()
    
    # 1. Breadth calculation per level
    # Group by subreddit and depth to count how many nodes there are at each level
    breadth_stats = df_valid.groupby(['subreddit', 'depth']).size().reset_index(name='node_count')
    
    # Normalize by number of unique posts (average width)
    posts_per_sub = df_valid[df_valid['type'] == 'post_header'].groupby('subreddit').size().to_dict()
    breadth_stats['avg_breadth'] = breadth_stats.apply(
        lambda x: x['node_count'] / posts_per_sub.get(x['subreddit'], 1), axis=1
    )
    
    return breadth_stats

def plot_structural_signature(breadth_stats):
    # Plot graph for average width vs depth

    plt.figure(figsize=(12, 6))
    sns.set_theme(style="whitegrid")
    
    # Line plot with seaborn
    line_plot = sns.lineplot(
        data=breadth_stats, 
        x='depth', 
        y='avg_breadth', 
        hue='subreddit', 
        marker='o'
    )
    
    plt.title('Assinatura Estrutural: Largura Média por Profundidade', fontsize=14)
    plt.xlabel('Profundidade da Cascata (Depth)', fontsize=12)
    plt.ylabel('Largura Média (Nodes/Post)', fontsize=12)
    plt.yscale('log') # Log scale to see deep levels with less nodes
    plt.legend(title='Subreddit')
    
    plt.tight_layout()
    plt.show()

if __name__ == "__main__":
    # Busca automática: Pega o MULTIMODAL_ mais recente na pasta definida no config.ini
    target_dir = config.get_path('PATHS', 'MULTIMODAL_PATH', fallback='./DATA/3-vision_processing')
    files = glob(os.path.join(target_dir, "MULTIMODAL_*.jsonl"))
    
    if not files:
        print(f"❌ Nenhum arquivo MULTIMODAL encontrado em: {target_dir}")
    else:
        # Ordena por data de modificação e pega o último
        latest_file = max(files, key=os.path.getmtime)
        
        stats = generate_cascade_stats(latest_file)
        plot_structural_signature(stats)