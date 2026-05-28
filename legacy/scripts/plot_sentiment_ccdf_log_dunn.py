"""
plot_sentiment_ccdf_log_dunn.py

Generates aesthetically enhanced CCDF plots for sentiment distributions.
Includes Kruskal-Wallis H-test AND Dunn's Post-Hoc Test (with Bonferroni correction)
to pinpoint exactly which BCC Taxonomy quadrants differ statistically.
"""

import json
import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.ticker as mtick
import seaborn as sns
from scipy.stats import kruskal
import scikit_posthocs as sp  # NOVO: Necessário para o Teste de Dunn
from collections import defaultdict

# ==========================================
# CONFIGURATION
# ==========================================
DATASET_PATH = "DATA/4-inferred/INFERRED_MULTIMODAL_FINAL.jsonl"
FEATURES_CSV = "audit/subreddit_features_matrix.csv"
OUTPUT_IMG = "results/OBJ-I_Sentiment_CCDF_Log_Analysis.png"

# Ensure output directory exists
os.makedirs(os.path.dirname(OUTPUT_IMG), exist_ok=True)

STATES = ['POSITIVE', 'NEUTRAL', 'NEGATIVE']
TAXONOMIES = ['Hostile Echoes', 'Chronic Conflict', 'Passive Consumption', 'Constructive Deliberation']

def assign_taxonomy(row, x_mid, y_mid):
    """Assigns taxonomy strictly based on the 4 quadrants divided by the medians."""
    x, y = row['Median_Virality'], row['Global_Toxicity'] * 100
    if x > x_mid and y > y_mid: return 'Chronic Conflict'
    elif x > x_mid and y <= y_mid: return 'Constructive Deliberation'
    elif x <= x_mid and y > y_mid: return 'Hostile Echoes'
    else: return 'Passive Consumption'

def main():
    print("\n" + "="*70)
    print(" 📈 CCDF + KRUSKAL-WALLIS + DUNN'S POST-HOC (SENTIMENT ANALYSIS) ")
    print("="*70)

    # 1. Define Taxonomy
    print("[*] Mapping Subreddit Taxonomies...")
    try:
        df_feat = pd.read_csv(FEATURES_CSV)
        x, y = df_feat['Median_Virality'], df_feat['Global_Toxicity'] * 100 
        x_mid, y_mid = x.median(), y.median()
        sub_to_tax = {row['Subreddit']: assign_taxonomy(row, x_mid, y_mid) for _, row in df_feat.iterrows()}
    except FileNotFoundError:
        print(f"[!] Error: Features file not found at {FEATURES_CSV}.")
        return

    # 2. Count Sentiments per Subreddit
    print("[*] Processing multimodal inferences...")
    sub_counts = defaultdict(lambda: {'POSITIVE': 0, 'NEUTRAL': 0, 'NEGATIVE': 0, 'TOTAL': 0})
    
    try:
        with open(DATASET_PATH, 'r', encoding='utf-8') as f:
            for line in f:
                if not line.strip(): continue
                try:
                    record = json.loads(line)
                    if record.get('type') == 'metadata_footer': continue
                    sub = record.get('subreddit')
                    if not sub or sub not in sub_to_tax: continue
                    label = record.get('ai_analysis', {}).get('label')
                    if label in STATES:
                        sub_counts[sub][label] += 1
                        sub_counts[sub]['TOTAL'] += 1
                except json.JSONDecodeError: 
                    continue
    except FileNotFoundError:
        print(f"[!] Error: Dataset file not found at {DATASET_PATH}.")
        return

    # 3. Calculate Proportions
    print("[*] Calculating empirical distributions...\n")
    rows = []
    for sub, counts in sub_counts.items():
        total = counts['TOTAL']
        if total == 0: continue
        tax = sub_to_tax[sub]
        for state in STATES:
            pct = (counts[state] / total) * 100
            rows.append({'Subreddit': sub, 'Taxonomy': tax, 'Sentiment': state, 'Percentage': pct})

    df_plot = pd.DataFrame(rows)
    if df_plot.empty:
        print("[!] No valid data extracted. Exiting.")
        return

    # 4. Plotting & Statistical Analysis
    sns.set_theme(style="ticks", rc={"axes.grid": True, "grid.alpha": 0.5, "grid.linestyle": "--"})
    fig, axes = plt.subplots(1, 3, figsize=(20, 7), sharey=True)
    fig.suptitle("Sentiment Distribution CCDF by BCC Taxonomy", fontsize=18, fontweight='bold', y=1.05)

    colors = sns.color_palette("viridis", len(TAXONOMIES))
    color_map = dict(zip(TAXONOMIES, colors))

    for i, sentiment in enumerate(STATES):
        ax = axes[i]
        groups_data = []
        active_taxonomies = [] # Keeps track of which quadrants actually have data
        
        for tax in TAXONOMIES:
            data = df_plot[(df_plot['Taxonomy'] == tax) & (df_plot['Sentiment'] == sentiment)]['Percentage'].values
            data = data[data > 0] # Filter out 0s for log scale
            if len(data) == 0: continue
                
            groups_data.append(data)
            active_taxonomies.append(tax)
            
            mean_val, std_val = np.mean(data), np.std(data)
            label_str = rf"{tax} ($\mu={mean_val:.1f}\%$, $\sigma={std_val:.1f}\%$)"
            
            # CCDF
            sorted_data = np.sort(data)
            y = 100 * (1 - np.arange(1, len(sorted_data) + 1) / len(sorted_data))
            sorted_data = np.append(sorted_data, sorted_data[-1])
            y = np.append(y, 0)
            
            ax.step(sorted_data, y, where='post', color=color_map[tax], label=label_str, linewidth=3, alpha=0.85)

        # ---------------------------------------------------------
        # STATISTICAL BLOCK: Kruskal-Wallis + Dunn's Post-Hoc
        # ---------------------------------------------------------
        print(f"--- ANÁLISE: SENTIMENTO {sentiment} ---")
        if len(groups_data) > 1:
            stat, p = kruskal(*groups_data)
            print(f"Kruskal-Wallis H: {stat:.4f} | p-value: {p:.4e}")
            
            if p < 0.05:
                print("Resultado: Diferença GLOBAL significativa encontrada. Executando Teste de Dunn (Post-Hoc)...")
                # Executa o post-hoc com correção de Bonferroni
                dunn_matrix = sp.posthoc_dunn(groups_data, p_adjust='bonferroni')
                # Renomeia linhas e colunas para os nomes dos quadrantes
                dunn_matrix.columns = active_taxonomies
                dunn_matrix.index = active_taxonomies
                
                print("\nMatriz de p-values (Dunn's Test - Bonferroni):")
                print("-" * 60)
                # Formata a matriz para facilitar a leitura (destaca valores < 0.05 com um '*')
                formatted_matrix = dunn_matrix.map(lambda x: f"{x:.4f}*" if x < 0.05 else f"{x:.4f} ")
                print(formatted_matrix)
                print("-" * 60)
                print("DICA: Valores com '*' (< 0.05) indicam que a diferença entre os dois cruzamentos é ESTATISTICAMENTE SIGNIFICATIVA.\n")
            else:
                print("Resultado: Sem diferença estatística significativa entre os quadrantes.\n")

        # Plot Formatting
        ax.set_title(f"{sentiment} Sentiment", fontsize=15, fontweight='600', pad=15)
        ax.set_xlabel("Proportion in Subreddit", fontsize=13, labelpad=10)
        if i == 0: ax.set_ylabel(rf"CCDF (% of Subreddits $\geq x$)", fontsize=13, labelpad=10)
        
        ax.set_xscale('log')
        ax.set_xlim(0.1, 100)
        ax.set_ylim(-2, 102)
        
        ax.xaxis.set_major_formatter(mtick.PercentFormatter(xmax=100, decimals=0))
        ax.yaxis.set_major_formatter(mtick.PercentFormatter(xmax=100))
        ax.tick_params(axis='both', which='major', labelsize=11)
        ax.legend(title='Taxonomy Quadrants', fontsize=11, title_fontsize=12, loc='upper right', frameon=True, shadow=True)

    sns.despine(fig, trim=False)
    plt.tight_layout()
    plt.savefig(OUTPUT_IMG, dpi=300, bbox_inches='tight')
    
    print(f"\n[+] Sucesso! Gráfico salvo em: {os.path.abspath(OUTPUT_IMG)}")

if __name__ == "__main__":
    main()