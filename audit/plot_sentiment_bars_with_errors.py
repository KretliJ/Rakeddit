"""
plot_sentiment_bars_with_errors.py

Generates a grouped Bar Chart of sentiment distribution by BCC Taxonomy,
including Standard Error of the Mean (SEM) bars to demonstrate intra-group statistical consistency.
"""

import json
import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from collections import defaultdict

# ==========================================
# CONFIGURATION
# ==========================================
DATASET_PATH = "DATA/4-inferred/INFERRED_MULTIMODAL_FINAL.jsonl"
FEATURES_CSV = "audit/subreddit_features_matrix.csv"
OUTPUT_IMG = "results/OBJ-I_Sentiment_Distribution_with_Errors.png"

# Ensure output directory exists
os.makedirs(os.path.dirname(OUTPUT_IMG), exist_ok=True)

STATES = ['POSITIVE', 'NEUTRAL', 'NEGATIVE']

# Viridis Color Palette for Sentiments
# Negative: Dark Purple, Neutral: Teal, Positive: Yellow-Green
COLORS = {'POSITIVE': '#fde725', 'NEUTRAL': '#21918c', 'NEGATIVE': '#440154'}

def assign_taxonomy(row, x_mid, y_mid):
    """
    Assigns taxonomy strictly based on the 4 quadrants divided by the medians.
    X-axis: Structural Virality | Y-axis: Conflict/Toxicity
    """
    x, y = row['Median_Virality'], row['Global_Toxicity'] * 100
    
    if x > x_mid and y > y_mid: 
        return 'Chronic Conflict'
    elif x > x_mid and y <= y_mid: 
        return 'Constructive Deliberation'
    elif x <= x_mid and y > y_mid: 
        return 'Hostile Echoes'
    else: 
        return 'Passive Consumption'

def main():
    print("\n" + "="*60)
    print(" 📊 SEM BAR CHART GENERATOR (SENTIMENT DISTRIBUTION) ")
    print("="*60)

    # 1. Define Taxonomy
    print("[*] Mapping Subreddit Taxonomies...")
    df_feat = pd.read_csv(FEATURES_CSV)
    x, y = df_feat['Median_Virality'], df_feat['Global_Toxicity'] * 100 
    x_mid, y_mid = x.median(), y.median()
    
    sub_to_tax = {row['Subreddit']: assign_taxonomy(row, x_mid, y_mid) for _, row in df_feat.iterrows()}

    # 2. Count Sentiments per Subreddit
    print("[*] Processing multimodal inferences...")
    sub_counts = defaultdict(lambda: {'POSITIVE': 0, 'NEUTRAL': 0, 'NEGATIVE': 0, 'TOTAL': 0})
    
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
            except: 
                continue

    # 3. Prepare DataFrame for Seaborn (Long Format)
    print("[*] Calculating proportions and structuring statistical data...")
    rows = []
    for sub, counts in sub_counts.items():
        total = counts['TOTAL']
        if total == 0: continue
        tax = sub_to_tax[sub]
        
        for state in STATES:
            pct = (counts[state] / total) * 100
            rows.append({
                'Subreddit': sub,
                'Taxonomy': tax,
                'Sentiment': state,
                'Percentage': pct
            })

    df_plot = pd.DataFrame(rows)

    # 4. Plotting with Seaborn
    print("[*] Generating Bar Chart with Standard Error (SEM) bars...")
    sns.set_theme(style="whitegrid")
    
    # Logical order of the quadrants
    tax_order = ['Chronic Conflict', 'Constructive Deliberation', 'Hostile Echoes', 'Passive Consumption']
    # Filter to only include those present in the data
    tax_order = [t for t in tax_order if t in df_plot['Taxonomy'].unique()]

    plt.figure(figsize=(14, 8))
    
    # Seaborn barplot automatically calculates the mean and Standard Error (errorbar='se') 
    # based on the variance between subreddits of the same group.
    ax = sns.barplot(
        data=df_plot,
        x='Taxonomy',
        y='Percentage',
        hue='Sentiment',
        hue_order=STATES,
        order=tax_order,
        palette=COLORS,
        errorbar='se',      # Standard Error
        capsize=0.05,       # Size of the error bar "cap"
        err_kws={'linewidth': 1.5, 'color': 'black'},
        edgecolor='black',
        linewidth=1
    )

    # Plot Aesthetics
    plt.title('Mean Sentiment Distribution by Taxonomy\nwith Standard Error of the Mean (SEM)', 
              fontsize=16, fontweight='bold', pad=20)
    plt.xlabel('Framework Quadrants', fontsize=12, fontweight='bold')
    plt.ylabel('Mean Proportion (%)', fontsize=12, fontweight='bold')
    
    # Legend Adjustment
    plt.legend(title='Semantic Valence', title_fontsize='11', loc='upper right', framealpha=0.9)
    
    # Improve axis readability
    plt.xticks(fontsize=11)
    plt.yticks(fontsize=11)
    ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda val, _: f'{val:.0f}%'))

    plt.tight_layout()
    plt.savefig(OUTPUT_IMG, dpi=300, bbox_inches='tight')
    
    print(f"\n[+] Absolute Success! Chart saved as: {OUTPUT_IMG}")

if __name__ == "__main__":
    main()