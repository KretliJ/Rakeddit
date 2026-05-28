"""
plot_sentiment_ccdf_final.py
- Correctly reads sentiment from ai_analysis.label
- Includes all subreddits (assigns taxonomy only to those in CSV, others as 'Other')
- Plots CCDF with Viridis, no grid, English.
"""

import json
import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.ticker as mtick
import seaborn as sns
from scipy.stats import kruskal
from collections import defaultdict

DATASET_PATH = "DATA/4-inferred/INFERRED_MULTIMODAL_FINAL.jsonl"
FEATURES_CSV = "audit/subreddit_features_matrix.csv"
OUTPUT_IMG = "results/OBJ-I_Sentiment_CCDF_Taxonomy.png"
os.makedirs(os.path.dirname(OUTPUT_IMG), exist_ok=True)

STATES = ['POSITIVE', 'NEUTRAL', 'NEGATIVE']
TAXONOMIES = ['Hostile Echoes', 'Chronic Conflict', 'Passive Consumption', 'Constructive Deliberation', 'Other']

COLORS = sns.color_palette("viridis", len(TAXONOMIES))
COLOR_MAP = dict(zip(TAXONOMIES, COLORS))

def assign_taxonomy(row, x_mid, y_mid):
    x = row['Median_Virality']
    y = row['Global_Toxicity'] * 100
    if x > x_mid and y > y_mid:
        return 'Chronic Conflict'
    elif x > x_mid and y <= y_mid:
        return 'Constructive Deliberation'
    elif x <= x_mid and y > y_mid:
        return 'Hostile Echoes'
    else:
        return 'Passive Consumption'

def compute_ccdf(data):
    if len(data) == 0:
        return np.array([]), np.array([])
    if np.all(data == data[0]):
        x = np.array([data[0], data[0]])
        y = np.array([1.0, 0.0])
        return x, y
    sorted_x = np.sort(data)
    ccdf = 1.0 - np.arange(len(sorted_x)) / len(sorted_x)
    sorted_x = np.append(sorted_x, sorted_x[-1])
    ccdf = np.append(ccdf, 0.0)
    return sorted_x, ccdf

def main():
    print("\n" + "="*65)
    print(" CCDF OF SENTIMENT PROPORTIONS BY SUBREDDIT TAXONOMY ")
    print("="*65)

    # Load taxonomy from CSV
    sub_to_tax = {}
    try:
        df_feat = pd.read_csv(FEATURES_CSV)
        x_mid = df_feat['Median_Virality'].median()
        y_mid = (df_feat['Global_Toxicity'] * 100).median()
        for _, row in df_feat.iterrows():
            sub_to_tax[row['Subreddit']] = assign_taxonomy(row, x_mid, y_mid)
        print(f"[*] Loaded {len(sub_to_tax)} subreddits with taxonomy.")
    except FileNotFoundError:
        print("[!] CSV not found. All subreddits will be 'Other'.")

    # Count sentiments per subreddit (correct field)
    sub_counts = defaultdict(lambda: {s: 0 for s in STATES})
    total_lines = 0
    with open(DATASET_PATH, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            total_lines += 1
            try:
                rec = json.loads(line)
                if rec.get('type') == 'metadata_footer':
                    continue
                sub = rec.get('subreddit')
                if not sub:
                    continue
                # CORRECTED: sentiment is inside ai_analysis.label
                label = rec.get('ai_analysis', {}).get('label')
                if label not in STATES:
                    label = 'NEUTRAL'
                sub_counts[sub][label] += 1
            except:
                continue
    print(f"[*] Processed {total_lines} lines, found {len(sub_counts)} subreddits.")

    # Assign taxonomy: if sub not in sub_to_tax, use 'Other'
    all_subs = set(sub_counts.keys())
    for sub in all_subs:
        if sub not in sub_to_tax:
            sub_to_tax[sub] = 'Other'

    # Compute proportions per subreddit
    plot_data = []
    for sub, counts in sub_counts.items():
        total = sum(counts.values())
        if total == 0:
            continue
        tax = sub_to_tax[sub]
        for sent in STATES:
            prop = (counts[sent] / total) * 100.0
            plot_data.append({
                'Subreddit': sub,
                'Taxonomy': tax,
                'Sentiment': sent,
                'Proportion': prop
            })
    df_plot = pd.DataFrame(plot_data)
    print(f"[*] {len(df_plot)} rows (subreddit × sentiment).")

    # Debug print
    print("\n[DEBUG] Sample data (first 10 rows):")
    print(df_plot.head(10))

    print("\n[DEBUG] Proportion summary (mean, std) per sentiment/taxonomy:")
    for sent in STATES:
        for tax in TAXONOMIES:
            data = df_plot[(df_plot['Taxonomy'] == tax) & (df_plot['Sentiment'] == sent)]['Proportion'].values
            if len(data) > 0:
                print(f"    {sent} | {tax}: n={len(data)}, mean={np.mean(data):.2f}%, std={np.std(data):.2f}")
            else:
                print(f"    {sent} | {tax}: no data")

    # Plot CCDF with legend boxes BELOW each subplot (outside the plot area)
    print("\n[*] Generating CCDF plots (Viridis, no grid, xlim=0-70%)...")
    sns.set_theme(style="ticks")
    fig, axes = plt.subplots(1, 3, figsize=(18, 6), sharey=False)
    fig.suptitle("CCDF of Sentiment Proportions by Subreddit Taxonomy\nJosemar et al. (2025)",
                 fontsize=16, fontweight='bold', y=1.05)

    for idx, sentiment in enumerate(STATES):
        ax = axes[idx]
        groups = []
        all_props = []
        
        for tax in TAXONOMIES:
            proportions = df_plot[(df_plot['Taxonomy'] == tax) & (df_plot['Sentiment'] == sentiment)]['Proportion'].values
            if len(proportions) == 0:
                continue
            groups.append(proportions)
            all_props.extend(proportions)
            x_vals, y_vals = compute_ccdf(proportions)
            y_vals_percent = y_vals * 100
            mean_val = np.mean(proportions)
            std_val = np.std(proportions)
            label = rf"{tax} ($\mu={mean_val:.1f}\%$, $\sigma={std_val:.1f}\%$)"
            ax.step(x_vals, y_vals_percent, where='post',
                    color=COLOR_MAP[tax], linewidth=2.0,
                    label=label, alpha=0.9)

        # Fixed X axis from 0 to 70%
        ax.set_xlim(0, 70)
        ax.set_ylim(0, 100)
        ax.set_xlabel("Proportion in Subreddit (%)", fontsize=12)
        if idx == 0:
            ax.set_ylabel(r"CCDF (% of Subreddits $\geq x$)", fontsize=12)

        # Y ticks every 10%, X ticks every 10%
        ax.yaxis.set_major_locator(plt.MultipleLocator(10))
        ax.yaxis.set_major_formatter(mtick.PercentFormatter(decimals=0))
        ax.xaxis.set_major_locator(plt.MultipleLocator(10))
        ax.xaxis.set_major_formatter(mtick.PercentFormatter(decimals=0))

        # Kruskal-Wallis test title
        if len(groups) >= 2:
            try:
                stat, p = kruskal(*groups)
                if np.isnan(p):
                    p_str = "constant data"
                else:
                    p_str = f"{p:.2e}"
            except:
                p_str = "error"
            ax.set_title(f"Root: {sentiment}\n(Kruskal-Wallis p={p_str})", fontweight='bold', pad=10)
        else:
            ax.set_title(f"Root: {sentiment}", fontweight='bold', pad=10)

        # Legend BELOW the subplot (as a box, outside the plot)
        if groups:
            ax.legend(fontsize=9, framealpha=0.95, fancybox=True, shadow=True,
                     loc='upper center', bbox_to_anchor=(0.5, -0.15),
                     ncol=1)  # vertical block
        else:
            ax.text(0.5, 0.5, f"No subreddits with\n'{sentiment}' sentiment",
                    ha='center', va='center', transform=ax.transAxes,
                    fontsize=11, color='gray', style='italic')
            ax.set_xticks([])
            ax.set_yticks([])

    # Adjust layout to make room for legends below each subplot
    plt.tight_layout(rect=[0, 0, 1, 1])
    plt.subplots_adjust(bottom=0.12)  # Add space at bottom for legends
    plt.savefig(OUTPUT_IMG, dpi=300, bbox_inches='tight')
    sns.despine()
    print(f"\n[SUCCESS] Figure saved to: {os.path.abspath(OUTPUT_IMG)}")
if __name__ == "__main__":
    main()