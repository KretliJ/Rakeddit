"""
markov_transitions_heatmap.py

Calcula e visualiza Cadeias de Markov de transição de sentimento (Pai -> Filho)
comparando os diferentes quadrantes.
Gera Matrizes de Calor (Heatmaps) em linha com paleta viridis.
"""

import json
import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns

# ==========================================
# CONFIGURATION
# ==========================================
DATASET_PATH = "DATA/4-inferred/INFERRED_MULTIMODAL_FINAL.jsonl"
FEATURES_CSV = "audit/subreddit_features_matrix.csv"
OUTPUT_IMG = "results/Markov_Heatmaps.png"

STATES = ['NEGATIVE', 'NEUTRAL', 'POSITIVE']
STATE_LABELS = ['NEG', 'NEU', 'POS']  # Abreviado para caber melhor

# Original BCC taxonomy quadrants (em CAIXA ALTA)
TAXONOMIES = ['HOSTILE ECHOES', 'CHRONIC CONFLICT', 'PASSIVE CONSUMPTION', 'CONSTRUCTIVE DELIBERATION']

def assign_taxonomy(row, x_mid, y_mid):
    """Assign taxonomy based on medians of Structural Virality and Global Toxicity."""
    x = row['Median_Virality']
    y = row['Global_Toxicity'] * 100
    if x > x_mid and y > y_mid:
        return 'CHRONIC CONFLICT'
    elif x > x_mid and y <= y_mid:
        return 'CONSTRUCTIVE DELIBERATION'
    elif x <= x_mid and y > y_mid:
        return 'HOSTILE ECHOES'
    else:
        return 'PASSIVE CONSUMPTION'

def main():
    print("\n" + "="*60)
    print(" MARKOV TRANSITION HEATMAPS BY TAXONOMY ")
    print("="*60)

    # ------------------------------------------------------------------
    # 1. Load Taxonomy from CSV
    # ------------------------------------------------------------------
    print("[1] Loading subreddit taxonomy...")
    df = pd.read_csv(FEATURES_CSV)
    x = df['Median_Virality']
    y = df['Global_Toxicity'] * 100
    x_mid, y_mid = x.median(), y.median()
    
    print(f"    Median Virality = {x_mid:.4f}")
    print(f"    Median Toxicity = {y_mid:.2f}%")

    sub_to_tax = {
        row['Subreddit']: assign_taxonomy(row, x_mid, y_mid)
        for _, row in df.iterrows()
    }
    
    # Count subreddits per taxonomy
    print("\n[2] Subreddit distribution by taxonomy:")
    for tax in TAXONOMIES:
        count = sum(1 for v in sub_to_tax.values() if v == tax)
        print(f"    {tax}: {count} subreddits")

    # ------------------------------------------------------------------
    # 2. Load nodes and their sentiments
    # ------------------------------------------------------------------
    print("\n[3] Loading sentiment data from JSONL...")
    node_memory = {}

    with open(DATASET_PATH, 'r', encoding='utf-8') as f:
        for line in f:
            if not line.strip():
                continue
            try:
                record = json.loads(line)
                if record.get('type') == 'metadata_footer':
                    continue
                sub = record.get('subreddit')
                if not sub or sub not in sub_to_tax:
                    continue
                label = record.get('ai_analysis', {}).get('label')
                if label in ['POSITIVE', 'NEUTRAL', 'NEGATIVE']:
                    # Convert to abbreviated form for consistency
                    if label == 'POSITIVE':
                        label_abbr = 'POSITIVE'
                    elif label == 'NEUTRAL':
                        label_abbr = 'NEUTRAL'
                    else:
                        label_abbr = 'NEGATIVE'
                    node_memory[record['id']] = {
                        'label': label_abbr,
                        'parent': record.get('parent_id'),
                        'tax': sub_to_tax[sub],
                    }
            except Exception:
                continue

    print(f"    Loaded {len(node_memory):,} nodes with sentiment labels.")

    # ------------------------------------------------------------------
    # 3. Count transitions (Parent -> Child)
    # ------------------------------------------------------------------
    print("\n[4] Computing transition probabilities...")
    transitions = {
        tax: {p: {c: 0 for c in STATES} for p in STATES}
        for tax in TAXONOMIES
    }

    for n_id, data in node_memory.items():
        p_id = data['parent']
        if p_id and p_id in node_memory:
            parent_label = node_memory[p_id]['label']
            child_label = data['label']
            transitions[data['tax']][parent_label][child_label] += 1

    # ------------------------------------------------------------------
    # 4. Convert to probability matrices
    # ------------------------------------------------------------------
    prob_matrices = {}
    for tax in TAXONOMIES:
        matrix = transitions[tax]
        prob_matrix = pd.DataFrame(matrix).T
        prob_matrix = prob_matrix.div(prob_matrix.sum(axis=1), axis=0).fillna(0)
        prob_matrix = prob_matrix.reindex(index=STATES, columns=STATES)
        prob_matrices[tax] = prob_matrix

    # ------------------------------------------------------------------
    # 5. Plot Heatmaps in a SINGLE ROW (1x4)
    # ------------------------------------------------------------------
    print("\n[5] Generating heatmaps in horizontal layout (1x4)...")
    
    fig, axes = plt.subplots(1, 4, figsize=(20, 5))
    
    for idx, tax in enumerate(TAXONOMIES):
        ax = axes[idx]
        matrix = prob_matrices[tax] * 100  # Convert to percentage
        
        # Create heatmap with larger annotations
        sns.heatmap(
            matrix,
            annot=True,
            fmt=".1f",
            cmap="viridis",
            cbar=(idx == 3),  # Show colorbar only on the last plot
            cbar_kws={'label': 'Transition Probability (%)', 'shrink': 0.8} if idx == 3 else None,
            ax=ax,
            vmin=0,
            vmax=100,
            annot_kws={'size': 14, 'weight': 'bold'},
            xticklabels=STATE_LABELS,
            yticklabels=STATE_LABELS,
            square=True,  # Make cells square
        )
        
        # Title with taxonomy name (already in uppercase)
        ax.set_title(tax, fontsize=13, fontweight='bold', pad=15)
        
        # Axis labels with abbreviations
        if idx == 0:
            ax.set_ylabel("P SENT", fontsize=12, fontweight='bold')
        else:
            ax.set_ylabel("")
        
        ax.set_xlabel("C SENT", fontsize=12, fontweight='bold')
        
        # Increase tick label size
        ax.tick_params(labelsize=11, rotation=0)
    
    # Overall title
    plt.suptitle(
        "Markov Transition Probabilities: Parent → Child Sentiment\nBy Taxonomy Quadrant",
        fontsize=16, fontweight='bold', y=1.05
    )
    
    plt.tight_layout()
    plt.savefig(OUTPUT_IMG, dpi=300, bbox_inches='tight')
    print(f"\n[SUCCESS] Heatmaps saved to: {os.path.abspath(OUTPUT_IMG)}")

    # ------------------------------------------------------------------
    # 6. Print transition matrices as text
    # ------------------------------------------------------------------
    print("\n[6] Transition probability matrices (as percentages):")
    for tax in TAXONOMIES:
        print(f"\n--- {tax} ---")
        matrix_pct = prob_matrices[tax] * 100
        # Rename index/columns for cleaner display
        matrix_pct.index = STATE_LABELS
        matrix_pct.columns = STATE_LABELS
        print(matrix_pct.round(1).to_string())
        
        # Row sums verification
        print(f"\n    Row sums: {matrix_pct.sum(axis=1).round(1).to_list()}")

if __name__ == "__main__":
    main()