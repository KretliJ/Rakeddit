"""
summary_table.py

Generates a summary table: Taxonomy Quadrant | # Subreddits | # Messages | # Unique Users
Based on the BCC taxonomy from subreddit_features_matrix.csv
"""

import json
import os
import pandas as pd
from collections import defaultdict

# ==========================================
# CONFIGURATION
# ==========================================
DATASET_PATH = "DATA/4-inferred/INFERRED_MULTIMODAL_FINAL.jsonl"
FEATURES_CSV = "audit/subreddit_features_matrix.csv"
OUTPUT_TABLE = "results/taxonomy_summary_table.csv"
OUTPUT_IMAGE = "results/taxonomy_summary_table.png"  # optional, for paper

os.makedirs(os.path.dirname(OUTPUT_TABLE), exist_ok=True)

TAXONOMIES = ['Hostile Echoes', 'Chronic Conflict', 'Passive Consumption', 'Constructive Deliberation']

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

def main():
    print("\n" + "="*60)
    print(" TAXONOMY SUMMARY TABLE GENERATOR ")
    print("="*60)

    # 1. Load taxonomy from CSV
    print("[1] Loading subreddit taxonomy...")
    try:
        df_feat = pd.read_csv(FEATURES_CSV)
        x_mid = df_feat['Median_Virality'].median()
        y_mid = (df_feat['Global_Toxicity'] * 100).median()
        sub_to_tax = {}
        for _, row in df_feat.iterrows():
            sub = row['Subreddit']
            sub_to_tax[sub] = assign_taxonomy(row, x_mid, y_mid)
        print(f"    Loaded {len(sub_to_tax)} subreddits with taxonomy.")
    except FileNotFoundError:
        print(f"[ERROR] CSV not found: {FEATURES_CSV}")
        return

    # 2. Process JSONL to count messages and unique users per subreddit
    print("[2] Processing messages and users per subreddit...")
    sub_msgs = defaultdict(int)      # total messages per subreddit
    sub_users = defaultdict(set)     # unique users per subreddit

    with open(DATASET_PATH, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
                if rec.get('type') == 'metadata_footer':
                    continue
                sub = rec.get('subreddit')
                if not sub or sub not in sub_to_tax:
                    continue
                author = rec.get('author')
                sub_msgs[sub] += 1
                if author and author != '[deleted]':
                    sub_users[sub].add(author)
            except:
                continue

    print(f"    Processed {sum(sub_msgs.values()):,} messages across {len(sub_msgs)} subreddits.")

    # 3. Aggregate by taxonomy quadrant
    print("[3] Aggregating by taxonomy quadrant...")
    quad_stats = defaultdict(lambda: {
        'subreddits': set(),
        'total_messages': 0,
        'total_users': set()
    })

    for sub in sub_to_tax.keys():
        tax = sub_to_tax[sub]
        quad_stats[tax]['subreddits'].add(sub)
        quad_stats[tax]['total_messages'] += sub_msgs.get(sub, 0)
        quad_stats[tax]['total_users'].update(sub_users.get(sub, set()))

    # 4. Build DataFrame
    rows = []
    for tax in TAXONOMIES:
        stats = quad_stats[tax]
        rows.append({
            'Taxonomy Quadrant': tax,
            '# Subreddits': len(stats['subreddits']),
            '# Messages': stats['total_messages'],
            '# Unique Users': len(stats['total_users'])
        })

    df_summary = pd.DataFrame(rows)
    
    # Add total row
    total_row = pd.DataFrame([{
        'Taxonomy Quadrant': 'TOTAL',
        '# Subreddits': df_summary['# Subreddits'].sum(),
        '# Messages': df_summary['# Messages'].sum(),
        '# Unique Users': df_summary['# Unique Users'].sum()
    }])
    df_summary = pd.concat([df_summary, total_row], ignore_index=True)

    # 5. Save and display
    print("\n[4] Summary Table:")
    print(df_summary.to_string(index=False))
    
    # Format numbers with thousand separators for CSV
    df_summary['# Messages'] = df_summary['# Messages'].apply(lambda x: f"{x:,}")
    df_summary['# Unique Users'] = df_summary['# Unique Users'].apply(lambda x: f"{x:,}")
    
    df_summary.to_csv(OUTPUT_TABLE, index=False)
    print(f"\n[SUCCESS] Table saved to: {os.path.abspath(OUTPUT_TABLE)}")

    # Optional: generate a figure for the paper
    print("\n[5] Generating figure for paper...")
    fig, ax = plt.subplots(figsize=(10, 3))
    ax.axis('tight')
    ax.axis('off')
    
    # Recompute without formatting for display
    df_display = pd.DataFrame(rows)
    df_display.loc[len(df_display)] = ['TOTAL', 
                                        df_display['# Subreddits'].sum(), 
                                        df_display['# Messages'].sum(), 
                                        df_display['# Unique Users'].sum()]
    
    table = ax.table(cellText=df_display.values,
                     colLabels=df_display.columns,
                     cellLoc='center',
                     loc='center',
                     colWidths=[0.25, 0.15, 0.3, 0.3])
    table.auto_set_font_size(False)
    table.set_fontsize(10)
    table.scale(1.2, 1.5)
    
    plt.title("Table 1: Taxonomy Quadrant Summary", fontsize=14, fontweight='bold', pad=20)
    plt.tight_layout()
    plt.savefig(OUTPUT_IMAGE, dpi=300, bbox_inches='tight')
    print(f"[SUCCESS] Figure saved to: {os.path.abspath(OUTPUT_IMAGE)}")

if __name__ == "__main__":
    import matplotlib.pyplot as plt
    main()