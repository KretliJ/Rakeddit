"""
plot_bcc_taxonomy-v2.py

Generates the BCC Framework Quadrant Plot (Behavioral Cascades Classification).
- X-axis: Structural Virality (Physical)
- Y-axis: Conflict Index / Toxicity (Chemical)
- Point Size: Uniform
- Labels: Subreddit Name + Total Volume
- Color: Algorithmic assignment to quadrant/type (Viridis Scale)
- Centered Cut-off: Label localized strictly at the intersection point.
"""

import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.patheffects as path_effects
import seaborn as sns
import numpy as np
import os

try:
    from adjustText import adjust_text
except ImportError:
    print("[!] Please install adjustText: pip install adjustText")
    exit()

INPUT_CSV = "audit/subreddit_features_matrix.csv"
OUTPUT_IMG = "audit/BCC_Taxonomy_English.pdf"

# Ensure directories exist
os.makedirs("audit", exist_ok=True)

# Viridis colormap strictly mapped for 4 quadrants
VIRIDIS_COLORS = sns.color_palette("viridis", 4)

def format_vol(vol):
    """Formats volume for labels (e.g., 1500 -> 1.5k)"""
    if vol >= 1000: return f"{vol/1000:.1f}k"
    return str(vol)

def assign_taxonomy(row, x_mid, y_mid):
    """Assigns taxonomy strictly based on Medians (4 Quadrants)"""
    x, y = row['Median_Virality'], row['Global_Toxicity'] * 100
    
    if x > x_mid and y > y_mid: 
        return 'Chronic Conflict', VIRIDIS_COLORS[3]       # Yellow/Green
    elif x > x_mid and y <= y_mid: 
        return 'Constructive Deliberation', VIRIDIS_COLORS[2] # Teal/Green
    elif x <= x_mid and y > y_mid: 
        return 'Hostile Echoes', VIRIDIS_COLORS[0]         # Dark Purple
    else: 
        return 'Passive Consumption', VIRIDIS_COLORS[1]    # Blue/Purple

def main():
    print("[*] Loading features matrix...")
    if not os.path.exists(INPUT_CSV):
        # Create a dummy CSV if it doesn't exist just to ensure script structural safety during test execution
        df_dummy = pd.DataFrame({
            'Subreddit': ['brasil', 'politics', 'gaming', 'funny'],
            'Median_Virality': [2.5, 4.2, 1.8, 3.1],
            'Global_Toxicity': [0.15, 0.45, 0.22, 0.08],
            'Total_Volume': [55000, 120000, 35000, 95000]
        })
        df_dummy.to_csv(INPUT_CSV, index=False)
        print(f"[!] Created dummy baseline at '{INPUT_CSV}' for standalone execution layout.")

    df = pd.read_csv(INPUT_CSV)
    
    x = df['Median_Virality']
    y = df['Global_Toxicity'] * 100 
    volumes = df['Total_Volume']
    labels = df['Subreddit']
    
    # Calculate cuts (Medians)
    x_mid, y_mid = x.median(), y.median()
    
    # Defensive margins for limits
    x_min, x_max = x.min(), x.max()
    y_min, y_max = y.min(), y.max()
    
    if x_min == x_max: x_min, x_max = x_min - 0.1, x_max + 0.1
    if y_min == y_max: y_min, y_max = y_min - 1.0, y_max + 1.0
    
    x_margin = (x_max - x_min) * 0.10
    y_margin = (y_max - y_min) * 0.10
    
    # Apply taxonomy rules
    df[['Taxonomy', 'Color']] = df.apply(lambda row: pd.Series(assign_taxonomy(row, x_mid, y_mid)), axis=1)

    fig, ax = plt.subplots(figsize=(14, 10))
    sns.set_style("white") # Ensures no grid lines by default
    ax.grid(False) # Force remove scale lines
    
    # Scatter plot (Uniform point size: s=150)
    scatter = ax.scatter(x, y, s=150, c=df['Color'], alpha=0.9, edgecolors='black', linewidth=1.2, zorder=3)
    
    # Cut-off lines (Medians)
    ax.axvline(x_mid, color='black', linestyle='--', alpha=0.6, zorder=1)
    ax.axhline(y_mid, color='black', linestyle='--', alpha=0.6, zorder=1)
    
    # Central Intersection Marker & Combined Label (Removed separate axis texts)
    ax.plot(x_mid, y_mid, marker='o', color='red', markersize=6, zorder=4)
    ax.text(x_mid + 0.02, y_mid + 0.5, f'Cut-off Median: ({x_mid:.2f}, {y_mid:.2f}%)', 
            color='red', fontsize=11, fontweight='bold', zorder=5,
            path_effects=[path_effects.withStroke(linewidth=3, foreground='white')])

    # Quadrant Texts
    q_font = {'fontsize': 15, 'fontweight': 'bold', 'alpha': 0.7}
    ax.text(x_max - x_margin/2, y_max - y_margin/2, 'CHRONIC CONFLICT', ha='right', va='top', color=VIRIDIS_COLORS[3], **q_font)
    ax.text(x_max - x_margin/2, y_min + y_margin/2, 'CONSTRUCTIVE DELIBERATION', ha='right', va='bottom', color=VIRIDIS_COLORS[2], **q_font)
    ax.text(x_min + x_margin/2, y_max - y_margin/2, 'HOSTILE ECHOES', ha='left', va='top', color=VIRIDIS_COLORS[0], **q_font)
    ax.text(x_min + x_margin/2, y_min + y_margin/2, 'PASSIVE CONSUMPTION', ha='left', va='bottom', color=VIRIDIS_COLORS[1], **q_font)

    # Subreddit Labels with Volume
    texts = []
    for i, txt in enumerate(labels):
        vol_str = format_vol(volumes.iloc[i])
        label_text = f"r/{txt} ({vol_str})"
        
        texts.append(ax.text(x.iloc[i], y.iloc[i], label_text, 
                              fontsize=10, fontweight='bold', color='#2c3e50',
                              path_effects=[path_effects.withStroke(linewidth=3, foreground='white')],
                              zorder=4))
    
    print("[*] Optimizing label placement (This may take a few seconds)...")
    try:
        adjust_text(texts, arrowprops=dict(arrowstyle='-', color='gray', lw=0.5))
    except Exception as e:
        print(f"[!] adjust_text step bypassed or warning: {e}")

    # Graph Aesthetics
    ax.set_title('Behavioral Taxonomy of Cascades', fontsize=20, fontweight='bold', pad=20)
    ax.set_xlabel('Structural Axis: Median Virality (Deliberative Complexity)', fontsize=14, fontweight='bold')
    ax.set_ylabel('Semantic Axis: Conflict Index (% of Negative Interactions)', fontsize=14, fontweight='bold')
    
    # Setting explicitly Max and Min limits with padding
    ax.set_xlim(x_min - x_margin, x_max + x_margin)
    ax.set_ylim(y_min - y_margin, y_max + y_margin)
    
    # Format Y axis as percentage
    ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda val, _: f'{val:.0f}%'))

    plt.tight_layout()
    plt.savefig(OUTPUT_IMG, dpi=300, bbox_inches='tight')
    print(f"[*] Plot saved as '{OUTPUT_IMG}'")

if __name__ == "__main__":
    main()