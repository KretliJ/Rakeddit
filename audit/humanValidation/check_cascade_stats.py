# check_cascade_stats.py
# Calcula estatísticas descritivas das cascatas por quartil de negatividade.
# Rodar com: python check_cascade_stats.py

import os
import sys
import pandas as pd
import numpy as np

# Sobe um nível para importar o Utilities.py (que está na pasta audit)
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from Utilities import Config

def main():
    cache_path = Config.CACHE_PATH

    if not os.path.exists(cache_path):
        print(f"❌ Cache não encontrado em: {cache_path}")
        print("   Execute o pipeline principal (Methods.py) para gerar o cache primeiro.")
        return

    print(f"[*] Carregando cache: {cache_path}")
    df = pd.read_parquet(cache_path)
    print(f"[*] Total de cascatas carregadas: {len(df):,}")

    # Define os quartis com base no Perc_Negative (mesma lógica do paper)
    bins = [-1.0, 25.0, 50.0, 75.0, 100.0]
    labels = ['Q1', 'Q2', 'Q3', 'Q4']
    df['quartile'] = pd.cut(df['Perc_Negative'], bins=bins, labels=labels, include_lowest=True)

    # Métricas estruturais que você usa no paper (Figura 1)
    metrics = ['Cascade_Size', 'Structural_Virality', 'Max_Depth', 'Max_Breadth']

    print("\n" + "="*80)
    print("ESTATÍSTICAS DESCRITIVAS POR QUARTIL DE NEGATIVIDADE")
    print("="*80)

    for q in labels:
        subset = df[df['quartile'] == q]
        count = len(subset)
        print(f"\n--- {q} (N={count:,} cascatas) ---")
        for m in metrics:
            mean_val = subset[m].mean()
            median_val = subset[m].median()
            std_val = subset[m].std()
            print(f"  {m:20s}: Média = {mean_val:.4f} | Mediana = {median_val:.4f} | Desv. Padrão = {std_val:.4f}")

    # Comparação direta Q3 vs Q4 (o coração da sua tese)
    q3 = df[df['quartile'] == 'Q3']
    q4 = df[df['quartile'] == 'Q4']

    print("\n" + "="*80)
    print("COMPARAÇÃO DIRETA: Q3 (pico) vs Q4 (colapso)")
    print("="*80)

    for m in metrics:
        q3_mean = q3[m].mean()
        q4_mean = q4[m].mean()
        diff = q3_mean - q4_mean
        pct_diff = (diff / q4_mean) * 100 if q4_mean != 0 else float('inf')

        # Marca com emoji se a diferença for grande
        flag = "🔴 COLAPSO CONFIRMADO" if diff > 0 else "⚠️ ATENÇÃO: Q3 < Q4"
        print(f"  {m:20s}: Q3 = {q3_mean:.4f} | Q4 = {q4_mean:.4f} | Δ = {diff:+.4f} ({pct_diff:+.2f}%) {flag}")

    # Bônus: mostra o número de cascatas em cada quartil (já mostramos, mas reforça)
    print("\n" + "="*80)
    print("DISTRIBUIÇÃO DAS CASCATAS POR QUARTIL")
    print("="*80)
    for q in labels:
        count = len(df[df['quartile'] == q])
        pct = (count / len(df)) * 100
        print(f"  {q}: {count:,} cascatas ({pct:.2f}% do total)")

if __name__ == "__main__":
    main()