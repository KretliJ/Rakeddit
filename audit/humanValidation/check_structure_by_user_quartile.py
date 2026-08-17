# check_structure_by_user_quartile.py
# Calcula estrutura das cascatas agrupadas pelo quartil de usuário dominante (UQ1-UQ4)

import os
import sys
import pandas as pd
import numpy as np

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from Utilities import Config

def main():
    cache_path = Config.CACHE_PATH
    if not os.path.exists(cache_path):
        print(f"❌ Cache de cascatas não encontrado em: {cache_path}")
        return

    print("[*] Carregando cache de cascatas...")
    df_cascades = pd.read_parquet(cache_path)
    print(f"    {len(df_cascades):,} cascatas carregadas.")

    # Tenta carregar o cache NLP (comentários com User_Type)
    nlp_cache = os.path.join(Config.RESULTS_DIR, "nlp_dataframe_cache.parquet")
    if not os.path.exists(nlp_cache):
        print("❌ Cache NLP não encontrado. Não é possível mapear usuários por cascata.")
        print("   Execute o pipeline NLP (Analytical_NLP_Engine.py) primeiro.")
        return

    print("[*] Carregando cache NLP (comentários)...")
    df_comments = pd.read_parquet(nlp_cache)
    print(f"    {len(df_comments):,} comentários carregados.")

    # Mapeia User_Type para número (UQ1=1, UQ2=2, UQ3=3, UQ4=4)
    user_type_map = {'UQ1': 1, 'UQ2': 2, 'UQ3': 3, 'UQ4': 4}
    df_comments['user_type_num'] = df_comments['User_Type'].map(user_type_map)

    # Remove linhas com User_Type nulo (ex: usuários sem classificação)
    df_comments = df_comments.dropna(subset=['user_type_num'])

    # Converte explicitamente para float (para evitar erro de categoria)
    df_comments['user_type_num'] = df_comments['user_type_num'].astype(float)

    # Agrupa por Cascade_ID e calcula a média do user_type_num
    cascade_user_avg = df_comments.groupby('Cascade_ID')['user_type_num'].mean().reset_index()

    # Atribui quartil de usuário com base na média
    cascade_user_avg['user_quartile'] = pd.cut(
        cascade_user_avg['user_type_num'],
        bins=[0, 1.5, 2.5, 3.5, 4.5],
        labels=['UQ1', 'UQ2', 'UQ3', 'UQ4'],
        include_lowest=True
    )

    # Junta com as métricas estruturais
    df_merged = df_cascades.merge(cascade_user_avg, on='Cascade_ID', how='inner')
    print(f"[*] {len(df_merged):,} cascatas com dados de usuário mapeados.")

    if df_merged.empty:
        print("❌ Nenhuma cascata com dados de usuário. Verifique o mapeamento.")
        return

    print("\n" + "="*80)
    print("ESTRUTURA DAS CASCATAS POR QUARTIL DE USUÁRIO DOMINANTE (UQ1-UQ4)")
    print("="*80)

    metrics = ['Cascade_Size', 'Structural_Virality', 'Max_Depth', 'Max_Breadth']
    quartiles = ['UQ1', 'UQ2', 'UQ3', 'UQ4']

    # Tabela de médias
    results = {}
    for q in quartiles:
        subset = df_merged[df_merged['user_quartile'] == q]
        n = len(subset)
        if n == 0:
            print(f"\n--- {q} (N=0) ---")
            continue
        print(f"\n--- {q} (N={n:,} cascatas) ---")
        row = {'N': n}
        for m in metrics:
            mean_val = subset[m].mean()
            median_val = subset[m].median()
            std_val = subset[m].std()
            print(f"  {m:20s}: Média = {mean_val:.4f} | Mediana = {median_val:.4f} | Desv. = {std_val:.4f}")
            row[m] = mean_val
        results[q] = row

    # Comparação UQ3 vs UQ4
    uq3 = df_merged[df_merged['user_quartile'] == 'UQ3']
    uq4 = df_merged[df_merged['user_quartile'] == 'UQ4']

    if len(uq3) > 0 and len(uq4) > 0:
        print("\n" + "="*80)
        print("COMPARAÇÃO DIRETA: UQ3 (pico esperado) vs UQ4 (colapso esperado)")
        print("="*80)

        for m in metrics:
            m3 = uq3[m].mean()
            m4 = uq4[m].mean()
            diff = m3 - m4
            pct = (diff / m4) * 100 if m4 != 0 else 0
            flag = "🔴 COLAPSO CONFIRMADO" if diff > 0 else "⚠️ ATENÇÃO: UQ3 < UQ4"
            print(f"  {m:20s}: UQ3 = {m3:.4f} | UQ4 = {m4:.4f} | Δ = {diff:+.4f} ({pct:+.2f}%) {flag}")
    else:
        print("\n⚠️  Não há cascatas suficientes em UQ3 ou UQ4 para comparação.")

if __name__ == "__main__":
    main()