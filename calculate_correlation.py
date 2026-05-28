"""
calculate_correlation.py
Calcula a correlação estatística entre o Índice de Conflito e a Viralidade.
"""

import pandas as pd
from scipy.stats import pearsonr, spearmanr

INPUT_CSV = "results/subreddit_features_matrix.csv"

def calculate_trend():
    print("[*] Lendo a Matriz de Características...\n")
    df = pd.read_csv(INPUT_CSV)
    
    # Extraindo as duas variáveis
    viralidade = df['Median_Virality']
    conflito = df['Global_Toxicity']
    
    # Correlação de Pearson (Mede a relação linear direta)
    pearson_coef, p_p = pearsonr(viralidade, conflito)
    
    # Correlação de Spearman (Mede a relação monotônica, ideal para dados com outliers)
    spearman_coef, p_s = spearmanr(viralidade, conflito)
    
    print("="*50)
    print(" 📈 ANÁLISE DE CORRELAÇÃO: VIRALIDADE vs CONFLITO ")
    print("="*50)
    print(f"Coeficiente de Pearson:  {pearson_coef:.4f} (p-value: {p_p:.4f})")
    print(f"Coeficiente de Spearman: {spearman_coef:.4f} (p-value: {p_s:.4f})")
    print("-" * 50)
    
    if pearson_coef > 0.5:
        print("[+] Conclusão Estatística: Forte correlação POSITIVA.")
        print("    O atrito semântico impulsiona diretamente a complexidade estrutural.")

if __name__ == "__main__":
    calculate_trend()