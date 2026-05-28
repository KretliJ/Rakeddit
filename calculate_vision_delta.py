"""
calculate_vision_delta.py

O Clímax do TCC.
Cruza o dataset Multimodal (com visão) e o dataset Cego ponto a ponto.
Calcula a taxa de divergência, a Matriz de Confusão de Sentimento e o Impacto por Subreddit.
"""

import json
import os
import pandas as pd
from collections import defaultdict, Counter

# ==========================================
# CONFIGURAÇÃO DOS ARQUIVOS
# ==========================================
# A Fonte da Verdade (O que a IA viu com imagens)
MULTIMODAL_FILE = "DATA/4-inferred/INFERRED_MULTIMODAL_FINAL.jsonl"

# O Dataset Cego Inferido (Substitua pelo nome real gerado pela sua pipeline)
BLIND_FILE = "DATA/4-inferred/INFERRED_PERFECT_BLIND_DATASET.jsonl" 

OUTPUT_CSV = "DATA/4-inferred/results_vision_delta_by_subreddit.csv"

def calculate_delta():
    print("\n" + "="*60)
    print(" 👁️ vs 🙈 CÁLCULO DO DELTA DE COMPREENSÃO MULTIMODAL ")
    print("="*60)

    if not os.path.exists(MULTIMODAL_FILE) or not os.path.exists(BLIND_FILE):
        print("[-] Erro: Arquivos não encontrados. Verifique os caminhos.")
        return

    # 1. Carregar a memória do Dataset Cego
    print(f"[*] A carregar a inferência cega para a RAM: {os.path.basename(BLIND_FILE)}")
    blind_memory = {}
    with open(BLIND_FILE, 'r', encoding='utf-8') as f:
        for line in f:
            if not line.strip(): continue
            try:
                record = json.loads(line)
                n_id = record.get("id")
                if n_id and "ai_analysis" in record:
                    blind_memory[n_id] = record["ai_analysis"].get("label", "UNKNOWN")
            except: continue

    print(f"[+] Nós cegos processados: {len(blind_memory):,}")

    # 2. Varrer o Multimodal e Comparar
    print(f"\n[*] A cruzar com a Fonte da Verdade: {os.path.basename(MULTIMODAL_FILE)}")
    
    total_compared = 0
    total_divergence = 0
    
    # Matriz de Confusão: transitions[cega][multimodal]
    transitions = defaultdict(Counter)
    
    # Estatísticas por subreddit
    sub_totals = Counter()
    sub_divergences = Counter()

    with open(MULTIMODAL_FILE, 'r', encoding='utf-8') as f:
        for line in f:
            if not line.strip(): continue
            try:
                record = json.loads(line)
                n_id = record.get("id")
                sub = record.get("subreddit")
                
                if n_id and sub and "ai_analysis" in record:
                    mm_label = record["ai_analysis"].get("label", "UNKNOWN")
                    blind_label = blind_memory.get(n_id)
                    
                    if blind_label and mm_label in ['POSITIVE', 'NEGATIVE', 'NEUTRAL'] and blind_label in ['POSITIVE', 'NEGATIVE', 'NEUTRAL']:
                        total_compared += 1
                        sub_totals[sub] += 1
                        
                        # Matriz
                        transitions[blind_label][mm_label] += 1
                        
                        # Divergência
                        if mm_label != blind_label:
                            total_divergence += 1
                            sub_divergences[sub] += 1
                            
            except: continue

    # 3. Relatório Analítico
    if total_compared == 0:
        print("[-] Erro: Nenhum nó correspondente encontrado para comparação.")
        return

    global_delta = (total_divergence / total_compared) * 100

    print("\n" + "="*60)
    print(" 📊 RESULTADOS GLOBAIS ")
    print("="*60)
    print(f"Total de interações comparadas: {total_compared:,}")
    print(f"Total de divergências (IA mudou de ideias): {total_divergence:,}")
    print(f"Taxa de Impacto da Visão (Delta Global): {global_delta:.2f}%")

    print("\n" + "-"*60)
    print(" 🔄 MATRIZ DE TRANSIÇÃO (Cego -> Com Visão)")
    print("-" * 60)
    for b_lbl in ['POSITIVE', 'NEUTRAL', 'NEGATIVE']:
        print(f"Quando a IA Cega disse {b_lbl}, com visão ela percebeu que era:")
        for mm_lbl in ['POSITIVE', 'NEUTRAL', 'NEGATIVE']:
            count = transitions[b_lbl][mm_lbl]
            if count > 0:
                print(f"  -> {mm_lbl}: {count:,} vezes")

    # 4. Impacto por Subreddit
    print("\n" + "-"*60)
    print(" 🏆 DEPENDÊNCIA VISUAL POR SUBREDDIT ")
    print("-" * 60)
    
    sub_data = []
    for sub, total in sub_totals.items():
        divs = sub_divergences[sub]
        delta_pct = (divs / total) * 100
        sub_data.append({
            "Subreddit": sub,
            "Total_Nodes": total,
            "Divergences": divs,
            "Vision_Dependency_Rate_Pct": delta_pct
        })
        
    # Ordenar pelos que mais mudaram de opinião
    sub_data.sort(key=lambda x: x["Vision_Dependency_Rate_Pct"], reverse=True)
    
    for row in sub_data:
        print(f"r/{row['Subreddit']:<18} | Delta: {row['Vision_Dependency_Rate_Pct']:.2f}% ({row['Divergences']:,} mudanças)")

    # 5. Exportar CSV
    df = pd.DataFrame(sub_data)
    df.to_csv(OUTPUT_CSV, index=False)
    print(f"\n[+] Relatório por subreddit salvo em: {OUTPUT_CSV}")

if __name__ == "__main__":
    calculate_delta()