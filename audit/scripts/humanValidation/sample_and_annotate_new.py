#!/usr/bin/env python3
# sample_and_annotate_new.py
# Sorteia 1000 comentários aleatórios do dataset completo (seed 123).
# Apresenta para anotação manual (sem mostrar o label da IA).
# Depois extrai os mesmos IDs dos datasets XLM e BERTabaporu para comparação.

import json
import random
import os
import sys

# ============================================================
# CONFIGURAÇÕES
# ============================================================

# Dataset completo (fonte para sortear)
COMPLETE_DATASET = "XLMR_QWEN3_MULTIMODAL_FINAL.jsonl"

# Datasets dos modelos
XLM_DATASET = "XLMR_QWEN3_MULTIMODAL_FINAL.jsonl"  # XLM-RoBERTa (original)
BERTA_DATASET = "BERTabaporu_QWEN3_MULTIMODAL_FINAL.jsonl"  # BERTabaporu

# Arquivos de saída
OUTPUT_SAMPLED = "RANDOM_SAMPLE_1000.jsonl"  # Comentários sorteados (para anotação)
OUTPUT_GT = "HUMAN_GROUND_TRUTH_RANDOM_1000.jsonl"  # Rótulos humanos
OUTPUT_XLM = "RANDOM_SAMPLE_XLM_1000.jsonl"  # Mesmos IDs, do XLM
OUTPUT_BERTA = "RANDOM_SAMPLE_BERTA_1000.jsonl"  # Mesmos IDs, do BERTabaporu

# Configuração
SAMPLE_SIZE = 1000
RANDOM_SEED = 123

# Labels
VALID_LABELS = {"POS", "NEU", "NEG"}
LABEL_DESCRIPTIONS = {
    "POS": "Contexto/sentimento positivo, uso de palavras positivas",
    "NEG": "Contexto/sentimento negativo, uso de palavras negativas, ofensas",
    "NEU": "Nenhum dos dois / incerto / objetivo"
}

# ============================================================
# FUNÇÕES
# ============================================================

def load_all_comments(filepath):
    """Carrega todos os comentários válidos de um JSONL."""
    comments = []
    with open(filepath, 'r', encoding='utf-8') as f:
        for line in f:
            try:
                rec = json.loads(line)
                if rec.get('type') == 'comment' and rec.get('is_valid_text'):
                    comments.append(rec)
            except json.JSONDecodeError:
                continue
    print(f"[*] Carregados {len(comments):,} comentários válidos")
    return comments

def sample_random(comments, sample_size=1000, seed=42):
    """Sorteia uma amostra aleatória."""
    random.seed(seed)
    if len(comments) <= sample_size:
        return comments
    return random.sample(comments, sample_size)

def load_previous_annotations(output_file):
    """Carrega anotações anteriores."""
    annotated = {}
    if os.path.exists(output_file):
        with open(output_file, 'r', encoding='utf-8') as f:
            for line in f:
                try:
                    rec = json.loads(line)
                    cid = rec.get('id')
                    if cid and 'human_label' in rec and rec['human_label'] is not None:
                        annotated[cid] = rec['human_label']
                except json.JSONDecodeError:
                    continue
        print(f"[*] Carregadas {len(annotated)} anotações anteriores")
    return annotated

def save_progress(output_file, all_comments, annotated_dict):
    """Salva todas as anotações."""
    with open(output_file, 'w', encoding='utf-8') as f:
        for rec in all_comments:
            rec_copy = rec.copy()
            rec_copy['human_label'] = annotated_dict.get(rec['id'])
            f.write(json.dumps(rec_copy, ensure_ascii=False) + '\n')
    print(f"[*] Progresso salvo em {os.path.basename(output_file)}")

def annotate_comments(sampled_comments, output_file):
    """Apresenta comentários para anotação manual."""
    
    print("\n" + "="*60)
    print("  VALIDAÇÃO MANUAL — 1000 COMENTÁRIOS ALEATÓRIOS")
    print("="*60)
    print("\nInstruções:")
    print("  POS - Contexto/sentimento positivo, uso de palavras positivas")
    print("  NEG - Contexto/sentimento negativo, uso de palavras negativas, ofensas")
    print("  NEU - Nenhum dos dois / incerto / objetivo")
    print("\nComandos: POS, NEG, NEU, 'next' (skip), 'prev' (voltar), 'save' (salvar e sair)")
    print("="*60 + "\n")
    
    annotated = load_previous_annotations(output_file)
    to_annotate = [c for c in sampled_comments if c['id'] not in annotated]
    
    if not to_annotate:
        print("[*] Todos os comentários já foram anotados!")
        return annotated
    
    print(f"[*] Restam {len(to_annotate)} comentários para anotar.")
    
    idx = 0
    total = len(to_annotate)
    
    while idx < total:
        rec = to_annotate[idx]
        cid = rec['id']
        body = rec.get('body', '')
        
        print(f"\n--- Comentário {idx+1}/{total} (ID: {cid}) ---")
        print(f"Corpo: {body[:500]}{'...' if len(body) > 500 else ''}")
        print(f"POS: {LABEL_DESCRIPTIONS['POS']}")
        print(f"NEG: {LABEL_DESCRIPTIONS['NEG']}")
        print(f"NEU: {LABEL_DESCRIPTIONS['NEU']}")
        
        user_input = input("> ").strip().upper()
        
        if user_input in VALID_LABELS:
            annotated[cid] = user_input
            rec['human_label'] = user_input
            save_progress(output_file, sampled_comments, annotated)
            idx += 1
        elif user_input == "PREV":
            if idx > 0:
                prev_id = to_annotate[idx-1]['id']
                if prev_id in annotated:
                    del annotated[prev_id]
                save_progress(output_file, sampled_comments, annotated)
                idx -= 1
                print("Voltou um comentário.")
            else:
                print("Já está no primeiro comentário.")
        elif user_input == "NEXT":
            idx += 1
            print("Pulado.")
        elif user_input == "SAVE":
            save_progress(output_file, sampled_comments, annotated)
            print("Progresso salvo. Saindo...")
            return annotated
        elif user_input == "QUIT":
            print("Saindo sem salvar...")
            return annotated
        else:
            print("Comando inválido. Use POS, NEG, NEU, PREV, NEXT, SAVE ou QUIT.")
    
    print("\n[SUCCESS] Todos os comentários anotados!")
    save_progress(output_file, sampled_comments, annotated)
    return annotated

def extract_by_id(ids, dataset_path, output_path):
    """Extrai comentários de um dataset pelos IDs."""
    ids_set = set(ids)
    extracted = []
    
    with open(dataset_path, 'r', encoding='utf-8') as f:
        for line in f:
            try:
                rec = json.loads(line)
                if rec.get('id') in ids_set:
                    extracted.append(rec)
            except json.JSONDecodeError:
                continue
    
    with open(output_path, 'w', encoding='utf-8') as f:
        for rec in extracted:
            f.write(json.dumps(rec, ensure_ascii=False) + '\n')
    
    print(f"[*] Extraídos {len(extracted)} IDs de {os.path.basename(dataset_path)}")
    return len(extracted)

# ============================================================
# MAIN
# ============================================================

def main():
    print("="*70)
    print("  VALIDAÇÃO ALEATÓRIA — 1000 COMENTÁRIOS")
    print("  Seed: 123 (independente da anterior)")
    print("="*70)
    
    # 1. Carregar todos os comentários
    print("\n[PASSO 1] Carregando comentários do dataset completo...")
    all_comments = load_all_comments(COMPLETE_DATASET)
    if not all_comments:
        print("[ERROR] Nenhum comentário carregado.")
        sys.exit(1)
    
    # 2. Sortear amostra
    print(f"\n[PASSO 2] Sorteando {SAMPLE_SIZE} comentários (seed={RANDOM_SEED})...")
    sampled = sample_random(all_comments, SAMPLE_SIZE, RANDOM_SEED)
    print(f"[*] Sorteados {len(sampled)} comentários")
    
    # Salvar amostra
    with open(OUTPUT_SAMPLED, 'w', encoding='utf-8') as f:
        for rec in sampled:
            f.write(json.dumps(rec, ensure_ascii=False) + '\n')
    print(f"[*] Amostra salva em {OUTPUT_SAMPLED}")
    
    # 3. Anotar
    print("\n[PASSO 3] Anotar comentários...")
    annotate_comments(sampled, OUTPUT_GT)
    
    # 4. Extrair IDs dos modelos
    print("\n[PASSO 4] Extraindo IDs dos modelos...")
    ids = [rec['id'] for rec in sampled]
    
    # XLM
    extract_by_id(ids, XLM_DATASET, OUTPUT_XLM)
    
    # BERTabaporu
    extract_by_id(ids, BERTA_DATASET, OUTPUT_BERTA)
    
    print("\n" + "="*70)
    print("  PROCESSO CONCLUÍDO")
    print("="*70)
    print("  Arquivos gerados:")
    print(f"    Amostra:        {OUTPUT_SAMPLED}")
    print(f"    Ground Truth:   {OUTPUT_GT}")
    print(f"    XLM-RoBERTa:    {OUTPUT_XLM}")
    print(f"    BERTabaporu:    {OUTPUT_BERTA}")
    print("="*70)

if __name__ == "__main__":
    main()