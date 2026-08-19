#!/usr/bin/env python3
# sample_comments.py
# Sorteia 100 mensagens aleatórias (seed 42), exibe para validação manual,
# e gera matriz de confusão + médias de confiança por categoria.

import json
import random
import os
import sys
from collections import defaultdict

# Configurações
INPUT_FILE = "INFERRED_MULTIMODAL_FINAL.jsonl"
SAMPLE_SIZE = 100
RANDOM_SEED = 42
OUTPUT_SAMPLED = "VALIDATION_SAMPLE_100.jsonl"
OUTPUT_LABELED = "VALIDATION_LABELED_100.jsonl"
OUTPUT_REPORT = "VALIDATION_REPORT_100.txt"

# Mapeamento de labels do modelo para short labels
LABEL_MAP = {
    "POSITIVE": "POS",
    "NEUTRAL": "NEU",
    "NEGATIVE": "NEG"
}

# Labels válidos para o humano
VALID_LABELS = {"POS", "NEU", "NEG"}

# Descrições dos labels
LABEL_DESCRIPTIONS = {
    "POS": "Contexto/sentimento positivo, uso de palavras positivas",
    "NEG": "Contexto/sentimento negativo, uso de palavras negativas, ofensas",
    "NEU": "Nenhum dos dois / incerto / objetivo"
}

def load_comments(input_path, sample_size=100, seed=42):
    """Carrega todos os comentários válidos e sorteia uma amostra aleatória."""
    comments = []
    with open(input_path, 'r', encoding='utf-8') as f:
        for line in f:
            try:
                record = json.loads(line)
                # Apenas comentários válidos com AI label
                if record.get('type') != 'comment':
                    continue
                if not record.get('is_valid_text', False):
                    continue
                ai = record.get('ai_analysis')
                if not ai or 'label' not in ai:
                    continue
                label = ai['label']
                if label not in LABEL_MAP:
                    continue
                comments.append(record)
            except json.JSONDecodeError:
                continue
    
    print(f"[*] Carregados {len(comments):,} comentários válidos.")
    
    # Sorteio aleatório com seed fixa
    random.seed(seed)
    sampled = random.sample(comments, min(sample_size, len(comments)))
    print(f"[*] Sorteados {len(sampled)} comentários (seed={seed}).")
    
    return sampled

def annotate_comments(sampled_comments, output_labeled):
    """Exibe comentários para anotação manual e salva os resultados."""
    
    print("\n" + "="*60)
    print("  VALIDAÇÃO MANUAL DE 100 COMENTÁRIOS")
    print("="*60)
    print("\nInstruções:")
    print("  POS - Contexto/sentimento positivo, uso de palavras positivas")
    print("  NEG - Contexto/sentimento negativo, uso de palavras negativas, ofensas")
    print("  NEU - Nenhum dos dois / incerto / objetivo")
    print("\nComandos: POS, NEG, NEU, 'next' (skip), 'prev' (voltar), 'save' (salvar e sair)")
    print("="*60 + "\n")
    
    # Carrega anotações anteriores se existirem
    labeled = {}
    if os.path.exists(output_labeled):
        with open(output_labeled, 'r', encoding='utf-8') as f:
            for line in f:
                try:
                    rec = json.loads(line)
                    if 'human_label' in rec:
                        labeled[rec['id']] = rec['human_label']
                except:
                    continue
        print(f"[*] Carregadas {len(labeled)} anotações anteriores.")
    
    # Filtra apenas os não anotados
    to_annotate = [c for c in sampled_comments if c['id'] not in labeled]
    
    if not to_annotate:
        print("[*] Todos os comentários já foram anotados!")
        return labeled
    
    print(f"[*] Restam {len(to_annotate)} comentários para anotar.")
    
    idx = 0
    total = len(to_annotate)
    
    while idx < total:
        rec = to_annotate[idx]
        cid = rec['id']
        body = rec.get('body', '')
        # ai_label = rec.get('ai_analysis', {}).get('label', 'N/A')
        confidence = rec.get('ai_analysis', {}).get('confidence', 0.0)
        
        print(f"\n--- Comentário {idx+1}/{total} (ID: {cid}) ---")
        # print(f"AI Label: {ai_label} (confiança: {confidence:.3f})")
        print(f"Corpo: {body[:300]}{'...' if len(body) > 300 else ''}")
        print(f"POS: {LABEL_DESCRIPTIONS['POS']}")
        print(f"NEG: {LABEL_DESCRIPTIONS['NEG']}")
        print(f"NEU: {LABEL_DESCRIPTIONS['NEU']}")
        
        user_input = input("> ").strip().upper()
        
        if user_input in VALID_LABELS:
            labeled[cid] = user_input
            rec['human_label'] = user_input
            # Salva progresso
            save_progress(sampled_comments, output_labeled, labeled)
            idx += 1
        elif user_input == "PREV":
            if idx > 0:
                prev_id = to_annotate[idx-1]['id']
                if prev_id in labeled:
                    del labeled[prev_id]
                save_progress(sampled_comments, output_labeled, labeled)
                idx -= 1
                print("Voltou um comentário.")
            else:
                print("Já está no primeiro comentário.")
        elif user_input == "NEXT":
            # Pula sem anotar (deixa como None)
            idx += 1
            print("Pulado.")
        elif user_input == "SAVE":
            save_progress(sampled_comments, output_labeled, labeled)
            print("Progresso salvo. Saindo...")
            return labeled
        elif user_input == "QUIT":
            print("Saindo sem salvar...")
            return labeled
        else:
            print("Comando inválido. Use POS, NEG, NEU, PREV, NEXT, SAVE ou QUIT.")
    
    print("\n[SUCCESS] Todos os comentários anotados!")
    save_progress(sampled_comments, output_labeled, labeled)
    return labeled

def save_progress(all_comments, output_file, labeled_dict):
    """Salva o progresso atual no arquivo."""
    with open(output_file, 'w', encoding='utf-8') as f:
        for rec in all_comments:
            rec_copy = rec.copy()
            rec_copy['human_label'] = labeled_dict.get(rec['id'])
            f.write(json.dumps(rec_copy, ensure_ascii=False) + '\n')
    print(f"[*] Progresso salvo em: {output_file}")

def generate_report(sampled_comments, labeled, output_file):
    """Gera matriz de confusão e médias de confiança."""
    
    # Mapeia AI label -> short label
    y_true = []  # humano
    y_pred = []  # AI
    confidences = defaultdict(list)
    
    for rec in sampled_comments:
        cid = rec['id']
        if cid not in labeled:
            continue
        human_label = labeled[cid]
        if human_label is None:
            continue
        
        ai_label = rec.get('ai_analysis', {}).get('label', '')
        ai_short = LABEL_MAP.get(ai_label, 'UNKNOWN')
        confidence = rec.get('ai_analysis', {}).get('confidence', 0.0)
        
        y_true.append(human_label)
        y_pred.append(ai_short)
        confidences[human_label].append(confidence)
    
    if not y_true:
        print("[WARN] Nenhum comentário anotado. Execute a validação primeiro.")
        return
    
    from collections import Counter
    classes = sorted(set(y_true) | set(y_pred))
    
    # Matriz de confusão
    cm = {c: {c2: 0 for c2 in classes} for c in classes}
    for t, p in zip(y_true, y_pred):
        cm[t][p] += 1
    
    # Métricas
    total_correct = sum(cm[c][c] for c in classes)
    total = sum(sum(row.values()) for row in cm.values())
    accuracy = total_correct / total if total > 0 else 0
    
    # Médias de confiança
    mean_conf = {}
    for c in classes:
        if confidences[c]:
            mean_conf[c] = sum(confidences[c]) / len(confidences[c])
        else:
            mean_conf[c] = 0.0
    
    overall_mean_conf = sum(mean_conf.values()) / len(mean_conf) if mean_conf else 0.0
    
    # Per-class metrics
    per_class = {}
    for c in classes:
        tp = cm[c][c]
        fp = sum(cm[other][c] for other in classes if other != c)
        fn = sum(cm[c][other] for other in classes if other != c)
        precision = tp / (tp + fp) if (tp + fp) > 0 else 0
        recall = tp / (tp + fn) if (tp + fn) > 0 else 0
        f1 = 2 * precision * recall / (precision + recall) if (precision + recall) > 0 else 0
        per_class[c] = {'precision': precision, 'recall': recall, 'f1': f1}
    
    # Escreve relatório
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write("="*60 + "\n")
        f.write("   RELATÓRIO DE VALIDAÇÃO MANUAL (100 amostras)\n")
        f.write("="*60 + "\n\n")
        
        f.write(f"Total de amostras anotadas: {len(y_true)}\n")
        f.write(f"Acurácia geral: {accuracy:.4f} ({accuracy*100:.2f}%)\n\n")
        
        f.write("Matriz de Confusão:\n")
        f.write(" " + " ".join(f"{c:>8}" for c in classes) + "\n")
        for true in classes:
            row = [cm[true].get(pred, 0) for pred in classes]
            f.write(f"{true:3} " + " ".join(f"{v:8d}" for v in row) + "\n")
        
        f.write("\nMétricas por Classe:\n")
        for c in classes:
            m = per_class[c]
            f.write(f"  {c}: Prec={m['precision']:.4f}, Rec={m['recall']:.4f}, F1={m['f1']:.4f}\n")
        
        f.write("\nMédias de Confiança do Classificador:\n")
        for c in sorted(mean_conf.keys()):
            f.write(f"  {c}: {mean_conf[c]:.4f} (n={len(confidences[c])})\n")
        f.write(f"  Média Geral: {overall_mean_conf:.4f}\n")
        
        f.write("\nDetalhamento por Comentário:\n")
        for rec in sampled_comments:
            cid = rec['id']
            if cid not in labeled or labeled[cid] is None:
                continue
            ai_label = rec.get('ai_analysis', {}).get('label', 'N/A')
            ai_short = LABEL_MAP.get(ai_label, 'UNKNOWN')
            confidence = rec.get('ai_analysis', {}).get('confidence', 0.0)
            human = labeled[cid]
            match = "✓" if human == ai_short else "✗"
            f.write(f"  {cid}: AI={ai_short} ({confidence:.3f}) | Humano={human} | {match}\n")
    
    print(f"\n[SUCCESS] Relatório gerado: {output_file}")
    print(f"  Acurácia: {accuracy:.4f} ({accuracy*100:.2f}%)")
    print(f"  Confiança média geral: {overall_mean_conf:.4f}")

def main():
    # Localiza o arquivo de entrada
    input_path = INPUT_FILE
    if not os.path.exists(input_path):
        alternatives = [
            os.path.join("..", "DATA", "4-inferred", INPUT_FILE),
            os.path.join("..", "..", "DATA", "4-inferred", INPUT_FILE),
            os.path.join("..", "..", "..", "DATA", "4-inferred", INPUT_FILE),
        ]
        found = False
        for alt in alternatives:
            if os.path.exists(alt):
                input_path = alt
                found = True
                break
        if not found:
            print(f"[ERROR] Arquivo {INPUT_FILE} não encontrado.")
            sys.exit(1)
    
    print(f"[*] Usando arquivo: {input_path}")
    
    # Carrega ou sorteia a amostra
    if os.path.exists(OUTPUT_SAMPLED):
        print(f"[*] Carregando amostra existente: {OUTPUT_SAMPLED}")
        with open(OUTPUT_SAMPLED, 'r', encoding='utf-8') as f:
            sampled = [json.loads(line) for line in f]
    else:
        sampled = load_comments(input_path, SAMPLE_SIZE, RANDOM_SEED)
        with open(OUTPUT_SAMPLED, 'w', encoding='utf-8') as f:
            for rec in sampled:
                f.write(json.dumps(rec, ensure_ascii=False) + '\n')
        print(f"[*] Amostra salva em: {OUTPUT_SAMPLED}")
    
    # Validação manual
    labeled = annotate_comments(sampled, OUTPUT_LABELED)
    
    # Gera relatório
    generate_report(sampled, labeled, OUTPUT_REPORT)

if __name__ == "__main__":
    main()