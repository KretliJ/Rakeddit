#!/usr/bin/env python3
# compare_independent_models.py
# Compara XLM-RoBERTa e BERTabaporu contra o ground truth humano
# Usa a nova amostra independente de 1000 comentários.

import json
import os
import sys
from collections import defaultdict
import numpy as np
from sklearn.metrics import accuracy_score, precision_recall_fscore_support, confusion_matrix

# ============================================================
# CONFIGURAÇÕES
# ============================================================

# Arquivos de entrada
GROUND_TRUTH = "HUMAN_GROUND_TRUTH_RANDOM_1000.jsonl"
XLM_FILE = "RANDOM_SAMPLE_XLM_1000.jsonl"
BERTA_FILE = "RANDOM_SAMPLE_BERTA_1000.jsonl"

# Mapeamento de labels
LABEL_MAP = {
    "POSITIVE": "POS",
    "NEGATIVE": "NEG",
    "NEUTRAL": "NEU"
}

LABEL_ORDER = ["POS", "NEU", "NEG"]

# ============================================================
# FUNÇÕES
# ============================================================

def load_ground_truth(filepath):
    """Carrega os rótulos humanos do ground truth."""
    labels = {}
    with open(filepath, 'r', encoding='utf-8') as f:
        for line in f:
            try:
                rec = json.loads(line)
                cid = rec.get('id')
                human_label = rec.get('human_label')
                if cid and human_label in LABEL_ORDER:
                    labels[cid] = human_label
            except json.JSONDecodeError:
                continue
    print(f"[*] Carregados {len(labels)} rótulos humanos")
    return labels

def load_model_predictions(filepath, human_labels):
    """Carrega predições de um modelo e alinha com os rótulos humanos."""
    predictions = {}
    with open(filepath, 'r', encoding='utf-8') as f:
        for line in f:
            try:
                rec = json.loads(line)
                cid = rec.get('id')
                if cid not in human_labels:
                    continue
                ai_label = rec.get('ai_analysis', {}).get('label')
                if ai_label in LABEL_MAP:
                    predictions[cid] = LABEL_MAP[ai_label]
            except json.JSONDecodeError:
                continue
    print(f"[*] Alinhados {len(predictions)} predições com ground truth")
    return predictions

def compute_metrics(y_true, y_pred, model_name):
    """Calcula métricas de avaliação."""
    # Garante que todos os labels estão presentes
    labels = LABEL_ORDER
    y_true_labels = [l for l in labels if l in y_true or l in y_pred]
    
    acc = accuracy_score(y_true, y_pred)
    prec, rec, f1, _ = precision_recall_fscore_support(
        y_true, y_pred,
        labels=labels,
        average=None,
        zero_division=0
    )
    macro_prec = precision_recall_fscore_support(
        y_true, y_pred,
        labels=labels,
        average='macro',
        zero_division=0
    )[0]
    macro_rec = precision_recall_fscore_support(
        y_true, y_pred,
        labels=labels,
        average='macro',
        zero_division=0
    )[1]
    macro_f1 = precision_recall_fscore_support(
        y_true, y_pred,
        labels=labels,
        average='macro',
        zero_division=0
    )[2]
    
    cm = confusion_matrix(y_true, y_pred, labels=labels)
    
    return {
        'accuracy': acc,
        'macro_precision': macro_prec,
        'macro_recall': macro_rec,
        'macro_f1': macro_f1,
        'per_class': {
            label: {
                'precision': prec[i],
                'recall': rec[i],
                'f1': f1[i],
                'support': sum(1 for y in y_true if y == label)
            }
            for i, label in enumerate(labels)
        },
        'confusion_matrix': cm,
        'labels': labels
    }

def print_report(metrics, model_name):
    """Imprime relatório formatado."""
    print(f"\n{'='*70}")
    print(f"  REPORT: {model_name}")
    print(f"{'='*70}")
    print(f"Accuracy: {metrics['accuracy']:.4f}")
    print(f"Macro Precision: {metrics['macro_precision']:.4f}")
    print(f"Macro Recall: {metrics['macro_recall']:.4f}")
    print(f"Macro F1: {metrics['macro_f1']:.4f}")
    
    print("\nPer-class:")
    for label, m in metrics['per_class'].items():
        print(f"  {label}: Prec={m['precision']:.4f}, Rec={m['recall']:.4f}, F1={m['f1']:.4f} (n={m['support']})")
    
    print("\nConfusion Matrix (rows=true, cols=pred):")
    labels = metrics['labels']
    print("     " + " ".join(f"{l:>6}" for l in labels))
    for i, true in enumerate(labels):
        row = metrics['confusion_matrix'][i]
        print(f"{true:4} " + " ".join(f"{v:6d}" for v in row))

# ============================================================
# MAIN
# ============================================================

def main():
    print("="*70)
    print("  COMPARAÇÃO INDEPENDENTE: XLM-RoBERTa vs BERTabaporu")
    print("="*70)
    
    # 1. Carregar ground truth
    print("\n[STEP 1] Loading ground truth...")
    human_labels = load_ground_truth(GROUND_TRUTH)
    if not human_labels:
        print("[ERROR] No human labels found.")
        sys.exit(1)
    
    # 2. Carregar predições dos modelos
    print("\n[STEP 2] Loading model predictions...")
    
    xlm_pred = load_model_predictions(XLM_FILE, human_labels)
    berta_pred = load_model_predictions(BERTA_FILE, human_labels)
    
    # 3. Verificar alinhamento
    print("\n[STEP 3] Checking alignment...")
    common_ids = set(human_labels.keys()) & set(xlm_pred.keys()) & set(berta_pred.keys())
    print(f"[*] Common IDs: {len(common_ids)}")
    
    if len(common_ids) < 100:
        print(f"[WARN] Only {len(common_ids)} common IDs found.")
    
    # 4. Calcular métricas
    print("\n[STEP 4] Computing metrics...")
    
    y_true = [human_labels[cid] for cid in common_ids]
    y_xlm = [xlm_pred[cid] for cid in common_ids]
    y_berta = [berta_pred[cid] for cid in common_ids]
    
    metrics_xlm = compute_metrics(y_true, y_xlm, "XLM-RoBERTa")
    metrics_berta = compute_metrics(y_true, y_berta, "BERTabaporu")
    
    # 5. Imprimir relatórios
    print_report(metrics_xlm, "XLM-RoBERTa")
    print_report(metrics_berta, "BERTabaporu")
    
    # 6. Resumo comparativo
    print("\n" + "="*70)
    print("  SUMMARY COMPARISON")
    print("="*70)
    print(f"{'Model':<20} {'Accuracy':<12} {'Macro F1':<12}")
    print("-"*50)
    print(f"{'XLM-RoBERTa':<20} {metrics_xlm['accuracy']:.4f}     {metrics_xlm['macro_f1']:.4f}")
    print(f"{'BERTabaporu':<20} {metrics_berta['accuracy']:.4f}     {metrics_berta['macro_f1']:.4f}")
    
    # 7. Diferença
    diff_acc = metrics_berta['accuracy'] - metrics_xlm['accuracy']
    diff_f1 = metrics_berta['macro_f1'] - metrics_xlm['macro_f1']
    
    print(f"\nDifference (BERTabaporu - XLM-RoBERTa):")
    print(f"  Accuracy: {diff_acc:+.4f} ({diff_acc*100:+.2f}%)")
    print(f"  Macro F1: {diff_f1:+.4f} ({diff_f1*100:+.2f}%)")
    print("="*70)

if __name__ == "__main__":
    main()