#!/usr/bin/env python3
# compare_labels.py
# Computes accuracy, precision, recall, F1 between AI and human labels.

import json
import os
from collections import defaultdict

# Files to compare
FILES = [
    ("RANK_BASE_Q.jsonl", "HUMAN_RANK_BASE_Q.jsonl", "Cascade Quartile"),
    ("RANK_BASE_UQ.jsonl", "HUMAN_RANK_BASE_UQ.jsonl", "User Quartile")
]

# Labels mapping (human labels are POS/NEG/NEU; AI labels are POSITIVE/NEGATIVE/NEUTRAL)
LABEL_MAP_AI = {
    "POSITIVE": "POS",
    "NEGATIVE": "NEG",
    "NEUTRAL": "NEU"
}
# For human labels, we keep as is.
# We'll convert AI label to the same short form.

def load_annotated(input_file, human_file):
    """Load both files and align records by id."""
    # Load base records
    base = {}
    with open(input_file, 'r', encoding='utf-8') as f:
        for line in f:
            rec = json.loads(line)
            base[rec['id']] = rec

    # Load human labels
    human = {}
    if os.path.exists(human_file):
        with open(human_file, 'r', encoding='utf-8') as f:
            for line in f:
                rec = json.loads(line)
                if 'human_label' in rec:
                    human[rec['id']] = rec['human_label']

    # Merge: only keep records with both AI and human labels
    merged = []
    for cid, rec in base.items():
        if cid in human and human[cid] is not None:
            ai_label = rec.get('ai_analysis', {}).get('label')
            if ai_label in LABEL_MAP_AI:
                ai_short = LABEL_MAP_AI[ai_label]
                merged.append({
                    'id': cid,
                    'quartile': rec.get('sampled_quartile', 'unknown'),
                    'ai': ai_short,
                    'human': human[cid]
                })
    return merged

def compute_metrics(y_true, y_pred):
    """Calculate accuracy, precision, recall, F1 per class and macro averages."""
    from collections import Counter
    classes = sorted(set(y_true) | set(y_pred))
    # Confusion matrix
    cm = {c: {c2: 0 for c2 in classes} for c in classes}
    for t, p in zip(y_true, y_pred):
        cm[t][p] += 1

    # Per-class metrics
    metrics = {}
    for c in classes:
        tp = cm[c][c]
        fp = sum(cm[other][c] for other in classes if other != c)
        fn = sum(cm[c][other] for other in classes if other != c)
        tn = sum(cm[o1][o2] for o1 in classes if o1 != c for o2 in classes if o2 != c)
        precision = tp / (tp + fp) if (tp + fp) > 0 else 0
        recall = tp / (tp + fn) if (tp + fn) > 0 else 0
        f1 = 2 * precision * recall / (precision + recall) if (precision + recall) > 0 else 0
        accuracy = (tp + tn) / (tp + tn + fp + fn) if (tp + tn + fp + fn) > 0 else 0
        metrics[c] = {'precision': precision, 'recall': recall, 'f1': f1, 'accuracy': accuracy}

    # Overall accuracy
    total_correct = sum(cm[c][c] for c in classes)
    total = sum(sum(row.values()) for row in cm.values())
    overall_acc = total_correct / total if total > 0 else 0

    # Macro averages
    macro_prec = sum(m['precision'] for m in metrics.values()) / len(classes)
    macro_rec = sum(m['recall'] for m in metrics.values()) / len(classes)
    macro_f1 = sum(m['f1'] for m in metrics.values()) / len(classes)

    return {
        'confusion_matrix': cm,
        'per_class': metrics,
        'overall_accuracy': overall_acc,
        'macro_precision': macro_prec,
        'macro_recall': macro_rec,
        'macro_f1': macro_f1
    }

def print_report(data, title):
    print(f"\n{'='*60}")
    print(f"REPORT: {title}")
    print(f"{'='*60}")
    print(f"Total records: {len(data)}")

    # Separate by quartile
    quartiles = sorted(set(d['quartile'] for d in data))
    for q in quartiles:
        subset = [d for d in data if d['quartile'] == q]
        y_true = [d['human'] for d in subset]
        y_pred = [d['ai'] for d in subset]
        if not y_true:
            continue
        metrics = compute_metrics(y_true, y_pred)
        print(f"\n--- Quartile {q} ({len(subset)} records) ---")
        print(f"Accuracy: {metrics['overall_accuracy']:.4f}")
        print(f"Macro Precision: {metrics['macro_precision']:.4f}")
        print(f"Macro Recall: {metrics['macro_recall']:.4f}")
        print(f"Macro F1: {metrics['macro_f1']:.4f}")
        # Per-class
        print("Per-class:")
        for cls, m in metrics['per_class'].items():
            print(f"  {cls}: Prec={m['precision']:.4f}, Rec={m['recall']:.4f}, F1={m['f1']:.4f}")
        # Confusion matrix
        print("Confusion matrix (rows=true, cols=pred):")
        classes = sorted(metrics['per_class'].keys())
        print("     " + " ".join(f"{c:>6}" for c in classes))
        for true in classes:
            row = [metrics['confusion_matrix'][true].get(pred, 0) for pred in classes]
            print(f"{true:4} " + " ".join(f"{v:6d}" for v in row))

    # Overall across all quartiles
    y_true_all = [d['human'] for d in data]
    y_pred_all = [d['ai'] for d in data]
    overall = compute_metrics(y_true_all, y_pred_all)
    print(f"\n--- OVERALL (all quartiles) ---")
    print(f"Accuracy: {overall['overall_accuracy']:.4f}")
    print(f"Macro Precision: {overall['macro_precision']:.4f}")
    print(f"Macro Recall: {overall['macro_recall']:.4f}")
    print(f"Macro F1: {overall['macro_f1']:.4f}")
    print("Per-class (overall):")
    for cls, m in overall['per_class'].items():
        print(f"  {cls}: Prec={m['precision']:.4f}, Rec={m['recall']:.4f}, F1={m['f1']:.4f}")

def main():
    for base_file, human_file, title in FILES:
        if not os.path.exists(base_file):
            print(f"Warning: {base_file} not found. Skipping.")
            continue
        if not os.path.exists(human_file):
            print(f"Warning: {human_file} not found. Skipping. (Run human_annotation.py first)")
            continue
        data = load_annotated(base_file, human_file)
        if not data:
            print(f"No matching records with human labels in {human_file}.")
            continue
        print_report(data, title)

if __name__ == "__main__":
    main()