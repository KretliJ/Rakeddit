#!/usr/bin/env python3
# fine_tune_bertabaporu.py
# Fine-tunes BERTabaporu on human-annotated Reddit comments.

import os
import sys
import json
import random
import numpy as np
import torch
import transformers
from sklearn.model_selection import train_test_split
from sklearn.metrics import classification_report, confusion_matrix
from transformers import (
    AutoTokenizer,
    AutoModelForSequenceClassification,
    TrainingArguments,
    Trainer,
    EarlyStoppingCallback
)
from datasets import Dataset, DatasetDict

# ============================================================
# CONFIGURATION (ajustada para RTX 4060 Ti - 8GB VRAM)
# ============================================================

# Paths to human-labeled data
HUMAN_Q_PATH = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "HUMAN_RANK_BASE_Q.jsonl"
)
HUMAN_UQ_PATH = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "HUMAN_RANK_BASE_UQ.jsonl"
)

# Model to fine-tune
# BASE_MODEL = "neuralmind/bert-base-portuguese-cased"          # ~110MB (seguro)
BASE_MODEL = "pablocosta/bertabaporu-large-uncased"         # ~1.48GB (descomente se quiser testar)
OUTPUT_DIR = "./bertabaporu_finetuned"

# Training parameters (ajustados para VRAM)
BATCH_SIZE = 4                     # 4 funciona bem para base model
GRADIENT_ACCUMULATION = 2          # Simula batch = 8 (4 * 2 = 8)
EPOCHS = 5
LEARNING_RATE = 2e-5
MAX_LENGTH = 256
TEST_SIZE = 0.2
RANDOM_SEED = 42

# Label mapping (human labels are POS/NEG/NEU)
LABEL_MAP = {
    "POS": 0,
    "NEU": 1,
    "NEG": 2
}

# ============================================================
# LOAD HUMAN LABELS
# ============================================================

def load_human_labels(filepath):
    """Load human-annotated comments from JSONL."""
    comments = []
    labels = []
    with open(filepath, 'r', encoding='utf-8') as f:
        for line in f:
            try:
                record = json.loads(line)
                body = record.get('body', '')
                human_label = record.get('human_label')
                if human_label is None or human_label not in LABEL_MAP:
                    continue
                if body and len(body.strip()) > 5:
                    comments.append(body)
                    labels.append(LABEL_MAP[human_label])
            except json.JSONDecodeError:
                continue
    return comments, labels

def load_all_human_labels():
    """Load labels from both Q and UQ files."""
    print("[*] Loading human labels from Q and UQ files...")
    
    all_comments = []
    all_labels = []
    
    if os.path.exists(HUMAN_Q_PATH):
        print(f"[*] Loading: {HUMAN_Q_PATH}")
        c, l = load_human_labels(HUMAN_Q_PATH)
        all_comments.extend(c)
        all_labels.extend(l)
        print(f"    Loaded {len(c)} comments from Q")
    else:
        print(f"[WARN] File not found: {HUMAN_Q_PATH}")
    
    if os.path.exists(HUMAN_UQ_PATH):
        print(f"[*] Loading: {HUMAN_UQ_PATH}")
        c, l = load_human_labels(HUMAN_UQ_PATH)
        all_comments.extend(c)
        all_labels.extend(l)
        print(f"    Loaded {len(c)} comments from UQ")
    else:
        print(f"[WARN] File not found: {HUMAN_UQ_PATH}")
    
    print(f"[*] Total: {len(all_comments)} comments loaded")
    return all_comments, all_labels

# ============================================================
# MAIN
# ============================================================

def main():
    print("="*60)
    print("  FINE-TUNE BERTABAPORU ON HUMAN LABELS")
    print("="*60)
    
    # 1. Load human labels
    comments, labels = load_all_human_labels()
    if len(comments) < 10:
        print("[ERROR] Not enough human labels. Need at least 10 samples.")
        sys.exit(1)
    
    # 2. Check label distribution
    print("\n[*] Label distribution:")
    for label_name, label_id in LABEL_MAP.items():
        count = labels.count(label_id)
        print(f"    {label_name}: {count} ({count/len(labels)*100:.1f}%)")
    
    # 3. Prepare dataset
    print("\n[*] Preparing dataset...")
    random.seed(RANDOM_SEED)
    combined = list(zip(comments, labels))
    random.shuffle(combined)
    comments, labels = zip(*combined)
    
    X_train, X_val, y_train, y_val = train_test_split(
        comments, labels,
        test_size=TEST_SIZE,
        random_state=RANDOM_SEED,
        stratify=labels
    )
    
    print(f"    Train: {len(X_train)} samples")
    print(f"    Validation: {len(X_val)} samples")
    
    train_data = Dataset.from_dict({
        'text': list(X_train),
        'label': list(y_train)
    })
    val_data = Dataset.from_dict({
        'text': list(X_val),
        'label': list(y_val)
    })
    
    dataset = DatasetDict({
        'train': train_data,
        'validation': val_data
    })
    
    # 4. Load tokenizer and model
    print(f"\n[*] Loading model: {BASE_MODEL}...")
    tokenizer = AutoTokenizer.from_pretrained(BASE_MODEL)
    model = AutoModelForSequenceClassification.from_pretrained(
        BASE_MODEL,
        num_labels=3,
        id2label={0: 'POS', 1: 'NEU', 2: 'NEG'},
        label2id={'POS': 0, 'NEU': 1, 'NEG': 2}
    )
    
    # 5. Tokenize dataset
    print("[*] Tokenizing dataset...")

    def tokenize_function(examples):
        return tokenizer(
            examples['text'],
            padding='max_length',
            truncation=True,
            max_length=MAX_LENGTH
        )

    # Tokenize without removing columns
    tokenized_dataset = dataset.map(
        tokenize_function,
        batched=True,
        remove_columns=None  # Mantém TODAS as colunas (incluindo 'label')
    )

    # Now rename 'label' to 'labels'
    tokenized_dataset = tokenized_dataset.rename_column("label", "labels")

    # If there are still issues, print columns for debugging
    print(f"[DEBUG] Tokenized dataset columns: {tokenized_dataset['train'].column_names}")
    
    # 6. Training arguments (compatível com 8GB VRAM)
    print("[*] Configuring training arguments...")
    print(f"[*] Transformers version: {transformers.__version__}")
    
    training_args = TrainingArguments(
        output_dir=OUTPUT_DIR,
        eval_strategy="epoch",
        save_strategy="epoch",
        learning_rate=LEARNING_RATE,
        per_device_train_batch_size=BATCH_SIZE,
        per_device_eval_batch_size=BATCH_SIZE,
        gradient_accumulation_steps=GRADIENT_ACCUMULATION,
        num_train_epochs=EPOCHS,
        weight_decay=0.01,
        logging_steps=10,
        load_best_model_at_end=True,
        metric_for_best_model='f1_neg',
        greater_is_better=True,
        save_total_limit=2,
        report_to="none",
        fp16=False,                     # Desativado para evitar instabilidade
        dataloader_num_workers=0,       # Evita deadlock no Windows
    )
    
    # 7. Trainer
    print("[*] Initializing trainer...")

    def compute_metrics(eval_pred):
        from sklearn.metrics import accuracy_score, precision_recall_fscore_support
        predictions, labels = eval_pred
        predictions = np.argmax(predictions, axis=1)
        
        acc = accuracy_score(labels, predictions)
        prec, rec, f1, _ = precision_recall_fscore_support(
            labels, predictions, 
            average=None, 
            labels=[0, 1, 2],
            zero_division=0
        )
        
        return {
            'accuracy': acc,
            'precision_pos': prec[0],
            'recall_pos': rec[0],
            'f1_pos': f1[0],
            'precision_neu': prec[1],
            'recall_neu': rec[1],
            'f1_neu': f1[1],
            'precision_neg': prec[2],
            'recall_neg': rec[2],
            'f1_neg': f1[2],
        }

    trainer = Trainer(
        model=model,
        args=training_args,
        train_dataset=tokenized_dataset['train'],
        eval_dataset=tokenized_dataset['validation'],
        # tokenizer=tokenizer,
        compute_metrics=compute_metrics,
        callbacks=[EarlyStoppingCallback(early_stopping_patience=2)],
    )
    # 8. Train
    print(f"\n[*] Training for {EPOCHS} epochs...")
    print(f"    Batch size: {BATCH_SIZE}")
    print(f"    Grad accumulation: {GRADIENT_ACCUMULATION}")
    print(f"    Effective batch: {BATCH_SIZE * GRADIENT_ACCUMULATION}")
    print("-"*60)
    trainer.train()
    
    # 9. Save model
    print("\n[*] Saving model...")
    trainer.save_model(OUTPUT_DIR)
    tokenizer.save_pretrained(OUTPUT_DIR)
    print(f"[SUCCESS] Model saved to: {OUTPUT_DIR}")
    
    # 10. Final evaluation
    print("\n[*] Final evaluation on validation set...")
    eval_results = trainer.evaluate()
    print("\n[EVALUATION RESULTS]")
    for key, value in eval_results.items():
        print(f"  {key}: {value:.4f}")
    
    # Show confusion matrix
    print("\n[Confusion Matrix on Validation Set]")
    preds = trainer.predict(tokenized_dataset['validation'])
    y_pred = np.argmax(preds.predictions, axis=1)
    y_true = preds.label_ids
    cm = confusion_matrix(y_true, y_pred)
    print("           Predicted")
    print("            POS  NEU  NEG")
    print(f"POS (0):    {cm[0,0]:3d}  {cm[0,1]:3d}  {cm[0,2]:3d}")
    print(f"NEU (1):    {cm[1,0]:3d}  {cm[1,1]:3d}  {cm[1,2]:3d}")
    print(f"NEG (2):    {cm[2,0]:3d}  {cm[2,1]:3d}  {cm[2,2]:3d}")
    
    print("\n[SUCCESS] Fine-tuning complete!")
    print(f"  Model saved to: {OUTPUT_DIR}")
    print("  To use the fine-tuned model, update infer_engine.py with:")
    print(f"    'bertabaporu': '{OUTPUT_DIR}'")

if __name__ == "__main__":
    main()