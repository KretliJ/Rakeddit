#!/usr/bin/env python3
# infer_engine.py
# Batch sentiment inference engine with multi-model support (XLM-RoBERTa, BERTabaporu, Sabiá-4).
# Preserves original behavior when run without arguments.

import os
import sys
import json
import time
import torch
from transformers import pipeline, AutoTokenizer, AutoModelForSequenceClassification
from config_loader import config

# ============================================================
# GLOBALS
# ============================================================

BASE_PATH = config.get_path('PATHS', 'BASE_PATH')
MAIN_INFER = config.get('MODELS', 'MAIN_INFER', fallback="cardiffnlp/twitter-xlm-roberta-base-sentiment")
INFERRED_PATH = config.get_path('PATHS', 'INFERRED_PATH', fallback="./DATA/4-inferred")

# Global classifier instance (lazy-loaded)
sentiment_classifier = None
current_model_name = None

# ============================================================
# HARDWARE DIAGNOSTICS
# ============================================================

def print_hardware_diagnostics():
    """Print GPU/CPU diagnostics."""
    print("\n" + "="*60)
    print(" 🛠️  INFERENCE ENGINE DIAGNOSTICS ")
    print("="*60)
    print(f" -> PyTorch Version: {torch.__version__}")
    
    cuda_available = torch.cuda.is_available()
    print(f" -> CUDA Available: {cuda_available}")
    
    if cuda_available:
        print(f" -> GPU Detected: {torch.cuda.get_device_name(0)}")
        print(f" -> GPU Memory: {torch.cuda.get_device_properties(0).total_memory / 1024**3:.1f} GB")
    else:
        print(" [!] WARNING: No GPU detected. Running on CPU (will be slow).")
    print("="*60 + "\n")

# ============================================================
# MODEL LOADING
# ============================================================

# Model name aliases for easier selection
MODEL_ALIASES = {
    # XLM-RoBERTa (baseline)
    "xlm-roberta": "cardiffnlp/twitter-xlm-roberta-base-sentiment",
    "xlm": "cardiffnlp/twitter-xlm-roberta-base-sentiment",
    "xlmr": "cardiffnlp/twitter-xlm-roberta-base-sentiment",
    
    # BERTabaporu (Portuguese BERT)
    # "bertabaporu": "pablocosta/bertabaporu-large-uncased",
    "bertabaporu": "./bertabaporu_finetuned",
    "bertabaporu-large": "pablocosta/bertabaporu-large-uncased",
    "bertabaporu-base": "neuralmind/bert-base-portuguese-cased",
    
    # Sabiá-4 (Brazilian LLM) — placeholder
    "sabia": "Sabiá-4",
    "sabia-4": "Sabiá-4",
}

# Models that support sentiment-analysis pipeline directly
SENTIMENT_PIPELINE_MODELS = {
    "cardiffnlp/twitter-xlm-roberta-base-sentiment",
    "pysentimiento/bert-base-spanish-wwm-uncased",
}

def normalize_model_name(model_name):
    """Resolve alias to actual model name."""
    if model_name is None:
        return MAIN_INFER
    return MODEL_ALIASES.get(model_name.lower(), model_name)

def load_sentiment_model(model_name=None, force_reload=False, verbose=True):
    """
    Load a sentiment analysis model.
    
    Args:
        model_name: Model name or alias. If None, uses MAIN_INFER from config.
        force_reload: If True, reload even if already loaded.
        verbose: If True, print loading progress.
    
    Returns:
        Pipeline object or None on failure.
    """
    global sentiment_classifier, current_model_name
    
    # Resolve model name
    resolved_name = normalize_model_name(model_name)
    
    # Check if already loaded
    if not force_reload and sentiment_classifier is not None and current_model_name == resolved_name:
        if verbose:
            print(f"[INFO] Model already loaded: {resolved_name}")
        return sentiment_classifier
    
    if verbose:
        print(f"[MODEL] Loading: {resolved_name}...")
        if model_name != resolved_name:
            print(f"[MODEL]   (resolved from alias: {model_name})")
    
    # Detect GPU device
    device = 0 if torch.cuda.is_available() else -1
    
    try:
        # --- CASE 1: Direct sentiment pipeline (XLM-RoBERTa, etc.) ---
        if any(key in resolved_name for key in ["cardiffnlp", "twitter-xlm", "pysentimiento"]):
            if verbose:
                print("[MODEL] Using direct sentiment-analysis pipeline")
            
            sentiment_classifier = pipeline(
                "sentiment-analysis",
                model=resolved_name,
                tokenizer=resolved_name,
                device=device,
                truncation=True,
                max_length=512
            )
            current_model_name = resolved_name
            if verbose:
                print(f"[MODEL] ✅ Loaded successfully: {resolved_name}")
            return sentiment_classifier
        
        # --- CASE 2: BERTabaporu (needs classification head) ---
        elif "neuralmind" in resolved_name or "bertabaporu" in resolved_name.lower():
            if verbose:
                print("[MODEL] BERTabaporu: Adding classification head for 3 classes")
            
            tokenizer = AutoTokenizer.from_pretrained(resolved_name)
            model = AutoModelForSequenceClassification.from_pretrained(
                resolved_name,
                num_labels=3  # POSITIVE, NEUTRAL, NEGATIVE
            )
            
            if torch.cuda.is_available():
                model = model.to('cuda')
                if verbose:
                    print("[MODEL] Model moved to GPU")
            
            sentiment_classifier = pipeline(
                "text-classification",
                model=model,
                tokenizer=tokenizer,
                device=device,
                truncation=True,
                max_length=512
            )
            current_model_name = resolved_name
            if verbose:
                print(f"[MODEL] ✅ Loaded successfully: {resolved_name}")
            return sentiment_classifier
        
        # --- CASE 3: Sabiá-4 (placeholder) ---
        elif "Sabiá" in resolved_name:
            print(f"[WARN] Sabiá-4 not yet implemented via pipeline.")
            print("[WARN] Falling back to XLM-RoBERTa as baseline.")
            return load_sentiment_model("xlm-roberta", force_reload=True, verbose=verbose)
        
        # --- CASE 4: Generic fallback ---
        else:
            if verbose:
                print(f"[WARN] Unknown model type: {resolved_name}")
                print("[WARN] Attempting generic pipeline load...")
            
            sentiment_classifier = pipeline(
                "sentiment-analysis",
                model=resolved_name,
                tokenizer=resolved_name,
                device=device,
                truncation=True,
                max_length=512
            )
            current_model_name = resolved_name
            if verbose:
                print(f"[MODEL] ✅ Loaded (generic): {resolved_name}")
            return sentiment_classifier
    
    except Exception as e:
        print(f"[ERROR] Failed to load model: {resolved_name}")
        print(f"[ERROR] Details: {e}")
        print("[ERROR] Attempting fallback to XLM-RoBERTa...")
        return load_sentiment_model("xlm-roberta", force_reload=True, verbose=verbose)

# ============================================================
# INFERENCE ENGINE
# ============================================================

def analyze_batch_sentiment(texts, batch_size=64):
    """
    Run sentiment analysis on a batch of texts.
    
    Args:
        texts: List of text strings.
        batch_size: Number of texts to process at once.
    
    Returns:
        List of dicts with 'label' and 'confidence' keys.
    """
    global sentiment_classifier
    
    if sentiment_classifier is None:
        raise RuntimeError("Sentiment classifier not loaded. Call load_sentiment_model() first.")
    
    if not texts:
        return []
    
    try:
        results = sentiment_classifier(texts, batch_size=batch_size)
        
        processed = []
        for res in results:
            label = res['label'].upper()
            score = res['score']
            
            # Normalize to 3-class system
            if label in ['POSITIVE', 'POS']:
                label = 'POSITIVE'
            elif label in ['NEGATIVE', 'NEG']:
                label = 'NEGATIVE'
            elif label in ['NEUTRAL', 'NEU']:
                label = 'NEUTRAL'
            else:
                # Fallback heuristic
                if 'NEG' in label:
                    label = 'NEGATIVE'
                elif 'POS' in label:
                    label = 'POSITIVE'
                else:
                    label = 'NEUTRAL'
            
            processed.append({
                "label": label,
                "confidence": round(score, 3)
            })
        
        return processed
    
    except Exception as e:
        print(f"[ERROR] Batch inference failed: {e}")
        return [{"label": "NEUTRAL", "confidence": 0.0} for _ in texts]

# ============================================================
# ORCHESTRATION
# ============================================================

def orchestrate_full_inference(jsonl_filepath, model_name=None, batch_size=64):
    """
    Run full inference on a JSONL dataset.
    
    Args:
        jsonl_filepath: Path to input JSONL file.
        model_name: Model name/alias (optional).
        batch_size: Batch size for GPU inference.
    """
    global sentiment_classifier, current_model_name
    
    # --- Model loading ---
    if model_name is None:
        model_name = MAIN_INFER
        print(f"[INFO] Using default model from config: {model_name}")
    else:
        print(f"[INFO] Using requested model: {model_name}")
    
    resolved_name = normalize_model_name(model_name)
    print(f"[INFO] Resolved model: {resolved_name}")
    
    # Load model (with force reload if different from current)
    force = (current_model_name != resolved_name)
    sentiment_classifier = load_sentiment_model(model_name, force_reload=force)
    
    if sentiment_classifier is None:
        print("[ERROR] Failed to load sentiment classifier. Aborting.")
        return
    
    # --- File setup ---
    if not os.path.exists(jsonl_filepath):
        print(f"[ERROR] Input file not found: {jsonl_filepath}")
        return
    
    os.makedirs(INFERRED_PATH, exist_ok=True)
    
    base_name = os.path.basename(jsonl_filepath)
    # Sanitize model name for filename
    safe_model = resolved_name.replace('/', '_')
    out_path = os.path.join(INFERRED_PATH, f"INFERRED_{safe_model}_{base_name}")
    
    print(f"\n[ORCHESTRATOR] Starting batched inference")
    print(f"  Input:  {jsonl_filepath}")
    print(f"  Output: {out_path}")
    print(f"  Model:  {resolved_name}")
    print(f"  Device: GPU (CUDA)" if torch.cuda.is_available() else "  Device: CPU")
    print(f"  Batch:  {batch_size}")
    print("-" * 60)
    
    # --- Resume logic ---
    processed_ids = set()
    if os.path.exists(out_path):
        with open(out_path, 'r', encoding='utf-8') as f:
            for line in f:
                try:
                    record = json.loads(line)
                    if 'id' in record:
                        processed_ids.add(record['id'])
                except json.JSONDecodeError:
                    pass
        if processed_ids:
            print(f"[RESUME] Found {len(processed_ids):,} already processed records. Skipping them.")
    
    # --- Process ---
    text_buffer = []
    record_buffer = []
    total_processed = 0
    bypassed_count = 0
    start_time = time.time()
    
    with open(jsonl_filepath, 'r', encoding='utf-8') as f_in, \
         open(out_path, 'a', encoding='utf-8') as f_out:
        
        for line_num, line in enumerate(f_in, 1):
            try:
                record = json.loads(line)
            except json.JSONDecodeError:
                continue
            
            # Skip metadata
            if record.get('type') == 'metadata_footer':
                continue
            
            record_id = record.get('id')
            if not record_id or record_id in processed_ids:
                continue
            
            # Prepare text
            original_body = record.get('body', '')
            body_text = original_body
            
            if record.get('type') == 'post_header':
                title = record.get('title', '')
                body_text = f"{title}. {body_text}".strip()
            
            # Bypass invalid texts
            if not record.get('is_valid_text', True) or not body_text:
                record['ai_analysis'] = {"label": "BYPASS_EMPTY", "confidence": 0.0}
                f_out.write(json.dumps(record, ensure_ascii=False) + "\n")
                bypassed_count += 1
                continue
            
            # Add to batch
            text_buffer.append(body_text)
            record_buffer.append(record)
            
            # Process batch when full
            if len(text_buffer) >= batch_size:
                results = analyze_batch_sentiment(text_buffer, batch_size=batch_size)
                
                for rec, ai_data in zip(record_buffer, results):
                    rec['ai_analysis'] = ai_data
                    f_out.write(json.dumps(rec, ensure_ascii=False) + "\n")
                
                f_out.flush()
                total_processed += len(text_buffer)
                
                elapsed = time.time() - start_time
                rate = total_processed / elapsed if elapsed > 0 else 0
                print(f"  [BATCH] Processed: {total_processed:,} | "
                      f"Bypassed: {bypassed_count:,} | "
                      f"Rate: {rate:.1f} rec/s", end="\r")
                
                text_buffer.clear()
                record_buffer.clear()
        
        # Process remaining
        if text_buffer:
            results = analyze_batch_sentiment(text_buffer, batch_size=len(text_buffer))
            for rec, ai_data in zip(record_buffer, results):
                rec['ai_analysis'] = ai_data
                f_out.write(json.dumps(rec, ensure_ascii=False) + "\n")
            total_processed += len(text_buffer)
    
    # --- Final report ---
    elapsed = time.time() - start_time
    print(f"\n\n[SUCCESS] Inference complete!")
    print(f"  Records processed: {total_processed:,}")
    print(f"  Records bypassed:  {bypassed_count:,}")
    print(f"  Total time:        {elapsed:.1f}s")
    print(f"  Average rate:      {total_processed/elapsed:.1f} rec/s")
    print(f"  Output:            {out_path}")

# ============================================================
# MAIN ENTRY POINT
# ============================================================

if __name__ == "__main__":
    # Print hardware diagnostics
    print_hardware_diagnostics()
    
    # Parse arguments
    if len(sys.argv) < 2:
        print("Usage: python infer_engine.py <input.jsonl> [model_name]")
        print()
        print("Available models:")
        print("  xlm-roberta   - XLM-RoBERTa (default, from config.ini)")
        print("  bertabaporu   - BERTabaporu (Portuguese BERT)")
        print("  sabia-4       - Sabiá-4 (placeholder)")
        print()
        print("Examples:")
        print("  python infer_engine.py dataset.jsonl")
        print("  python infer_engine.py dataset.jsonl bertabaporu")
        sys.exit(1)
    
    input_file = sys.argv[1]
    model = sys.argv[2] if len(sys.argv) > 2 else None
    
    # Run inference
    orchestrate_full_inference(input_file, model_name=model)