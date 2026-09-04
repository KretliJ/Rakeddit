#!/usr/bin/env python3
# extract_bertabaporu_samples.py
# Extracts the same comment IDs from the BERTabaporu-inferred dataset
# to create RANK_BASE_Q(BERTabaporu).jsonl and RANK_BASE_UQ(BERTabaporu).jsonl

import os
import sys
import json
from collections import defaultdict

# ============================================================
# CONFIGURATION
# ============================================================

# Paths to the original sample files (with IDs to extract)
RANK_Q_PATH = "audit/scripts/humanValidation/RANK_BASE_Q.jsonl"
RANK_UQ_PATH = "audit/scripts/humanValidation/RANK_BASE_UQ.jsonl"

# Path to the BERTabaporu-inferred dataset
BERTABAPORU_PATH = "results/BERTabaporu_QWEN3_MULTIMODAL_FINAL.jsonl"

# Output paths
OUTPUT_Q_PATH = "audit/scripts/humanValidation/RANK_BASE_Q(BERTabaporu).jsonl"
OUTPUT_UQ_PATH = "audit/scripts/humanValidation/RANK_BASE_UQ(BERTabaporu).jsonl"

# ============================================================
# FUNCTIONS
# ============================================================

def load_ids_from_file(filepath):
    """Extract all comment IDs from a JSONL file."""
    ids = set()
    if not os.path.exists(filepath):
        print(f"[WARN] File not found: {filepath}")
        return ids
    
    with open(filepath, 'r', encoding='utf-8') as f:
        for line in f:
            try:
                rec = json.loads(line)
                cid = rec.get('id')
                if cid:
                    ids.add(cid)
            except json.JSONDecodeError:
                continue
    
    print(f"[*] Loaded {len(ids):,} IDs from {os.path.basename(filepath)}")
    return ids

def extract_comments_from_dataset(ids, dataset_path):
    """Extract comments matching the given IDs from the dataset."""
    if not os.path.exists(dataset_path):
        print(f"[ERROR] Dataset not found: {dataset_path}")
        return {}
    
    print(f"[*] Scanning {os.path.basename(dataset_path)} for {len(ids):,} IDs...")
    
    extracted = {}
    found_count = 0
    line_count = 0
    
    with open(dataset_path, 'r', encoding='utf-8') as f:
        for line in f:
            line_count += 1
            if line_count % 500000 == 0:
                print(f"    ... scanned {line_count:,} lines, found {found_count:,}/{len(ids):,}")
            
            try:
                rec = json.loads(line)
                cid = rec.get('id')
                if cid and cid in ids:
                    extracted[cid] = rec
                    found_count += 1
                    if found_count == len(ids):
                        break
            except json.JSONDecodeError:
                continue
    
    print(f"[*] Found {found_count:,}/{len(ids):,} matching IDs")
    
    # Report missing IDs
    missing = ids - set(extracted.keys())
    if missing:
        print(f"[WARN] {len(missing):,} IDs not found in the dataset")
        # Print first 10 missing IDs for debugging
        sample_missing = list(missing)[:10]
        print(f"    Sample missing IDs: {sample_missing}")
    
    return extracted

def save_extracted(extracted, output_path, sort_by_id=True):
    """Save extracted comments to a JSONL file."""
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    
    # Sort by ID for deterministic ordering
    items = sorted(extracted.items()) if sort_by_id else extracted.items()
    
    with open(output_path, 'w', encoding='utf-8') as f:
        for cid, rec in items:
            f.write(json.dumps(rec, ensure_ascii=False) + '\n')
    
    print(f"[*] Saved {len(extracted):,} comments to {output_path}")

def validate_output(output_path, original_ids):
    """Validate that all comments in the output have valid IDs."""
    with open(output_path, 'r', encoding='utf-8') as f:
        for line in f:
            try:
                rec = json.loads(line)
                cid = rec.get('id')
                if not cid:
                    print(f"[WARN] Comment without ID found in {output_path}")
                    return False
            except json.JSONDecodeError:
                print(f"[WARN] Invalid JSON in {output_path}")
                return False
    print(f"[*] Validation passed: {output_path} has valid JSON lines")
    return True

# ============================================================
# MAIN
# ============================================================

def main():
    print("="*60)
    print("  EXTRACT BERTABAPORU SAMPLES BY ID")
    print("="*60)
    
    # 1. Load IDs from the original sample files
    print("\n[STEP 1] Loading IDs from original sample files...")
    q_ids = load_ids_from_file(RANK_Q_PATH)
    uq_ids = load_ids_from_file(RANK_UQ_PATH)
    
    if not q_ids and not uq_ids:
        print("[ERROR] No IDs found. Check the input files.")
        sys.exit(1)
    
    # Combine IDs for efficient single pass
    all_ids = q_ids | uq_ids
    print(f"[*] Total unique IDs to extract: {len(all_ids):,}")
    
    # 2. Extract comments from the BERTabaporu dataset
    print("\n[STEP 2] Extracting comments from BERTabaporu dataset...")
    extracted = extract_comments_from_dataset(all_ids, BERTABAPORU_PATH)
    
    if not extracted:
        print("[ERROR] No comments extracted. Check the dataset path.")
        sys.exit(1)
    
    # 3. Split by original sets
    print("\n[STEP 3] Splitting extracted comments by original sets...")
    
    q_extracted = {cid: extracted[cid] for cid in q_ids if cid in extracted}
    uq_extracted = {cid: extracted[cid] for cid in uq_ids if cid in extracted}
    
    print(f"[*] Q set: {len(q_extracted):,}/{len(q_ids):,} extracted")
    print(f"[*] UQ set: {len(uq_extracted):,}/{len(uq_ids):,} extracted")
    
    # 4. Save outputs
    print("\n[STEP 4] Saving outputs...")
    save_extracted(q_extracted, OUTPUT_Q_PATH)
    save_extracted(uq_extracted, OUTPUT_UQ_PATH)
    
    # 5. Validate
    print("\n[STEP 5] Validating outputs...")
    validate_output(OUTPUT_Q_PATH, q_ids)
    validate_output(OUTPUT_UQ_PATH, uq_ids)
    
    # 6. Summary
    print("\n" + "="*60)
    print("  EXTRACTION COMPLETE")
    print("="*60)
    print(f"  Total IDs requested: {len(all_ids):,}")
    print(f"  Total extracted:     {len(extracted):,}")
    print(f"  Q output:            {OUTPUT_Q_PATH} ({len(q_extracted):,} comments)")
    print(f"  UQ output:           {OUTPUT_UQ_PATH} ({len(uq_extracted):,} comments)")
    print("="*60)

if __name__ == "__main__":
    main()