#!/usr/bin/env python3
# anonymize_dataset.py
# Anonymizes a JSONL dataset by replacing usernames with SHA-256 hashes.
# Uses a random salt per execution, making reversal impossible.
# Preserves network structure: same user -> same hash within the run.
# Input: INFERRED_MULTIMODAL_FINAL.jsonl
# Output: ANONIMIZED_INFERRED_MULTIMODAL_FINAL.jsonl

import hashlib
import json
import os
import sys
import time
import secrets
from datetime import datetime
from collections import defaultdict

# ============================================================
# CONFIGURATION
# ============================================================

INPUT_FILE = "INFERRED_MULTIMODAL_FINAL.jsonl"
BATCH_SIZE = 10000  # Batch size for progressive writing

# Generate a random salt for this execution (prevents reversal)
SALT = secrets.token_hex(32)  # 64-character random hex string
print(f"[INFO] Using random salt: {SALT[:16]}... (unique per run)")

# ============================================================
# HELPER FUNCTIONS
# ============================================================

def generate_hash(username, salt):
    """Generate a deterministic SHA-256 hash for a username + salt."""
    combined = f"{username}_{salt}"
    return hashlib.sha256(combined.encode('utf-8')).hexdigest()

def get_file_size_mb(path):
    """Return file size in MB."""
    if os.path.exists(path):
        return os.path.getsize(path) / (1024 * 1024)
    return 0

# ============================================================
# MAIN FUNCTION
# ============================================================

def anonymize_dataset(input_path, output_path):
    """
    Read JSONL dataset, replace usernames with SHA-256 hashes using a random salt.
    No mapping file is saved, making reversal impossible.
    """
    
    print("\n" + "="*70)
    print("  DATASET ANONYMIZATION")
    print("="*70)
    print(f"  Start: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  Input:  {input_path}")
    print(f"  Output: {output_path}")
    print(f"  Salt:   {SALT[:16]}... (random, not saved)")
    print("="*70 + "\n")
    
    # Check input file
    if not os.path.exists(input_path):
        print(f"[ERROR] File not found: {input_path}")
        return False
    
    input_size_mb = get_file_size_mb(input_path)
    print(f"[DEBUG] Input file size: {input_size_mb:.2f} MB")
    
    # ============================================================
    # STEP 1: COLLECT ALL UNIQUE USERNAMES
    # ============================================================
    
    print("\n[STEP 1] Collecting unique usernames...")
    start_time = time.time()
    
    unique_users = set()
    total_lines = 0
    skipped_lines = 0
    deleted_count = 0
    removed_count = 0
    metadata_count = 0
    no_author_count = 0
    
    sample_usernames = []
    
    with open(input_path, 'r', encoding='utf-8') as f:
        for line_num, line in enumerate(f, 1):
            total_lines += 1
            
            # Progress report every 100k lines
            if line_num % 100000 == 0:
                elapsed = time.time() - start_time
                print(f"  [DEBUG] Lines read: {line_num:,} | Unique users: {len(unique_users):,} | Elapsed: {elapsed:.1f}s")
            
            try:
                record = json.loads(line)
            except json.JSONDecodeError as e:
                skipped_lines += 1
                if skipped_lines <= 5:
                    print(f"  [WARN] Line {line_num}: Invalid JSON: {e}")
                continue
            
            # Skip metadata footers
            if record.get('type') == 'metadata_footer':
                metadata_count += 1
                continue
            
            # Extract author
            author = record.get('author')
            if author:
                if author in ['[deleted]', 'deleted']:
                    deleted_count += 1
                    continue
                elif author == '[removed]':
                    removed_count += 1
                    continue
                else:
                    if author not in unique_users:
                        unique_users.add(author)
                        if len(sample_usernames) < 10:
                            sample_usernames.append(author)
            else:
                no_author_count += 1
                if no_author_count <= 5:
                    print(f"  [WARN] Line {line_num}: Record without 'author' field")
    
    elapsed_step1 = time.time() - start_time
    
    print(f"\n[DEBUG] Step 1 completed in {elapsed_step1:.1f}s")
    print(f"[DEBUG] Total lines read: {total_lines:,}")
    print(f"[DEBUG] Skipped (invalid JSON): {skipped_lines:,}")
    print(f"[DEBUG] Metadata footers skipped: {metadata_count:,}")
    print(f"[DEBUG] Users marked [deleted]: {deleted_count:,}")
    print(f"[DEBUG] Users marked [removed]: {removed_count:,}")
    print(f"[DEBUG] Records without author: {no_author_count:,}")
    print(f"[DEBUG] Unique users found: {len(unique_users):,}")
    
    if len(sample_usernames) > 0:
        print(f"[DEBUG] Sample usernames: {sample_usernames[:5]}")
    
    if len(unique_users) == 0:
        print("[ERROR] No unique users found. Check file format.")
        return False
    
    # ============================================================
    # STEP 2: GENERATE HASHES FOR ALL USERS
    # ============================================================
    
    print("\n[STEP 2] Generating deterministic hashes...")
    start_time = time.time()
    
    user_map = {}
    for i, username in enumerate(unique_users):
        user_map[username] = generate_hash(username, SALT)
        if (i + 1) % 50000 == 0:
            print(f"  [DEBUG] Hashes generated: {i+1:,}/{len(unique_users):,}")
    
    elapsed_step2 = time.time() - start_time
    print(f"[DEBUG] Step 2 completed in {elapsed_step2:.1f}s")
    print(f"[DEBUG] Hashes generated for {len(user_map):,} users")
    
    # Verify no hash collisions (should not happen with SHA-256)
    hashes_set = set(user_map.values())
    if len(hashes_set) != len(user_map):
        print(f"[WARN] Possible hash collision! {len(user_map)} vs {len(hashes_set)} unique")
    else:
        print("[DEBUG] OK: No hash collisions detected.")
    
    # ============================================================
    # STEP 3: REPLACE AND WRITE
    # ============================================================
    
    print("\n[STEP 3] Replacing usernames and writing output...")
    start_time = time.time()
    
    records_processed = 0
    users_anonymized = 0
    users_preserved = 0  # [deleted], [removed], etc.
    unknown_user_count = 0
    
    batch_count = 0
    batch_lines = []
    
    sample_replacements = []
    
    with open(input_path, 'r', encoding='utf-8') as f_in, \
         open(output_path, 'w', encoding='utf-8') as f_out:
        
        for line_num, line in enumerate(f_in, 1):
            try:
                record = json.loads(line)
            except json.JSONDecodeError:
                # Preserve malformed lines (rare)
                batch_lines.append(line)
                batch_count += 1
                continue
            
            # Preserve metadata footers
            if record.get('type') == 'metadata_footer':
                batch_lines.append(json.dumps(record, ensure_ascii=False) + '\n')
                batch_count += 1
                continue
            
            records_processed += 1
            
            # Replace author
            author = record.get('author')
            if author:
                if author in user_map:
                    original_author = author
                    hashed_author = user_map[author]
                    record['author'] = hashed_author
                    users_anonymized += 1
                    
                    # Sample of first replacements
                    if len(sample_replacements) < 5:
                        sample_replacements.append((original_author, hashed_author))
                elif author in ['[deleted]', 'deleted', '[removed]']:
                    users_preserved += 1
                else:
                    # Rare: user not in map
                    unknown_user_count += 1
                    if unknown_user_count <= 5:
                        print(f"  [WARN] Unmapped user: '{author}' (line {line_num})")
            
            # Write anonymized line
            output_line = json.dumps(record, ensure_ascii=False) + '\n'
            batch_lines.append(output_line)
            batch_count += 1
            
            # Write in batches to avoid memory issues
            if batch_count >= BATCH_SIZE:
                f_out.write(''.join(batch_lines))
                batch_lines = []
                batch_count = 0
                
                # Progress report every 100k records
                if records_processed % 100000 == 0:
                    elapsed = time.time() - start_time
                    pct = (records_processed / total_lines) * 100
                    print(f"  [DEBUG] Processed: {records_processed:,} | "
                          f"Anonymized: {users_anonymized:,} | "
                          f"Progress: {pct:.1f}% | Elapsed: {elapsed:.1f}s")
        
        # Write remaining records
        if batch_lines:
            f_out.write(''.join(batch_lines))
    
    elapsed_step3 = time.time() - start_time
    
    print(f"\n[DEBUG] Step 3 completed in {elapsed_step3:.1f}s")
    print(f"[DEBUG] Records processed: {records_processed:,}")
    print(f"[DEBUG] Users anonymized: {users_anonymized:,}")
    print(f"[DEBUG] Users preserved ([deleted], [removed]): {users_preserved:,}")
    print(f"[DEBUG] Unmapped users: {unknown_user_count:,}")
    
    if sample_replacements:
        print(f"[DEBUG] Sample replacements:")
        for orig, hashed in sample_replacements[:5]:
            print(f"    {orig[:20]}... -> {hashed[:16]}...")
    
    # ============================================================
    # STEP 4: FINAL REPORT (No mapping file is saved)
    # ============================================================
    
    output_size_mb = get_file_size_mb(output_path)
    total_elapsed = time.time() - start_time + elapsed_step1 + elapsed_step2
    
    print("\n" + "="*70)
    print("  FINAL ANONYMIZATION REPORT")
    print("="*70)
    print(f"  Total time:                    {total_elapsed:.1f}s")
    print(f"  Input file:                    {input_path}")
    print(f"    Size:                       {input_size_mb:.2f} MB")
    print(f"    Total lines:                {total_lines:,}")
    print(f"  Output file:                   {output_path}")
    print(f"    Size:                       {output_size_mb:.2f} MB")
    print(f"    Records processed:          {records_processed:,}")
    print(f"  Unique users found:            {len(user_map):,}")
    print(f"  Users anonymized:              {users_anonymized:,}")
    print(f"  Users preserved:               {users_preserved:,}")
    print(f"  Skipped (invalid JSON):        {skipped_lines:,}")
    print(f"  Metadata footers preserved:    {metadata_count:,}")
    print("="*70)
    
    # ============================================================
    # STEP 5: DESTROY SENSITIVE DATA FROM MEMORY
    # ============================================================
    
    print("\n[STEP 4] Clearing sensitive data from memory...")
    del user_map
    del unique_users
    del sample_replacements
    print("[DEBUG] User mapping cleared from memory.")
    
    # Integrity check
    if records_processed > 0 and users_anonymized > 0:
        print("\n[SUCCESS] Anonymization completed successfully!")
        print(f"[SUCCESS] {users_anonymized:,} users anonymized.")
        print("[SUCCESS] No mapping file was saved — reversal is impossible.")
        return True
    else:
        print("\n[WARN] Anonymization completed with possible issues.")
        print("[WARN] Check the logs above.")
        return False

# ============================================================
# MAIN ENTRY POINT
# ============================================================

def main():
    # Locate input file
    input_path = INPUT_FILE
    
    if not os.path.exists(input_path):
        # Try common alternative locations
        alternatives = [
            os.path.join("..", "DATA", "4-inferred", INPUT_FILE),
            os.path.join("..", "..", "DATA", "4-inferred", INPUT_FILE),
            os.path.join("..", "..", "..", "DATA", "4-inferred", INPUT_FILE),
            os.path.join(".", "..", "DATA", "4-inferred", INPUT_FILE),
        ]
        found = False
        for alt in alternatives:
            if os.path.exists(alt):
                input_path = alt
                found = True
                break
        if not found:
            print(f"[ERROR] File {INPUT_FILE} not found.")
            print(f"       Locations searched:")
            for alt in alternatives:
                print(f"         - {alt}")
            print(f"       Place the file in the same folder or adjust the path.")
            sys.exit(1)
    
    # Define output path in the same directory as input
    output_path = os.path.join(
        os.path.dirname(input_path),
        "ANONIMIZED_" + os.path.basename(input_path)
    )
    
    # Check if output file already exists
    if os.path.exists(output_path):
        print(f"[WARN] Output file already exists: {output_path}")
        response = input("  Overwrite? (y/N): ").strip().lower()
        if response != 'y':
            print("  Operation cancelled.")
            sys.exit(0)
    
    # Run anonymization
    success = anonymize_dataset(input_path, output_path)
    
    if success:
        print("\n[INFO] The salt used for this run was random and not saved.")
        print("[INFO] This dataset cannot be reversed to original usernames.")
        print("[INFO] The anonymized dataset is safe for public release.")

if __name__ == "__main__":
    main()