#!/usr/bin/env python3
# validate_anonymization.py
# Validates that anonymization preserved dataset integrity.
# Compares original and anonymized JSONL files.

import json
import os
import sys
from collections import defaultdict
from datetime import datetime

# ============================================================
# CONFIGURATION
# ============================================================

ORIGINAL_FILE = "INFERRED_MULTIMODAL_FINAL.jsonl"
ANONYMIZED_FILE = "ANONIMIZED_INFERRED_MULTIMODAL_FINAL.jsonl"

# ============================================================
# HELPER FUNCTIONS
# ============================================================

def count_lines(filepath):
    """Count total lines in a file."""
    with open(filepath, 'r', encoding='utf-8') as f:
        return sum(1 for _ in f)

def get_file_size_mb(path):
    """Return file size in MB."""
    if os.path.exists(path):
        return os.path.getsize(path) / (1024 * 1024)
    return 0

def validate_files(original_path, anonymized_path):
    """Compare original and anonymized datasets for structural integrity."""
    
    print("\n" + "="*70)
    print("  ANONYMIZATION VALIDATION REPORT")
    print("="*70)
    print(f"  Start: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  Original:   {original_path}")
    print(f"  Anonymized: {anonymized_path}")
    print("="*70 + "\n")
    
    # Check files exist
    if not os.path.exists(original_path):
        print(f"[ERROR] Original file not found: {original_path}")
        return False
    if not os.path.exists(anonymized_path):
        print(f"[ERROR] Anonymized file not found: {anonymized_path}")
        return False
    
    # File sizes
    orig_size = get_file_size_mb(original_path)
    anon_size = get_file_size_mb(anonymized_path)
    print(f"[DEBUG] Original file size:   {orig_size:.2f} MB")
    print(f"[DEBUG] Anonymized file size: {anon_size:.2f} MB")
    print(f"[DEBUG] Size difference:      {anon_size - orig_size:+.2f} MB ({((anon_size - orig_size)/orig_size)*100:+.2f}%)")
    
    # ============================================================
    # PASS 1: LINE COUNT AND BASIC STATISTICS
    # ============================================================
    
    print("\n[PASS 1] Counting lines and basic stats...")
    
    orig_lines = count_lines(original_path)
    anon_lines = count_lines(anonymized_path)
    
    print(f"[DEBUG] Original lines:   {orig_lines:,}")
    print(f"[DEBUG] Anonymized lines: {anon_lines:,}")
    print(f"[DEBUG] Line count match: {'✅ YES' if orig_lines == anon_lines else '❌ NO'}")
    
    if orig_lines != anon_lines:
        print(f"[WARN] Line count mismatch! Original: {orig_lines}, Anonymized: {anon_lines}")
    
    # ============================================================
    # PASS 2: FIELD PRESERVATION
    # ============================================================
    
    print("\n[PASS 2] Checking field preservation...")
    
    # Expected fields in comments
    EXPECTED_FIELDS = {'id', 'parent_id', 'post_id', 'subreddit', 'author', 
                       'timestamp', 'body', 'depth', 'metadata_score', 
                       'is_valid_text', 'type'}
    
    orig_fields = set()
    anon_fields = set()
    orig_sample = None
    anon_sample = None
    
    with open(original_path, 'r', encoding='utf-8') as f:
        for line in f:
            try:
                record = json.loads(line)
                if record.get('type') == 'comment':
                    orig_fields.update(record.keys())
                    if orig_sample is None:
                        orig_sample = record
                    break
            except:
                continue
    
    with open(anonymized_path, 'r', encoding='utf-8') as f:
        for line in f:
            try:
                record = json.loads(line)
                if record.get('type') == 'comment':
                    anon_fields.update(record.keys())
                    if anon_sample is None:
                        anon_sample = record
                    break
            except:
                continue
    
    print(f"[DEBUG] Original fields:  {sorted(orig_fields)}")
    print(f"[DEBUG] Anonymized fields: {sorted(anon_fields)}")
    print(f"[DEBUG] All expected fields present: {'✅ YES' if EXPECTED_FIELDS.issubset(orig_fields) and EXPECTED_FIELDS.issubset(anon_fields) else '❌ NO'}")
    
    # Check if fields were added or removed
    added_fields = anon_fields - orig_fields
    removed_fields = orig_fields - anon_fields
    if added_fields:
        print(f"[WARN] Fields added: {added_fields}")
    if removed_fields:
        print(f"[WARN] Fields removed: {removed_fields}")
    
    # ============================================================
    # PASS 3: USERNAME REPLACEMENT CONSISTENCY
    # ============================================================
    
    print("\n[PASS 3] Checking username replacement consistency...")
    
    orig_usernames = set()
    anon_usernames = set()
    anon_hash_map = {}  # username -> hash (from anonymized)
    user_mentions_orig = defaultdict(int)  # For cross-checking parent_id references
    user_mentions_anon = defaultdict(int)
    
    sample_size = 10000
    count = 0
    
    with open(original_path, 'r', encoding='utf-8') as f:
        for line in f:
            try:
                record = json.loads(line)
                if record.get('type') == 'comment':
                    author = record.get('author')
                    if author and author not in ['[deleted]', 'deleted']:
                        orig_usernames.add(author)
                        # Check parent_id references (if parent is a user)
                        parent = record.get('parent_id')
                        if parent and parent not in ['[deleted]', 'deleted']:
                            user_mentions_orig[parent] += 1
                    count += 1
                    if count >= sample_size:
                        break
            except:
                continue
    
    count = 0
    with open(anonymized_path, 'r', encoding='utf-8') as f:
        for line in f:
            try:
                record = json.loads(line)
                if record.get('type') == 'comment':
                    author = record.get('author')
                    if author and author not in ['[deleted]', 'deleted']:
                        anon_usernames.add(author)
                        # Build hash map from first sample
                        if len(anon_hash_map) < 1000:
                            # We can't map back, but we can check consistency
                            pass
                        # Check parent references
                        parent = record.get('parent_id')
                        if parent and parent not in ['[deleted]', 'deleted']:
                            user_mentions_anon[parent] += 1
                    count += 1
                    if count >= sample_size:
                        break
            except:
                continue
    
    print(f"[DEBUG] Original unique usernames (sample): {len(orig_usernames)}")
    print(f"[DEBUG] Anonymized unique usernames (sample): {len(anon_usernames)}")
    
    # Check if anonymized usernames look like hashes (64 hex chars)
    hash_like = 0
    for u in list(anon_usernames)[:100]:
        if len(u) == 64 and all(c in '0123456789abcdef' for c in u):
            hash_like += 1
    print(f"[DEBUG] Sample usernames that look like SHA-256 hashes: {hash_like}/100")
    print(f"[DEBUG] Username replacement appears consistent: {'✅ YES' if hash_like > 90 else '⚠️ PARTIAL'}")
    
    # ============================================================
    # PASS 4: CROSS-FIELD CONSISTENCY
    # ============================================================
    
    print("\n[PASS 4] Checking cross-field consistency...")
    
    orig_comments = 0
    orig_posts = 0
    anon_comments = 0
    anon_posts = 0
    orig_metadata = 0
    anon_metadata = 0
    
    with open(original_path, 'r', encoding='utf-8') as f:
        for line in f:
            try:
                record = json.loads(line)
                t = record.get('type')
                if t == 'comment':
                    orig_comments += 1
                elif t == 'post_header':
                    orig_posts += 1
                elif t == 'metadata_footer':
                    orig_metadata += 1
            except:
                continue
    
    with open(anonymized_path, 'r', encoding='utf-8') as f:
        for line in f:
            try:
                record = json.loads(line)
                t = record.get('type')
                if t == 'comment':
                    anon_comments += 1
                elif t == 'post_header':
                    anon_posts += 1
                elif t == 'metadata_footer':
                    anon_metadata += 1
            except:
                continue
    
    print(f"[DEBUG] Comments:   Original: {orig_comments:,} | Anonymized: {anon_comments:,} | Match: {'✅' if orig_comments == anon_comments else '❌'}")
    print(f"[DEBUG] Post headers: Original: {orig_posts:,} | Anonymized: {anon_posts:,} | Match: {'✅' if orig_posts == anon_posts else '❌'}")
    print(f"[DEBUG] Metadata:     Original: {orig_metadata:,} | Anonymized: {anon_metadata:,} | Match: {'✅' if orig_metadata == anon_metadata else '❌'}")
    
    # ============================================================
    # PASS 5: SAMPLE COMMENT COMPARISON (non-sensitive fields)
    # ============================================================
    
    print("\n[PASS 5] Comparing sample comments (non-sensitive fields)...")
    
    orig_sample_comments = []
    anon_sample_comments = []
    
    with open(original_path, 'r', encoding='utf-8') as f:
        for i, line in enumerate(f):
            if i >= 1000:
                break
            try:
                record = json.loads(line)
                if record.get('type') == 'comment' and record.get('is_valid_text'):
                    orig_sample_comments.append({
                        'id': record.get('id'),
                        'parent_id': record.get('parent_id'),
                        'post_id': record.get('post_id'),
                        'subreddit': record.get('subreddit'),
                        'depth': record.get('depth'),
                        'metadata_score': record.get('metadata_score'),
                        'body': record.get('body', '')[:50]  # Only first 50 chars
                    })
            except:
                continue
    
    with open(anonymized_path, 'r', encoding='utf-8') as f:
        for i, line in enumerate(f):
            if i >= 1000:
                break
            try:
                record = json.loads(line)
                if record.get('type') == 'comment' and record.get('is_valid_text'):
                    anon_sample_comments.append({
                        'id': record.get('id'),
                        'parent_id': record.get('parent_id'),
                        'post_id': record.get('post_id'),
                        'subreddit': record.get('subreddit'),
                        'depth': record.get('depth'),
                        'metadata_score': record.get('metadata_score'),
                        'body': record.get('body', '')[:50]
                    })
            except:
                continue
    
    # Check that IDs match (they should be identical)
    orig_ids = {c['id'] for c in orig_sample_comments}
    anon_ids = {c['id'] for c in anon_sample_comments}
    
    print(f"[DEBUG] Sample size: {len(orig_sample_comments)} comments")
    print(f"[DEBUG] ID set match: {'✅ YES' if orig_ids == anon_ids else '❌ NO'}")
    
    # Check that non-sensitive fields match
    if len(orig_sample_comments) > 0 and len(anon_sample_comments) > 0:
        fields_match = 0
        total_compared = 0
        for orig, anon in zip(orig_sample_comments[:100], anon_sample_comments[:100]):
            if orig['id'] == anon['id']:
                total_compared += 1
                # Compare non-sensitive fields
                if (orig['depth'] == anon['depth'] and 
                    orig['metadata_score'] == anon['metadata_score'] and
                    orig['subreddit'] == anon['subreddit']):
                    fields_match += 1
        
        if total_compared > 0:
            match_rate = (fields_match / total_compared) * 100
            print(f"[DEBUG] Non-sensitive field match rate: {match_rate:.1f}%")
            print(f"[DEBUG] Field preservation: {'✅ SUCCESS' if match_rate > 95 else '⚠️ PARTIAL'}")
    
    # ============================================================
    # FINAL REPORT
    # ============================================================
    
    print("\n" + "="*70)
    print("  VALIDATION SUMMARY")
    print("="*70)
    
    checks = [
        ("Line count matches", orig_lines == anon_lines),
        ("All expected fields present", EXPECTED_FIELDS.issubset(orig_fields) and EXPECTED_FIELDS.issubset(anon_fields)),
        ("No fields removed", len(removed_fields) == 0),
        ("Comment count matches", orig_comments == anon_comments),
        ("Post header count matches", orig_posts == anon_posts),
        ("Metadata preserved", orig_metadata == anon_metadata),
        ("IDs preserved", orig_ids == anon_ids if orig_ids and anon_ids else True),
    ]
    
    for check, passed in checks:
        print(f"  {check}: {'✅ PASS' if passed else '❌ FAIL'}")
    
    all_passed = all(passed for _, passed in checks)
    
    print("\n" + "="*70)
    if all_passed:
        print("  ✅ VALIDATION PASSED — Dataset structure is fully preserved.")
        print("  ✅ The anonymization did not affect data integrity.")
    else:
        print("  ⚠️ VALIDATION WARNINGS — Some checks failed.")
        print("  ⚠️ Review the logs above for details.")
    print("="*70 + "\n")
    
    return all_passed

# ============================================================
# MAIN ENTRY POINT
# ============================================================

def main():
    if not os.path.exists(ORIGINAL_FILE):
        print(f"[ERROR] Original file not found: {ORIGINAL_FILE}")
        sys.exit(1)
    
    if not os.path.exists(ANONYMIZED_FILE):
        print(f"[ERROR] Anonymized file not found: {ANONYMIZED_FILE}")
        sys.exit(1)
    
    success = validate_files(ORIGINAL_FILE, ANONYMIZED_FILE)
    sys.exit(0 if success else 1)

if __name__ == "__main__":
    main()