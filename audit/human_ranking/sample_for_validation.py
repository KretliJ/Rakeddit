"""
sample_for_validation.py

Draws 500 stratified samples from an inferred JSONL file for human annotation.
Splits into 5 toxicity tiers of 100 samples each, based on RoBERTa toxicity_score:

    Tier 1 — Clearly Non-Toxic    : 0.00 – 0.20
    Tier 2 — Probably Non-Toxic   : 0.20 – 0.40
    Tier 3 — Ambiguous            : 0.40 – 0.60  ← most important for threshold calibration
    Tier 4 — Probably Toxic       : 0.60 – 0.80
    Tier 5 — Clearly Toxic        : 0.80 – 1.00

Output: CSV ready for manual annotation (open in Excel/Sheets and add a 'human_label' column)

Usage:
    python sample_for_validation.py --input YOUR_INFERRED_FILE.jsonl --output validation_sample.csv
    python sample_for_validation.py --input YOUR_INFERRED_FILE.jsonl  # uses default output name
"""

import json
import random
import csv
import argparse
import os
from collections import defaultdict

# ==========================================
# CONFIG
# ==========================================
TIERS = [
    ("1_clearly_non_toxic",   0.00, 0.20),
    ("2_probably_non_toxic",  0.20, 0.40),
    ("3_ambiguous",           0.40, 0.60),
    ("4_probably_toxic",      0.60, 0.80),
    ("5_clearly_toxic",       0.80, 1.01),  # 1.01 to include exactly 1.0
]
SAMPLES_PER_TIER = 100
SEED = 42

# Fields to include in the output CSV
# Add or remove fields as needed for your annotation workflow
OUTPUT_FIELDS = [
    "tier",
    "toxicity_score",
    "ai_label",
    "ai_confidence",
    "type",
    "subreddit",
    "depth",
    "body",
    "title",          # only on post_header nodes
    "id",
    "parent_id",
    "author",
    "human_label",    # blank column for annotator to fill
    "annotator_notes" # blank column for annotator comments
]

# ==========================================
# FILTERING
# Skips bypass nodes — they have no real text for a human to judge
# ==========================================
SKIP_LABELS = {"BYPASS_EMPTY", "REMOVED_BY_MOD", "USER_DELETED", "AUTOMOD_WARNING", "ERROR"}

def is_annotatable(record):
    """Returns True if this record has real text a human can evaluate."""
    ai_label = record.get('ai_analysis', {}).get('label', '')
    if ai_label in SKIP_LABELS:
        return False
    body = record.get('body', '').strip()
    if not body or len(body) < 10:
        return False
    if body.startswith('[CONTEUDO VISUAL:') and len(body) < 30:
        return False  # vision fallback with no real description
    return True

# ==========================================
# MAIN
# ==========================================
def main(input_path, output_path):
    random.seed(SEED)

    print(f"\n[*] Reading: {os.path.basename(input_path)}")

    # Bucket records by tier
    buckets = defaultdict(list)

    with open(input_path, 'r', encoding='utf-8') as f:
        for line in f:
            try:
                record = json.loads(line)
            except json.JSONDecodeError:
                continue

            if record.get('type') == 'metadata_footer':
                continue
            if not is_annotatable(record):
                continue

            score = record.get('toxicity_score')
            if score is None:
                continue

            for tier_name, low, high in TIERS:
                if low <= score < high:
                    buckets[tier_name].append(record)
                    break

    # Report bucket sizes before sampling
    print("\n[*] Records per tier before sampling:")
    for tier_name, low, high in TIERS:
        count = len(buckets[tier_name])
        status = "✓" if count >= SAMPLES_PER_TIER else f"⚠️  ONLY {count} AVAILABLE"
        print(f"    {tier_name}: {count:,} records {status}")

    # Sample from each tier
    sampled = []
    for tier_name, low, high in TIERS:
        pool = buckets[tier_name]
        n = min(SAMPLES_PER_TIER, len(pool))
        drawn = random.sample(pool, n)

        for record in drawn:
            ai = record.get('ai_analysis', {})
            row = {
                "tier":            tier_name,
                "toxicity_score":  round(record.get('toxicity_score', 0.0), 4),
                "ai_label":        ai.get('label', ''),
                "ai_confidence":   ai.get('confidence', ''),
                "type":            record.get('type', ''),
                "subreddit":       record.get('subreddit', ''),
                "depth":           record.get('depth', ''),
                "body":            record.get('body', '').replace('\n', ' ').strip(),
                "title":           record.get('title', ''),
                "id":              record.get('id', ''),
                "parent_id":       record.get('parent_id', ''),
                "author":          record.get('author', ''),
                "human_label":     '',   # annotator fills this: 0 = not toxic, 1 = toxic
                "annotator_notes": ''    # annotator fills this: free text
            }
            sampled.append(row)

    # Shuffle so tiers aren't in order (reduces annotation bias)
    random.shuffle(sampled)

    # Write CSV
    with open(output_path, 'w', encoding='utf-8', newline='') as f_out:
        writer = csv.DictWriter(f_out, fieldnames=OUTPUT_FIELDS)
        writer.writeheader()
        writer.writerows(sampled)

    print(f"\n[+] Done. {len(sampled)} samples written to: {output_path}")
    print(f"\n[ANNOTATION INSTRUCTIONS]")
    print(f"    Open the CSV in Excel or Google Sheets.")
    print(f"    Fill the 'human_label' column: 0 = not toxic, 1 = toxic.")
    print(f"    Use 'annotator_notes' for borderline cases or reasoning.")
    print(f"    The 'tier' column is the model's guess — try not to let it anchor you.")
    print(f"    Tier 3 (ambiguous, 0.40-0.60) is the most important tier for threshold calibration.")
    print(f"    Focus on Brazilian Portuguese context — irony, political slang, and memes matter.\n")

    # Print tier distribution in final sample
    from collections import Counter
    tier_counts = Counter(row['tier'] for row in sampled)
    print("[*] Final sample distribution:")
    for tier_name, _, _ in TIERS:
        print(f"    {tier_name}: {tier_counts.get(tier_name, 0)} samples")


# ==========================================
# ENTRY POINT
# ==========================================
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Sample JSONL for human toxicity annotation")
    parser.add_argument(
        '--input', '-i',
        required=True,
        help='Path to inferred JSONL file (e.g. INFERRED_MULTIMODAL_FINAL.jsonl)'
    )
    parser.add_argument(
        '--output', '-o',
        default='validation_sample.csv',
        help='Output CSV path (default: validation_sample.csv)'
    )
    args = parser.parse_args()

    if not os.path.exists(args.input):
        print(f"[ERROR] Input file not found: {args.input}")
        exit(1)

    main(args.input, args.output)
