#!/usr/bin/env python3
# sample_comments.py
# Extracts 100 random comments per quartile (cascade and user) from the multimodal dataset.

import json
import random
import os
from collections import defaultdict

# Fixed seed for reproducibility
RANDOM_SEED = 42
random.seed(RANDOM_SEED)

# Input file (expected in the same directory as this script)
INPUT_FILE = "INFERRED_MULTIMODAL_FINAL.jsonl"

# Output files
OUTPUT_CASCADE = "RANK_BASE_Q.jsonl"
OUTPUT_USER = "RANK_BASE_UQ.jsonl"

# Quartile boundaries (same as in the original project)
BINS = [(-float('inf'), 25, "Q1"),
        (25, 50, "Q2"),
        (50, 75, "Q3"),
        (75, float('inf'), "Q4")]

def assign_quartile(perc):
    """Assign quartile based on negativity percentage."""
    for lower, upper, label in BINS:
        if lower < perc <= upper:
            return label
    return "Q4"  # fallback

def main():
    if not os.path.exists(INPUT_FILE):
        print(f"Error: {INPUT_FILE} not found. Place it in this folder.")
        return

    print("Reading dataset...")
    # Aggregate per cascade (post_id) and per user
    cascade_counts = defaultdict(lambda: {'total': 0, 'neg': 0})
    user_counts = defaultdict(lambda: {'total': 0, 'neg': 0})
    comments = []  # store all valid comments with relevant fields

    with open(INPUT_FILE, 'r', encoding='utf-8') as f:
        for line in f:
            try:
                rec = json.loads(line)
            except json.JSONDecodeError:
                continue
            # Only comments with valid text and AI label
            if rec.get('type') != 'comment':
                continue
            if not rec.get('is_valid_text', False):
                continue
            ai = rec.get('ai_analysis')
            if not ai or 'label' not in ai:
                continue
            label = ai['label']
            if label not in ('POSITIVE', 'NEUTRAL', 'NEGATIVE'):
                continue

            # Collect comment info
            comment = {
                'id': rec['id'],
                'post_id': rec['post_id'],
                'author': rec['author'],
                'body': rec['body'],
                'ai_label': label,
                'original': rec
            }
            comments.append(comment)

            # Update cascade counts
            post_id = rec['post_id']
            cascade_counts[post_id]['total'] += 1
            if label == 'NEGATIVE':
                cascade_counts[post_id]['neg'] += 1

            # Update user counts
            author = rec['author']
            user_counts[author]['total'] += 1
            if label == 'NEGATIVE':
                user_counts[author]['neg'] += 1

    print(f"Loaded {len(comments)} valid comments.")

    # Compute negativity percentages
    cascade_perc = {pid: (cnt['neg'] / cnt['total']) * 100 if cnt['total'] > 0 else 0
                    for pid, cnt in cascade_counts.items()}
    user_perc = {auth: (cnt['neg'] / cnt['total']) * 100 if cnt['total'] > 0 else 0
                 for auth, cnt in user_counts.items()}

    # Assign quartiles to each comment
    for c in comments:
        post_id = c['post_id']
        author = c['author']
        c['cascade_quartile'] = assign_quartile(cascade_perc.get(post_id, 0))
        c['user_quartile'] = assign_quartile(user_perc.get(author, 0))

    # Sampling function
    def sample_by_quartile(comments, key, sample_size=100):
        """Return a list of sampled comments per quartile."""
        quartile_groups = defaultdict(list)
        for c in comments:
            quartile_groups[c[key]].append(c)
        sampled = []
        for q in ['Q1', 'Q2', 'Q3', 'Q4']:
            pool = quartile_groups[q]
            if len(pool) >= sample_size:
                sampled.extend(random.sample(pool, sample_size))
            else:
                # If less than desired, take all (with a warning)
                print(f"Warning: {key} quartile {q} has only {len(pool)} comments, taking all.")
                sampled.extend(pool)
        return sampled

    cascade_samples = sample_by_quartile(comments, 'cascade_quartile')
    user_samples = sample_by_quartile(comments, 'user_quartile')

    # Write output files
    def write_samples(samples, outfile, quartile_key):
        with open(outfile, 'w', encoding='utf-8') as f:
            for c in samples:
                # Include quartile and original record
                rec = c['original'].copy()
                rec['sampled_quartile'] = c[quartile_key]
                rec['sampling_type'] = quartile_key.replace('_quartile', '')
                f.write(json.dumps(rec, ensure_ascii=False) + '\n')
        print(f"Written {len(samples)} samples to {outfile}")

    write_samples(cascade_samples, OUTPUT_CASCADE, 'cascade_quartile')
    write_samples(user_samples, OUTPUT_USER, 'user_quartile')

    print("Sampling complete.")

if __name__ == "__main__":
    main()