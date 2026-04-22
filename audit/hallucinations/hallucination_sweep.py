import json
import os

# ==========================================
# PATH CONFIG
# ==========================================
# Change this to the name of your final file
INPUT_FILE = 'inputs/INFERRED_MULTIMODAL_SUBREDDIT_data_normalized_TIMESTAMP.jsonl' # GIVEN EXAMPLE
base_name = os.path.basename(INPUT_FILE)
# Exit file handling
CLEAN_FILE = os.path.join('./outputs', base_name.replace('.jsonl', '_CLEAN.jsonl'))
DIRTY_FILE = os.path.join('./outputs', base_name.replace('.jsonl', '_HALLUCINATED.jsonl'))

def sanitize_dataset(input_path, clean_path, dirty_path):
    stats = {"total": 0, "clean": 0, "dirty": 0, "errors_json": 0}
    
    print(f"\n🧹 [SWEEP] Initiating hallucination sweep in: {input_path}")
    
    with open(input_path, 'r', encoding='utf-8') as f_in, \
         open(clean_path, 'w', encoding='utf-8') as f_clean, \
         open(dirty_path, 'w', encoding='utf-8') as f_dirty:
        
        for line in f_in:
            stats["total"] += 1
            
            # 1. Attempts to decode JSON (in case the model decides to break quotes)
            try:
                record = json.loads(line)
            except json.JSONDecodeError:
                stats["errors_json"] += 1
                stats["dirty"] += 1
                f_dirty.write(line) # Saves broken line for debug
                continue

            analysis = record.get('ai_analysis', {})
            
            # 2. Strict sanity check using the upper limits of f1-f5 and aggro
            try:
                flags_valid = all(analysis.get(f) in [0, 1] for f in ['f1', 'f2', 'f3', 'f4', 'f5'])
                aggro_valid = analysis.get('aggro') in [0, 1, 2, 3]
                
                if flags_valid and aggro_valid:
                    f_clean.write(json.dumps(record, ensure_ascii=False) + '\n')
                    stats["clean"] += 1
                else:
                    f_dirty.write(json.dumps(record, ensure_ascii=False) + '\n')
                    stats["dirty"] += 1
            except AttributeError:
                # If 'ai_analysis' came empty or null
                f_dirty.write(json.dumps(record, ensure_ascii=False) + '\n')
                stats["dirty"] += 1

    # ==========================================
    # FINAL REPORT
    # ==========================================
    if stats["total"] > 0:
        clean_pct = (stats["clean"] / stats["total"]) * 100
        dirty_pct = (stats["dirty"] / stats["total"]) * 100
    else:
        clean_pct = dirty_pct = 0.0

    print("\n==================================================")
    print("📊 HALLUCINATION SWEEP")
    print("==================================================")
    print(f"Total analyzed registries:       {stats['total']}")
    print(f"✅ Clean registries:            {stats['clean']} ({clean_pct:.2f}%)")
    print(f"❌ Hallucinations:              {stats['dirty']} ({dirty_pct:.2f}%)")
    
    if stats["errors_json"] > 0:
        print(f"   ↳ JSON Parse Errors:      {stats['errors_json']}")
        
    print("==================================================")
    print(f"📁 Validated based saved in: {clean_path}")

if __name__ == "__main__":
    os.makedirs('./outputs', exist_ok=True)
    os.makedirs('./inputs', exist_ok=True)
    if not os.path.exists(INPUT_FILE):
        print(f"❌ Error: '{INPUT_FILE}' could not be found.")
        print("Update INPUT_FILE with the correct path.")
    else:
        sanitize_dataset(INPUT_FILE, CLEAN_FILE, DIRTY_FILE)