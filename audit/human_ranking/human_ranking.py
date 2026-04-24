import json
import os
import sys

# --- CONFIGURATION & WEIGHTS ---
INPUT_FILE = "audit\human_ranking\inputs\opiniaoburra_sample.jsonl"
OUTPUT_FILE = "audit\human_ranking\inputs\opiniaoburra_HUMAN-AUDITED.jsonl"

WEIGHTS = {
    'f1': 0.5,   # Profanity/Obscenity (Art 233 CP)
    'f2': 1.0,   # Threats (Art 147 CP)
    'f3': 1.0,   # Insult/Ad Hominem (Art 140 CP)
    'f4': 2.0,   # Identity Hate (Lei 7.716/89)
    'f5': 0.25   # Perturbation/Trolling (Art 42 LCP)
}

AGGRO_MULTIPLIERS = {
    0: 0.875,  # Attenuant
    1: 1.0,    # Generic Intent
    2: 1.125,  # Aggravating Circumstances
    3: 1.25    # Qualifying Circumstances
}

CHEAT_SHEET = """
======================== CHEAT SHEET ========================
[f1] Profanity (0/1): Swearing, obscenities, breach of decorum.
[f2] Threats (0/1): Actionable promise of unjust/grave damage.
[f3] Insult (0/1): Direct Ad Hominem against the interlocutor.
[f4] Identity Hate (0/1): Dehumanization of protected groups.
[f5] Trolling (0/1): Spam, baiting, disruption of peace.
[aggro] Dosimetry (0-3): 0=Functional, 1=Sarcasm, 2=Hostile, 3=Rage.
=============================================================
"""

def calculate_toxicity_score(flags, aggro):
    """Recalculates the toxicity score based on the methodology formula."""
    total_weight = sum(WEIGHTS.values()) # 4.75
    score_sum = sum(flags[k] * WEIGHTS[k] for k in WEIGHTS)
    multiplier = AGGRO_MULTIPLIERS.get(aggro, 1.0)
    
    final_score = (score_sum / total_weight) * multiplier
    return round(min(1.0, final_score), 2)

def build_context_chain(target_id, records_dict):
    """Reconstructs the conversation context based on parent_id."""
    chain = []
    current_id = target_id
    
    while current_id in records_dict:
        parent_id = records_dict[current_id].get("parent_id")
        # Prevent infinite loops and ensure parent exists and is not self
        if not parent_id or parent_id == current_id or parent_id not in records_dict:
            break
        
        parent_record = records_dict[parent_id]
        author = parent_record.get("author", "Unknown")
        body = parent_record.get("body", "")
        chain.insert(0, f"[{author}]: {body}")
        
        current_id = parent_id
        
    return "\n".join(chain)

def clear_screen():
    os.system('cls' if os.name == 'nt' else 'clear')

def main():
    # 1. Load Data
    if not os.path.exists(INPUT_FILE):
        print(f"Error: {INPUT_FILE} not found.")
        sys.exit(1)

    records = []
    records_dict = {}
    
    with open(INPUT_FILE, 'r', encoding='utf-8') as f:
        for line in f:
            if line.strip():
                record = json.loads(line)
                records.append(record)
                records_dict[record["id"]] = record

    # Load already audited data to resume progress
    audited_ids = set()
    if os.path.exists(OUTPUT_FILE):
        with open(OUTPUT_FILE, 'r', encoding='utf-8') as f:
            for line in f:
                if line.strip():
                    audited_ids.add(json.loads(line)["id"])

    # 2. Main Audit Loop
    i = 0
    while i < len(records):
        record = records[i]
        
        if record["id"] in audited_ids:
            i += 1
            continue

        clear_screen()
        print(CHEAT_SHEET)
        
        # Build and display Context
        context_chain = build_context_chain(record["id"], records_dict)
        print("--- CONVERSATION CONTEXT ---")
        if context_chain:
            print(context_chain)
        else:
            print("[No parent context found or root post]")
        
        print("\n--- TARGET COMMENT ---")
        print(f"[{record.get('author', 'Unknown')}]: {record.get('body', '')}")
        
        # AI Analysis display
        ai_data = record.get("ai_analysis", {})
        ai_score = record.get("toxicity_score", 0.0)
        print("\n--- AI CLASSIFICATION ---")
        print(f"f1: {ai_data.get('f1', 0)} | f2: {ai_data.get('f2', 0)} | f3: {ai_data.get('f3', 0)} | f4: {ai_data.get('f4', 0)} | f5: {ai_data.get('f5', 0)}")
        print(f"Aggro: {ai_data.get('aggro', 0)} | S (Score): {ai_score}")
        
        print("\nOptions: [y] Accept AI | [n] Manual Edit | [back] Previous | [q] Quit")
        choice = input("Select action: ").strip().lower()
        
        if choice == 'q':
            print("Progress saved. Exiting...")
            break
        elif choice == 'back':
            # Remove the last audited ID from the file to redo it
            if i > 0:
                i -= 1
                last_record_id = records[i]["id"]
                if last_record_id in audited_ids:
                    audited_ids.remove(last_record_id)
                    # Rewrite the output file without the last record
                    with open(OUTPUT_FILE, 'r', encoding='utf-8') as f:
                        lines = f.readlines()
                    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
                        for line in lines:
                            if json.loads(line)["id"] != last_record_id:
                                f.write(line)
            continue
        elif choice == 'y' or choice == '':
            record["human_audited"] = True
            record["human_analysis"] = ai_data
            record["human_toxicity_score"] = ai_score
        elif choice == 'n':
            print("\n--- MANUAL OVERRIDE ---")
            new_flags = {}
            for flag in ['f1', 'f2', 'f3', 'f4', 'f5']:
                while True:
                    try:
                        val = int(input(f"Enter {flag} (0 or 1): "))
                        if val in [0, 1]:
                            new_flags[flag] = val
                            break
                    except ValueError:
                        pass
                    print("Invalid input. Please enter 0 or 1.")
            
            while True:
                try:
                    aggro_val = int(input("Enter aggro (0, 1, 2, or 3): "))
                    if aggro_val in [0, 1, 2, 3]:
                        break
                except ValueError:
                    pass
                print("Invalid input. Please enter 0, 1, 2, or 3.")
            
            new_score = calculate_toxicity_score(new_flags, aggro_val)
            print(f"\nNew Recalculated Score: {new_score}")
            
            # Confirm correction
            confirm = input("Save this correction? [y/N]: ").strip().lower()
            if confirm != 'y':
                continue # Re-do this record
            
            record["human_audited"] = True
            record["human_analysis"] = new_flags
            record["human_analysis"]["aggro"] = aggro_val
            record["human_toxicity_score"] = new_score

        else:
            print("Invalid command.")
            continue
            
        # Append to output file immediately to prevent data loss
        with open(OUTPUT_FILE, 'a', encoding='utf-8') as out_f:
            out_f.write(json.dumps(record, ensure_ascii=False) + "\n")
        
        audited_ids.add(record["id"])
        i += 1

if __name__ == "__main__":
    main()