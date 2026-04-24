import json
import os

# --- CONFIGURATION ---
AUDITED_FILE = "audit\human_ranking\inputs\opiniaoburra_sample.jsonl"
THRESHOLD = 0.35

def calculate_metrics(tp, fp, fn, tn):
    precision = tp / (tp + fp) if (tp + fp) > 0 else 0
    recall = tp / (tp + fn) if (tp + fn) > 0 else 0
    f1 = 2 * (precision * recall) / (precision + recall) if (precision + recall) > 0 else 0
    accuracy = (tp + tn) / (tp + fp + fn + tn) if (tp + fp + fn + tn) > 0 else 0
    return precision, recall, f1, accuracy

def main():
    if not os.path.exists(AUDITED_FILE):
        print(f"Error: {AUDITED_FILE} not found. Run human_ranking.py first.")
        return

    # Metrics accumulators
    # General: Any AI Score > 0 is positive
    gen_tp, gen_fp, gen_fn, gen_tn = 0, 0, 0, 0
    
    # Thresholded: AI Score >= 0.35 is positive
    thr_tp, thr_fp, thr_fn, thr_tn = 0, 0, 0, 0

    with open(AUDITED_FILE, 'r', encoding='utf-8') as f:
        for line in f:
            data = json.loads(line)
            
            # Ground Truth from Human Auditor
            is_truly_toxic = data["human_toxicity_score"] >= THRESHOLD
            
            # AI Predictions
            ai_score = data["toxicity_score"]
            ai_pred_gen = ai_score > 0
            ai_pred_thr = ai_score >= THRESHOLD

            # 1. Evaluate General Scenario (Baseline)
            if ai_pred_gen and is_truly_toxic: gen_tp += 1
            elif ai_pred_gen and not is_truly_toxic: gen_fp += 1
            elif not ai_pred_gen and is_truly_toxic: gen_fn += 1
            else: gen_tn += 1

            # 2. Evaluate Thresholded Scenario (Proposed Architecture)
            if ai_pred_thr and is_truly_toxic: thr_tp += 1
            elif ai_pred_thr and not is_truly_toxic: thr_fp += 1
            elif not ai_pred_thr and is_truly_toxic: thr_fn += 1
            else: thr_tn += 1

    # Calculate final numbers
    p_gen, r_gen, f1_gen, acc_gen = calculate_metrics(gen_tp, gen_fp, gen_fn, gen_tn)
    p_thr, r_thr, f1_thr, acc_thr = calculate_metrics(thr_tp, thr_fp, thr_fn, thr_tn)

    # --- OUTPUT REPORT ---
    print("\n" + "="*50)
    print("ARCHITECTURAL PERFORMANCE REPORT")
    print("="*50)
    print(f"Total Samples Analyzed: {gen_tp + gen_fp + gen_fn + gen_tn}")
    print(f"Normative Threshold applied: {THRESHOLD}")
    print("-" * 50)
    
    print(f"{'Metric':<15} | {'General (S > 0)':<18} | {'Thresholded (S >= 0.35)':<18}")
    print("-" * 50)
    print(f"{'Precision':<15} | {p_gen:<18.4f} | {p_thr:<18.4f}")
    print(f"{'Recall':<15} | {r_gen:<18.4f} | {r_thr:<18.4f}")
    print(f"{'F1-Score':<15} | {f1_gen:<18.4f} | {f1_thr:<18.4f}")
    print(f"{'Accuracy':<15} | {acc_gen:<18.4f} | {acc_thr:<18.4f}")
    print("-" * 50)
    
    print("\nRAW CONFUSION MATRIX (Thresholded):")
    print(f"TP: {thr_tp} | FP: {thr_fp}")
    print(f"FN: {thr_fn} | TN: {thr_tn}")
    print("="*50)

if __name__ == "__main__":
    main()