import json
import numpy as np
from sklearn.metrics import accuracy_score, f1_score, confusion_matrix

def evaluate_roberta_baseline(jsonl_path, threshold=0.5):
    print(f"[*] Carregando predições do RoBERTa de: {jsonl_path}")
    
    y_true = []
    y_pred = []
    
    with open(jsonl_path, 'r', encoding='utf-8') as f:
        for line in f:
            try:
                record = json.loads(line)
                
                # Ignora metadados
                if record.get('type') not in ['post_header', 'comment']:
                    continue
                
                # Aqui você precisa colocar o nome do campo onde está o seu Rótulo Real (Ground Truth)
                # que você usou para treinar/validar a GNN. Vou assumir 'label' ou 'is_toxic'.
                # Troque 'label' pelo nome correto da sua chave de ground truth no JSON!
                if 'label' not in record: 
                    continue
                    
                true_label = int(record['toxicity_score'])
                
                # Pega a probabilidade gerada pelo motor de inferência (RoBERTa)
                tox_score = float(record.get('toxicity_score', 0.0))
                
                # Aplica o Threshold: Se a chance for >= 50%, é tóxico (1), senão pacífico (0)
                pred_label = 1 if tox_score >= threshold else 0
                
                y_true.append(true_label)
                y_pred.append(pred_label)
                
            except json.JSONDecodeError:
                continue

    if not y_true:
        print("[-] Nenhum dado com 'label' (Ground Truth) encontrado no arquivo.")
        return

    # Cálculos usando Scikit-Learn
    acc = accuracy_score(y_true, y_pred)
    f1 = f1_score(y_true, y_pred)
    cm = confusion_matrix(y_true, y_pred)
    
    # Extraindo os valores da Matriz de Confusão
    # Assumindo classes [0, 1] onde 1 é Tóxico
    tn, fp, fn, tp = cm.ravel()

    print("\n[========== RESULTADOS BASELINE: XLM-RoBERTa ==========]")
    print(f"[+] Threshold de Corte : {threshold}")
    print(f"[+] Accuracy Geral     : {acc:.4f}")
    print(f"[+] F1-Score (Tóxico)  : {f1:.4f}")
    print("[+] Matriz de Confusão :")
    print(f"    TN (Falso Pacífico): {tn} | FP (Alarme Falso) : {fp}")
    print(f"    FN (Ódio Não Visto): {fn} | TP (Ódio Detectado): {tp}")
    
    return f1

if __name__ == "__main__":
    # Teste no dataset Multimodal (Visão + YouTube)
    print("\n--- TESTE 1: RoBERTa com Visão/YouTube ---")
    evaluate_roberta_baseline('./DATA/4-inferred/INFERRED_MULTIMODAL_FINAL.jsonl', threshold=0.5)
    
    # Se você já rodou a inferência no dataset "Cego" (sem visão), descomente abaixo:
    # print("\n--- TESTE 2: RoBERTa Cego (Ablation) ---")
    # evaluate_roberta_baseline('./DATA/4-inferred/INFERRED_BLIND.jsonl', threshold=0.5)