import json
import os
from datetime import datetime
from transformers import pipeline
# Recomendado rodar: pip install transformers torch

def apply_bert_filter(jsonl_filepath):
    print(f"\n[BERT FILTER] Iniciando triagem rápida no arquivo: {os.path.basename(jsonl_filepath)}")
    
    # 1. Carregar modelo leve (Pode ser o BERTimbau finetunado para toxicidade ou um multilingue rápido)
    # Exemplo de um modelo minúsculo e rápido:
    classifier = pipeline("text-classification", model="citizenlab/distilbert-base-multilingual-cased-toxicity", device=0) 
    
    base_dir = os.path.dirname(jsonl_filepath)
    filename = os.path.basename(jsonl_filepath).replace("MULTIMODAL_", "FILTERED_")
    out_path = os.path.join(base_dir, filename)
    
    total_comments = 0
    flagged_for_llama = 0
    
    with open(jsonl_filepath, 'r', encoding='utf-8') as f_in, \
         open(out_path, 'w', encoding='utf-8') as f_out:
        
        for line in f_in:
            record = json.loads(line)
            total_comments += 1
            
            # Truncar o texto para caber no limite padrão do BERT (512 tokens)
            text_to_analyze = record.get('body', '')[:1500] 
            
            if not text_to_analyze.strip():
                record['needs_llama'] = False
            else:
                # 2. Inferência rápida do BERT
                result = classifier(text_to_analyze)[0]
                
                # Exemplo: O modelo retorna label 'toxic' ou 'non-toxic' com um score
                # Se for tóxico ou se o score de toxicidade for maior que 0.15, manda pro Llama
                is_toxic = result['label'] == 'toxic'
                score = result['score']
                
                if is_toxic or (result['label'] != 'toxic' and score > 0.85): # Ajuste a lógica de acordo com o modelo escolhido
                    record['needs_llama'] = True
                    flagged_for_llama += 1
                else:
                    record['needs_llama'] = False
            
            f_out.write(json.dumps(record, ensure_ascii=False) + '\n')
            
    print(f"[BERT FILTER] Triagem concluída! {flagged_for_llama}/{total_comments} marcados para análise profunda.")
    return out_path