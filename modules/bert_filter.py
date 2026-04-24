import os
import json
import torch
from transformers import pipeline

bert_model="ruanchaves/bert-base-portuguese-cased-hatebr"

def apply_bert_filter(jsonl_filepath, threshold=0.15):
    """
    Filtro de triagem em cascata usando BERTimbau (HateBR).
    Processa o dataset multimodal e marca comentários suspeitos para o Llama-3.
    """
    print(f"\n[BERT FILTER] Iniciando triagem rápida no arquivo: {os.path.basename(jsonl_filepath)}")
    
    # Detecção automática de Hardware (Usa a GPU se disponível, senão cai para CPU)
    device = 0 if torch.cuda.is_available() else -1
    if device == 0:
        print("[BERT FILTER] Aceleração CUDA (GPU) detectada e ativada.")
    else:
        print("[BERT FILTER] GPU não detectada. Rodando na CPU.")

    # Carrega o modelo especialista em discurso de ódio do Brasil
    print("[BERT FILTER] Carregando modelo ruanchaves/bert-base-portuguese-cased-hatebr...")
    classifier = pipeline(
        "text-classification", 
        model="ruanchaves/bert-base-portuguese-cased-hatebr", 
        device=device,
        top_k=None # Força o modelo a retornar os scores de todas as labels
    )
    
    base_dir = os.path.dirname(jsonl_filepath)
    filename = os.path.basename(jsonl_filepath).replace("MULTIMODAL_", "FILTERED_")
    out_path = os.path.join(base_dir, filename)
    
    total_comments = 0
    flagged_for_llama = 0
    
    with open(jsonl_filepath, 'r', encoding='utf-8') as f_in, \
         open(out_path, 'w', encoding='utf-8') as f_out:
        
        for line in f_in:
            if not line.strip():
                continue
                
            record = json.loads(line)
            total_comments += 1
            
            # O BERT base aceita até 512 tokens. 
            # Cortamos em 1500 caracteres para garantir que não dê erro de OOM ou Crash.
            text_to_analyze = record.get('body', '')[:1500] 
            
            # Se o comentário for vazio (ex: post deletado ou apenas um link), pula a IA
            if not text_to_analyze.strip():
                record['needs_llama'] = False
            else:
                try:
                    # Inferência
                    results = classifier(text_to_analyze)[0]
                    
                    # Procura a probabilidade da label de ofensa ('LABEL_1' no HateBR)
                    needs_llama = False
                    for label_data in results:
                        if label_data['label'] == 'LABEL_1':
                            # Se a probabilidade de ofensa for maior que o threshold, aciona o LLM
                            if label_data['score'] >= threshold:
                                needs_llama = True
                            break
                            
                    record['needs_llama'] = needs_llama
                    if needs_llama:
                        flagged_for_llama += 1
                        
                except Exception as e:
                    # Em caso de erro na inferência de um comentário específico, 
                    # a política de "segurança em primeiro lugar" manda para o Llama avaliar.
                    print(f"[BERT FILTER] Erro ao processar comentário {record.get('id')}: {e}")
                    record['needs_llama'] = True
                    flagged_for_llama += 1
            
            # Grava no novo JSONL
            f_out.write(json.dumps(record, ensure_ascii=False) + '\n')
            
    print(f"\n[BERT FILTER] --- Triagem Concluída ---")
    print(f"[BERT FILTER] Total de Registros: {total_comments}")
    print(f"[BERT FILTER] Marcados para Llama-3: {flagged_for_llama} ({(flagged_for_llama/total_comments)*100:.1f}%)")
    print(f"[BERT FILTER] Salvo em: {out_path}")
    
    return out_path