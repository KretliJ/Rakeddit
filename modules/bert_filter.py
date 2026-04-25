import os
import gc
import json
import torch
from tqdm import tqdm
from transformers import pipeline, AutoConfig, AutoModelForSequenceClassification, AutoTokenizer

bert_model_name = "ruanchaves/bert-base-portuguese-cased-hatebr"

# --- [FIX] ---
def _fix_and_load_local_model():
    local_dir = "./DATA/models/hatebr_fixed"
    model_weight_path = os.path.join(local_dir, "pytorch_model.bin")
    safetensors_path = os.path.join(local_dir, "model.safetensors")
    
    if not (os.path.exists(model_weight_path) or os.path.exists(safetensors_path)):
        print(f"[BERT FILTER] Baixando arquivos brutos...")
        from huggingface_hub import snapshot_download
        snapshot_download(repo_id=bert_model_name, local_dir=local_dir, local_dir_use_symlinks=False)
        
        config_path = os.path.join(local_dir, "config.json")
        with open(config_path, 'r', encoding='utf-8') as f:
            config_data = json.load(f)
        config_data['id2label'] = {str(k): str(v) for k, v in config_data.get('id2label', {}).items()}
        config_data['label2id'] = {str(k): int(v) for k, v in config_data.get('label2id', {}).items()}
        with open(config_path, 'w', encoding='utf-8') as f:
            json.dump(config_data, f, indent=2)

    config = AutoConfig.from_pretrained(local_dir)
    tokenizer = AutoTokenizer.from_pretrained(local_dir)
    model = AutoModelForSequenceClassification.from_pretrained(local_dir, config=config)
    return model, tokenizer

# --- GERADOR DE DADOS PARA EFICIÊNCIA ---
def data_generator(filepath):
    with open(filepath, 'r', encoding='utf-8') as f:
        for line in f:
            if line.strip():
                yield json.loads(line)

def apply_bert_filter(jsonl_filepath, threshold=0.15, batch_size=32):
    print(f"\n[BERT FILTER] Iniciando triagem em BATCH: {os.path.basename(jsonl_filepath)}")
    
    device = 0 if torch.cuda.is_available() else -1
    model, tokenizer = _fix_and_load_local_model()

    classifier = pipeline(
        "text-classification", 
        model=model, 
        tokenizer=tokenizer,
        device=device,
        top_k=None,
        truncation=True,
        max_length=512
    )

    base_dir = os.path.dirname(jsonl_filepath)
    filename = os.path.basename(jsonl_filepath).replace("MULTIMODAL_", "FILTERED_")
    out_path = os.path.join(base_dir, filename)
    
    # Carregando registros
    records = []
    with open(jsonl_filepath, 'r', encoding='utf-8') as f:
        for line in f:
            if line.strip():
                records.append(json.loads(line))
                
    texts = [r.get('body', '')[:1500] for r in records]

    print(f"[BERT FILTER] Rodando inferência paralela na GPU (Batch Size: {batch_size})...")
    
    all_results = []
    
    # Envolvemos a chamada do classificador no tqdm para gerar a barra
    for out in tqdm(classifier(texts, batch_size=batch_size), total=len(texts), desc="[BERT] Triagem", unit="comentários"):
        all_results.append(out)

    flagged_count = 0
    with open(out_path, 'w', encoding='utf-8') as f_out:
        for record, results in zip(records, all_results):
            needs_llama = False
            for label_data in results:
                if str(label_data['label']) in ['LABEL_1', 'True', '1']:
                    if label_data['score'] >= threshold:
                        needs_llama = True
                    break
            
            record['needs_llama'] = needs_llama
            if needs_llama: 
                flagged_count += 1
            f_out.write(json.dumps(record, ensure_ascii=False) + '\n')

    print(f"\n[BERT FILTER] --- Triagem Concluída ---")
    print(f"[BERT FILTER] Total de Registros: {len(records)}")
    print(f"[BERT FILTER] Marcados para Llama-3: {flagged_count} ({(flagged_count/max(len(records), 1))*100:.1f}%)")
    print(f"[BERT FILTER] Salvo em: {out_path}")
    
    # 1. Deleta os objetos pesados
    del classifier
    del model
    del tokenizer
    
    # 2. Chama o Garbage Collector do Python
    gc.collect()
    
    # 3. Limpa o cache de memória do PyTorch na GPU
    if torch.cuda.is_available():
        torch.cuda.empty_cache()
        torch.cuda.synchronize() # Garante que a limpeza terminou antes de prosseguir
        
    print("[BERT FILTER] VRAM liberada com sucesso para o Llama-3.")
    return out_path