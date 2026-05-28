import os
import json
import torch # type: ignore
from transformers import pipeline
from modules.config_loader import config

BASE_PATH = config.get_path('PATHS', 'BASE_PATH') 
MAIN_INFER = config.get('MODELS', 'MAIN_INFER', fallback="cardiffnlp/twitter-xlm-roberta-base-sentiment")

# ==========================================
# DIAGNÓSTICO DE HARDWARE E INICIALIZAÇÃO
# ==========================================
print("\n" + "="*60)
print(f" 🛠️  DIAGNÓSTICO DO MOTOR DE INFERÊNCIA (BATCH MODE) ")
print("="*60)
print(f" -> PyTorch Version: {torch.__version__}")

cuda_available = torch.cuda.is_available()
print(f" -> CUDA Disponível: {cuda_available}")

if cuda_available:
    device_id = 0
    print(f" -> GPU Detectada: {torch.cuda.get_device_name(0)}")
else:
    device_id = -1
    print(" [!] ALERTA: GPU não detectada. Rodando na CPU.")

print(f" -> Carregando modelo: {MAIN_INFER}...")

try:
    sentiment_classifier = pipeline(
        "sentiment-analysis", 
        model=MAIN_INFER, 
        tokenizer=MAIN_INFER, 
        device=device_id, 
        truncation=True, 
        max_length=512
    )
    print(" [+] Modelo carregado com sucesso na VRAM!")
except Exception as e:
    print(f"\n[!!!] ERRO FATAL AO CARREGAR O MODELO [!!!]")
    print(f"Detalhes do Erro: {e}")
    sentiment_classifier = None

print("="*60 + "\n")

# ==========================================
# MOTOR DE INFERÊNCIA EM LOTE (BATCHING)
# ==========================================
def analyze_batch_sentiment(texts, batch_size=64):
    """Passa um lote (lista) de textos pela GPU simultaneamente."""
    if not sentiment_classifier:
        raise RuntimeError("Motor off")
    if not texts:
        return []

    try:
        results = sentiment_classifier(texts, batch_size=batch_size)
        
        processed_results = []
        for res in results:
            label = res['label'].upper()
            score = res['score']
            
            # Appends only label and confidence
            processed_results.append({"label": label, "confidence": round(score, 3)})
            
        return processed_results
        
    except Exception as e:
        print(f"\n[!] Falha no processamento do lote. Erro: {e}")
        return [{"label": "ERROR", "confidence": 0.0} for _ in texts]


def orchestrate_full_inference(jsonl_filepath):
    if not sentiment_classifier:
        print("❌ Orquestração cancelada: O Motor não está online.")
        return
        
    print(f"\n[ORCHESTRATOR] Iniciando Análise Batched: {os.path.basename(jsonl_filepath)}")
    
    INFERRED = config.get_path('PATHS', 'INFERRED_PATH', fallback="./DATA/4-inferred")
    os.makedirs(INFERRED, exist_ok=True)
    out_path = os.path.join(INFERRED, f"INFERRED_{os.path.basename(jsonl_filepath)}")

    # --- RESUME LOGIC ---
    processed_ids = set()
    if os.path.exists(out_path):
        with open(out_path, 'r', encoding='utf-8') as f:
            for line in f:
                try:
                    record = json.loads(line)
                    if 'id' in record: processed_ids.add(record['id'])
                except json.JSONDecodeError: pass
        print(f"[RESUME] {len(processed_ids)} nós já classificados. Retomando...\n")

    # --- CONFIGURAÇÃO DO BATCH ---
    BATCH_SIZE = 64  # Ajuste fino para a RTX 4060 Ti (Pode subir para 128 se a VRAM aguentar)
    text_buffer = []
    record_buffer = []
    
    total_processed_ai = 0
    bypassed_count = 0

    with open(jsonl_filepath, 'r', encoding='utf-8') as f_in, \
         open(out_path, "a", encoding="utf-8") as f_out:
        
        for line in f_in:
            try:
                record = json.loads(line)
            except json.JSONDecodeError:
                continue
                
            if record.get('type') == 'metadata_footer': continue
                
            record_id = record.get('id')
            if not record_id or record_id in processed_ids: continue

            original_body = record.get('body', '')
            body_text = original_body
            
            if record.get('type') == 'post_header':
                title = record.get('title', '')
                body_text = f"{title}. {body_text}".strip()

            # --- BYPASS E HEURÍSTICA ---
            is_bypass = False
            if original_body == '[removed]':
                record['ai_analysis'] = {"label": "REMOVED_BY_MOD", "confidence": 1.0}
                is_bypass = True
            elif original_body == '[deleted]':
                record['ai_analysis'] = {"label": "USER_DELETED", "confidence": 1.0}
                is_bypass = True
            elif original_body == '[AutoModerator]':
                record['ai_analysis'] = {"label": "AUTOMOD_WARNING", "confidence": 1.0}
                is_bypass = True
            elif not record.get('is_valid_text', True) or not body_text:
                record['ai_analysis'] = {"label": "BYPASS_EMPTY", "confidence": 0.0}
                is_bypass = True

            if is_bypass:
                # Escreve escapes imediatamente
                f_out.write(json.dumps(record, ensure_ascii=False) + "\n")
                bypassed_count += 1
                continue

            # --- ADICIONA AO BUFFER DA GPU ---
            text_buffer.append(body_text)
            record_buffer.append(record)

            # Quando o buffer enche, dispara o lote para a GPU
            if len(text_buffer) >= BATCH_SIZE:
                results = analyze_batch_sentiment(text_buffer, batch_size=BATCH_SIZE)
                
                # Mapeia resultados de volta aos registros e salva
                for rec, ai_data in zip(record_buffer, results):
                    rec['ai_analysis'] = ai_data
                    f_out.write(json.dumps(rec, ensure_ascii=False) + "\n")
                
                f_out.flush() # Salva no disco com segurança
                total_processed_ai += len(text_buffer)
                print(f" ⚡ GPU Batch Concluído -> AI: {total_processed_ai} | Heurísticas: {bypassed_count}")
                
                # Limpa os buffers
                text_buffer.clear()
                record_buffer.clear()

        # --- PROCESSA O RESTO (Se o arquivo acabar antes de encher o buffer final) ---
        if text_buffer:
            results = analyze_batch_sentiment(text_buffer, batch_size=len(text_buffer))
            for rec, ai_data in zip(record_buffer, results):
                rec['ai_analysis'] = ai_data
                f_out.write(json.dumps(rec, ensure_ascii=False) + "\n")
            
            total_processed_ai += len(text_buffer)
            
    print(f"\n[SUCCESS] Análise finalizada! IA Processou: {total_processed_ai} | Bypasses: {bypassed_count}")
    print(f"Arquivo gerado em: {out_path}")