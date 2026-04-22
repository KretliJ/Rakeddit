import json
import os

# ==========================================
# CONFIGURAÇÃO DE CAMINHOS
# ==========================================
# Coloque aqui o nome do arquivo final gerado pelo seu pipeline
INPUT_FILE = 'inputs/INFERRED_MULTIMODAL_BRASIL_data_normalized_2026-04-21_01-34-57.jsonl' 
base_name = os.path.basename(INPUT_FILE)
# Nomes dos arquivos de saída
CLEAN_FILE = os.path.join('./outputs', base_name.replace('.jsonl', '_CLEAN.jsonl'))
DIRTY_FILE = os.path.join('./outputs', base_name.replace('.jsonl', '_HALLUCINATED.jsonl'))

def sanitize_dataset(input_path, clean_path, dirty_path):
    stats = {"total": 0, "limpo": 0, "sujo": 0, "erros_json": 0}
    
    print(f"\n🧹 [SWEEP] Iniciando a varredura de alucinações em: {input_path}")
    
    with open(input_path, 'r', encoding='utf-8') as f_in, \
         open(clean_path, 'w', encoding='utf-8') as f_clean, \
         open(dirty_path, 'w', encoding='utf-8') as f_dirty:
        
        for line in f_in:
            stats["total"] += 1
            
            # 1. Tenta decodificar o JSON (Llama-3 pode ter quebrado as aspas)
            try:
                record = json.loads(line)
            except json.JSONDecodeError:
                stats["erros_json"] += 1
                stats["sujo"] += 1
                f_dirty.write(line) # Salva a linha quebrada para debug futuro
                continue

            analysis = record.get('ai_analysis', {})
            
            # 2. Validação Estrita de Limites (The Sanity Check)
            try:
                flags_valid = all(analysis.get(f) in [0, 1] for f in ['f1', 'f2', 'f3', 'f4', 'f5'])
                aggro_valid = analysis.get('aggro') in [0, 1, 2, 3]
                
                if flags_valid and aggro_valid:
                    f_clean.write(json.dumps(record, ensure_ascii=False) + '\n')
                    stats["limpo"] += 1
                else:
                    f_dirty.write(json.dumps(record, ensure_ascii=False) + '\n')
                    stats["sujo"] += 1
            except AttributeError:
                # Caso 'ai_analysis' tenha vindo como string vazia ou nulo
                f_dirty.write(json.dumps(record, ensure_ascii=False) + '\n')
                stats["sujo"] += 1

    # ==========================================
    # RELATÓRIO FINAL
    # ==========================================
    if stats["total"] > 0:
        limpo_pct = (stats["limpo"] / stats["total"]) * 100
        sujo_pct = (stats["sujo"] / stats["total"]) * 100
    else:
        limpo_pct = sujo_pct = 0.0

    print("\n==================================================")
    print("📊 RELATÓRIO DE SANITIZAÇÃO (HALLUCINATION SWEEP)")
    print("==================================================")
    print(f"Total de registros analisados: {stats['total']}")
    print(f"✅ Registros Limpos:           {stats['limpo']} ({limpo_pct:.2f}%)")
    print(f"❌ Alucinações/Lixo:           {stats['sujo']} ({sujo_pct:.2f}%)")
    
    if stats["erros_json"] > 0:
        print(f"   ↳ Erros de Parse JSON:      {stats['erros_json']}")
        
    print("==================================================")
    print(f"📁 Base validada salva em: {clean_path}")

if __name__ == "__main__":
    os.makedirs('./outputs', exist_ok=True)
    os.makedirs('./inputs', exist_ok=True)
    if not os.path.exists(INPUT_FILE):
        print(f"❌ Erro: O arquivo '{INPUT_FILE}' não foi encontrado.")
        print("Atualize a variável INPUT_FILE no script com o caminho correto.")
    else:
        sanitize_dataset(INPUT_FILE, CLEAN_FILE, DIRTY_FILE)