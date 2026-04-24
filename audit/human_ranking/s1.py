import json
import os

# --- CONFIGURAÇÕES ---
INPUT_FILE = "audit\human_ranking\inputs\opiniaoburra_HUMAN-AUDITED.jsonl"      # Seu arquivo atual
OUTPUT_FILE = "audit\human_ranking\inputs\opiniaoburra_sample.jsonl" # Novo arquivo com as tags injetadas

def inject_human_tags():
    if not os.path.exists(INPUT_FILE):
        print(f"Erro: Arquivo '{INPUT_FILE}' não encontrado.")
        return

    processed_count = 0
    updated_count = 0

    with open(INPUT_FILE, 'r', encoding='utf-8') as infile, \
         open(OUTPUT_FILE, 'w', encoding='utf-8') as outfile:
        
        for line in infile:
            if not line.strip():
                continue
                
            record = json.loads(line)
            processed_count += 1
            
            # Verifica se a tag já existe para não sobrescrever auditorias prontas
            if "human_audited" not in record:
                record["human_audited"] = False
                
                # Inicializa a estrutura de análise humana com valores nulos (None/null)
                # ou você pode mudar para espelhar record.get("ai_analysis", {})
                record["human_analysis"] = {
                    "f1": None,
                    "f2": None,
                    "f3": None,
                    "f4": None,
                    "f5": None,
                    "aggro": None
                }
                record["human_toxicity_score"] = None
                updated_count += 1
                
            # Salva o registro no novo arquivo
            outfile.write(json.dumps(record, ensure_ascii=False) + '\n')

    print(f"--- MIGRAÇÃO CONCLUÍDA ---")
    print(f"Total de registros lidos: {processed_count}")
    print(f"Registros atualizados com novas tags: {updated_count}")
    print(f"Arquivo salvo em: {OUTPUT_FILE}")

if __name__ == "__main__":
    inject_human_tags()