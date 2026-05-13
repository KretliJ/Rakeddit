import os
import glob
import json

def build_blind_aggregate(input_dir, output_filepath):
    """
    Une os arquivos JSONL da etapa de extração (raw/processed), 
    garantindo que não haja NENHUM contexto visual ou de YouTube.
    """
    print(f"[*] Iniciando Protocolo de Ablação (Cegueira Multimodal)...")
    
    # Garante que a pasta de destino existe
    os.makedirs(os.path.dirname(output_filepath), exist_ok=True)
    
    input_files = glob.glob(os.path.join(input_dir, "*.jsonl"))
    if not input_files:
        print(f"[-] Nenhum arquivo .jsonl encontrado na pasta: {input_dir}")
        return

    total_records = 0
    clean_records = 0

    with open(output_filepath, 'w', encoding='utf-8') as outfile:
        for filepath in input_files:
            print(f"  -> Processando: {os.path.basename(filepath)}")
            
            with open(filepath, 'r', encoding='utf-8') as infile:
                for line in infile:
                    try:
                        record = json.loads(line)
                        total_records += 1
                        
                        # Ignora metadados inúteis para a inferência
                        if record.get('type') not in ['post_header', 'comment', 'metadata_footer']:
                            continue
                            
                        # --- O PROCESSO DE "CEGUEIRA" ---
                        # Se for um nó de conteúdo, arrancamos qualquer contexto externo
                        if record.get('type') in ['post_header', 'comment']:
                            # Remove as chaves geradas pela etapa 3, se existirem por acidente
                            record.pop('vision_context', None)
                            record.pop('youtube_context', None)
                            record.pop('ai_analysis', None)
                            record.pop('toxicity_score', None)
                            
                            # Mantemos apenas o 'body' ou 'selftext' original.
                            # URLs para vídeos e placeholders como [IMAGE] ficarão crus.
                            # É exatamente assim que o RoBERTa se confunde.
                            
                        outfile.write(json.dumps(record, ensure_ascii=False) + '\n')
                        clean_records += 1
                        
                    except json.JSONDecodeError:
                        continue

    print(f"\n[========== ESTUDO DE ABLAÇÃO ==========]")
    print(f"[+] Leituras Totais : {total_records}")
    print(f"[+] Nós Válidos     : {clean_records}")
    print(f"[+] Aggregate Cego salvo em: {output_filepath}")
    print(f"[*] PRÓXIMO PASSO: Rodar o motor de inferência (infer_engine.py) apontando para este arquivo.")

if __name__ == "__main__":
    # Ajuste os caminhos conforme a estrutura das suas pastas!
    
    # Pasta onde ficam os JSONLs logo após o scraper do Reddit (antes do Qwen/YouTube)
    PASTA_ORIGEM = "./DATA/2-aggregates" 
    
    # O arquivo final que vai alimentar o motor de inferência cego
    ARQUIVO_DESTINO = "./DATA/3-vision_processing/BLIND_AGGREGATE.jsonl" 
    
    build_blind_aggregate(PASTA_ORIGEM, ARQUIVO_DESTINO)