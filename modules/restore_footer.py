"""
restore_full_footer.py

Varre o dataset para calcular contagem de nós e janela temporal usando a chave "timestamp", 
gerando o metadata_footer completo no final do arquivo.
"""

import json
import os
from datetime import datetime

FILE_PATH = "datasets/BLIND_BASELINE.jsonl" 

def restore_full_footer():
    if not os.path.exists(FILE_PATH):
        print(f"[-] Erro: Arquivo não encontrado em {FILE_PATH}")
        return

    print("\n" + "="*50)
    print(" 🚑 RESTAURANDO METADATA FOOTER COMPLETO ")
    print("="*50)
    
    count = 0
    min_utc = float('inf')
    max_utc = 0.0
    
    print(f"[*] Lendo {os.path.basename(FILE_PATH)} para recalcular a janela temporal...")

    with open(FILE_PATH, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if not line: continue
            
            try:
                record = json.loads(line)
            except json.JSONDecodeError:
                continue
                
            if record.get("type") == "metadata_footer":
                continue
                
            count += 1
            
            # A correção cirúrgica: apontando para "timestamp"
            timestamp = record.get("timestamp")
            if timestamp is not None:
                try:
                    utc_val = float(timestamp)
                    if utc_val < min_utc: min_utc = utc_val
                    if utc_val > max_utc: max_utc = utc_val
                except ValueError:
                    pass

    if min_utc == float('inf'):
        min_utc = 0.0

    human_start = datetime.fromtimestamp(min_utc).strftime('%Y-%m-%d %H:%M:%S') if min_utc > 0 else "N/A"
    human_end = datetime.fromtimestamp(max_utc).strftime('%Y-%m-%d %H:%M:%S') if max_utc > 0 else "N/A"
    duration_days = round((max_utc - min_utc) / 86400.0, 2) if max_utc > min_utc else 0.0
    generated_at = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    footer = {
        "type": "metadata_footer",
        "total_records": count,
        "temporal_window": {
            "unix_start": min_utc if min_utc > 0 else None,
            "unix_end": max_utc if max_utc > 0 else None,
            "human_start": human_start,
            "human_end": human_end,
            "duration_days": duration_days
        },
        "generated_at": generated_at
    }

    with open(FILE_PATH, 'a', encoding='utf-8') as f:
        f.write(json.dumps(footer, ensure_ascii=False) + '\n')
        
    print(f"\n[+] Cirurgia concluída!")
    print(f"    - Total Records: {count:,}")
    print(f"    - Início:        {human_start}")
    print(f"    - Fim:           {human_end}")
    print(f"    - Duração:       {duration_days} dias")
    print(f"[+] Footer reconstruído e adicionado com sucesso!")

if __name__ == "__main__":
    restore_full_footer()