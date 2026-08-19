import json
import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt # type: ignore
import seaborn as sns # type: ignore
from collections import defaultdict

try:
    from lifelines import KaplanMeierFitter, CoxPHFitter # type: ignore
except ImportError:
    print("[ERRO] A biblioteca 'lifelines' não está instalada.")
    print("Por favor, execute: pip install lifelines")
    exit(1)

# Caminho para o ficheiro final do seu pipeline
JSONL_PATH = "results/INFERRED_MULTIMODAL_FINAL.jsonl"
OUTPUT_DIR = "results/Survival_Analysis"

os.makedirs(OUTPUT_DIR, exist_ok=True)

def extract_label(data_dict):
    """Extrai o sentimento independentemente da estrutura do JSON."""
    ai_data = data_dict.get("ai_analysis")
    if isinstance(ai_data, dict):
        label = ai_data.get("label")
        if label: return label.upper()
            
    for key in ['sentiment', 'label', 'roberta_sentiment', 'qwen_sentiment']:
        val = data_dict.get(key)
        if isinstance(val, dict):
            return str(val.get('label', val.get('sentiment', ''))).upper()
        if isinstance(val, str):
            return val.upper()
    return "UNKNOWN"

def build_survival_dataset():
    print(f"[*] 1. A ler o ficheiro {JSONL_PATH}...")
    
    messages_data = {}
    parent_map = {}
    
    with open(JSONL_PATH, 'r', encoding='utf-8') as f:
        for line in f:
            try:
                obj = json.loads(line)
                
                # Extração robusta de IDs igual ao Methods.py
                msg_id_raw = obj.get('id')
                if not msg_id_raw: continue
                msg_id = str(msg_id_raw).split('_')[-1]
                
                parent_raw = obj.get('parent_id')
                p_id = str(parent_raw).split('_')[-1] if parent_raw else None
                parent_map[msg_id] = p_id
                
                author = obj.get('author', '').lower()
                
                # Extração robusta do timestamp (por vezes vem aninhado no ai_analysis)
                ts = obj.get('created_utc') or obj.get('timestamp')
                if not ts and isinstance(obj.get('ai_analysis'), dict):
                    ts = obj.get('ai_analysis').get('created_utc')
                    
                label = extract_label(obj)
                
                if author not in ['[deleted]', 'automoderator', 'redditcaresresources'] and ts is not None:
                    messages_data[msg_id] = {
                        'author': author,
                        'timestamp': float(ts),
                        'is_negative': 1 if label == 'NEGATIVE' else 0
                    }
            except Exception:
                continue

    print("[*] 2. A reconstruir as árvores de discussão (cascatas)...")
    threads = defaultdict(list)
    root_cache = {}
    
    def get_root(m_id):
        curr = m_id
        visited = set()
        while curr and curr not in visited:
            visited.add(curr)
            if curr in root_cache:
                return root_cache[curr]
            if curr not in parent_map or parent_map[curr] is None:
                break
            curr = parent_map[curr]
        root_cache[m_id] = curr
        return curr

    # Alocar cada mensagem na sua cascata correspondente
    for m_id, data in messages_data.items():
        root_id = get_root(m_id)
        if root_id:
            threads[root_id].append(data)
            
    print(f"[*] Foram reconstruídas {len(threads):,} cascatas globais.")
    print("[*] 3. A extrair métricas de exposição e sobrevivência por utilizador...")
    
    survival_records = []
    
    for thread_id, messages in threads.items():
        # Ignorar discussões muito curtas (precisamos de cascatas longas para medir a "fadiga")
        if len(messages) < 10: 
            continue
            
        # Ordenar cronologicamente
        sorted_msgs = sorted(messages, key=lambda x: x['timestamp'])
        total_steps = len(sorted_msgs)
        
        user_stats = {}
        current_neg_count = 0
        
        for idx, msg in enumerate(sorted_msgs):
            author = msg['author']
            current_neg_count += msg['is_negative']
            
            if author not in user_stats:
                user_stats[author] = {
                    'start_idx': idx,
                    'start_neg_global': current_neg_count,
                    'last_idx': idx,
                    'last_neg_global': current_neg_count
                }
            else:
                user_stats[author]['last_idx'] = idx
                user_stats[author]['last_neg_global'] = current_neg_count

        # Construir métricas de sobrevivência para cada utilizador nesta cascata
        for author, stats in user_stats.items():
            duration = stats['last_idx'] - stats['start_idx'] + 1
            
            # Só avaliar utilizadores que participaram (ficaram pelo menos 2 turnos expostos)
            if duration <= 1:
                continue
                
            # Exposição: Quantas mensagens negativas ocorreram enquanto o utilizador esteve na thread?
            neg_seen = stats['last_neg_global'] - stats['start_neg_global']
            
            # Normalizar exposição (Taxa de negatividade do ambiente durante a estadia)
            exposure_ratio = neg_seen / duration if duration > 0 else 0
            
            # Evento (1 = Desistiu, 0 = Censurado/Thread acabou com ele lá)
            # Se a thread continuou por mais de 5 mensagens depois de ele falar pela última vez, ele desistiu
            event = 1 if (total_steps - stats['last_idx']) >= 5 else 0
            
            survival_records.append({
                'author': author,
                'thread_id': thread_id,
                'duration': duration,
                'event': event,
                'exposure_ratio': exposure_ratio
            })
            
    df_survival = pd.DataFrame(survival_records)
    print(f"[*] Base de dados de sobrevivência validada com {len(df_survival):,} eventos (linhas de tempo).")
    return df_survival

def run_survival_analysis(df):
    print("[*] 4. A aplicar o estimador de Kaplan-Meier...")
    
    sns.set_theme(style="whitegrid")
    fig, ax = plt.subplots(figsize=(10, 6))
    
    kmf_low = KaplanMeierFitter()
    kmf_high = KaplanMeierFitter()
    
    # Dividir a exposição no P75 (Exposição Extrema vs Normal)
    threshold = df['exposure_ratio'].quantile(0.75)
    
    mask_high = df['exposure_ratio'] >= threshold
    mask_low = df['exposure_ratio'] < threshold
    
    kmf_low.fit(df[mask_low]['duration'], event_observed=df[mask_low]['event'], label=f"Exposição Moderada/Baixa (< {threshold:.1%})")
    kmf_high.fit(df[mask_high]['duration'], event_observed=df[mask_high]['event'], label=f"Exposição Extrema (>= {threshold:.1%})")
    
    kmf_low.plot_survival_function(ax=ax, color='#3498db', linewidth=2.5)
    kmf_high.plot_survival_function(ax=ax, color='#e74c3c', linewidth=2.5)
    
    ax.set_title("Curvas de Sobrevivência de Kaplan-Meier\n(Fadiga de Conflito em Discussões no Reddit)", fontsize=14, fontweight='bold', pad=15)
    ax.set_xlabel("Duração da Estadia (Turnos/Mensagens)", fontsize=12, fontweight='bold')
    ax.set_ylabel("Probabilidade de Permanência na Discussão", fontsize=12, fontweight='bold')
    
    # Focar nos primeiros 50 turnos, onde a maioria das interações acontece
    ax.set_xlim(0, 50)
    sns.despine()
    
    plt.tight_layout()
    plt.savefig(os.path.join(OUTPUT_DIR, "Survival_Curves_Conflict_Fatigue.pdf"), dpi=300)
    plt.close()
    
    print("[*] 5. A ajustar o Modelo de Riscos Proporcionais de Cox...")
    cph = CoxPHFitter()
    
    # Usar apenas as colunas necessárias para o modelo Cox
    df_cox = df[['duration', 'event', 'exposure_ratio']].copy()
    
    # Multiplicar por 100 para que o coeficiente reflita o impacto de +1% de negatividade
    df_cox['exposure_percent'] = df_cox['exposure_ratio'] * 100 
    df_cox = df_cox[['duration', 'event', 'exposure_percent']]
    
    try:
        cph.fit(df_cox, duration_col='duration', event_col='event')
        
        summary = cph.summary
        hazard_ratio = np.exp(summary.loc['exposure_percent', 'coef'])
        p_val = summary.loc['exposure_percent', 'p']
        
        with open(os.path.join(OUTPUT_DIR, "Cox_Model_Report.txt"), "w", encoding='utf-8') as f:
            f.write("="*70 + "\n")
            f.write("   RELATÓRIO DO MODELO DE COX (FADIGA DE CONFLITO)\n")
            f.write("="*70 + "\n\n")
            f.write(cph.summary.to_string())
            f.write("\n\n" + "="*70 + "\n")
            f.write("INTERPRETAÇÃO:\n")
            if p_val < 0.05:
                f.write(f"- A exposição à negatividade é ESTATISTICAMENTE SIGNIFICATIVA (p = {p_val:.2e}).\n")
                if hazard_ratio > 1:
                    aumento = (hazard_ratio - 1) * 100
                    f.write(f"- Hazard Ratio: {hazard_ratio:.4f}\n")
                    f.write(f"- CONCLUSÃO: Por cada 1% de aumento na proporção de negatividade na discussão,\n")
                    f.write(f"  o risco (probabilidade instantânea) do utilizador abandonar o debate aumenta em {aumento:.2f}%.\n")
            else:
                f.write("- A exposição à negatividade NÃO TEM significância estatística neste modelo.\n")

        print(f"[*] Análise concluída! Relatórios guardados em: {OUTPUT_DIR}/")
    except Exception as e:
        print(f"[ERRO] Falha ao ajustar modelo Cox. Verifique colinearidade. Detalhes: {e}")

if __name__ == "__main__":
    df_surv = build_survival_dataset()
    if not df_surv.empty:
        run_survival_analysis(df_surv)
    else:
        print("[AVISO] Não foram encontrados dados válidos para a análise de sobrevivência.")