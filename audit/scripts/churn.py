import json
import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt # type: ignore
import seaborn as sns # type: ignore

try:
    from lifelines import KaplanMeierFitter, CoxPHFitter # type: ignore
except ImportError:
    print("[ERRO] A biblioteca 'lifelines' não está instalada.")
    print("Por favor, execute: pip install lifelines")
    exit(1)

JSONL_PATH = "results/INFERRED_MULTIMODAL_FINAL.jsonl"
OUTPUT_DIR = "results/Survival_Analysis"
os.makedirs(OUTPUT_DIR, exist_ok=True)

def extract_label(data_dict):
    ai_data = data_dict.get("ai_analysis")
    if isinstance(ai_data, dict):
        label = ai_data.get("label")
        if label: return label.upper()
    for key in ['sentiment', 'label', 'roberta_sentiment', 'qwen_sentiment']:
        val = data_dict.get(key)
        if isinstance(val, dict): return str(val.get('label', val.get('sentiment', ''))).upper()
        if isinstance(val, str): return val.upper()
    return "UNKNOWN"

def build_churn_dataset():
    print(f"[*] 1. A ler o ficheiro {JSONL_PATH} para análise macro (Churn)...")
    
    user_stats = {}
    global_max_ts = 0.0
    
    with open(JSONL_PATH, 'r', encoding='utf-8') as f:
        for line in f:
            try:
                obj = json.loads(line)
                author = obj.get('author', '').lower()
                
                ts_raw = obj.get('created_utc') or obj.get('timestamp')
                if not ts_raw and isinstance(obj.get('ai_analysis'), dict):
                    ts_raw = obj.get('ai_analysis').get('created_utc')
                    
                label = extract_label(obj)
                
                if author not in ['[deleted]', 'automoderator', 'redditcaresresources'] and ts_raw is not None:
                    ts = float(ts_raw)
                    is_neg = 1 if label == 'NEGATIVE' else 0
                    
                    if ts > global_max_ts:
                        global_max_ts = ts
                        
                    if author not in user_stats:
                        user_stats[author] = {'first_ts': ts, 'last_ts': ts, 'total_msgs': 1, 'neg_msgs': is_neg}
                    else:
                        if ts < user_stats[author]['first_ts']: user_stats[author]['first_ts'] = ts
                        if ts > user_stats[author]['last_ts']: user_stats[author]['last_ts'] = ts
                        user_stats[author]['total_msgs'] += 1
                        user_stats[author]['neg_msgs'] += is_neg
            except Exception:
                continue

    print(f"[*] 2. Última data registada no dataset: Timestamp {global_max_ts}")
    print("[*] 3. A extrair métricas de ciclo de vida por usuário...")
    
    churn_records = []
    CHURN_THRESHOLD_DAYS = 30
    SECONDS_IN_DAY = 86400
    
    for author, stats in user_stats.items():
        # Filtrar "turistas" (exigir pelo menos 5 mensagens no histórico para ter um perfil real)
        if stats['total_msgs'] < 5:
            continue
            
        # Duração em dias na plataforma
        duration_days = (stats['last_ts'] - stats['first_ts']) / SECONDS_IN_DAY
        if duration_days < 0.5:
            duration_days = 0.5 # Mínimo de meio dia para o modelo não quebrar com zeros
            
        # O usuário deu Churn? (Não postou nada nos últimos 30 dias do dataset)
        days_since_last_post = (global_max_ts - stats['last_ts']) / SECONDS_IN_DAY
        event = 1 if days_since_last_post > CHURN_THRESHOLD_DAYS else 0
        
        neg_ratio = stats['neg_msgs'] / stats['total_msgs']
        
        churn_records.append({
            'author': author,
            'duration_days': duration_days,
            'event': event,
            'neg_ratio': neg_ratio,
            'total_msgs': stats['total_msgs']
        })
        
    df_churn = pd.DataFrame(churn_records)
    
    # Criar os Quartis UQ1 a UQ4 usando os percentis reais da distribuição
    quantiles = df_churn['neg_ratio'].quantile([0.25, 0.50, 0.75]).to_dict()
    def assign_quartile(val):
        if val <= quantiles[0.25]: return 'UQ1'
        elif val <= quantiles[0.50]: return 'UQ2'
        elif val <= quantiles[0.75]: return 'UQ3'
        else: return 'UQ4'
        
    df_churn['user_quartile'] = df_churn['neg_ratio'].apply(assign_quartile)
    
    print(f"[*] Base de Churn validada com {len(df_churn):,} usuários recorrentes.")
    return df_churn

def run_churn_analysis(df):
    print("[*] 4. A gerar as curvas de Kaplan-Meier para Churn Global...")
    
    sns.set_theme(style="whitegrid")
    fig, ax = plt.subplots(figsize=(10, 6))
    
    colors = {'UQ1': '#2ecc71', 'UQ2': '#3498db', 'UQ3': '#f39c12', 'UQ4': '#e74c3c'}
    labels = {'UQ1': 'UQ1 (Baixa Negatividade)', 'UQ2': 'UQ2 (Negatividade Moderada-Baixa)', 
              'UQ3': 'UQ3 (Negatividade Moderada-Alta)', 'UQ4': 'UQ4 (Extrema Negatividade)'}
    
    kmfs = {}
    for q in ['UQ1', 'UQ2', 'UQ3', 'UQ4']:
        mask = df['user_quartile'] == q
        kmf = KaplanMeierFitter()
        kmf.fit(df[mask]['duration_days'], event_observed=df[mask]['event'], label=labels[q])
        kmf.plot_survival_function(ax=ax, color=colors[q], linewidth=2.5, ci_show=False)
        kmfs[q] = kmf
        
    ax.set_title("Sobrevivência Global (Churn) por Perfil de Usuário\n(Ciclo de Vida na Comunidade)", fontsize=14, fontweight='bold', pad=15)
    ax.set_xlabel("Dias Ativos na Comunidade (Tempo de Vida)", fontsize=12, fontweight='bold')
    ax.set_ylabel("Probabilidade de Continuar Ativo", fontsize=12, fontweight='bold')
    
    ax.set_xlim(0, 365) # Focando no 1º ano
    sns.despine()
    
    plt.tight_layout()
    plt.savefig(os.path.join(OUTPUT_DIR, "Survival_Curves_Global_Churn.pdf"), dpi=300)
    plt.close()
    
    print("[*] 5. A ajustar o Modelo de Cox para o Churn Macro...")
    cph = CoxPHFitter()
    
    df_cox = df[['duration_days', 'event', 'neg_ratio']].copy()
    df_cox['neg_percent'] = df_cox['neg_ratio'] * 100 
    df_cox = df_cox[['duration_days', 'event', 'neg_percent']]
    
    try:
        cph.fit(df_cox, duration_col='duration_days', event_col='event')
        summary = cph.summary
        hazard_ratio = np.exp(summary.loc['neg_percent', 'coef'])
        p_val = summary.loc['neg_percent', 'p']
        
        with open(os.path.join(OUTPUT_DIR, "Cox_Model_Churn_Report.txt"), "w", encoding='utf-8') as f:
            f.write("="*70 + "\n")
            f.write("   RELATÓRIO DO MODELO DE COX (CHURN MACRO)\n")
            f.write("="*70 + "\n\n")
            f.write(cph.summary.to_string())
            f.write("\n\n" + "="*70 + "\n")
            f.write("INTERPRETAÇÃO (CICLO DE VIDA GLOBAL):\n")
            if p_val < 0.05:
                f.write(f"- O perfil de negatividade afeta o Churn ESTATISTICAMENTE SIGNIFICATIVA (p = {p_val:.2e}).\n")
                if hazard_ratio > 1:
                    aumento = (hazard_ratio - 1) * 100
                    f.write(f"- Hazard Ratio: {hazard_ratio:.4f}\n")
                    f.write(f"- CONCLUSÃO: Usuários mais tóxicos (UQ4) dão CHURN MAIS RÁPIDO.\n")
                    f.write(f"  Cada 1% a mais na toxicidade global do usuário aumenta o risco de ele sumir do Reddit em {aumento:.2f}%.\n")
                else:
                    reducao = (1 - hazard_ratio) * 100
                    f.write(f"- Hazard Ratio: {hazard_ratio:.4f}\n")
                    f.write(f"- CONCLUSÃO: Usuários mais tóxicos (UQ4) SÃO MAIS FIÉIS À PLATAFORMA.\n")
                    f.write(f"  Cada 1% a mais na toxicidade global reduz o risco de Churn em {reducao:.2f}%.\n")
            else:
                f.write("- O nível de negatividade do usuário NÃO afeta significativamente o Churn global.\n")

        print(f"[*] Análise concluída! Relatórios guardados em: {OUTPUT_DIR}/")
    except Exception as e:
        print(f"[ERRO] Falha ao ajustar modelo Cox. Detalhes: {e}")

if __name__ == "__main__":
    df_churn = build_churn_dataset()
    if not df_churn.empty:
        run_churn_analysis(df_churn)