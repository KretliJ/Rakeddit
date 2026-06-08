import json
import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt # type: ignore
import seaborn as sns # type: ignore
from collections import defaultdict

class SentimentMotifAnalytics:
    def __init__(self):
        self.MULTIMODAL_PATH = "DATA/4-inferred/INFERRED_MULTIMODAL_FINAL.jsonl"
        self.RESULTS_DIR = "results/RQ2" 
        os.makedirs(self.RESULTS_DIR, exist_ok=True)
        
        self.CATEGORIES = ['PUBLIC ARENAS', 'HUMOR', 'SOCIOCULTURAL', 'HOBBIES']
        self.CATEGORY_MAP = {
            'brasil': 'PUBLIC ARENAS', 'brasilivre': 'PUBLIC ARENAS', 'brasildob': 'PUBLIC ARENAS', 'debatesbr': 'PUBLIC ARENAS', 'noticiasbr': 'PUBLIC ARENAS',
            'botecodoreddit': 'HUMOR', 'farialimabets': 'HUMOR', 'memesbr': 'HUMOR', 'shitpostbr': 'HUMOR',
            'antitrampo': 'SOCIOCULTURAL', 'opiniaoburra': 'SOCIOCULTURAL', 'opiniaoimpopular': 'SOCIOCULTURAL', 'filosofiabar': 'SOCIOCULTURAL', 'infernosocial': 'SOCIOCULTURAL',
            'futebol': 'HOBBIES', 'gamesecultura': 'HOBBIES', 'videogamesbrasil': 'HOBBIES', 'carros': 'HOBBIES', 'computadores': 'HOBBIES', 'saopaulo': 'HOBBIES'
        }
        
        # Mapeamento rigoroso das relações triádicas de interesse
        self.TRIAD_MAPPING = {
            ('NEGATIVE', 'NEGATIVE', 'NEGATIVE'): 'Negative Persistence',
            ('POSITIVE', 'POSITIVE', 'POSITIVE'): 'Positive Persistence',
            ('POSITIVE', 'NEGATIVE', 'NEGATIVE'): 'Negative Convergence (from pos)',
            ('NEUTRAL', 'NEGATIVE', 'NEGATIVE'):  'Negative Convergence (from neu)',
            ('NEGATIVE', 'POSITIVE', 'POSITIVE'): 'Positive Convergence (from neg)',
            ('NEUTRAL', 'POSITIVE', 'POSITIVE'):  'Positive Convergence (from neu)',
            ('POSITIVE', 'POSITIVE', 'NEGATIVE'): 'Shift (to neg)',
            ('NEGATIVE', 'NEGATIVE', 'POSITIVE'): 'Shift (to pos)',
            ('POSITIVE', 'NEGATIVE', 'POSITIVE'): 'Oscillation (a)',
            ('NEGATIVE', 'POSITIVE', 'NEGATIVE'): 'Oscillation (b)',
            ('POSITIVE', 'NEUTRAL', 'NEGATIVE'):  'Mixed Transition (to neg)',
            ('NEGATIVE', 'NEUTRAL', 'POSITIVE'):  'Mixed Transition (to pos)'
        }
        
        # Estrutura de contagem: {Categoria: {Nome_da_Triade: Contagem_A}}
        self.triad_counts = {cat: defaultdict(int) for cat in self.CATEGORIES}

    def extract_triadic_sentiments(self):
        print("[*] RQ2: Extraindo Motifs Temporais de Sentimento...")
        node_memory = {}
        
        with open(self.MULTIMODAL_PATH, 'r', encoding='utf-8') as f:
            for line in f:
                try:
                    record = json.loads(line)
                    sub = record.get('subreddit', '').lower()
                    if sub not in self.CATEGORY_MAP: continue
                    
                    n_id_raw = record.get('id', '')
                    p_id_raw = record.get('parent_id')
                    
                    n_id = str(n_id_raw).split('_')[-1]
                    p_id = str(p_id_raw).split('_')[-1] if p_id_raw and pd.notna(p_id_raw) else None
                        
                    label = record.get('ai_analysis', {}).get('label', 'UNKNOWN')
                    
                    node_memory[n_id] = {
                        'p_id': p_id,
                        'label': label,
                        'sub': sub
                    }
                except: continue

        # Varredura por cadeias de profundidade 3 (Avô -> Pai -> Filho)
        for n3_id, n3_data in node_memory.items():
            cat = self.CATEGORY_MAP.get(n3_data['sub'])
            if not cat: continue
            
            p1_id = n3_data['p_id'] # Pai
            if not p1_id or p1_id not in node_memory: continue
            
            p2_id = node_memory[p1_id]['p_id'] # Avô
            if not p2_id or p2_id not in node_memory: continue
            
            # Extraindo a sequência cronológica de sentimentos: T1 -> T2 -> T3
            l1 = node_memory[p2_id]['label'] # Avô
            l2 = node_memory[p1_id]['label'] # Pai
            l3 = n3_data['label']            # Filho
            
            triad_tuple = (l1, l2, l3)
            
            if triad_tuple in self.TRIAD_MAPPING:
                triad_name = self.TRIAD_MAPPING[triad_tuple]
                self.triad_counts[cat][triad_name] += 1

    def plot_normalized_heatmap(self):
        print("[*] Normalizando matriz (A/Z) e renderizando Heatmap (Figura 5)...")
        
        # Construindo o DataFrame com as contagens absolutas (A)
        df_counts = pd.DataFrame(self.triad_counts).fillna(0)
        
        # Garante a ordem exata em que você listou no prompt para facilitar a leitura no TCC
        ordered_triads = [
            'Negative Persistence', 'Positive Persistence',
            'Negative Convergence (from pos)', 'Negative Convergence (from neu)',
            'Positive Convergence (from neg)', 'Positive Convergence (from neu)',
            'Shift (to neg)', 'Shift (to pos)',
            'Oscillation (a)', 'Oscillation (b)',
            'Mixed Transition (to neg)', 'Mixed Transition (to pos)'
        ]
        
        # Adiciona triades que não tiveram nenhuma ocorrência (para não quebrar a matriz)
        for t in ordered_triads:
            if t not in df_counts.index:
                df_counts.loc[t] = 0.0
                
        df_counts = df_counts.reindex(ordered_triads, columns=self.CATEGORIES)
        
        # Normalização Matemática
        Z = df_counts.to_numpy().sum() # Soma global de TODAS as relações de interesse na base
        df_normalized = (df_counts / Z) * 100 # Em porcentagem do ecossistema
        
        print(f"   -> Valor de Z (Total global de triades de interesse detectadas): {int(Z)}")
        
        fig, ax = plt.subplots(figsize=(12, 10))
        
        # Paleta inferno/magma invertida para que tons escuros/fortes mostrem a maior concentração
        sns.heatmap(df_normalized, annot=True, fmt=".2f", cmap="magma_r", 
                    cbar_kws={'label': 'Global Concentration (A / Z) %'}, 
                    linewidths=1, ax=ax, annot_kws={'size': 14, 'weight': 'bold'})
        
        ax.set_ylabel("TRIADIC SENTIMENT RELATIONS (T1 → T2 → T3)", fontsize=14, fontweight='bold')
        ax.set_xlabel("SUBREDDIT CATEGORY", fontsize=14, fontweight='bold')
        ax.tick_params(labelsize=12)
        plt.xticks(rotation=0)
        
        # Marcações no eixo Y para indicar o fluxo exato
        yticklabels = [f"{t}" for t in ordered_triads]
        ax.set_yticklabels(yticklabels, rotation=0)

        plt.tight_layout()
        out_file = os.path.join(self.RESULTS_DIR, "Fig5_RQ2_Triadic_Sentiment_Motifs.pdf")
        plt.savefig(out_file, dpi=300, bbox_inches='tight')
        plt.close()
        print(f"[SUCCESS] Heatmap salvo em: {out_file}")

if __name__ == "__main__":
    app = SentimentMotifAnalytics()
    app.extract_triadic_sentiments()
    app.plot_normalized_heatmap()