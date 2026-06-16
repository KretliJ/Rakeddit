import os
import json
import re
import pandas as pd
import matplotlib.pyplot as plt # type: ignore
from wordcloud import WordCloud # type: ignore
from bertopic import BERTopic # type: ignore
from sentence_transformers import SentenceTransformer # type: ignore
import numpy as np
import random
from umap import UMAP # type: ignore
from sklearn.feature_extraction.text import CountVectorizer

# Importa as configurações e as classes
from Utilities import Config
from Methods import AnalyticsEngine 

# Importando dicionário
from stopwordsiso import stopwords as get_stopwords # type: ignore
from dictionary import CUSTOM_BR, DOMAIN_STOPWORDS

class NLPEngine:
    def __init__(self, sample_size=None):
        self.sample_size = sample_size
        self.output_dir = os.path.join(Config.RESULTS_DIR, "NLP_Analysis")
        os.makedirs(self.output_dir, exist_ok=True)
        
        # 1. Carrega o léxico acadêmico ISO
        academic_stopwords = get_stopwords(["pt"])
        
        # 2. Funde com os seus dicionários customizados (lista de listas -> set único)
        # O set() elimina duplicatas automaticamente
        self.pt_stopwords = list(set(academic_stopwords) | set(CUSTOM_BR) | set(DOMAIN_STOPWORDS))
        
        print(f"[*] Engine inicializado com {len(self.pt_stopwords):,} stopwords combinadas.")

    def _get_cascade_quartiles(self):
        print("[*] Extraindo fronteiras estruturais das cascatas...")
        engine = AnalyticsEngine()
        engine.load_or_extract_data() 
        df_cascades = engine._prepare_quartiles(interactive_only=False)
        return dict(zip(df_cascades['Cascade_ID'], df_cascades['neg_quartile']))

    def load_and_map_texts(self):
        cascade_map = self._get_cascade_quartiles()
        
        print("[*] Carregando e mapeando textos (RAM Memory)...")
        data_path = Config.MULTIMODAL_PATH 
        
        parent_map = {}
        messages = {}
        
        print("   -> Lendo estrutura hierárquica e aplicando filtros de exclusão...")
        with open(data_path, 'r', encoding='utf-8') as f:
            for line in f:
                try:
                    obj = json.loads(line)
                    msg_id = str(obj.get('id', '')).split('_')[-1]
                    parent_raw = obj.get('parent_id')
                    p_id = str(parent_raw).split('_')[-1] if parent_raw else None
                    
                    author = str(obj.get('author', '')).lower()
                    body = obj.get('body', '').strip()
                    is_valid = obj.get('is_valid_text', False)
                    
                    # FILTROS DE LIMPEZA ECOLÓGICA DO TEXTO
                    is_deleted = author in ['[deleted]', 'deleted']
                    is_mod_bot = author in ['automoderator'] or 'modteam' in author
                    
                    body_lower = body.lower()
                    is_visual_context = "[CONTEUDO VISUAL:" in body_lower
                    
                    if msg_id:
                        parent_map[msg_id] = p_id
                        # Cyrilic fix
                        is_foreign_script = bool(re.search(r'[\u0400-\u04FF]', body))

                        if is_valid and body and not (is_deleted or is_mod_bot or is_visual_context or is_foreign_script):
                            messages[msg_id] = body
                except Exception:
                    continue
                    
        print("   -> Resolvendo as árvores de cascatas e alocando quartis...")
        texts_by_quartile = {'Q1': [], 'Q2': [], 'Q3': [], 'Q4': []}
        all_docs = []
        all_classes = []
        
        def get_cascade_quartile(m_id):
            curr = m_id
            visited = set()
            while curr and curr not in visited:
                visited.add(curr)
                if curr in cascade_map:
                    return cascade_map[curr]
                curr = parent_map.get(curr)
            return None
            
        count = 0
        for m_id, body in messages.items():
            if self.sample_size and count >= self.sample_size:
                break
                
            quartile = get_cascade_quartile(m_id)
            if pd.notna(quartile):
                texts_by_quartile[quartile].append(body)
                all_docs.append(body)
                all_classes.append(quartile)
                count += 1
                
        print(f"   -> Sucesso! Foram alocadas {len(all_docs):,} mensagens limpas aos seus respectivos quartis.")
        return texts_by_quartile, all_docs, all_classes
    
    def run_wordclouds(self, texts_by_quartile):
        print("[*] Gerando WordClouds por Quartil de Negatividade (Cores Escuras)...")
        wc_dir = os.path.join(self.output_dir, "WordClouds")
        os.makedirs(wc_dir, exist_ok=True)
        
        # Gerador de cores travado em tons escuros e legíveis
        def get_color_func(quartile):
            # Matizes de roxo a amarelo
            hue_map = {'Q1': 261, 'Q2': 290, 'Q3': 30, 'Q4': 55}
            hue = hue_map.get(quartile, 210)
            
            def color_func(word, font_size, position, orientation, random_state=None, **kwargs):
                # hsl(Matiz, Saturação 70-100%, Luminosidade 20-45%) -> Garante leitura no fundo branco
                return f"hsl({hue}, {random.randint(70, 100)}%, {random.randint(20, 45)}%)"
            return color_func

        for quartile, texts in texts_by_quartile.items():
            if not texts:
                continue
                
            text_corpus = " ".join(texts)
            wordcloud = WordCloud(
                width=1600, height=800, 
                background_color='white',
                stopwords=self.pt_stopwords,
                color_func=get_color_func(quartile), # Usa nossa função restritiva de brilho
                max_words=200,
                collocations=False
            ).generate(text_corpus)
            
            plt.figure(figsize=(10, 5))
            plt.imshow(wordcloud, interpolation='bilinear')
            plt.axis("off")
    
            out_file = os.path.join(wc_dir, f"WordCloud_{quartile}.pdf")
            plt.savefig(out_file, dpi=300, bbox_inches='tight')
            plt.close()
        print(f"   -> WordClouds salvas em: {wc_dir}")

    def run_bertopic_analysis(self, docs, classes):
        print("[*] Inicializando Modelagem de Tópicos (BERTopic)...")
        print("   -> (Aviso: A geração de embeddings neurais pode levar um tempo considerável)")
        
        # Congelando a aleatoriedade (Determinismo)
        seed = 42
        np.random.seed(seed)
        random.seed(seed)
        os.environ['PYTHONHASHSEED'] = str(seed)
        
        # Instanciando o UMAP com random_state fixo para reprodutibilidade acadêmica
        umap_model = UMAP(
            n_neighbors=15, 
            n_components=5, 
            min_dist=0.0, 
            metric='cosine', 
            random_state=seed,
            low_memory=False, # Usa RAM extra
            n_jobs=-1         # Usa todos os núcleos do processador
        )
        
        embedding_model = SentenceTransformer("paraphrase-multilingual-MiniLM-L12-v2")
        vectorizer_model = CountVectorizer(stop_words=self.pt_stopwords)
        
        topic_model = BERTopic(
            embedding_model=embedding_model, 
            vectorizer_model=vectorizer_model,
            umap_model=umap_model, 
            language="multilingual", 
            calculate_probabilities=False,
            verbose=True
        )
        
        topics, probs = topic_model.fit_transform(docs)
        topics_per_class = topic_model.topics_per_class(docs, classes=classes)
        
        freq = topic_model.get_topic_info()
        freq.to_csv(os.path.join(self.output_dir, "BERTopic_All_Topics.csv"), index=False)
        
        print("[*] Processando o Top 5 Tópicos por Quartil...")
        latex_text = ""
        
        for q in sorted(set(classes)):
            class_topics = topics_per_class[topics_per_class["Class"] == q]
            top_5 = class_topics[class_topics["Topic"] != -1].nlargest(5, "Frequency")
            
            latex_text += f"\\textbf{{Quartil {q} - Top 5 Tópicos:}}\n\\begin{{itemize}}\n"
            for _, row in top_5.iterrows():
                words = ", ".join([w.split('_')[0] for w in row['Words'].split(',')[:5]])
                freq_val = row['Frequency']
                latex_text += f"    \\item \\textit{{{words}}} (Frequência: {freq_val})\n"
            latex_text += "\\end{itemize}\n\n"
            
        with open(os.path.join(self.output_dir, "Top5_Topics_Per_Quartile_LaTeX.txt"), "w", encoding="utf-8") as f:
            f.write(latex_text)
            
        print(f"   -> Concluído! Relatórios salvos na pasta: {self.output_dir}")

if __name__ == "__main__":
    nlp = NLPEngine(sample_size=None) 
    quartile_texts, all_documents, document_classes = nlp.load_and_map_texts()
    
    if all_documents:
        nlp.run_wordclouds(quartile_texts)
        nlp.run_bertopic_analysis(all_documents, document_classes)
    else:
        print("[!] Nenhum dado processado. Verifique os caminhos e a execução da base.")