import os
import json
import re
import time
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
        print("[*] A carregar e a mapear textos (Memória RAM)...")
        
        parent_map = {}
        messages = {}
        author_map = {} 
        
        with open(Config.MULTIMODAL_PATH, 'r', encoding='utf-8') as f:
            for line in f:
                try:
                    obj = json.loads(line)
                    msg_id = str(obj.get('id', '')).split('_')[-1]
                    parent_raw = obj.get('parent_id')
                    p_id = str(parent_raw).split('_')[-1] if parent_raw else None
                    
                    author = str(obj.get('author', '')).lower()
                    body = obj.get('body', '').strip()
                    is_valid = obj.get('is_valid_text', False)
                    
                    is_deleted = author in ['[deleted]', 'deleted']
                    is_mod_bot = author in ['automoderator'] or 'modteam' in author
                    is_visual_context = "[CONTEUDO VISUAL:" in body.lower()
                    is_foreign_script = bool(re.search(r'[\u0400-\u04FF]', body))
                    
                    if msg_id:
                        parent_map[msg_id] = p_id
                        if is_valid and body and not (is_deleted or is_mod_bot or is_visual_context or is_foreign_script):
                            messages[msg_id] = body
                            author_map[msg_id] = author
                except Exception:
                    continue
                    
        print("   -> A resolver as árvores de cascatas e a alocar quartis...")
        
        def get_cascade_quartile_and_root(m_id):
            curr = m_id
            visited = set()
            root_id = m_id
            while curr and curr not in visited:
                visited.add(curr)
                root_id = curr
                if curr in cascade_map:
                    return cascade_map[curr], root_id
                curr = parent_map.get(curr)
            return None, None
            
        data_rows = []
        count = 0
        print("   -> [DEBUG] Iniciando a varredura das árvores. Isso pode demorar...")
        start_load = time.time()
        
        for m_id, body in messages.items():
            if self.sample_size and count >= self.sample_size:
                break
            quartile, root_id = get_cascade_quartile_and_root(m_id)
            if pd.notna(quartile):
                data_rows.append({
                    'Message_ID': m_id,
                    'Cascade_ID': root_id,
                    'Author': author_map[m_id],
                    'Quartile': quartile,
                    'Body': body
                })
                count += 1
                # NOVO: Rastreador de progresso
                if count % 10000 == 0:
                    print(f"      [DEBUG] Já alocou {count:,} mensagens nas cascatas... ({(time.time() - start_load):.2f}s)")
                
        df_comments = pd.DataFrame(data_rows)
        print(f"   -> Sucesso! {len(df_comments):,} mensagens estruturadas no DataFrame em {(time.time() - start_load):.2f}s.")
        return df_comments
    
    def get_cached_or_infer(self):
        import os
        import pandas as pd
        from Utilities import Config
        
        cache_file = os.path.join(Config.RESULTS_DIR, "nlp_dataframe_cache.parquet")
        
        if os.path.exists(cache_file):
            print(f"\n[*] 🔥 CACHE NLP ENCONTRADO! Pulando o inferno dos embeddings...")
            print(f"   -> Carregando textos e tópicos instantaneamente de: {cache_file}")
            df_comments = pd.read_parquet(cache_file)
            return df_comments
            
        print("\n[*] 🐢 Cache não encontrado. Aperte os cintos, vamos inferir o BERTopic do zero...")
        df_comments = self.load_and_map_texts()
        
        if not df_comments.empty:
            # Executa a inteligência artificial pesada e anexa a coluna 'Topic' no DataFrame
            self.run_bertopic_analysis(df_comments)
            
            # Salva o resultado na RAM direto para o disco
            print(f"[*] 💾 Salvando Cache NLP em disco para execuções futuras instantâneas...")
            df_comments.to_parquet(cache_file, index=False)
            
        return df_comments

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

    def run_bertopic_analysis(self, df_comments):
        print("[*] A inicializar a Modelagem de Tópicos (BERTopic)...")
        docs = df_comments['Body'].tolist()
        classes = df_comments['Quartile'].tolist()
        
        seed = 42
        np.random.seed(seed)
        random.seed(seed)
        os.environ['PYTHONHASHSEED'] = str(seed)
        
        umap_model = UMAP(n_neighbors=15, n_components=5, min_dist=0.0, metric='cosine', random_state=seed, low_memory=False, n_jobs=-1)
        embedding_model = SentenceTransformer("paraphrase-multilingual-MiniLM-L12-v2")
        vectorizer_model = CountVectorizer(stop_words=self.pt_stopwords)
        
        topic_model = BERTopic(embedding_model=embedding_model, vectorizer_model=vectorizer_model, umap_model=umap_model, language="multilingual", calculate_probabilities=False, verbose=True)
        
        topics, probs = topic_model.fit_transform(docs)
        
        # Anexa os tópicos resultantes ao DataFrame para o cálculo da Entropia
        df_comments['Topic'] = topics
        
        freq = topic_model.get_topic_info()
        freq.to_csv(os.path.join(self.output_dir, "BERTopic_All_Topics.csv"), index=False)
        
        print("[*] A processar o Top 10 Tópicos por Quartil...")
        topics_per_class = topic_model.topics_per_class(docs, classes=classes)
        latex_text = ""
        
        for q in sorted(set(classes)):
            class_topics = topics_per_class[topics_per_class["Class"] == q]
            top_10 = class_topics[class_topics["Topic"] != -1].nlargest(10, "Frequency")
            
            latex_text += f"\\textbf{{Quartil {q} - Top 10 Tópicos:}}\n\\begin{{itemize}}\n"
            for _, row in top_10.iterrows():
                words = ", ".join([w.split('_')[0] for w in row['Words'].split(',')])
                freq_val = row['Frequency']
                latex_text += f"    \\item \\textit{{{words}}} (Frequência: {freq_val})\n"
            latex_text += "\\end{itemize}\n\n"
            
        with open(os.path.join(self.output_dir, "Top10_Topics_Per_Quartile_LaTeX.txt"), "w", encoding="utf-8") as f:
            f.write(latex_text)
            
        print(f"   -> Top 10 guardado. A iniciar o pipeline de Entropia Temática...")

    def run_entropy_pipeline(self, df_comments):
        from scipy.stats import entropy
        import matplotlib.ticker as mtick
        import seaborn as sns # type: ignore
        
        print("[*] A iniciar Análise de Entropia (Diversidade Temática)...")
        
        # Puxa os dados de rede para mapear o UQ (User Type)
        engine = AnalyticsEngine()
        engine.load_or_extract_data()
        
        def calculate_shannon_entropy(series):
            probabilities = series.value_counts(normalize=True).values
            return entropy(probabilities, base=np.e)

        # Filtra outliers do BERTopic (Tópico -1) para medir apenas a diversidade semântica real
        df_valid = df_comments[df_comments['Topic'] != -1]

        # ---------------------------------------------------------
        # A. Entropia por Cascata
        # ---------------------------------------------------------
        df_cascades_entropy = df_valid.groupby('Cascade_ID')['Topic'].agg(calculate_shannon_entropy).reset_index()
        df_cascades_entropy.rename(columns={'Topic': 'Thematic_Entropy'}, inplace=True)
        cascade_map = df_valid[['Cascade_ID', 'Quartile']].drop_duplicates()
        df_cascades_entropy = df_cascades_entropy.merge(cascade_map, on='Cascade_ID', how='inner')

        # ---------------------------------------------------------
        # B. Entropia por Utilizador (Homofilia)
        # ---------------------------------------------------------
        user_data = []
        for author, counts in engine.user_sentiments.items():
            if counts['total'] > 0:
                user_data.append({'Author': author, 'perc_negative': (counts['negative'] / counts['total']) * 100})
        
        df_users = pd.DataFrame(user_data)
        bins = [-1.0, 25.0, 50.0, 75.0, 100.0]
        df_users['User_Type'] = pd.cut(df_users['perc_negative'], bins=bins, labels=['UQ1', 'UQ2', 'UQ3', 'UQ4'])
        user_quartile_map = df_users[['Author', 'User_Type']]
        
        df_users_entropy = df_valid.groupby('Author')['Topic'].agg(calculate_shannon_entropy).reset_index()
        df_users_entropy.rename(columns={'Topic': 'Thematic_Entropy'}, inplace=True)
        df_users_entropy = df_users_entropy.merge(user_quartile_map, on='Author', how='inner')

        # ---------------------------------------------------------
        # C. Plotagem das Curvas CCDF
        # ---------------------------------------------------------
        Config.set_sns_theme()
        
        def _plot(df_ent, entity_type, group_col, groups_list):
            fig, ax = plt.subplots(figsize=(10, 7))
            for i, cat in enumerate(groups_list):
                data = df_ent[df_ent[group_col] == cat]['Thematic_Entropy'].dropna().values
                if len(data) == 0: continue
                mean_val, std_val = np.mean(data), np.std(data)
                label_text = f"{cat} (μ={mean_val:.2f}, σ={std_val:.2f})"
                
                sorted_data = np.sort(data)
                y = (1.0 - np.arange(len(sorted_data)) / len(sorted_data)) * 100
                ax.plot(sorted_data, y, color=engine.colors['COLOR_SCHEME'][i], 
                        linestyle=['-', '--', '-.', ':'][i % 4], linewidth=3.5, label=label_text)

            entity_label = "CASCADES" if entity_type == 'Cascades' else "USERS"
            ax.set_xlabel(f'SHANNON ENTROPY (THEMATIC DIVERSITY IN {entity_label})', fontsize=18, fontweight='bold')
            ax.set_ylabel('CCDF (%)', fontsize=18, fontweight='bold')
            ax.yaxis.set_major_formatter(mtick.PercentFormatter(decimals=0))
            ax.set_xlim(left=0)
            ax.legend(fontsize=16, loc='upper right', framealpha=0.9, edgecolor='black')
            ax.tick_params(labelsize=14)
            sns.despine()
            
            out_file = os.path.join(self.output_dir, f"Fig_CCDF_Entropy_{entity_type}.pdf")
            plt.tight_layout()
            plt.savefig(out_file, dpi=300, bbox_inches='tight')
            plt.close()
            print(f"   -> Guardado: Fig_CCDF_Entropy_{entity_type}.pdf")

        _plot(df_cascades_entropy, 'Cascades', 'Quartile', ['Q1', 'Q2', 'Q3', 'Q4'])
        _plot(df_users_entropy, 'Users', 'User_Type', ['UQ1', 'UQ2', 'UQ3', 'UQ4'])
        print("[*] Análise de Entropia concluída com sucesso!")

    def run_valence_analysis(self, df_comments):
        import numpy as np
        import pandas as pd
        import matplotlib.pyplot as plt # type: ignore
        import seaborn as sns # type: ignore
        from sklearn.feature_extraction.text import CountVectorizer
        
        print("[*] Iniciando Cálculo de Valência Semântica (Adaptação da Equação 2)...")
        
        # 1. Recupera o mapeamento de UQ e a Paleta de Cores
        engine = AnalyticsEngine()
        engine.load_or_extract_data()
        
        # Extrai as 4 cores oficiais do seu COLOR_SCHEME (inferno_r)
        c_q1, c_q2, c_q3, c_q4 = engine.colors['COLOR_SCHEME']
        
        user_data = []
        for author, counts in engine.user_sentiments.items():
            if counts['total'] > 0:
                user_data.append({'Author': author, 'perc_negative': (counts['negative'] / counts['total']) * 100})
        
        df_users = pd.DataFrame(user_data)
        bins = [-1.0, 25.0, 50.0, 75.0, 100.0]
        df_users['User_Type'] = pd.cut(df_users['perc_negative'], bins=bins, labels=['UQ1', 'UQ2', 'UQ3', 'UQ4'])
        
        # Injeta o User_Type no DataFrame de textos
        df_valid = df_comments.merge(df_users[['Author', 'User_Type']], on='Author', how='left')
        df_valid = df_valid[df_valid['Body'].notna() & (df_valid['Body'] != '')]

        # =========================================================
        # CRIAÇÃO DAS METADES (AGRUPANDO TODOS OS QUARTIS)
        # =========================================================
        df_valid['Cascade_Halves'] = df_valid['Quartile'].map({'Q1': 'Q1+Q2', 'Q2': 'Q1+Q2', 'Q3': 'Q3+Q4', 'Q4': 'Q3+Q4'})
        df_valid['User_Halves'] = df_valid['User_Type'].map({'UQ1': 'UQ1+UQ2', 'UQ2': 'UQ1+UQ2', 'UQ3': 'UQ3+UQ4', 'UQ4': 'UQ3+UQ4'})

        # Adicionado os parâmetros color_g1 e color_g2 na função
        def compute_and_plot_valence(df, col_name, g1_label, g2_label, title_prefix, color_g1, color_g2):
            print(f"   -> Processando Valência: {g1_label} (-1) vs {g2_label} (+1)...")
            
            texts_g1 = df[df[col_name] == g1_label]['Body'].tolist()
            texts_g2 = df[df[col_name] == g2_label]['Body'].tolist()
            
            if not texts_g1 or not texts_g2:
                print(f"      [!] Aviso: Dados insuficientes para {g1_label} vs {g2_label}.")
                return
            
            # Vetorização (min_df=10 para ignorar ruído)
            vectorizer = CountVectorizer(stop_words=self.pt_stopwords, min_df=10)
            X = vectorizer.fit_transform(texts_g1 + texts_g2)
            vocab = vectorizer.get_feature_names_out()
            
            X_g1 = X[:len(texts_g1)]
            X_g2 = X[len(texts_g1):]
            
            # Frequências
            count_g1 = np.asarray(X_g1.sum(axis=0)).flatten()
            count_g2 = np.asarray(X_g2.sum(axis=0)).flatten()
            N_g1 = count_g1.sum()
            N_g2 = count_g2.sum()
            
            freq_g1 = count_g1 / N_g1 if N_g1 > 0 else np.zeros_like(count_g1)
            freq_g2 = count_g2 / N_g2 if N_g2 > 0 else np.zeros_like(count_g2)
            
            # Equação de Valência
            denominator = freq_g1 + freq_g2
            valid_mask = denominator > 0
            
            valence = np.zeros_like(denominator)
            valence[valid_mask] = 2 * (freq_g2[valid_mask] / denominator[valid_mask]) - 1
            
            df_val = pd.DataFrame({
                'Term': vocab,
                'Valence': valence,
                'Total_Count': count_g1 + count_g2
            })
            
            # Top 15% mais falados
            min_mentions_threshold = np.percentile(df_val['Total_Count'], 85) 
            df_val_filtered = df_val[df_val['Total_Count'] >= min_mentions_threshold]
            
            top_g1 = df_val_filtered.nsmallest(20, 'Valence').copy()
            top_g2 = df_val_filtered.nlargest(20, 'Valence').copy()
            
            df_plot = pd.concat([top_g1, top_g2]).sort_values(by='Valence', ascending=True)
            
            # APLICAÇÃO DA PALETA COLOR_SCHEME DO UTILITIES
            df_plot['Color'] = df_plot['Valence'].apply(lambda x: color_g1 if x < 0 else color_g2)
            
            # Plotagem
            Config.set_sns_theme()
            fig, ax = plt.subplots(figsize=(12, 10))
            bars = ax.barh(df_plot['Term'], df_plot['Valence'], color=df_plot['Color'], edgecolor='black', height=0.7)
            
            ax.axvline(0, color='black', linewidth=1.5)
            ax.set_xlabel(f'SEMANTIC VALENCE\n(← {g1_label} Dominated | {g2_label} Dominated →)', fontsize=14, fontweight='bold')
            ax.set_title(f'Semantic Valence: {title_prefix} ({g1_label} vs {g2_label})', fontsize=16, fontweight='bold', pad=20)
            ax.tick_params(labelsize=12)
            
            for bar in bars:
                width = bar.get_width()
                label_x_pos = width - 0.02 if width < 0 else width + 0.02
                ha = 'right' if width < 0 else 'left'
                ax.text(label_x_pos, bar.get_y() + bar.get_height()/2, f'{width:.2f}', 
                        va='center', ha=ha, fontsize=10, fontweight='bold')

            sns.despine()
            
            # Limpa caracteres problemáticos para o nome do ficheiro (ex: o "+" no "Q1+Q2")
            safe_g1 = g1_label.replace('+', '_plus_')
            safe_g2 = g2_label.replace('+', '_plus_')
            out_pdf = os.path.join(self.output_dir, f"Fig_Valence_{title_prefix}_{safe_g1}_vs_{safe_g2}.pdf")
            
            plt.tight_layout()
            plt.savefig(out_pdf, dpi=300)
            plt.close()
            
            # Exportação LaTeX
            latex_table = f"""\\begin{{table}}[htbp]
    \\centering
    \\caption{{Termos com maior polaridade de Valência entre {g1_label} e {g2_label} ({title_prefix}).}}
    \\label{{tab:valence_{title_prefix.lower()}_{safe_g1}_{safe_g2}}}
    \\begin{{tabular}}{{lcc|lcc}}
        \\toprule
        \\multicolumn{{3}}{{c}}{{\\textbf{{Vocabulário Predominante em {g1_label}}}}} & \\multicolumn{{3}}{{c}}{{\\textbf{{Vocabulário Predominante em {g2_label}}}}} \\\\
        \\textbf{{Termo}} & \\textbf{{Valência}} & \\textbf{{Freq}} & \\textbf{{Termo}} & \\textbf{{Valência}} & \\textbf{{Freq}} \\\\
        \\midrule\n"""
            
            for i in range(20):
                row_g1 = top_g1.iloc[i]
                row_g2 = top_g2.iloc[19-i] 
                latex_table += f"        {row_g1['Term']} & {row_g1['Valence']:.2f} & {int(row_g1['Total_Count'])} & "
                latex_table += f"{row_g2['Term']} & {row_g2['Valence']:.2f} & {int(row_g2['Total_Count'])} \\\\\n"
                
            latex_table += """        \\bottomrule
    \\end{tabular}
\\end{table}\n"""
            
            with open(os.path.join(self.output_dir, f"Table_Valence_{title_prefix}_{safe_g1}_vs_{safe_g2}.txt"), "w", encoding="utf-8") as f:
                f.write(latex_table)

        # =========================================================
        # 2. EXECUÇÃO DE TODOS OS CENÁRIOS
        # =========================================================
        # Cenário 1: Polos Extremos (Q1 vs Q4) -> Usa cor 1 e cor 4 do inferno_r
        compute_and_plot_valence(df_valid, 'Quartile', 'Q1', 'Q4', 'Cascades_Extremes', c_q1, c_q4)
        compute_and_plot_valence(df_valid, 'User_Type', 'UQ1', 'UQ4', 'Users_Extremes', c_q1, c_q4)
        
        # Cenário 2: O Núcleo do Debate (Q2 vs Q3) -> Usa cor 2 e cor 3 do inferno_r
        compute_and_plot_valence(df_valid, 'Quartile', 'Q2', 'Q3', 'Cascades_Core', c_q2, c_q3)
        compute_and_plot_valence(df_valid, 'User_Type', 'UQ2', 'UQ3', 'Users_Core', c_q2, c_q3)

        # Cenário 3: Metades Agrupadas ("Todos os Quartis": Q1+Q2 vs Q3+Q4) -> Mantém as cores dos extremos 
        compute_and_plot_valence(df_valid, 'Cascade_Halves', 'Q1+Q2', 'Q3+Q4', 'Cascades_All', c_q1, c_q4)
        compute_and_plot_valence(df_valid, 'User_Halves', 'UQ1+UQ2', 'UQ3+UQ4', 'Users_All', c_q1, c_q4)
        
        print(f"[*] Análise de Valência Múltipla concluída. As 6 Figuras e Tabelas foram guardadas.")

    def run_liwc_analysis(self, df_comments, dic_path="audit/Brazilian_Portuguese_LIWC2015_dictionary.dic"):
        import numpy as np
        import pandas as pd
        import matplotlib.pyplot as plt # type: ignore
        import seaborn as sns # type: ignore
        from scipy.stats import spearmanr
        import re
        import os
        import codecs
        
        print(f"[*] Iniciando Análise Psicolinguística (LIWC)...")
        
        try:
            import liwc
        except ImportError:
            print("[!] ERRO: A biblioteca 'liwc' não está instalada. Execute: pip install liwc")
            return

        if not os.path.exists(dic_path):
            print(f"[!] ERRO: Dicionário LIWC não encontrado em: {dic_path}")
            return

        # 1. Sanitizador de Arquivo .dic
        print("   -> Lendo e sanitizando o arquivo .dic...")
        temp_dic = os.path.join(self.output_dir, "temp_cleaned_liwc.dic")
        try:
            with codecs.open(dic_path, 'r', encoding='utf-8-sig', errors='ignore') as f:
                content = f.read()
            content = content.replace('\r\n', '\n').replace('\r', '\n').strip()
            if not content.startswith('%'):
                content = '%\n' + content
            with open(temp_dic, 'w', encoding='utf-8') as f:
                f.write(content)
        except Exception as e:
            print(f"[!] Erro ao sanitizar o dicionário: {e}")
            return
            
        parse, category_names = liwc.load_token_parser(temp_dic)
        print(f"   -> Sucesso! Dicionário carregado com {len(category_names)} categorias base.")
        if os.path.exists(temp_dic): os.remove(temp_dic)

        # 2. Mapeamento Direto (Resolve o problema dos parênteses)
        target_concepts = ['anger', 'sad', 'anx', 'swear', 'netspeak', 'power', 'risk', 'social', 'certain']
        actual_categories = {}
        
        for concept in target_concepts:
            for real_cat in category_names:
                # Se a categoria do dic for "anger (Anger)", ele vai dar match perfeito com "anger"
                if real_cat.lower().startswith(concept):
                    actual_categories[concept] = real_cat
                    break

        if not actual_categories:
            print("[!] ERRO CRÍTICO: Nenhuma categoria alvo foi mapeada.")
            print(f"    Categorias encontradas: {category_names[:10]}")
            return

        negative_concepts = ['anger', 'sad', 'anx', 'swear']
        
        # 3. Agrupamento de Textos por Cascata
        print("   -> Calculando ativações léxicas nas cascatas...")
        df_cascades_text = df_comments.groupby('Cascade_ID').agg({
            'Body': lambda x: ' '.join(x),
            'Quartile': 'first'
        }).reset_index()
        
        engine = AnalyticsEngine()
        engine.load_or_extract_data()
        df_cascades_meta = engine.df_cascades[['Cascade_ID', 'Perc_Negative']]
        df_cascades_text = df_cascades_text.merge(df_cascades_meta, on='Cascade_ID', how='inner')

        # 4. Processamento Léxico
        liwc_results = []
        for _, row in df_cascades_text.iterrows():
            tokens = re.findall(r'\b\w+\b', row['Body'].lower())
            total_words = len(tokens)
            counts = {concept: 0 for concept in target_concepts}
            
            if total_words > 0:
                for token in tokens:
                    for cat in parse(token):
                        for concept, real_cat in actual_categories.items():
                            if cat == real_cat:
                                counts[concept] += 1
                
                perc_counts = {concept: (val / total_words) * 100 for concept, val in counts.items()}
            else:
                perc_counts = {concept: 0.0 for concept in target_concepts}
                
            perc_counts['Cascade_ID'] = row['Cascade_ID']
            liwc_results.append(perc_counts)

        df_liwc = pd.DataFrame(liwc_results)
        df_final = df_cascades_text.merge(df_liwc, on='Cascade_ID')

        # 5. Geração dos Gráficos em Grid
        print("   -> Gerando grid de Barplots com Erro Padrão...")
        Config.set_sns_theme()
        palette_dict = dict(zip(['Q1', 'Q2', 'Q3', 'Q4'], engine.colors['COLOR_SCHEME']))
        
        cols = 3
        rows = (len(target_concepts) + cols - 1) // cols
        fig, axes = plt.subplots(rows, cols, figsize=(18, 5 * rows))
        axes = axes.flatten()
        
        for i, cat in enumerate(target_concepts):
            ax = axes[i]
            sns.barplot(data=df_final, x='Quartile', y=cat, order=['Q1', 'Q2', 'Q3', 'Q4'],
                        palette=palette_dict, errorbar='se', capsize=.1, ax=ax, 
                        edgecolor='black', linewidth=1.2)
            
            ax.set_title(f"LIWC Category: {cat.upper()}", fontsize=14, fontweight='bold', pad=10)
            ax.set_ylabel(f'Mean Frequency (%)', fontsize=12, fontweight='bold')
            ax.set_xlabel('Cascade Negativity Quartile', fontsize=12, fontweight='bold')
            ax.tick_params(labelsize=11)
            
        for j in range(i + 1, len(axes)):
            fig.delaxes(axes[j])
            
        sns.despine()
        plt.tight_layout(pad=3.0)
        out_pdf = os.path.join(self.output_dir, "Fig_LIWC_Categories_Grid.pdf")
        plt.savefig(out_pdf, dpi=300)
        plt.close()

        # 6. Correlações de Spearman
        print("   -> Calculando Matriz de Correlação (Toxidade x Léxico)...")
        stats_results = []
        
        for cat in target_concepts:
            if df_final[cat].nunique() > 1 and df_final['Perc_Negative'].nunique() > 1:
                corr, p_val = spearmanr(df_final['Perc_Negative'], df_final[cat])
            else:
                corr, p_val = float('nan'), float('nan')
                
            is_neg_core = "Yes" if cat in negative_concepts else "No"
            
            stats_results.append({
                'LIWC_Category': cat.upper(),
                'Negative_Core': is_neg_core,
                'Spearman_Rho': corr,
                'P_Value': p_val
            })
            
        df_stats = pd.DataFrame(stats_results).sort_values(by='Spearman_Rho', ascending=False, na_position='last')
        
        latex_table = f"""\\begin{{table}}[htbp]
    \\centering
    \\caption{{Correlação de Spearman ($\\rho$) entre a proporção de negatividade das cascatas e a frequência de ativação das categorias léxicas (LIWC).}}
    \\label{{tab:liwc_correlation}}
    \\begin{{tabular}}{{lccc}}
        \\toprule
        \\textbf{{Categoria LIWC}} & \\textbf{{Núcleo Negativo?}} & \\textbf{{$\\rho$ (Spearman)}} & \\textbf{{Valor-$p$}} \\\\
        \\midrule\n"""
        
        for _, row in df_stats.iterrows():
            if pd.isna(row['Spearman_Rho']):
                rho_str, p_str = "N/A", "N/A"
            else:
                rho_str = f"{row['Spearman_Rho']:.4f}"
                p_str = "< 0.001" if row['P_Value'] < 0.001 else f"{row['P_Value']:.4f}"
                
            latex_table += f"        {row['LIWC_Category']} & {row['Negative_Core']} & {rho_str} & {p_str} \\\\\n"
            
        latex_table += """        \\bottomrule
    \\end{tabular}
\\end{table}\n"""

        with open(os.path.join(self.output_dir, "Table_LIWC_Correlations.txt"), "w", encoding="utf-8") as f:
            f.write(latex_table)

        print(f"[*] Pipeline LIWC concluído! Resultados salvos em: {self.output_dir}")

if __name__ == "__main__":
    nlp = NLPEngine(sample_size=None) 
    
    # Agora ele tenta buscar do Cache primeiro. Se achar, pula o BERTopic e carrega instantâneo!
    df_comments = nlp.get_cached_or_infer()
    
    if not df_comments.empty:
        print("\n[*] Iniciando bateria de geração de Relatórios e Gráficos...")
        
        # 1. WordClouds
        texts_by_quartile = {q: df_comments[df_comments['Quartile'] == q]['Body'].tolist() for q in ['Q1', 'Q2', 'Q3', 'Q4']}
        nlp.run_wordclouds(texts_by_quartile)
        
        # 2. Entropia de Shannon (Diversidade Temática)
        nlp.run_entropy_pipeline(df_comments)
        
        # 3. Valência Semântica (A Polaridade das Palavras)
        nlp.run_valence_analysis(df_comments)
        
        # 4. LIWC (Psicolinguística - Certifique-se do caminho do .dic)
        nlp.run_liwc_analysis(df_comments, dic_path="audit/Brazilian_Portuguese_LIWC2015_dictionary.dic")
        
        print("\n[🚀] TUDO PRONTO! O script rodou usando o cache.")
    else:
        print("[!] Nenhum dado processado. Verifique os caminhos e a execução da base.")