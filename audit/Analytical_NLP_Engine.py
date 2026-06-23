import os
import gc
import json
import re
import time
import random
import pandas as pd
import numpy as np
import threading
import matplotlib.pyplot as plt # type: ignore
import matplotlib.ticker as mtick # type: ignore
import seaborn as sns # type: ignore
from wordcloud import WordCloud # type: ignore
from bertopic import BERTopic # type: ignore
from sentence_transformers import SentenceTransformer # type: ignore
from cuml.manifold import UMAP # type: ignore
from sklearn.feature_extraction.text import CountVectorizer
from scipy.stats import spearmanr, entropy, kruskal, ks_2samp

# Project config and engine
from Utilities import Config
from Methods import AnalyticsEngine

# Stopword dictionaries
from stopwordsiso import stopwords as get_stopwords # type: ignore
from dictionary import CUSTOM_BR, DOMAIN_STOPWORDS
import itertools

class ConsoleSpinner:
    def __init__(self, message="Aguarde"):
        self.spinner = itertools.cycle(['[    ]', '[=   ]', '[==  ]', '[=== ]', '[====]', '[ ===]', '[  ==]', '[   =]'])
        self.message = message
        self.running = False
        self.thread = None

    def spin(self):
        while self.running:
            # O \r e o flush=True que configuramos no Tkinter farão a mágica aqui
            print(f"\r{self.message} {next(self.spinner)}   ", end="", flush=True)
            time.sleep(0.15) # Velocidade do giro

    def start(self):
        self.running = True
        self.thread = threading.Thread(target=self.spin, daemon=True)
        self.thread.start()

    def stop(self, success_message="Concluído!"):
        self.running = False
        if self.thread:
            self.thread.join()
        # Limpa a linha e mostra a mensagem de sucesso
        print(f"\r{self.message} {success_message}        \n", end="", flush=True)
        
class NLPEngine:
    def __init__(self, sample_size=None):
        
        self.sample_size = sample_size
        self.output_dir = os.path.join(Config.RESULTS_DIR, "NLP_Analysis")
        os.makedirs(self.output_dir, exist_ok=True)

        # ISO academic lexicon (PT + EN/ES to purge foreign noise)
        academic_stopwords_pt = get_stopwords(["pt"])
        academic_stopwords_en = get_stopwords(["en", "es"])

        # Reddit structural noise: bots, frequent usernames, artifacts
        reddit_noise = {
            'sound', 'message', 'redact', 'comments', 'black',
            'sneakpeekbot', 'np', 'content', 'isjesusagain',
            'deleted', 'image', 'october', 'statement', 'blue', 'joint', 'death', 'dead'
        }

        self.pt_stopwords = list(set(academic_stopwords_pt) | set(academic_stopwords_en) | set(CUSTOM_BR) | set(DOMAIN_STOPWORDS) | reddit_noise)
        print(f"[INFO] ℹ️  Engine initialized with {len(self.pt_stopwords):,} combined stopwords (PT+EN+ES+Custom+Noise).")

    def _get_cascade_quartiles(self):
        print("[INFO] ℹ️  Extracting cascade structural boundaries...")
        engine = AnalyticsEngine()
        engine.load_or_extract_data()
        df_cascades = engine._prepare_quartiles(interactive_only=False)
        return dict(zip(df_cascades['Cascade_ID'], df_cascades['neg_quartile']))

    def load_and_map_texts(self):
        cascade_map = self._get_cascade_quartiles()
        print("[INFO] ℹ️  Loading and mapping texts into RAM...")
        start_load = time.time()

        parent_map = {}
        messages = {}
        author_map = {}

        spinner_read = ConsoleSpinner("ℹ️ Reading hierarchical structure and applying exclusion filters...")
        spinner_read.start()
        
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
                    
        spinner_read.stop("✅ Hierarchical structure mapped.")

        print("[*] Resolving cascade trees and allocating quartiles...")
        data_rows = []
        count = 0

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
                if count % 10000 == 0:
                    print(f"\r[DEBUG] Allocated {count:,} messages to cascades... ({(time.time() - start_load):.2f}s)", end="", flush=True)

        df_comments = pd.DataFrame(data_rows)

        # Inject User_Type (homophily) at root level for BERTopic and LIWC reports
        engine = AnalyticsEngine()
        engine.load_or_extract_data()
        user_data = []
        for author_name, counts in engine.user_sentiments.items():
            if counts['total'] > 0:
                user_data.append({'Author': author_name, 'perc_negative': (counts['negative'] / counts['total']) * 100})
        df_users = pd.DataFrame(user_data)
        df_users['User_Type'] = pd.cut(df_users['perc_negative'], bins=[-1.0, 25.0, 50.0, 75.0, 100.0], labels=['UQ1', 'UQ2', 'UQ3', 'UQ4'])
        df_comments = df_comments.merge(df_users[['Author', 'User_Type']], on='Author', how='left')

        print(f"\n[INFO] ✅ Done. {len(df_comments):,} messages structured in DataFrame in {(time.time() - start_load):.2f}s.")
        return df_comments

    def get_cached_or_infer(self):
        cache_file = os.path.join(Config.RESULTS_DIR, "nlp_dataframe_cache.parquet")
        if os.path.exists(cache_file):
            print(f"[INFO] ℹ️  NLP CACHE FOUND. Loading instantly from: {cache_file}")
            df_comments = pd.read_parquet(cache_file)

            # PATCH: Auto-update legacy caches missing User_Type
            if 'User_Type' not in df_comments.columns:
                print("[PATCH] Legacy cache detected. Injecting 'User_Type' to preserve inferred topics...")
                engine = AnalyticsEngine()
                engine.load_or_extract_data()
                user_data = []
                for author_name, counts in engine.user_sentiments.items():
                    if counts['total'] > 0:
                        user_data.append({'Author': author_name, 'perc_negative': (counts['negative'] / counts['total']) * 100})
                df_users = pd.DataFrame(user_data)
                df_users['User_Type'] = pd.cut(df_users['perc_negative'], bins=[-1.0, 25.0, 50.0, 75.0, 100.0], labels=['UQ1', 'UQ2', 'UQ3', 'UQ4'])
                df_comments = df_comments.merge(df_users[['Author', 'User_Type']], on='Author', how='left')

                print("[PATCH] Writing updated cache to disk...")
                df_comments.to_parquet(cache_file, index=False)

            return df_comments

        print("[WARN] ⚠️ No cache found. Starting deep BERTopic inference...")
        df_comments = self.load_and_map_texts()
        if not df_comments.empty:
            self.run_bertopic_analysis(df_comments)
            print("[INFO] ℹ️ Writing nlp_dataframe_cache.parquet to disk...")
            df_comments.to_parquet(cache_file, index=False)
        return df_comments

    def run_wordclouds(self, texts_by_quartile):
        print("[SYSTEM] WORDCLOUD GENERATION")
        wc_dir = os.path.join(self.output_dir, "WordClouds")
        os.makedirs(wc_dir, exist_ok=True)

        def get_color_func(quartile):
            hue_map = {'Q1': 261, 'Q2': 290, 'Q3': 30, 'Q4': 55}
            hue = hue_map.get(quartile, 210)
            return lambda *args, **kwargs: f"hsl({hue}, {random.randint(70, 100)}%, {random.randint(20, 45)}%)"

        spinner_wc = ConsoleSpinner("☁️ Generating WordClouds for quartiles...")
        spinner_wc.start()

        for quartile, texts in texts_by_quartile.items():
            is_empty = texts.empty if hasattr(texts, 'empty') else (not texts)
            if is_empty:
                continue

            # Guard: filter out any NaN/float values before joining
            text_data = [v for v in (texts.tolist() if hasattr(texts, 'tolist') else texts) if isinstance(v, str)]
            if not text_data:
                continue

            wordcloud = WordCloud(
                width=1600, height=800, background_color='white',
                stopwords=self.pt_stopwords, color_func=get_color_func(quartile),
                max_words=200, collocations=False
            ).generate(" ".join(text_data))

            plt.figure(figsize=(10, 5))
            plt.imshow(wordcloud, interpolation='bilinear')
            plt.axis("off")
            plt.savefig(os.path.join(wc_dir, f"WordCloud_{quartile}.pdf"), dpi=300, bbox_inches='tight')
            plt.close()
            
        spinner_wc.stop("✅ WordClouds exported.")
        print("\n[SYSTEM] SAVING OUTPUT FILES\n")

    def run_bertopic_analysis(self, df_comments):
        print("[SYSTEM] BERTOPIC TOPIC MODELING")
        start_bertopic = time.time()

        docs = df_comments['Body'].tolist()
        classes_cascades = df_comments['Quartile'].tolist()

        # Reproducibility seeds
        seed = 42
        np.random.seed(seed)
        random.seed(seed)
        os.environ['PYTHONHASHSEED'] = str(seed)

        umap_model = UMAP(n_neighbors=15, n_components=5, min_dist=0.0, metric='cosine', random_state=seed, low_memory=False, n_jobs=-1)
        embedding_model = SentenceTransformer("paraphrase-multilingual-MiniLM-L12-v2")
        vectorizer_model = CountVectorizer(
            stop_words=self.pt_stopwords,
            token_pattern=r'\b[a-zA-ZÀ-ÿ]{2,}\b'
        )

        # verbose=False para não atropelar o nosso spinner
        topic_model = BERTopic(embedding_model=embedding_model, vectorizer_model=vectorizer_model, umap_model=umap_model, language="multilingual", calculate_probabilities=False, verbose=False)

        spinner_bert = ConsoleSpinner(f"🤖 Running UMAP/Transformer inference on {len(docs):,} documents...")
        spinner_bert.start()
        
        start_fit = time.time()
        topics, _ = topic_model.fit_transform(docs)
        df_comments['Topic'] = topics
        
        spinner_bert.stop(f"✅ Inference completed in {(time.time() - start_fit)/60:.2f} minutes.")

        freq = topic_model.get_topic_info()
        freq.to_csv(os.path.join(self.output_dir, "BERTopic_All_Topics.csv"), index=False)

        def export_latex_topics(topics_per_class, class_labels, file_name):
            latex_text = ""
            
            def is_junk(w):
                w_str = str(w).strip().lower()
                if len(w_str) < 3: return True
                if w_str.isdigit(): return True
                if re.search(r'(.)\1{4,}', w_str): return True
                return False

            for q in sorted(set(class_labels)):
                if pd.isna(q) or str(q) == 'Unclassified': 
                    continue
                
                class_topics = topics_per_class[topics_per_class["Class"] == q]
                top_10 = class_topics[class_topics["Topic"] != -1].nlargest(10, "Frequency")
                
                valid_rows = []
                for _, row in top_10.iterrows():
                    if pd.isna(row['Words']) or not str(row['Words']).strip(): 
                        continue
                    
                    raw_words = [str(w).split('_')[0].strip() for w in str(row['Words']).split(',')]
                    clean_words = [w for w in raw_words if not is_junk(w)]
                    
                    if len(clean_words) >= 2:
                        display_words = ", ".join(clean_words[:5])
                        valid_rows.append((display_words, row['Frequency']))
                
                if valid_rows:
                    latex_text += f"\\textbf{{Quartil {q} - Top 10 Tópicos:}}\n\\begin{{itemize}}\n"
                    for words, freq in valid_rows:
                        latex_text += f"    \\item \\textit{{{words}}} (Frequência: {freq})\n"
                    latex_text += "\\end{itemize}\n\n"
                    
            with open(os.path.join(self.output_dir, file_name), "w", encoding="utf-8") as f:
                f.write(latex_text)

        # Export topics per cascade quartile (Q1–Q4)
        topics_per_cascade = topic_model.topics_per_class(docs, classes=classes_cascades)
        export_latex_topics(topics_per_cascade, classes_cascades, "Top10_Topics_Per_Quartile_LaTeX.txt")

        # Export topics per user quartile (UQ1–UQ4)
        classes_users = df_comments['User_Type'].astype(str).replace('nan', 'Unclassified').tolist()
        topics_per_user = topic_model.topics_per_class(docs, classes=classes_users)
        valid_user_classes = [c for c in set(classes_users) if c.startswith('UQ')]
        export_latex_topics(topics_per_user, valid_user_classes, "Top10_Topics_Per_User_Quartile_LaTeX.txt")

        print(f"[INFO] ✅ BERTopic completed in {(time.time() - start_bertopic)/60:.2f} total minutes.")
        print("\n[SYSTEM] SAVING OUTPUT FILES\n")

    def run_entropy_pipeline(self, df_comments):
        print("[SYSTEM] ENTROPY ANALYSIS (Normalized Thematic Diversity)")
        start_entropy = time.time()

        engine = AnalyticsEngine()
        engine.load_or_extract_data()
        
        spinner_ent = ConsoleSpinner("🧮 Calculating Shannon Entropy for Cascades and Users...")
        spinner_ent.start()
        
        df_valid = df_comments[df_comments['Topic'] != -1].copy()

        def min_max_scale(series):
            if series.max() > series.min():
                return (series - series.min()) / (series.max() - series.min())
            return series

        # Cascade entropy
        df_cascades_entropy = df_valid.groupby('Cascade_ID')['Topic'].agg(
            lambda x: entropy(x.value_counts(normalize=True).values, base=np.e)
        ).reset_index()
        df_cascades_entropy.rename(columns={'Topic': 'Thematic_Entropy'}, inplace=True)
        df_cascades_entropy['Thematic_Entropy'] = min_max_scale(df_cascades_entropy['Thematic_Entropy'])

        cascade_map = df_valid[['Cascade_ID', 'Quartile']].drop_duplicates()
        df_cascades_entropy = df_cascades_entropy.merge(cascade_map, on='Cascade_ID', how='inner')

        # User entropy (exclude system accounts)
        system_accounts = {'[deleted]', 'deleted', 'automoderator', 'redditcaresresources'}
        df_users_clean = df_valid[~df_valid['Author'].str.lower().isin(system_accounts)].copy()
        df_users_entropy = df_users_clean.groupby('Author')['Topic'].agg(
            lambda x: entropy(x.value_counts(normalize=True).values, base=np.e)
        ).reset_index()
        df_users_entropy.rename(columns={'Topic': 'Thematic_Entropy'}, inplace=True)
        df_users_entropy['Thematic_Entropy'] = min_max_scale(df_users_entropy['Thematic_Entropy'])

        user_quartile_map = df_users_clean[['Author', 'User_Type']].drop_duplicates()
        df_users_entropy = df_users_entropy.merge(user_quartile_map, on='Author', how='inner')
        
        spinner_ent.stop("✅ Entropy calculated.")

        Config.set_sns_theme()
        def _plot(df_ent, entity_type, group_col, groups_list):
            fig, ax = plt.subplots(figsize=(10, 7))
            for i, cat in enumerate(groups_list):
                data = df_ent[df_ent[group_col] == cat]['Thematic_Entropy'].dropna().values
                if len(data) == 0: continue
                mean_val, std_val = np.mean(data), np.std(data)

                sorted_data = np.sort(data)
                y = (1.0 - np.arange(len(sorted_data)) / len(sorted_data)) * 100
                ax.plot(sorted_data, y, color=engine.colors['COLOR_SCHEME'][i],
                        linestyle=['-', '--', '-.', ':'][i % 4], linewidth=3.5,
                        label=f"{cat} (μ={mean_val:.2f}, σ={std_val:.2f})")

            ax.set_xlabel(f'NORMALIZED SHANNON ENTROPY IN {entity_type.upper()}', fontsize=18, fontweight='bold')
            ax.set_ylabel('CCDF (%)', fontsize=18, fontweight='bold')
            ax.yaxis.set_major_formatter(mtick.PercentFormatter(decimals=0))
            ax.set_xlim(0, 1.05)
            ax.legend(fontsize=16, loc='upper right', framealpha=0.9, edgecolor='black')
            ax.tick_params(axis='both', which='major', labelsize=14)
            sns.despine()
            plt.tight_layout(pad=2.0)
            plt.savefig(os.path.join(self.output_dir, f"Fig_CCDF_Entropy_{entity_type}.pdf"), dpi=300)
            plt.close()

        _plot(df_cascades_entropy, 'Cascades', 'Quartile', ['Q1', 'Q2', 'Q3', 'Q4'])
        _plot(df_users_entropy, 'Users', 'User_Type', ['UQ1', 'UQ2', 'UQ3', 'UQ4'])

        # Statistical tests (Kruskal-Wallis and Kolmogorov-Smirnov)
        print("[INFO] ℹ️ Running Kruskal-Wallis and KS statistical tests...")
        with open(os.path.join(self.output_dir, "Entropy_Statistics.txt"), "w", encoding="utf-8") as f:
            f.write("=== SHANNON ENTROPY STATISTICAL TESTS ===\n\n")

            g_c = [df_cascades_entropy[df_cascades_entropy['Quartile'] == q]['Thematic_Entropy'].dropna().values for q in ['Q1', 'Q2', 'Q3', 'Q4']]
            if len(g_c) == 4 and all(len(g) > 0 for g in g_c):
                h, p = kruskal(*g_c)
                ks_s, ks_p = ks_2samp(g_c[0], g_c[3])
                f.write(f"[CASCADES] Kruskal-Wallis (Global Q1-Q4): H={h:.4f}, p={p:.4e}\n")
                f.write(f"[CASCADES] Kolmogorov-Smirnov (Q1 vs Q4): D={ks_s:.4f}, p={ks_p:.4e}\n\n")

            g_u = [df_users_entropy[df_users_entropy['User_Type'] == q]['Thematic_Entropy'].dropna().values for q in ['UQ1', 'UQ2', 'UQ3', 'UQ4']]
            if len(g_u) == 4 and all(len(g) > 0 for g in g_u):
                h_u, p_u = kruskal(*g_u)
                ks_s_u, ks_p_u = ks_2samp(g_u[0], g_u[3])
                f.write(f"[USERS] Kruskal-Wallis (Global UQ1-UQ4): H={h_u:.4f}, p={p_u:.4e}\n")
                f.write(f"[USERS] Kolmogorov-Smirnov (UQ1 vs UQ4): D={ks_s_u:.4f}, p={ks_p_u:.4e}\n")

        print(f"[INFO] ℹ️ Entropy analysis completed in {(time.time() - start_entropy):.2f}s.")
        print("\n[SYSTEM] SAVING OUTPUT FILES\n")

    def run_valence_analysis(self, df_comments):
        print("[SYSTEM] SEMANTIC VALENCE ANALYSIS (Top 10, no titles)")
        engine = AnalyticsEngine()
        engine.load_or_extract_data()
        c_q1, c_q2, c_q3, c_q4 = engine.colors['COLOR_SCHEME']

        spinner_val = ConsoleSpinner("⚖️ Processing Semantic Valence matrices...")
        spinner_val.start()

        df_valid = df_comments[df_comments['Body'].notna() & (df_comments['Body'] != '')].copy()
        df_valid['Cascade_Halves'] = df_valid['Quartile'].map({'Q1': 'Q1+Q2', 'Q2': 'Q1+Q2', 'Q3': 'Q3+Q4', 'Q4': 'Q3+Q4'})
        df_valid['User_Halves'] = df_valid['User_Type'].map({'UQ1': 'UQ1+UQ2', 'UQ2': 'UQ1+UQ2', 'UQ3': 'UQ3+UQ4', 'UQ4': 'UQ3+UQ4'})

        def compute_and_plot_valence(df, col_name, g1_label, g2_label, title_prefix, color_g1, color_g2):
            texts_g1 = df[df[col_name] == g1_label]['Body'].tolist()
            texts_g2 = df[df[col_name] == g2_label]['Body'].tolist()
            if not texts_g1 or not texts_g2: return

            # min_df=15 removes ultra-rare noise strings and typos
            vectorizer = CountVectorizer(stop_words=self.pt_stopwords, min_df=15)
            X = vectorizer.fit_transform(texts_g1 + texts_g2)
            vocab = vectorizer.get_feature_names_out()

            # Additional length filter: cut noise strings not separated by spaces
            valid_idx = [i for i, v in enumerate(vocab) if len(v) <= 16 and len(v) > 2]
            X = X[:, valid_idx]
            vocab = vocab[valid_idx]

            count_g1 = np.asarray(X[:len(texts_g1)].sum(axis=0)).flatten()
            count_g2 = np.asarray(X[len(texts_g1):].sum(axis=0)).flatten()

            freq_g1 = count_g1 / count_g1.sum() if count_g1.sum() > 0 else np.zeros_like(count_g1)
            freq_g2 = count_g2 / count_g2.sum() if count_g2.sum() > 0 else np.zeros_like(count_g2)

            denominator = freq_g1 + freq_g2
            valence = np.zeros_like(denominator)
            np.divide(freq_g2, denominator, out=valence, where=denominator > 0)
            valence = 2 * valence - 1

            df_val = pd.DataFrame({'Term': vocab, 'Valence': valence, 'Total_Count': count_g1 + count_g2})
            df_val_filtered = df_val[df_val['Total_Count'] >= np.percentile(df_val['Total_Count'], 85)]

            top_g1 = df_val_filtered.nsmallest(10, 'Valence').copy()
            top_g2 = df_val_filtered.nlargest(10, 'Valence').copy()
            df_plot = pd.concat([top_g1, top_g2]).sort_values(by='Valence')
            df_plot['Color'] = df_plot['Valence'].apply(lambda x: color_g1 if x < 0 else color_g2)

            Config.set_sns_theme()
            fig, ax = plt.subplots(figsize=(12, 10))
            bars = ax.barh(df_plot['Term'], df_plot['Valence'], color=df_plot['Color'], edgecolor='black', height=0.7)
            ax.axvline(0, color='black', linewidth=1.5)
            
            ax.tick_params(axis='y', pad=15)
            
            max_val = df_plot['Valence'].abs().max()
            ax.set_xlim(-max_val * 1.2, max_val * 1.2)
            
            ax.set_xlabel(f'SEMANTIC VALENCE\n(← {g1_label} Dominated | {g2_label} Dominated →)', fontsize=14, fontweight='bold')

            for bar in bars:
                w = bar.get_width()
                ax.text(w - 0.02 if w < 0 else w + 0.02, bar.get_y() + bar.get_height() / 2,
                        f'{w:.2f}', va='center', ha='right' if w < 0 else 'left', fontsize=12, fontweight='bold')

            sns.despine()
            safe_g1, safe_g2 = g1_label.replace('+', '_plus_'), g2_label.replace('+', '_plus_')
            plt.savefig(os.path.join(self.output_dir, f"Fig_Valence_{title_prefix}_{safe_g1}_vs_{safe_g2}.pdf"), dpi=300, bbox_inches='tight')
            plt.close()

            # LaTeX export of Top 10
            latex_table = f"""\\begin{{table}}[htbp]
    \\centering
    \\caption{{Termos com maior polaridade de Valência entre {g1_label} e {g2_label} ({title_prefix}).}}
    \\label{{tab:valence_{title_prefix.lower()}_{safe_g1}_{safe_g2}}}
    \\begin{{tabular}}{{lcc|lcc}}
        \\toprule
        \\multicolumn{{3}}{{c}}{{\\textbf{{Vocabulário {g1_label}}}}} & \\multicolumn{{3}}{{c}}{{\\textbf{{Vocabulário {g2_label}}}}} \\\\
        \\textbf{{Termo}} & \\textbf{{Valência}} & \\textbf{{Freq}} & \\textbf{{Termo}} & \\textbf{{Valência}} & \\textbf{{Freq}} \\\\
        \\midrule\n"""
            for i in range(10):
                row_g1 = top_g1.iloc[i]
                row_g2 = top_g2.iloc[9 - i]
                latex_table += f"        {row_g1['Term']} & {row_g1['Valence']:.2f} & {int(row_g1['Total_Count'])} & "
                latex_table += f"{row_g2['Term']} & {row_g2['Valence']:.2f} & {int(row_g2['Total_Count'])} \\\\\n"
            latex_table += """        \\bottomrule\n    \\end{tabular}\n\\end{table}\n"""
            with open(os.path.join(self.output_dir, f"Table_Valence_{title_prefix}_{safe_g1}_vs_{safe_g2}.txt"), "w", encoding="utf-8") as f:
                f.write(latex_table)

        compute_and_plot_valence(df_valid, 'Quartile', 'Q1', 'Q4', 'Cascades_Extremes', c_q1, c_q4)
        compute_and_plot_valence(df_valid, 'User_Type', 'UQ1', 'UQ4', 'Users_Extremes', c_q1, c_q4)
        compute_and_plot_valence(df_valid, 'Quartile', 'Q2', 'Q3', 'Cascades_Core', c_q2, c_q3)
        compute_and_plot_valence(df_valid, 'User_Type', 'UQ2', 'UQ3', 'Users_Core', c_q2, c_q3)
        compute_and_plot_valence(df_valid, 'Cascade_Halves', 'Q1+Q2', 'Q3+Q4', 'Cascades_All', c_q1, c_q4)
        compute_and_plot_valence(df_valid, 'User_Halves', 'UQ1+UQ2', 'UQ3+UQ4', 'Users_All', c_q1, c_q4)
        
        spinner_val.stop("✅ Valence matrices processed and exported.")
        print("\n[SYSTEM] SAVING OUTPUT FILES\n")

    def run_liwc_analysis(self, df_comments, dic_path="audit/Brazilian_Portuguese_LIWC2015_dictionary.dic"):
        import liwc # type: ignore
        import codecs
        print("[SYSTEM] LIWC ANALYSIS")
        if not os.path.exists(dic_path): return

        spinner_liwc = ConsoleSpinner("📚 Loading and sanitizing LIWC Dictionary...")
        spinner_liwc.start()

        temp_dic = os.path.join(self.output_dir, "temp_cleaned_liwc.dic")
        try:
            with codecs.open(dic_path, 'r', encoding='utf-8-sig', errors='ignore') as f:
                content = f.read()
            content = content.replace('\r\n', '\n').replace('\r', '\n').strip()
            if not content.startswith('%'): content = '%\n' + content
            with open(temp_dic, 'w', encoding='utf-8') as f: f.write(content)
        except Exception as e:
            spinner_liwc.stop(f"❌ Error while sanitizing dictionary: {e}")
            return

        parse, category_names = liwc.load_token_parser(temp_dic)
        if os.path.exists(temp_dic): os.remove(temp_dic)

        concept_map = {
            'anger': ['anger', 'ira', 'raiva', '53'],
            'swear': ['swear', 'palavr', 'palavrao', 'xingam', '121'],
            'power': ['power', 'poder', '83'],
            'risk': ['risk', 'risco', '85'],
            'certain': ['certain', 'certeza', 'cert', '55'],
            'netspeak': ['netspeak', 'net', 'redes', 'informal', '124']
        }

        target_concepts = list(concept_map.keys())
        actual_categories = {}
        for concept in target_concepts:
            for real_cat in category_names:
                if real_cat.lower().startswith(concept):
                    actual_categories[concept] = real_cat
                    break

        spinner_liwc.stop("✅ LIWC Dictionary loaded.")

        engine = AnalyticsEngine()
        engine.load_or_extract_data()
        c_q1, c_q2, c_q3, c_q4 = engine.colors['COLOR_SCHEME']

        # ================= LIWC BASE METHOD =================
        def process_and_plot_liwc(df_text, id_col, group_col, group_labels, filename_prefix):
            liwc_results = []
            total_rows = len(df_text)
            for row_num, (_, row) in enumerate(df_text.iterrows()):
                if row_num % 1000 == 0:
                    print(f"\r[*] Processing row {row_num}/{total_rows}...    ", end="", flush=True)
                try:
                    body = row['Body']
                    if not isinstance(body, str) or not body.strip():
                        tokens = []
                    else:
                        tokens = re.findall(r'\b\w+\b', body.lower())
                    total_words = len(tokens)
                    counts = {concept: 0 for concept in target_concepts}
                    if total_words > 0:
                        for token in tokens:
                            for cat in parse(token):
                                for concept, real_cat in actual_categories.items():
                                    if cat == real_cat: counts[concept] += 1
                        perc_counts = {c: (v / total_words) * 100 for c, v in counts.items()}
                    else:
                        perc_counts = {c: 0.0 for c in target_concepts}
                    perc_counts[id_col] = row[id_col]
                    liwc_results.append(perc_counts)
                except Exception as e:
                    print(f"[WARN] Row {row_num} ignored: {e}", flush=True)
                    continue
            print(f"\n[*] LIWC loop finished. {len(liwc_results)}/{total_rows} processed rows.", flush=True)

            df_final = df_text.merge(pd.DataFrame(liwc_results), on=id_col)

            Config.set_sns_theme()
            palette_dict = dict(zip(group_labels, [c_q1, c_q2, c_q3, c_q4]))

            cols = 3
            rows = (len(target_concepts) + cols - 1) // cols
            fig, axes = plt.subplots(rows, cols, figsize=(18, 5 * rows))
            axes = axes.flatten()

            for i, cat in enumerate(target_concepts):
                sns.barplot(data=df_final, x=group_col, y=cat, order=group_labels, palette=palette_dict,
                            errorbar='se', capsize=.1, ax=axes[i], edgecolor='black', linewidth=1.2)
                axes[i].set_title(cat.upper(), fontsize=16, fontweight='bold', pad=12)
                axes[i].set_ylabel('Mean Frequency (%)', fontsize=14, fontweight='bold')
                axes[i].set_xlabel('Negative Sentiment Quartile', fontsize=14, fontweight='bold')
                axes[i].tick_params(axis='both', which='major', labelsize=14)
            for j in range(i + 1, len(axes)): fig.delaxes(axes[j])
            plt.tight_layout(pad=3.0)
            plt.savefig(os.path.join(self.output_dir, f"Fig_LIWC_Categories_Grid_{filename_prefix}.pdf"), dpi=300)
            plt.close()

            stats_results = []
            for cat in target_concepts:
                if df_final[cat].nunique() > 1 and df_final['Perc_Negative'].nunique() > 1:
                    corr, p_val = spearmanr(df_final['Perc_Negative'], df_final[cat])
                else:
                    corr, p_val = float('nan'), float('nan')
                stats_results.append({'LIWC_Category': cat.upper(), 'Spearman_Rho': corr, 'P_Value': p_val})

            df_stats = pd.DataFrame(stats_results).sort_values(by='Spearman_Rho', ascending=False, na_position='last')

            latex_table = f"\\begin{{table}}[htbp]\n    \\centering\n    \\caption{{Spearman Correlation between Negativity and LIWC ({filename_prefix}).}}\n    \\begin{{tabular}}{{lcc}}\n        \\toprule\n        \\textbf{{LIWC Category}} & \\textbf{{$\\rho$ (Spearman)}} & \\textbf{{$p$-value}} \\\\\n        \\midrule\n"
            for _, row in df_stats.iterrows():
                rho_str = "N/A" if pd.isna(row['Spearman_Rho']) else f"{row['Spearman_Rho']:.4f}"
                p_str = "N/A" if pd.isna(row['P_Value']) else ("< 0.001" if row['P_Value'] < 0.001 else f"{row['P_Value']:.4f}")
                latex_table += f"        {row['LIWC_Category']} & {rho_str} & {p_str} \\\\\n"
            latex_table += "        \\bottomrule\n    \\end{tabular}\n\\end{table}"
            with open(os.path.join(self.output_dir, f"Table_LIWC_Correlations_{filename_prefix}.txt"), "w", encoding="utf-8") as f:
                f.write(latex_table)
            print("\n[SYSTEM] SAVING OUTPUT FILES\n")

        # 1. LIWC for CASCADES
        df_cascades_text = df_comments.groupby('Cascade_ID').agg(
            {'Body': lambda x: ' '.join(v for v in x if isinstance(v, str)), 'Quartile': 'first'}
        ).reset_index()
        df_cascades_text = df_cascades_text.merge(engine.df_cascades[['Cascade_ID', 'Perc_Negative']], on='Cascade_ID', how='inner')
        process_and_plot_liwc(df_cascades_text, 'Cascade_ID', 'Quartile', ['Q1', 'Q2', 'Q3', 'Q4'], 'Cascades')

        gc.collect()

        # 2. LIWC for USERS
        df_users_text = df_comments.dropna(subset=['User_Type']).groupby('Author').agg(
            {'Body': lambda x: ' '.join(v for v in x if isinstance(v, str)), 'User_Type': 'first'}
        ).reset_index()
        user_neg_map = {auth: (counts['negative'] / counts['total']) * 100 for auth, counts in engine.user_sentiments.items() if counts['total'] > 0}
        df_users_text['Perc_Negative'] = df_users_text['Author'].map(user_neg_map)
        process_and_plot_liwc(df_users_text, 'Author', 'User_Type', ['UQ1', 'UQ2', 'UQ3', 'UQ4'], 'Users')

        gc.collect()
        plt.close('all')
        print("[SYSTEM] COMPLETED")


if __name__ == "__main__":
    import sys
    action = sys.argv[1] if len(sys.argv) > 1 else "full"

    nlp = NLPEngine()

    if action == "liwc":
        df = nlp.get_cached_or_infer()
        nlp.run_liwc_analysis(df, dic_path="/app/audit/Brazilian_Portuguese_LIWC2015_dictionary.dic")

    elif action == "full":
        df = nlp.get_cached_or_infer()
        nlp.run_wordclouds(df)
        nlp.run_entropy_pipeline(df)
        nlp.run_valence_analysis(df)
        nlp.run_liwc_analysis(df, dic_path="/app/audit/Brazilian_Portuguese_LIWC2015_dictionary.dic")

    elif action == "bertopic":
        df = nlp.get_cached_or_infer()
        nlp.run_bertopic_analysis(df)