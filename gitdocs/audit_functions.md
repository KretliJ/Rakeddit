# Audit Functions

## Core Engine

### AnalyticsEngine (Methods.py)

#### Initialization and Data Loading

``python engine = AnalyticsEngine() ``
Initializes the analysis engine with color configurations and empty data structures.

``python engine.load_or_extract_data() ``
Loads data from cache (Parquet + JSONs) or performs full extraction if cache does not exist. Returns `True` on success.

``python engine.extract_and_compute_all() ``
Performs a single-pass extraction of the dataset, builds cascades, computes structural metrics (virality, depth, breadth, motifs), and saves to cache.

---

#### Data Preparation

``python engine._prepare_quartiles(interactive_only=False) ``
Prepares DataFrame with negativity quartiles (Q1–Q4) based on `Perc_Negative`. If `interactive_only=True`, filters only cascades with motifs (`Total_Motifs > 0`).

``python engine._get_grouping_config(grouping, df, interactive_only=False) ``
Returns grouping configuration: `group_col`, `groups_list`, `colors`, `output_dir`. Supports `'Categories'`, `'Quartiles'`, `'Sentiments'`.

---

#### Structural Analyses (RQ1)

``python engine.plot_structural_ccdfs(grouping="Categories", interactive_only=False) ``
Generates CCDF figures for: Structural Virality, Max Depth, Max Breadth, Cascade Size, Duration Hours, Longest Negative Run Ratio.

``python engine.run_motif_analysis(grouping="Categories", interactive_only=False) ``
Generates interaction motif heatmap (Dyads, Chains, Fan-In/Out, Triangles) with Kruskal-Wallis tests.

``python engine.run_figure3_average_score(grouping="Categories", interactive_only=False) ``
Generates CCDF of Average Score (upvotes - downvotes) by quartile.

``python engine.run_statistical_reports(grouping="Categories", interactive_only=False) ``
Generates comprehensive statistical report with Kruskal-Wallis, KS tests, Cliff's Delta, and Spearman correlations between negativity and structure.

``python engine.run_taxonomy_analysis(grouping="Categories", interactive_only=False) ``
Generates BCC taxonomy scatter plot with linear regression between virality and toxicity by subreddit.

``python engine.run_triadic_analysis(grouping="Categories", interactive_only=False) ``
Generates triad heatmap (persistence, convergence, shift, oscillation) by category.

``python engine.run_rq3_analysis(grouping="Categories", interactive_only=False) ``
Generates trend plots (virality vs negativity) by dominant sentiment (POS/NEU/NEG).

---

#### Interaction Analysis (RQ2)

``python engine.run_user_homophily_analysis(grouping="Categories", interactive_only=False) ``
Generates:

- Homophily barplot (H_i) by user quartile (UQ1–UQ4)
- Reply proportion heatmap between quartiles
- Statistics report (Kruskal-Wallis, KS tests)

---

#### Utilities

``python engine.get_dataset_overview_string(interactive_only=False) ``
Returns a string with dataset overview: sentiments, cascade quartiles, user quartiles.

``python engine.generate_cascade_diagram() ``
Generates an example cascade diagram (n-ary tree).

``python engine.run_ablation_matrix_analysis() ``
Generates confusion matrix comparing multimodal vs. blind (text-only) inference, exports to PDF and LaTeX.

---

## NLP Engine (Analytical_NLP_Engine.py)

### Initialization

``python nlp = NLPEngine(sample_size=None) ``
Initializes NLP engine with custom stopwords (PT+EN+ES+Reddit noise) and creates master files for statistics and LaTeX.

---

### Data Loading

``python nlp.load_and_map_texts() ``
Loads texts from multimodal dataset, resolves cascade trees, allocates quartiles, and injects `User_Type` for homophily.

``python nlp.get_cached_or_infer() ``
Loads NLP cache (`nlp_dataframe_cache.parquet`) if exists, or runs full inference (BERTopic) and saves cache.

---

### Topic Analysis (BERTopic)

``python nlp.run_bertopic_analysis(df_comments) ``
Runs BERTopic with:

- UMAP (GPU via cuML, CPU fallback via umap-learn)
- Sentence-BERT (paraphrase-multilingual-MiniLM-L12-v2)
- CountVectorizer with custom stopwords
- Exports topics to CSV and LaTeX (by cascade and user quartile)

---

### WordClouds

``python nlp.run_wordclouds(texts_by_quartile) ``
Generates wordclouds per quartile with specific colors (Q1=purple, Q2=magenta, Q3=orange, Q4=yellow).

---

### Thematic Entropy

``python nlp.run_entropy_pipeline(df_comments) ``
Computes Shannon entropy (normalized) for cascades and users. Generates CCDFs and runs statistical tests (Kruskal-Wallis, KS).

---

### Semantic Valence Analysis

``python nlp.run_valence_analysis(df_comments) ``
Computes semantic valence (freq_g2 / (freq_g1 + freq_g2) * 2 - 1) for quartile pairs (Q1 vs Q4, Q2 vs Q3, Q1+Q2 vs Q3+Q4). Generates Tornado/Butterfly charts and exports LaTeX.

---

### LIWC (Linguistic Inquiry and Word Count)

``python nlp.run_liwc_analysis(df_comments, dic_path=None) ``

- Loads LIWC dictionary (searches in `audit/resources/`)
- Processes 6 categories: anger, swear, power, risk, certain, netspeak
- Generates barplots and exports Spearman correlations + Kruskal-Wallis/KS tests to LaTeX

---

## Console Spinner

```python
spinner = ConsoleSpinner(message="Aguarde")
spinner.start()

# ... long operation ...

spinner.stop(success_message="Concluído!")
```

Animated spinner for visual feedback during long operations.

---

## Human Validation

### Comment Sampling

```python load_comments(input_path, sample_size=100, seed=42) # sample_comments.py ```

Loads valid comments and draws a random sample.

``python annotate_comments(sampled_comments, output_labeled) ``
Interactive interface for manual annotation (POS/NEG/NEU) with navigation (prev/next/save/quit).

``python generate_report(sampled_comments, labeled, output_file) ``
Generates confusion matrix, per-class metrics, and classifier confidence means.

---

### Label Comparison

```python load_annotated(input_file, human_file) # compare_labels.py ```

Loads and aligns AI and human labels by ID.

```python compute_metrics(y_true, y_pred) ``
Computes accuracy, precision, recall, and F1 per class with confusion matrix.

```python print_report(data, title) ```
Prints detailed report per quartile and overall.

---

### Dataset Anonymization

```python generate_hash(username, salt) # anonymize_dataset.py ```

Generates deterministic SHA-256 hash for username + salt.

```python anonymize_dataset(input_path, output_path) ```

- Collects all unique usernames
- Generates hashes with random salt (not saved)
- Replaces usernames in dataset
- **Does not save mapping** — reversal is impossible

---

### Anonymization Validation

```python validate_files(original_path, anonymized_path) ```

Verifies:

- Line count
- Field preservation
- Replacement consistency
- Count by type (`comment`, `post_header`, `metadata_footer`)
- Non-sensitive field comparison

---

## Churn and Survival Analysis

### Churn (User Lifetime)

```python build_churn_dataset() ```

Builds churn dataset with:

- `duration_days`: active time in community
- `event`: 1 if user churned (>30 days since last post)
- `neg_ratio`: proportion of negative comments

```python run_churn_analysis(df) ```
Generates Kaplan-Meier curves by user quartile and fits Cox proportional hazards model.

---

### Survival (Conflict Fatigue)

```python build_survival_dataset() ```

Builds survival dataset per cascade with:

- `duration`: user's stay duration in the cascade
- `event`: 1 if user abandoned the discussion
- `exposure_ratio`: proportion of negativity during stay

```python run_survival_analysis(df) ```
Generates Kaplan-Meier curves (high vs low exposure) and fits Cox proportional hazards model.

---

## Structural Utilities

### Cascade Statistics by Quartile

```python check_cascade_stats.py ```

Computes and displays descriptive statistics (mean, median, standard deviation) for `Cascade_Size`, `Structural_Virality`, `Max_Depth`, `Max_Breadth` by Q1–Q4 quartiles.

### Cascade Statistics by User Quartile

```python check_structure_by_user_quartile.py ```

Groups cascades by dominant user quartile (UQ1–UQ4) and computes structural statistics.

---

## GUI

### Network & Statistics Panel

```python AppGUI(root) # GUI.py ```

Interface for:

- Load/extract data
- Run individual analyses or full pipeline
- Cache management (nuke)
- Switch to NLP panel

### NLP & GPU Panel

```python NLPGUI(root) # GUI_NLP.py ```

Interface for:

- Run full NLP pipeline via Docker
- Run specific tasks: BERTopic, Cascade Extraction, LIWC
- Docker container management (start/stop/pause)
- Nuke NLP cache

---

## Configuration

### Utilities.py

```python
Config.MULTIMODAL_PATH 
Config.CACHE_PATH 
Config.BLIND_PATH 
Config.RESULTS_DIR 
Config.CATEGORIES 
Config.CATEGORY_MAP 
Config.TRIAD_MAPPING 
Config.ORDERED_TRIADS 
Config.get_colors() 
Config.setup_directories() 
Config.set_sns_theme() 
```

---

**Note:** All analysis functions (structural, homophily, motifs, taxonomy, triads) support `grouping` (`"Categories"`, `"Quartiles"`, `"Sentiments"`) and `interactive_only` (`True`/`False`) parameters to filter for interactive cascades only (with motifs > 0).
