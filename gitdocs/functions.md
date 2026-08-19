# Rakeddit — Functions Reference

This document provides a comprehensive reference for all core functions, classes, and scripts available in the Rakeddit ecosystem. Functions are organized by their primary module or responsibility.

---

## Configuration

### config.ini (Root Directory)

```ini
[HEADERS]
User-Agent = Research_Gentle_Harvesting_With_Jitter (contact: [YOUR EMAIL])

[PATHS]
BASE_PATH = ./DATA/1-json_dumps/
AGGREGATES_PATH = ./DATA/2-aggregates/
MEDIA_PATH = ./DATA/3-temp_media/
MULTIMODAL_PATH = ./DATA/3-vision_processing/
INFERRED_PATH = ./DATA/4-inferred/
LOGGING_PATH = ./logging/

[MODELS]
MAIN_INFER = cardiffnlp/twitter-xlm-roberta-base-sentiment
IMAGE_READER = qwen3-vl:2b-instruct
```

### ConfigLoader (modules/config_loader.py)

| Function | Description |
|----------|-------------|
| `config.get(section, key, fallback=None)` | Returns a configuration value as a string. |
| `config.get_int(section, key, fallback=0)` | Returns a configuration value as an integer. |
| `config.get_float(section, key, fallback=0.0)` | Returns a configuration value as a float. |
| `config.get_boolean(section, key, fallback=False)` | Returns a configuration value as a boolean. |
| `config.get_path(section, key, fallback=None)` | Returns an absolute path from a configuration value. |
| `prevent_sleep_windows(enable=True)` | Enables or disables Windows system sleep prevention. |

---

## Core Utilities (audit/core/Utilities.py)

### Config Class

| Attribute | Description |
|-----------|-------------|
| `Config.MULTIMODAL_PATH` | Path to the inferred multimodal dataset. |
| `Config.CACHE_PATH` | Path to the cascades DataFrame cache (Parquet). |
| `Config.BLIND_PATH` | Path to the blind (text-only) dataset. |
| `Config.RESULTS_DIR` | Output directory for unified analytics results. |
| `Config.CATEGORIES` | List of four thematic categories. |
| `Config.CATEGORY_MAP` | Dictionary mapping subreddit names to categories. |
| `Config.TRIAD_MAPPING` | Mapping of sentiment triples to triad labels. |
| `Config.ORDERED_TRIADS` | Ordered list of triad labels for heatmaps. |

| Method | Description |
|--------|-------------|
| `Config.get_colors()` | Returns color palettes for visualizations. |
| `Config.setup_directories()` | Creates necessary output directories. |
| `Config.set_sns_theme()` | Applies the seaborn theme for plots. |

---

## Main Pipeline (modules/)

### Orchestration (main.py)

| Class / Function | Description |
|------------------|-------------|
| `RakedditDatabaseBuilder(subreddits, limit=100, category="top", timeframe="all")` | Main orchestrator for the data collection pipeline. |
| `builder.run()` | Executes the full pipeline (harvest → flatten → enrich). |
| `builder.resume_visual(normalized_filepath, multimodal_filepath)` | Resumes vision enrichment from a partially processed file. |

---

### Harvesting (modules/json_harvester.py)

| Function | Description |
|----------|-------------|
| `get_json(url, max_retries=5)` | Fetches and parses a Reddit `.json` endpoint response. |
| `save_post(data, base_path=BASE_PATH)` | Saves a post's JSON data to the subreddit's folder. |
| `harvest_subreddit(subreddit_name, limit, category, timeframe)` | Harvests posts from a specific subreddit with jitter and rate limiting. |
| `downloader_function(url)` | Downloads media (images, GIFs) from a URL with passive caching. |

---

### Normalization & Structuring (modules/processor.py)

| Function | Description |
|----------|-------------|
| `extract_from_post(folder_path, limit="none", aggregates_dir=AGGREGATES)` | Flattens comment trees using DFS; injects depth, validity flags, and metadata footer. |
| `get_processed_count()` | Returns the number of records processed in the last extraction. |
| `write_metadata_footer(jsonl_path)` | Recalculates and appends a metadata footer to a JSONL dataset. |

---

### Multimodal Enrichment (modules/processor.py + ai_manager.py)

| Function | Description |
|----------|-------------|
| `process_media(jsonl_filepath)` | Enriches comments with visual context using AI vision models. |
| `process_visual_content(body_text)` | Flattens Reddit markdown, downloads media, and evokes vision AI. |
| `call_vision_ai(image_path, extension, model_name=IMAGE_READER)` | Sends an image to the vision model and returns a description. |
| `media_get_processed_count()` | Returns the total number of processed media items. |
| `media_get_media_count()` | Returns the number of successfully enriched media items. |
| `get_vision_telemetry()` | Returns AI call statistics (calls, total time, average time). |
| `process_youtube_links(body_text)` | Replaces YouTube links with video titles. |
| `apply_youtube_cleanup_only(input_path, output_path)` | Retroactive YouTube link cleanup on an existing dataset. |
| `apply_native_image_cleanup(input_path, output_path)` | Retroactively processes missed `![img]()` and `![gif]()` tags. |

---

### Sentiment Inference (modules/infer_engine.py)

| Function | Description |
|----------|-------------|
| `analyze_batch_sentiment(texts, batch_size=64)` | Runs XLM-RoBERTa sentiment analysis on a batch of texts (GPU/CPU). |
| `orchestrate_full_inference(jsonl_filepath)` | Orchestrates batched sentiment inference with resume support. |

---

## Audit Suite (audit/)

### Core Engines (audit/core/)

#### AnalyticsEngine (audit/core/Methods.py)

| Method | Description |
|--------|-------------|
| `engine.load_or_extract_data()` | Loads from cache or performs full extraction. |
| `engine.plot_structural_ccdfs(grouping="Categories", interactive_only=False)` | Generates CCDFs for structural metrics (RQ1). |
| `engine.run_motif_analysis(grouping="Categories", interactive_only=False)` | Generates interaction motif heatmap (Figure 2). |
| `engine.run_figure3_average_score(grouping="Categories", interactive_only=False)` | Generates Average Score CCDF (Figure 3). |
| `engine.run_statistical_reports(grouping="Categories", interactive_only=False)` | Generates comprehensive statistical reports. |
| `engine.run_taxonomy_analysis(grouping="Categories", interactive_only=False)` | Generates BCC taxonomy scatter plot. |
| `engine.run_triadic_analysis(grouping="Categories", interactive_only=False)` | Generates triadic sentiment heatmap (RQ2). |
| `engine.run_rq3_analysis(grouping="Categories", interactive_only=False)` | Generates taxonomy trendline plots. |
| `engine.run_user_homophily_analysis(grouping="Categories", interactive_only=False)` | Generates homophily barplot and reply heatmap (RQ2). |
| `engine.run_ablation_matrix_analysis()` | Compares multimodal vs. blind inference. |
| `engine.generate_cascade_diagram()` | Generates an example cascade tree diagram. |

#### NLPEngine (audit/core/Analytical_NLP_Engine.py)

| Method | Description |
|--------|-------------|
| `nlp.load_and_map_texts()` | Loads texts and maps them to cascades and quartiles. |
| `nlp.get_cached_or_infer()` | Loads NLP cache or runs full BERTopic inference. |
| `nlp.run_bertopic_analysis(df_comments)` | Runs BERTopic topic modeling (GPU/CPU fallback). |
| `nlp.run_wordclouds(texts_by_quartile)` | Generates wordclouds per negativity quartile. |
| `nlp.run_entropy_pipeline(df_comments)` | Computes normalized Shannon entropy for cascades and users. |
| `nlp.run_valence_analysis(df_comments)` | Computes semantic valence (Tornado/Butterfly charts). |
| `nlp.run_liwc_analysis(df_comments, dic_path=None)` | Runs LIWC psycholinguistic analysis (6 categories). |

---

### Human Validation (audit/scripts/human_validation/)

#### Sampling & Annotation (sample_comments.py)

| Function | Description |
|----------|-------------|
| `load_comments(input_path, sample_size=100, seed=42)` | Loads valid comments and draws a random sample. |
| `annotate_comments(sampled_comments, output_labeled)` | Interactive CLI for manual annotation (POS/NEG/NEU). |
| `generate_report(sampled_comments, labeled, output_file)` | Generates confusion matrix and confidence statistics. |
| `save_progress(all_comments, output_file, labeled_dict)` | Saves annotation progress to JSONL. |

#### Label Comparison (compare_labels.py)

| Function | Description |
|----------|-------------|
| `load_annotated(input_file, human_file)` | Aligns AI and human labels by comment ID. |
| `compute_metrics(y_true, y_pred)` | Computes accuracy, precision, recall, and F1 per class. |
| `print_report(data, title)` | Prints detailed validation report by quartile. |

#### Anonymization (anonymize_dataset.py)

| Function | Description |
|----------|-------------|
| `generate_hash(username, salt)` | Generates a deterministic SHA-256 hash for a username. |
| `anonymize_dataset(input_path, output_path)` | Replaces usernames with irreversible SHA-256 hashes using a random salt. |

#### Anonymization Validation (validate_anonymization.py)

| Function | Description |
|----------|-------------|
| `validate_files(original_path, anonymized_path)` | Validates that anonymization preserved structural integrity. |

#### Structural Utilities

| Script | Description |
|--------|-------------|
| `check_cascade_stats.py` | Computes descriptive statistics per cascade quartile (Q1–Q4). |
| `check_structure_by_user_quartile.py` | Computes statistics per dominant user quartile (UQ1–UQ4). |

---

### Churn & Survival Analysis (audit/scripts/)

#### Churn Analysis (churn.py)

| Function | Description |
|----------|-------------|
| `build_churn_dataset()` | Builds user lifetime dataset with churn events (30-day inactivity). |
| `run_churn_analysis(df)` | Generates Kaplan-Meier curves and fits Cox proportional hazards model. |

#### Survival Analysis (survival.py)

| Function | Description |
|----------|-------------|
| `build_survival_dataset()` | Builds per-cascade survival dataset (conflict fatigue). |
| `run_survival_analysis(df)` | Generates Kaplan-Meier curves by exposure and fits Cox model. |

---

### Graphical Interfaces (audit/gui/)

#### Network & Statistics Panel (GUI.py)

| Class | Description |
|-------|-------------|
| `AppGUI(root)` | Tkinter interface for structural analysis, motif extraction, and statistical reporting. |
| `AppGUI.switch_to_nlp()` | Switches to the NLP & GPU panel. |

#### NLP & GPU Panel (GUI_NLP.py)

| Class | Description |
|-------|-------------|
| `NLPGUI(root)` | Tkinter interface for NLP analysis with Docker/GPU orchestration. |
| `NLPGUI.run_selected_task(task)` | Runs a specific NLP task (BERTopic, LIWC, etc.). |
| `NLPGUI._ensure_container_is_ready()` | Manages Docker container lifecycle (create/start/pause). |

---

### Console Spinner (audit/core/Analytical_NLP_Engine.py)

| Class | Description |
|-------|-------------|
| `ConsoleSpinner(message="Aguarde")` | Animated spinner for visual feedback during long operations. |
| `spinner.start()` | Starts the spinner animation in a background thread. |
| `spinner.stop(success_message="Concluído!")` | Stops the spinner and displays a success message. |

---

## Common Parameters

| Parameter | Values | Description |
|-----------|--------|-------------|
| `grouping` | `"Categories"`, `"Quartiles"`, `"Sentiments"` | Grouping strategy for analyses. |
| `interactive_only` | `True`, `False` | If `True`, filters only cascades with `Total_Motifs > 0`. |
| `dic_path` | File path or `None` | Path to the LIWC dictionary (auto-detected). |

---

## Output Directories

| Directory | Contents |
|-----------|----------|
| `audit/results/unified_analytics/` | Structural analysis outputs (CCDFs, motifs, reports). |
| `audit/results/unified_analytics/NLP_Analysis/` | NLP outputs (BERTopic, LIWC, wordclouds, entropy). |
| `audit/results/unified_analytics/Homophily_Analysis/` | Homophily barplots and heatmaps. |
| `DATA/4-inferred/` | Inferred datasets with sentiment labels. |
| `DATA/3-vision_processing/` | Multimodal datasets with visual enrichments. |