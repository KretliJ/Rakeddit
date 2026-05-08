# Configs

### config.ini

    [HEADERS]
    User-Agent = Research_Gentle_Harvesting_With_Jitter (contact: [YOUR EMAIL])

    [PATHS]
    BASE_PATH = ./DATA/1-json_dumps/
    AGGREGATES_PATH = ./DATA/2-aggregates/
    MEDIA_PATH = ./DATA/temp_media/
    MULTIMODAL_PATH = ./DATA/3-vision_processing/
    LOGGING_PATH = ./logging/

    [MODELS]
    MAIN_INFER = llama3:8b-instruct-q8_0
    IMAGE_READER = qwen2.5-vl:3b

### Enables or disables suspension prevention for Windows systems

    prevent_sleep_windows(enable=True)
    
    # where: config_loader.py

# Orchestration (main.py)

### Manages the entire Rakeddit pipeline with traceability and telemetry

    orchestrator = RakedditOrchestrator(subreddits, limit_per_sub, category, timeframe)

    # where: main.py

### Runs the full pipeline from Stage 1 to 4
    
    orchestrator.run()

### Resumes the pipeline from a previously generated checkpoint file
    
    # filepath: Path to a MULTIMODAL_ or FILTERED_ .jsonl file
    orchestrator.resume_pipeline(filepath)

# Harvesting

### Lowest level auxiliary
    
    # Most basic element, extracts a JSON reddit endpoint response
    # url: string with request URL
    get_json(url)
    
    # where: json_harvester.py

### Second level auxiliary
    
    # Saves post json in ./json_dumps/subreddit_name
    # data: data in json
    save_post(data)

    # where: json_harvester.py

### Harvests from chosen subreddit
    
    # subreddit_name: string with subreddit name (e.g. "anime")
    # category: string with category to search into (e.g. "top")
    # limit: maximum of posts to rake down the chain
    # timeframe: "today","month", "all"
    harvest_subreddit(subreddit_name, limit, category, timeframe)

    # where: json_harvester.py

### Downloads pictures and gifs
    
    # url: Source Reddit URL 
    downloader_function(url)

    # where: json_harvester.py

# Normalization & Graph Structuring

### Flattens a comment tree using Depth-First Search and injects Graph variables

    # folder_path: Base path for subreddit folder
    # limit: "none" to read all, or string with subreddit name
    # Note: Adds 'depth', 'is_valid_text' flags, and a 'metadata_footer' for telemetry.
    extract_from_post(folder_path, limit="none")

    # where: processor.py

### Retrieves extraction telemetry counts

    get_processed_count()
    
    # where: processor.py

# Computer Vision (Multimodal)

### Enriches the comment body with description of attached image  
    
    # jsonl_filepath: Base path for normalized (flattened) comments
    process_media(jsonl_filepath)
    
    # where: processor.py

### Flattens reddit proprietary formatting, downloads media and evokes AI

    # body_text: '"body":' of a comment to be analyzed
    process_visual_content(body_text)

    # where: processor.py

### Uses AI defined in IMAGE_READER as a media interceptor

    # image_path: Media path in temp_media
    # extension: File type
    # model_name: Defined by IMAGE_READER by default
    call_vision_ai(image_path, extension, model_name=IMAGE_READER)

    # where: ai_manager.py

### Retrieves multimodal telemetry counts

    media_get_processed_count()
    media_get_media_count()

    # where: processor.py

# Structural Analytics

### Reads the dataset and calculates structural metrics per subreddit

    # jsonl_path: Path from multimodal analysis
    generate_cascade_stats(jsonl_path)
    
    # where: analytics.py

### Generates comparative plots of Average Breadth vs Depth

    # breadth_stats: DataFrame returned by generate_cascade_stats
    plot_structural_signature(breadth_stats)
    
    # where: analytics.py

# Cascade Inference (BERT Filter)

### High-speed syntactic filter to prune non-toxic comments and save LLM inference time

    # jsonl_filepath: Path from multimodal analysis
    # threshold: Confidence score minimum to flag for LLM
    # batch_size: Number of parallel sequences for GPU optimization
    apply_bert_filter(jsonl_filepath, threshold=0.15, batch_size=32)

    # where: bert_filter.py

# LLM Inference & Toxicity

### Climbs tree from target_id using parent_id to assemble context

    # target_id: target comment ID for tree rooting
    # data_dict: normalized JSONL dictionary { 'id': { ... } }
    # post_data: dictionary with original post data
    build_context_chain(target_id, data_dict, post_data)

    # where: ai_manager.py

### Structures prompts to be executed (Digital Forensic Auditor)

    # context_chain: Contextual list with chain of comments
    # target_comment_author: ID of what comment this is targeting
    # target_comment_body: body of what comment this is targeting
    # post_title: Original post title
    # post_content: Original post body text
    prompt_maker(context_chain, target_comment_author, target_comment_body, post_title, post_content)

    # where: ai_manager.py

### Calculates normalized toxicity index and dosimetry based on Penalty Law vectors.

    # ai_response_dict: Dictionary containing LLM json response
    calculate_toxicity(ai_response_dict)

    # where: ai_manager.py

    # 'f1': 'Profanity (explicit swear words, obscenities, or profane slurs)'
    # 'f2': 'Threats (actionable promise to inflict physical or unjust damage)'
    # 'f3': 'Insult/Ad Hominem (STRICTLY for direct attacks on an interlocutor's honor/character)'
    # 'f4': 'Identity Hate (dehumanizing or segregating protected groups)'
    # 'f5': 'Perturbation (functionally obstructive speech, spam, pure rage-bait, harassment)'
    # 'aggro': 'Dosimetry multiplier (0 to 3 scale measuring intensity and intent)'

### Finds raw JSON in BASE_PATH and extracts post title and body

    # subreddit: String with subreddit name
    # post_id: post id related to the desided comment
    get_original_post_content(subreddit, post_id)

    # where: infer_engine.py

### Mock function for pipeline testing without inference
    
    # prompt: Structured prompt as seen from prompt_maker()
    mock_local_ai(prompt)

    # where: infer_engine.py

### Sends prompt to local Ollama API and ensures return of Python dictionary

    # prompt Structured prompt as seen from prompt_maker()
    # model_name: Defined by MAIN_INFER by default
    run_ai(prompt, model_name=MAIN_INFER)

    # where: infer_engine.py

### Orchestrates inference for consolidated datasets

    # jsonl_filepath: Path from BERT filter analysis
    orchestrate_full_inference(jsonl_filepath)

    # where: infer_engine.py

### Reads dataset, reads post catalogue and propagates context to children

    # jsonl_filepath: Path from BERT filter analysis
    # post_catalog: Dictionary with (subreddit, post_id)
    run_inference_pipeline(jsonl_filepath, post_catalog)

    # where: infer_engine.py

# Audition

### Ingests dataset, looks for AI hallucinations and returns both clean and dirty datasets
    
    # input_path: Relative path for input dataset
    # clean_path: Relative path for clean output dataset
    # dirty_path: Relative path for dirty output dataset
    sanitize_dataset(input_path, clean_path, dirty_path)

    # where: hallucination_sweep.py