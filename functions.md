# Configs

### config.ini

    [HEADERS]
    User-Agent = Research_Gentle_Harvesting_With_Jitter (contact: [YOUR EMAIL])
    # CHANGE THIS TO REFLECT YOUR USE CASE

    [PATHS]
    # CHANGE BASE_PATH IF YOU WANT TO DUMP FILES ELSEWHERE
    BASE_PATH = ./DATA/json_dumps/
    AGGREGATES_PATH = ./DATA/aggregates/
    MEDIA_PATH = ./DATA/temp_media/
    MULTIMODAL_PATH = ./DATA/vision_processing/

    [MODELS]
    # CHANGE THESE TO USE DIFFERENT AGENTS
    MAIN_INFER = llama3
    IMAGE_READER = qwen3-vl:2b-instruct

# Harvesting

### Lowest level auxiliary
    
    # Most basic element, extracts a JSON reddit endpoint response
    # url: string with request URL
    get_json(url)
    
    # where: json_harvester.py

### Second level auxiliary
    
    # Saves post json in ./json_dumps/subreddit_name or .ini defined default
    # data: data in json

    save_post(data)

    # where: json_harvester.py
    

### Harvests from chosen subreddit
    
    # subreddit_name: string with subreddit name (e.g. "anime")
    # category: string with category to search into (e.g. "top")
    # limit: maximum of posts to rake down the chain

    harvest_subreddit(subreddit_name, category, limit)

    # where: json_harvester.py
    
### Flattens a comment tree using Depth-First Search

    # folder_path: Base path for subreddit folder (e.g. './json_dumps/')
    # limit: "none" to read all (default), or string with subreddit name (e.g. "anime")

    extract_from_post(folder_path, limit="none")

    # where: processor.py

### Downloads pictures and gifs
    
    # url: Source Reddit URL 

    downloader_function(url)

    # where: json_harvester.py

# Computer Vision

### Flattens reddit proprietary formatting, downloads media and evokes AI

    # body_text: '"body":' of a comment to be analyzed

    process_visual_content(body_text)

    # where: processor.py
    # because me from yesterday thought just ignoring media wasn't painful enough

### Enriches the comment body with description of attached image  
    
    # jsonl_filepath: Base path for normalized (flattened) comments

    process_media(jsonl_filepath)
    
    # where: processor.py
    
# LLM prep and inference

### Climbs tree from target_id using parent_id to assemble context (OPTIONAL - Recommended for big datasets) 

    # target_id: target comment ID for tree rooting
    # data_dict: normalized JSONL dictionary { 'id': { ... } }
    # post_data: dictionary with original post data

    build_context_chain(target_id, data_dict, post_data)

    # where: ai_manager.py

### Structures prompts to be executed (Current version is specifically tuned to brazilian pages)

    # context_chain: Contextual list with chain of comments
    # target_comment_author: ID of what comment this is targeting
    # target_comment_body: body of what comment this is targeting

    prompt_maker(context_chain, target_comment_author, target_comment_body)

    # where: ai_manager.py

### Calculates normalized toxicity index based on binary classification.

    # ai_response_dict: Dictionary containing LLM json response

    calculate_toxicity(ai_response_dict)

    # where: ai_manager.py

    # 'f1': 'Incivility (rude, disrespectful tone, or insults)',
    # 'f2': 'Deliberate Fallacies (intentional strawman, loaded question, or false dilemma — not honest mistakes)',
    # 'f3': 'Ad Hominem (attacking the person instead of the argument), ONLY when attacking the specific commenter being replying to. Attacking a political figure or group belongs in f4 (Hate Speech) if dehumanizing, or nowhere if just criticism.',
    # 'f4': 'Hate Speech (attacks on groups/identity or dehumanization)',
    # 'f5': 'Sarcasm/Irony (saying the opposite of what you mean for effect, e.g., "Great job..." after a failure. Does NOT include: analogies, ellipses, rhetorical questions, or simple disagreement.)'
    

### Uses AI defined in IMAGE_READER as a media interceptor

    # image_path: Media path in temp_media
    # extension: File type
    # model_name: Defined by IMAGE_READER by default

    call_vision_ai(image_path, extension, model_name=IMAGE_READER):

    # where: ai_manager.py

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

    # jsonl_filepath: Path from multimodal analysis
    
    orchestrate_full_inference(jsonl_filepath)

    # where: infer_engine.py

### Reads dataset, reads post catalogue and propagates context to children

    # jsonl_filepath: Path from multimodal analysis
    # post_catalog: Dictionary with (subreddit, post_id)
    
    run_inference_pipeline(jsonl_filepath, post_catalog)

    # where: infer_engine.py