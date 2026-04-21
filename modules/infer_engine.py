import os
import json
import configparser
import random
import requests
import re
from collections import defaultdict
from .ai_manager import prompt_maker, calculate_toxicity 

# Global configs
from modules.config_loader import config

HEADERS = {'User-Agent': config.get('HEADERS', 'User-Agent')}
BASE_PATH = config.get_path('PATHS', 'BASE_PATH') 
MAIN_INFER = config.get('MODELS', 'MAIN_INFER')

# ==========================================
#  SUPPORT FUNCTIONS
# ==========================================

def get_original_post_content(subreddit, post_id):

    # Finds raw JSON in BASE_PATH and extracts post title and body
    # Reminder: BASE_PATH comes from your config.ini

    target_path = os.path.join(BASE_PATH, subreddit, f"{post_id}.json")
    
    if not os.path.exists(target_path):
        return f"[ORIGINAL POST NOT FOUND: {post_id}]"

    try:
        with open(target_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
            post_data = data[0]['data']['children'][0]['data']
            title = post_data.get('title', '')
            selftext = post_data.get('selftext', '')
            return f"{title}\n{selftext}".strip()
    except Exception as e:
        return f"[ERROR READING POST: {e}]"

def mock_local_ai(prompt):
    # This is just a mocking function to test the pipeline without LLM  
    return {
        "f1": random.choice([0, 1]),
        "f2": random.choice([0, 1]),
        "f3": random.choice([0, 1]),
        "f4": random.choice([0, 1]),
        "f5": random.choice([0, 1]),
        "aggro": random.randint(0, 3)
    }

def run_ai(prompt, model_name=MAIN_INFER):
    # Sends prompt to local Ollama API and ensures return of Python dictionary

    url = "http://localhost:11434/api/generate"
    
    payload = {
        "model": model_name,
        "prompt": prompt,
        "stream": False,
        "format": "json", 
        "options": {
            "temperature": 0.0,  # Factualidade absoluta
            "top_p": 0.1,        # Corta a cauda de probabilidades (evita alucinações nas flags)
            "num_predict": 100,  # Trava de segurança contra o "Loop de Chaves"
            "seed": 42           # [CRÍTICO] Semente fixa para reprodutibilidade acadêmica
        }
    }

    try:
        # Long timeout to make up for local CPU/GPU inference delay
        response = requests.post(url, json=payload, timeout=120)
        response.raise_for_status() 
        
        raw_text = response.json().get("response", "")
        
        # Sanitizing: Removes markdown blocks if agent ignores format="json"
        clean_text = re.sub(r"```json\n?|```", "", raw_text).strip()
        
        # Converts JSON string to a Python dictionary
        result_dict = json.loads(clean_text)
        return result_dict
    except requests.exceptions.RequestException as e:
        print(f"\n[NETWORK ERROR] Failed to get in touch with the local AI. Is Ollama running? Error: {e}")
    except json.JSONDecodeError:
        print(f"\n[PARSER ERROR] AI did not return a valid JSON. Raw output: {raw_text}")
    except Exception as e:
        print(f"\n[UNKNOWN AI ERROR] {e}")

    # Fallback: If all else goes to hell, return zeroes not to break the entire pipeline
    return {
        "f1": 0, "f2": 0, "f3": 0, "f4": 0, "f5": 0, "aggro": 0
    }

# ==========================================
# INFERENCE ENGINE
# ==========================================
def run_inference_pipeline(jsonl_filepath, post_catalog):
    
    # 1. Config and paths setup
    current_dir = os.path.dirname(os.path.abspath(__file__))
    config_path = os.path.join(current_dir, '..', 'config.ini') 
    
    config = configparser.ConfigParser()
    config.read(config_path)
    INFERRED = config.get('PATHS', 'INFERRED_PATH', fallback="./DATA/4-inferred")    
    
    base = os.path.basename(jsonl_filepath)
    out_path = os.path.join(INFERRED, f"INFERRED_{base}")

    comments_dict = {}
    children_map = defaultdict(list)
    root_ids = []

    # 2. Dataset reading
    with open(jsonl_filepath, 'r', encoding='utf-8') as f:
        for line in f:
            record = json.loads(line)
            comments_dict[record['id']] = record
            if record['parent_id'] == record['post_id']:
                root_ids.append(record['id'])
            else:
                children_map[record['parent_id']].append(record['id'])

    # 3. Crossing and inferencing (DFS)
    stack = []
    for r_id in reversed(root_ids):
        p_id = comments_dict[r_id]['post_id']
        # Pulls the correct body from catalog. Else, uses placeholder
        body = post_catalog.get(p_id, "[POST BODY NOT FOUND]")
        stack.append((r_id, [f"[ORIGINAL POST ]: {body}"]))

    processed_count = 0
    total_to_process = len(comments_dict)

    with open(out_path, "w", encoding="utf-8") as out_f:
        while stack:
            current_id, current_context_list = stack.pop()
            record = comments_dict[current_id]
            
            context_string = "\n".join(current_context_list)
            
            # AI BLOCK
            prompt = prompt_maker(context_string, record['author'], record['body'])
            print(f"[INFO] Processing {processed_count + 1}/{total_to_process} | ID: {current_id}") 
            
            ai_response_json = run_ai(prompt)
            tox_score = calculate_toxicity(ai_response_json)
            
            record['ai_analysis'] = ai_response_json
            record['toxicity_score'] = tox_score
            
            out_f.write(json.dumps(record, ensure_ascii=False) + "\n")
            processed_count += 1
            
            # Propagation of context to children
            new_context_list = current_context_list.copy()
            new_context_list.append(f"[{record['author']}]: {record['body']}")
            
            for child_id in reversed(children_map[current_id]):
                stack.append((child_id, new_context_list))
                
    print(f"\n[SUCCESS] Multimodal dataset processed: {out_path}")

def orchestrate_full_inference(jsonl_filepath):

    # Orchestrates inference for consolidated datasets
    # Identifies unique posts, looks for their bodies and executes analysis 
    # - NOTE: never say this out of context

    print(f"\n[ORCHESTRATOR] Analyzing dataset: {os.path.basename(jsonl_filepath)}")
    
    # 1. Identify all unique (subreddit, post_id) in file
    post_targets = set()
    try:
        with open(jsonl_filepath, 'r', encoding='utf-8') as f:
            for line in f:
                record = json.loads(line)
                # Row (subreddit, post_id) to ensure unity
                post_targets.add((record['subreddit'], record['post_id']))
    except Exception as e:
        print(f"[ERROR] Failed to read file for mapping: {e}")
        return

    print(f"[INFO] {len(post_targets)} unique posts identified in file.")

    # 2. Create body catalogue { post_id: "body" }
    post_catalog = {}
    for sub, pid in post_targets:
        print(f"   -> Buscando conteúdo original: {sub}/{pid}")
        post_catalog[pid] = get_original_post_content(sub, pid)

    # 3. Run infer pipeline cycling through full catalogue
    # We pass the entire 'post_catalog' dictionary    
    run_inference_pipeline(jsonl_filepath, post_catalog)