import os
import json
import configparser
import requests
import re
import subprocess
import time
from modules.json_harvester import downloader_function
from datetime import datetime
from modules.ai_manager import call_vision_ai


from modules.config_loader import config

AGGREGATES = config.get_path('PATHS', 'AGGREGATES_PATH')
IMAGES = config.get_path('PATHS', 'MEDIA_PATH')
MULTIMODAL = config.get_path('PATHS', 'MULTIMODAL_PATH')


def extract_from_post(folder_path, limit="none", aggregates_dir=AGGREGATES):
    # Flattens a comment tree using Depth-First Search.
    # folder_path: Base path for subreddit folder (e.g. './json_dumps/')
    # limit: "none" to read all (default), or string with subreddit name (e.g. "anime")
    
    # Choose which folders to seek based on limit
    target_folders = []
    if limit.lower() == "none":
        target_folders = [os.path.join(folder_path, d) for d in os.listdir(folder_path) 
                          if os.path.isdir(os.path.join(folder_path, d))]
    else:
        target_folders = [os.path.join(folder_path, limit)]

    current_time = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    # Write new structured JSONL (JSON Lines)
    if limit != "none":
        out_file = f"{aggregates_dir}/{limit.upper()}_data_normalized_" + current_time + ".jsonl"
    else:
        out_file = f"{aggregates_dir}FULL_data_normalized_" + current_time + ".jsonl"

    print(f"\n[INFO] Data extraction started. Streaming to: {out_file}")

    processed_count = 0

    # Open output file once, append line by line
    with open(out_file, 'w', encoding='utf-8') as f_out:
        for folder in target_folders:
            if not os.path.exists(folder):
                print(f"[WARNING] Directory not found: {folder}")
                continue

            for filename in os.listdir(folder):
                if filename.endswith(".json"):
                    filepath = os.path.join(folder, filename)
                    print(f"\n==================================================")
                    print(f"READING FILE: {filepath}")
                    print(f"==================================================")
                    
                    with open(filepath, 'r', encoding='utf-8') as f_in:
                        try:
                            data = json.load(f_in)
                        except json.JSONDecodeError:
                            print(f"[ERROR] Failed to read JSON: {filepath}")
                            continue

                    try:
                        post_data = data[0]['data']['children'][0]['data']
                        post_id = post_data['id']
                        subreddit = post_data['subreddit']
                        comments_data = data[1]['data']['children']
                    except (IndexError, KeyError):
                        print(f"[ERROR] Unexpected structure in file: {filepath}")
                        continue
                    
                    stack = []
                    
                    # Inject root comments into stack
                    for c in reversed(comments_data):
                        if c.get('kind') == 't1':
                            stack.append((c, post_id))
                    
                    # DFS Loop
                    while stack:
                        current_comment, parent_id = stack.pop()
                        c_data = current_comment['data']
                        
                        c_id = c_data.get('id', 'unknown')
                        c_author = c_data.get('author', '[deleted]')
                        c_body = c_data.get('body', '[empty]')
                        
                        record = {
                            "id": c_id,
                            "parent_id": parent_id,
                            "post_id": post_id,
                            "subreddit": subreddit,
                            "author": c_author,
                            "timestamp": c_data.get('created_utc'),
                            "body": c_body,
                            "metadata_score": c_data.get('score', 0),
                            "metadata_controversiality": c_data.get('controversiality', 0)
                        }
                        
                        # Stream directly to disk (Giant Dataset Support)
                        f_out.write(json.dumps(record, ensure_ascii=False) + "\n")
                        processed_count += 1
                        
                        replies = c_data.get('replies')
                        if replies and isinstance(replies, dict):
                            children = replies['data']['children']
                            for r in reversed(children):
                                if r.get('kind') == 't1':
                                    stack.append((r, c_id))

    print(f"\n[SUCCESS] Processed {processed_count} comments.")
    print(f"[SUCCESS] Dataset saved to: {out_file}")
    return out_file

# ______________________________________________________________________________________________

def process_media(jsonl_filepath):
    # 1. loops over normalized dataset
    # 2. intercepts comments with media
    # 3. enriches text body via AI visualization
    # 4. saves new multimodal JSONL
    
    # ==========================================
    # CONFIG AND PATHS
    # ==========================================
    current_dir = os.path.dirname(os.path.abspath(__file__))
    config_path = os.path.join(current_dir, '..', 'config.ini')
    
    config = configparser.ConfigParser()
    config.read(config_path)
    AGGREGATES = config.get('PATHS', 'AGGREGATES_PATH', fallback='./aggregates')    
    
    base = os.path.basename(jsonl_filepath)
    file_name = f"MULTIMODAL_{base}"
    out_path = os.path.join(MULTIMODAL, file_name)

    print(f"\n[INFO] Initiating enriching pipeline.")
    print(f"[INFO] Reading: {base}")
    
    processed_count = 0
    media_count = 0

    # ==========================================
    # PROCESSING
    # ==========================================
    # Opens input file for reading and output for simultaneous manipulation
    with open(jsonl_filepath, 'r', encoding='utf-8') as f_in, \
         open(out_path, 'w', encoding='utf-8') as f_out:
        
        for line in f_in:
            record = json.loads(line)
            original_body = record.get('body', '')

            # "Fast Fail" optimization:
            # Only call process_visual_content if text has obvious image link or markdown formats
            if "http" in original_body or "![" in original_body:
                
                # AI call
                enriched_body = process_visual_content(original_body)
                
                # If text changed (processed media), updates the record
                if enriched_body != original_body:
                    record['body'] = enriched_body
                    media_count += 1
            
            # Writes registries (modified or not) in new JSONL
            f_out.write(json.dumps(record, ensure_ascii=False) + "\n")
            processed_count += 1

    # ==========================================
    # REPORT
    # ==========================================
    print(f"\n[SUCCESS] Visual processing completed.")
    print(f" -> Saved in: {out_path}")
    print(f" -> Total iterations: {processed_count}")
    print(f" -> Enriched iterations: {media_count}")
    
    return out_path

# ______________________________________________________________________________________________

def process_visual_content(body_text):
    # Flattens reddit proprietary formatting, downloads media and evokes AI
    
    if not body_text:
        return body_text

    # 1. Giphy treatment for Reddit markdown
    giphy_pattern = r'!\[gif\]\(giphy\|([a-zA-Z0-9]+)[^)]*\)'
    body_text = re.sub(giphy_pattern, r'https://i.giphy.com/\1.gif', body_text)

    # 2. Decompositon from preview.redd.it to i.redd.it
    preview_pattern = r'https?://preview\.redd\.it/([a-zA-Z0-9_-]+\.(?:jpeg|jpg|png|gif))(?:\?[^\s\])]*)?'
    body_text = re.sub(preview_pattern, r'https://i.redd.it/\1', body_text)

    # 3. URLs extraction from clean media
    media_pattern = r'(https?://\S+\.(?:jpg|jpeg|png|gif|mp4))'
    links = re.findall(media_pattern, body_text)

    if not links:
        return body_text

    # 4. Image processing with text filter
    for link in set(links):
        ext = link.split('.')[-1].split('?')[0].upper()
        
        image_data = downloader_function(link)
        print(f"[IMAGE AI - ({ext})] Analyzing from: {link}")
            
        description = call_vision_ai(image_data, extension=ext)
        replacement_tag = f"[VISUAL CONTENT: {description}]"
            
        body_text = body_text.replace(link, replacement_tag)
        
    
    return body_text.strip()

# ______________________________________________________________________________________________