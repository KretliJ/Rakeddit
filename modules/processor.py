import os
import json
import configparser
import requests
import re
import subprocess
import time
from modules.json_harvester import downloader_function
from datetime import datetime, timedelta
from modules.ai_manager import call_vision_ai


from modules.config_loader import config
# Globals
processed_count = 0
media_processed_count = 0
media_count = 0
vision_ai_calls = 0
vision_ai_total_time = 0.0
AGGREGATES = config.get_path('PATHS', 'AGGREGATES_PATH')
IMAGES = config.get_path('PATHS', 'MEDIA_PATH')
MULTIMODAL = config.get_path('PATHS', 'MULTIMODAL_PATH')


def extract_from_post(folder_path, limit="none", aggregates_dir=AGGREGATES):
    global processed_count
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
    min_ts = float('inf')
    max_ts = float('-inf')
    # Open output file once, append line by line
    with open(out_file, 'w', encoding='utf-8') as f_out:
        for folder in target_folders:
            for filename in os.listdir(folder):
                if filename.endswith(".json"):
                    filepath = os.path.join(folder, filename)
                    with open(filepath, 'r', encoding='utf-8') as f_in:
                        try:
                            data = json.load(f_in)
                            post_data = data[0]['data']['children'][0]['data']
                            comments_data = data[1]['data']['children']
                        except: continue
                    ts = post_data.get('created_utc')
                    if ts:
                        min_ts = min(min_ts, ts)
                        max_ts = max(max_ts, ts)
                    # --- 1. POST HEADER INSERTION (DEPTH 0) ---
                    post_record = {
                        "type": "post_header",
                        "id": post_data['id'],
                        "parent_id": None,
                        "post_id": post_data['id'],
                        "subreddit": post_data['subreddit'],
                        "author": post_data['author'],
                        "timestamp": post_data['created_utc'],
                        "title": post_data.get('title', ''),
                        "body": post_data.get('selftext', ''),
                        "depth": 0, # Raiz da cascata
                        "metadata_score": post_data.get('score', 0)
                    }
                    f_out.write(json.dumps(post_record, ensure_ascii=False) + "\n")
                    processed_count += 1

                    # --- 2. DFS COM RASTREAMENTO DE PROFUNDIDADE ---
                    # O stack agora guarda (objeto_comentario, id_do_pai, profundidade)
                    stack = []
                    for c in reversed(comments_data):
                        if c.get('kind') == 't1':
                            stack.append((c, post_data['id'], 1))
                    
                    while stack:
                        current_comment, parent_id, current_depth = stack.pop()
                        c_data = current_comment['data']

                        c_author = c_data.get('author', '[deleted]')    
                        c_body = c_data.get('body', '[empty]')

                        # We don't need automod or removed comments, but they must stay not to break the graphs
                        if c_author.lower() == "automoderator":
                            c_body = "[AutoModerator]"

                        is_valid = c_body not in ["[deleted]", "[removed]", "[AutoModerator]", "[empty]"]

                        ts_c = c_data.get('created_utc')
                        if ts_c:
                            min_ts = min(min_ts, ts_c)
                            max_ts = max(max_ts, ts_c)

                        comment_record = {
                            "type": "comment",
                            "id": c_data.get('id'),
                            "parent_id": parent_id,
                            "post_id": post_data['id'],
                            "subreddit": post_data['subreddit'],
                            "author": c_author,
                            "timestamp": c_data.get('created_utc'),
                            "body": c_body,
                            "depth": current_depth,
                            "metadata_score": c_data.get('score', 0),
                            "is_valid_text": is_valid
                        }
                        f_out.write(json.dumps(comment_record, ensure_ascii=False) + "\n")
                        processed_count += 1
                        
                        replies = c_data.get('replies')
                        if replies and isinstance(replies, dict):
                            children = replies['data']['children']
                            for r in reversed(children):
                                if r.get('kind') == 't1':
                                    # Incrementa a profundidade para os filhos
                                    stack.append((r, c_data.get('id'), current_depth + 1))
                        
        if processed_count > 0:
            metadata_eof = {
                "type": "metadata_footer",
                "total_records": processed_count,
                "temporal_window": {
                    "unix_start": min_ts,
                    "unix_end": max_ts,
                    "human_start": datetime.fromtimestamp(min_ts).strftime('%Y-%m-%d %H:%M:%S'),
                    "human_end": datetime.fromtimestamp(max_ts).strftime('%Y-%m-%d %H:%M:%S'),
                    "duration_days": round((max_ts - min_ts) / 86400, 2)
                },
                "generated_at": datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }
            f_out.write(json.dumps(metadata_eof, ensure_ascii=False) + "\n")

    return out_file

def get_processed_count():
    return processed_count

# ______________________________________________________________________________________________

def process_media(jsonl_filepath):
    # 1. loops over normalized dataset
    # 2. intercepts comments with media
    # 3. enriches text body via AI visualization
    # 4. saves new multimodal JSONL
    
    # ==========================================
    # CONFIG AND PATHS
    # ==========================================
    global media_processed_count, media_count
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
    
    # ==========================================
    # ETA CALC PRESCAN
    # ==========================================
    is_potentially_media = 0
    with open(jsonl_filepath, 'r', encoding='utf-8') as f_scan:
        for line in f_scan:
            # Raw string lookup is faster
            if "http" in line or "![" in line:
                is_potentially_media += 1
                
    avg_time_per_image = 1.24  # For author system that's 1.24s
    eta_seconds = int((is_potentially_media * avg_time_per_image)*2) # 2 * accounts for image download time
    eta_formatted = str(timedelta(seconds=eta_seconds))
    
    print(f"[ETA] Detected ~{is_potentially_media} items with potential media.")
    print(f"[ETA] Estimated time for step 3: {eta_formatted} ({avg_time_per_image}s/req)\n")

    media_processed_count = 0
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
            media_processed_count += 1

    # ==========================================
    # REPORT
    # ==========================================
    print(f"\n[SUCCESS] Visual processing completed.")
    print(f" -> Saved in: {out_path}")
    print(f" -> Total iterations: {media_processed_count}")
    print(f" -> Enriched iterations: {media_count}")
    
    return out_path

def media_get_processed_count():
    return media_processed_count

def media_get_media_count():
    return media_count
# ______________________________________________________________________________________________

def process_visual_content(body_text):
    global vision_ai_calls, vision_ai_total_time
    # Flattens reddit proprietary formatting, downloads media and evokes AI
    
    if not body_text:
        return body_text

    # 1. Giphy treatment
    giphy_pattern = r'!\[gif\]\(giphy\|([a-zA-Z0-9]+)[^)]*\)'
    body_text = re.sub(giphy_pattern, r'https://i.giphy.com/\1.gif', body_text)

    # 2. Decompositon from preview.redd.it
    preview_pattern = r'https?://preview\.redd\.it/([a-zA-Z0-9_-]+\.(?:jpeg|jpg|png|gif))(?:\?[^\s\])]*)?'
    body_text = re.sub(preview_pattern, r'https://i.redd.it/\1', body_text)

    # 3. URLs extraction
    media_pattern = r'(https?://\S+\.(?:jpg|jpeg|png|gif|mp4))'
    links = re.findall(media_pattern, body_text)

    if not links:
        return body_text

    # 4. Image processing with text filter
    for link in set(links):
        ext = link.split('.')[-1].split('?')[0].upper()
        
        image_data = downloader_function(link)
        print(f"[IMAGE AI - ({ext})] Analyzing from: {link}")
        
        # --- TELEMETRIA: Medindo apenas o tempo de inferência ---
        t_start = time.time()
        description = call_vision_ai(image_data, extension=ext)
        t_end = time.time()
        
        vision_ai_total_time += (t_end - t_start)
        vision_ai_calls += 1
        # --------------------------------------------------------

        replacement_tag = f"[VISUAL CONTENT: {description}]"
        body_text = body_text.replace(link, replacement_tag)
        
    return body_text.strip()

# ______________________________________________________________________________________________

def get_vision_telemetry():
    avg_time = (vision_ai_total_time / vision_ai_calls) if vision_ai_calls > 0 else 0.0
    return {
        "AI CALLS": vision_ai_calls,
        "TOTAL INF TIME": round(vision_ai_total_time, 2),
        "AVERAGE TIME": round(avg_time, 2)
    }