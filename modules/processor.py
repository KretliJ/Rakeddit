import os
import json
import configparser
from datetime import datetime
current_dir = os.path.dirname(os.path.abspath(__file__))
config_path = os.path.join(current_dir, '..', 'config.ini')
config = configparser.ConfigParser()
config.read(config_path)

AGGREGATES = config.get('PATHS', 'AGGREGATES_PATH')

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
        out_file = f"{aggregates_dir}/FULL_data_normalized_" + current_time + ".jsonl"

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