import requests
import time
import random
import os
import json

# Global configs
from modules.config_loader import config

HEADERS = {'User-Agent': config.get('HEADERS', 'User-Agent')}
BASE_PATH = config.get_path('PATHS', 'BASE_PATH')
IMAGES = config.get_path('PATHS', 'MEDIA_PATH')



# ______________________________________________________________________________________________
# Lowest level auxiliary. Extracts a JSON reddit endpoint response

def get_json(url, max_retries=5):
    base_delay = 1  # 1 s
    max_delay = 2000  # max 33 minutes
    
    for attempt in range(max_retries):
        response = requests.get(url, headers=HEADERS)
        
        if response.status_code == 200:
            return response.json()
        
        elif response.status_code == 429:
            # Exponencial clássico: 1, 2, 4, 8, 16
            wait = min(base_delay * (2 ** attempt), max_delay)
            print(f"Rate limit! Aguardando {wait}s (tentativa {attempt+1}/{max_retries})")
            time.sleep(wait)
    
    return None

# ______________________________________________________________________________________________
# Second level auxiliary. Saves post json in ./json_dumps/subreddit_name

def save_post(data, base_path=BASE_PATH):
    try:
        sub_name = data[0]['data']['children'][0]['data']['subreddit']
        post_id = data[0]['data']['children'][0]['data']['id']
        
        target_dir = os.path.join(base_path, sub_name)
        os.makedirs(target_dir, exist_ok=True)
        
        filepath = os.path.join(target_dir, f"{post_id}.json")
        
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=4)
            
        print(f"   [OK] Post {post_id} saved in: {target_dir}")
        
    except (KeyError, IndexError) as e:
        print(f"   [ERROR] Failed to parse JSON: {e}")

# ______________________________________________________________________________________________
# Harvests from chosen subreddit

def harvest_subreddit(subreddit_name, category="hot", limit=25):
    
    print(f"\n[HARVESTER] Harvesting r/{subreddit_name} | Target: {limit} posts")
    sub_url = f"https://www.reddit.com/r/{subreddit_name}/{category}/.json?limit={limit}"
    data = get_json(sub_url)

    if not data:
        print(f"[HARVESTER] [!] Failure to rake data from r/{subreddit_name}.")
        return

    posts = data['data']['children']
    for post in posts:
        post_data = post['data']
        title = post_data['title']
        permalink = post_data['permalink']
        
        print(f"\n--- Lendo Post: {title[:50]}... ---")
        
        comment_url = f"https://www.reddit.com{permalink}.json"
        comment_data = get_json(comment_url)
        
        if comment_data:
            save_post(comment_data)
            
            # Fast print for visual feedback
            comments = comment_data[1]['data']['children']
            for c in comments[:3]: # Reduced to 3 to avoid terminal pollution
                if c['kind'] == 't1': 
                    body = c['data'].get('body', '')
                    score = c['data'].get('score', 0)
                    print(f"   [{score}] {body[:40]}...")
        
        # Jitter between posts raking
        wait_time = random.uniform(3.0, 7.0)
        print(f"Await {wait_time:.2f}s to next query...")
        time.sleep(wait_time)
        
    print(f"\n[HARVESTER] Finished raking for r/{subreddit_name}.")

# ______________________________________________________________________________________________

def downloader_function(url):
    # Downloads a media file via URL using chunks and saves it locally
    # Passive cache means it doesn't download existing files
    # Returns absolute file path or None upon fail
    
    if not url:
        return None

    # Cleans URL to ensure a valid file name
    # e.g.: 'video.mp4?source=reddit' -> 'video.mp4'
    raw_filename = url.split('/')[-1]
    clean_filename = raw_filename.split('?')[0]
    
    file_path = os.path.join(IMAGES, clean_filename)
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    
    # 1. Cache verify (Avoiding duplicate request is even more gentle than timeouts and jittering)
    if os.path.exists(file_path):
        return file_path

    try:
        # 2. Safe request
        # stream=True to avoid RAM saturation.
        response = requests.get(url, headers=HEADERS, stream=True, timeout=15)
        response.raise_for_status()
        
        # 3. Chunk recording (8KB each)
        with open(file_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk: # Filters empty keep-alive chunks
                    f.write(chunk)
                    
        return file_path
        
    except requests.exceptions.RequestException as e:
        print(f"\n[ERROR] Failed to download from {url} -> {e}")
        return None
    except OSError as e:
        print(f"\n[ERROr] Failed while saving file to {file_path} -> {e}")
        return None
