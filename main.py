from modules.json_harvester import *
from modules.processor import *
from modules.infer_engine import orchestrate_full_inference
# Global configs
from modules.config_loader import config

HEADERS = {'User-Agent': config.get('HEADERS', 'User-Agent')}
BASE_PATH = config.get_path('PATHS', 'BASE_PATH')

if __name__  == "__main__":
    # To rake multiple subs use:
    # for sub subreddit_name in ["brasil", "portugal", "askacademico"]:
    # TODO: Make this mess save current state so it doesn't brake long runs

        subreddit_name = "botecodoreddit" # -> Comment this off if running loop
        # 1. HARVEST (RawJSONs)
        harvest_subreddit(subreddit_name, "top", limit=1)
        
        # 2. EXTRACT (Generates normalized .jsonl and returns dynamic PATH)
        path_normalized = extract_from_post(BASE_PATH, limit=subreddit_name)
        
        if path_normalized:
            # 3. MEDIA ENRICHMENT (Reads normalized, generates MULTIMODAL and returns PATH)
            path_multimodal = process_media(path_normalized)
            
            if path_multimodal:
                # 4. INFERENCE (Uses enriched multimodal path)
                # Since orchestrate_full_inference already manages the catalogue, 
                # it will hopefully shred the file in local Llama-3 or whatever is chosen
                orchestrate_full_inference(path_multimodal)
