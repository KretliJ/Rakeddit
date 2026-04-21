from modules.json_harvester import *
from modules.processor import *
from modules.infer_engine import orchestrate_full_inference
from modules.config_loader import prevent_sleep_windows
from modules.config_loader import config
import datetime
import platform
import os

HEADERS = {'User-Agent': config.get('HEADERS', 'User-Agent')}
BASE_PATH = config.get_path('PATHS', 'BASE_PATH')
LOGGING_PATH = config.get_path('PATHS', 'LOGGING_PATH')

subreddits = ["brasilivre"]
limit_of_posts = 100
run_inference = True
run_inference_only = True
category = "top"
timeframe = "month"
infer_only_file = 'DATA/3-vision_processing/MULTIMODAL_BRASILIVRE_data_normalized_2026-04-20_08-47-06.jsonl'
if __name__  == "__main__":
    
    if platform.system() == "Windows":
        prevent_sleep_windows(enable=True)
        
    if run_inference_only:
        try:
            orchestrate_full_inference(infer_only_file)
        except Exception as e:
            print(e)
        finally:
            # Restores windows sleeping settings
            if platform.system() == "Windows":
                prevent_sleep_windows(enable=False)
    else:
        start_time = datetime.datetime.now()
        
        start_banner = f"""
        🚀 RAKEDDIT PIPELINE - INITIALIZE                       
                                                                
        📅 Date: {start_time.strftime('%d/%m/%Y')}
        ⏰ Time: {start_time.strftime('%H:%M:%S')}
                                                       
        Subreddits: {subreddits}
        Limit: {limit_of_posts}, {category}, {timeframe}
        Inference: {run_inference}
        """
        
        print(start_banner)
        
        # Fixed filename for this sessions log
        log_filename = os.path.join(LOGGING_PATH,f"{start_time.strftime('%Y-%m-%d_%H-%M-%S')}_collection.md")
        
        try:
            # Open, record header, close
            with open(log_filename, "w", encoding="utf-8") as f:
                f.write(start_banner + "\n")
        
            for subreddit_name in subreddits:
                # TODO: Make this mess save current state so it doesn't break long runs
                harvest_subreddit(subreddit_name, limit_of_posts, category, timeframe)
                
                path_normalized = extract_from_post(BASE_PATH, subreddit_name) 
                
                if path_normalized:
                    path_multimodal = process_media(path_normalized)
                    
                    if path_multimodal and run_inference:
                        orchestrate_full_inference(path_multimodal)
            
            end_time = datetime.datetime.now()
            elapsed = end_time - start_time

            end_status = "✅ Infer completed" if run_inference else "⏸️ Infer on hold"
            
            end_banner = f"""
            🎯 RAKEDDIT PIPELINE - END                              
                                                                        
            📅 Date: {end_time.strftime('%d/%m/%Y')}
            ⏰ Time: {end_time.strftime('%H:%M:%S')}
            ⏱️ Elapsed: {elapsed}
            💤 Status:
                                                                    
            ✅ {len(subreddits)} subreddits raked
            ✅ Media enriched
            {end_status}
            """
            print(end_banner)

            # Appends for final logging
            with open(log_filename, "a", encoding="utf-8") as f:
                f.write(f"\nFim: {end_banner}\n")
                f.write(f"Elapsed: {elapsed}\n")
                f.write(f"Subreddits: {', '.join(subreddits)}\n")   

        except Exception as e:
            error_time = datetime.datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
            with open(os.path.join(LOGGING_PATH,f"{error_time}_error.md"), "w", encoding="utf-8") as f:
                f.write(f"❌ ERROR: {e}\n")
        finally:
            # Restores windows sleeping settings
            if platform.system() == "Windows":
                prevent_sleep_windows(enable=False)