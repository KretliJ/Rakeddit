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

subreddits = ["brasildob"]
limit_of_posts = 1
run_inference = True
run_inference_only = False
category = "top"
timeframe = "month"
infer_only_file = ' '

if __name__  == "__main__":
    
    if platform.system() == "Windows":
        prevent_sleep_windows(enable=True)
        
    if run_inference_only:
        try:
            orchestrate_full_inference(infer_only_file)
        except Exception as e:
            print(e)
        finally:
            if platform.system() == "Windows":
                prevent_sleep_windows(enable=False)
    else:
        start_time = datetime.datetime.now()
        
        start_banner = f"""# 🚀 RAKEDDIT PIPELINE - EXECUTION LOG

**Metadata:**
* **Date:** {start_time.strftime('%d/%m/%Y')}
* **Start Time:** {start_time.strftime('%H:%M:%S')}
* **OS:** {platform.system()} {platform.release()}
                                                       
**Configuration:**
* **Subreddits:** {', '.join(subreddits)}
* **Limit per sub:** {limit_of_posts} posts
* **Parameters:** {category} / {timeframe}
* **Inference Enabled:** {run_inference}

---
"""
        print(start_banner)
        log_filename = os.path.join(LOGGING_PATH, f"{start_time.strftime('%Y-%m-%d_%H-%M-%S')}_collection.md")
        
        try:
            with open(log_filename, "w", encoding="utf-8") as f:
                f.write(start_banner + "\n")
        
            for subreddit_name in subreddits:

                with open(log_filename, "a", encoding="utf-8") as f:
                    f.write(f"## Processing: r/{subreddit_name}\n\n")
                
                # 1. HARVESTING
                t0_harvest = datetime.datetime.now()
                harvest_subreddit(subreddit_name, limit_of_posts, category, timeframe)
                t1_harvest = datetime.datetime.now()
                
                # 2. EXTRACTION
                t0_extract = datetime.datetime.now()
                path_normalized = extract_from_post(BASE_PATH, subreddit_name) 
                t1_extract = datetime.datetime.now()
                
                harvest_stats = f"""### 💬 Data Extraction Stats
* **Target Posts:** {limit_of_posts}
* **API Batches:** {limit_of_posts//25}
* **Comments Flattened:** {get_processed_count()}
* **Harvesting Time:** {t1_harvest - t0_harvest}
* **Flattening Time:** {t1_extract - t0_extract}

"""
                print(harvest_stats)
                with open(log_filename, "a", encoding="utf-8") as f:
                    f.write(harvest_stats)
                
                # 3. VISUAL PROCESSING
                if path_normalized:
                    t0_media = datetime.datetime.now()
                    path_multimodal = process_media(path_normalized)
                    t1_media = datetime.datetime.now()
                    
                    media_stats = f"""### 👀 Visual Processing Stats (OCR & Vision AI)
* **Total Comments Scanned:** {media_get_processed_count()}
* **Media Enriched (Images processed):** {media_get_media_count()}
* **Vision Pipeline Time:** {t1_media - t0_media}
* **Average per media:** {(t1_media - t0_media)/media_get_media_count() if media_get_media_count() > 0 else '0:00:00'}

"""
                    print(media_stats)
                    with open(log_filename, "a", encoding="utf-8") as f:
                        f.write(media_stats)
                        
                    # 4. TEXT INFERENCE
                    if path_multimodal and run_inference:
                        t0_infer = datetime.datetime.now()
                        orchestrate_full_inference(path_multimodal)
                        t1_infer = datetime.datetime.now()
                        
                        infer_stats = f"""### 🧠 Text Inference Stats (Llama-3 8B)
* **Inference Pipeline Time:** {t1_infer - t0_infer}
* **Average per inference:** {(t1_infer - t0_infer)/media_get_processed_count() if media_get_processed_count() > 0 else '0:00:00'}

"""
                        with open(log_filename, "a", encoding="utf-8") as f:
                            f.write(infer_stats)
            
            end_time = datetime.datetime.now()
            elapsed = end_time - start_time
            end_status = "✅ Fully Completed" if run_inference else "⏸️ Inference Skipped"
            
            end_banner = f"""---
## 🎯 PIPELINE FINISHED                              
                                                                        
* **End Time:** {end_time.strftime('%H:%M:%S')}
* **Total Elapsed Time:** {elapsed}
* **Status:** {end_status}
* **Total Enriched Media:** {media_get_media_count()}
"""
            print(end_banner)

            with open(log_filename, "a", encoding="utf-8") as f:
                f.write(end_banner)

        except Exception as e:
            error_time = datetime.datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
            print(f"❌ FATAL ERROR: {e}")
            with open(os.path.join(LOGGING_PATH, f"{error_time}_error.md"), "w", encoding="utf-8") as f:
                f.write(f"# ❌ ERROR REPORT\n\n**Time:** {error_time}\n**Exception:** {e}\n")
        finally:
            if platform.system() == "Windows":
                prevent_sleep_windows(enable=False)