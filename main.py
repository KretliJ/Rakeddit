import datetime
import json
import sys
import time
import os
import logging
import platform
from modules.config_loader import config, prevent_sleep_windows
from modules.json_harvester import harvest_subreddit
from modules.processor import extract_from_post, process_media, get_processed_count, media_get_media_count, get_vision_telemetry, process_visual_content
import modules.processor as processor_module

class RakedditDatabaseBuilder:
    # Orchestrator focused on building and enriching a dataset
    
    def __init__(self, subreddits, limit=100, category="top", timeframe="all"):
        self.subreddits = subreddits
        self.limit = limit
        self.category = category
        self.timeframe = timeframe
        self.start_time = datetime.datetime.now()
        
        # Paths
        self.base_path = config.get_path('PATHS', 'BASE_PATH')
        # Fallback for LOGGING_PATH
        self.logs_path = config.get_path('PATHS', 'LOGGING_PATH', fallback="./DATA/logs") 
        os.makedirs(self.logs_path, exist_ok=True)
        
        self.log_filename = os.path.join(
            self.logs_path, 
            f"DATASET_BUILD_{self.start_time.strftime('%Y-%m-%d_%H-%M-%S')}.md"
        )
        
        self._init_logging()
        self._write_md_header()

    def _init_logging(self):
        # Configure logger for terminal
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s [%(levelname)s] %(message)s',
            handlers=[logging.StreamHandler()]
        )
        self.logger = logging.getLogger("RakedditBuilder")

    def _write_md_header(self):
        header = f"""# 📊 Dataset construction report
**Start date:** {self.start_time.strftime('%d/%m/%Y')} | **Hour:** {self.start_time.strftime('%H:%M:%S')}

## ⚙️ Collection parameters
* **Subreddits:** {', '.join(self.subreddits)}
* **Limits per Subreddit:** {self.limit} posts
* **Search Filter:** Category `{self.category}` | Period `{self.timeframe}`
* **System:** {platform.system()} {platform.release()}

"""
        with open(self.log_filename, "w", encoding="utf-8") as f:
            f.write(header)

    def _append_md_stage(self, sub, stage_name, metrics, duration):
        # Appends completed stage to report
        content = f"### r/{sub} - {stage_name}\n"
        content += f"* **Runtime:** {duration:.2f}s\n"
        for key, value in metrics.items():
            content += f"* **{key}:** {value}\n"
        content += "\n"
        
        with open(self.log_filename, "a", encoding="utf-8") as f:
            f.write(content)

    def run(self):
        self.logger.info(f"🚀 Building database")
        self.logger.info(f"📄 Recording details to: {self.log_filename}")
        
        try:
            for sub in self.subreddits:
                self.logger.info(f"--- Processing r/{sub} ---")
                
                # ==========================================
                # STEP 1: Raw collection (Harvesting)
                # ==========================================
                self.logger.info(f"[1/3] Harvesting...")
                t0 = time.time()
                harvest_subreddit(sub, self.limit, self.category, self.timeframe)
                dur1 = time.time() - t0
                
                self._append_md_stage(sub, "Step 1: Raw collection (Harvesting)", {
                    "Total posts found": "Check console logs", # Placeholder as harvesting doesn't return count directly here
                    "Status": "Concluído"
                }, dur1)
                self.logger.info(f"[1/3] Completed in {dur1:.2f}s")

                # ==========================================
                # STEP 2: Flattening and structuring (Headers + Depth)
                # ==========================================
                self.logger.info(f"[2/3] Initiating graph extraction...")
                t0 = time.time()
                path_norm = extract_from_post(self.base_path, sub)
                dur2 = time.time() - t0
                
                total_registries = get_processed_count()
                self._append_md_stage(sub, "Step 2: Flattening (DFS)", {
                    "Extracted nodes (Headers + Comments)": total_registries,
                    "Temporary file": os.path.basename(path_norm)
                }, dur2)
                self.logger.info(f"[2/3] Finish in {dur2:.2f}s. Nodes: {total_registries}")

                # ==========================================
                # STEP 3: Enrichment (Vision AI)
                # ==========================================
                self.logger.info(f"[3/3] Running Vision AI...")
                t0 = time.time()
                path_multi = process_media(path_norm)
                dur3 = time.time() - t0
                
                # Collecting media
                described_images = media_get_media_count()
                vision_telemetry = get_vision_telemetry()
                
                # Mount dictionary for MD report
                step_3_telemetry = {
                    "Total Images Processed": described_images,
                    "Average per Image": f"{vision_telemetry.get('AVERAGE TIME', 0)}s"
                }
                
                self._append_md_stage(sub, "Step 3: Vision AI", step_3_telemetry, dur3)
                
                self.logger.info(f"[3/3] Finished in {dur3:.2f}s. AI Calls: {vision_telemetry.get('AI CALLS', 0)}")
                self.logger.info(f"[3/3] Finished in {dur3:.2f}s. Total Inference Time: {vision_telemetry.get('TOTAL INF TIME', 0)}s")
                self.logger.info(f"[3/3] Finished in {dur3:.2f}s. Average per image: {vision_telemetry.get('AVERAGE TIME', 0)}s")
                self.logger.info(f"✅ Database for r/{sub} ready!")
            # Finalizing
            total_time = time.time() - self.start_time.timestamp()
            summary = f"\n## 🏁 Final report\n* **Total processing time:** {total_time:.2f}s\n* **Status:** ✅ SUCCESS\n"
            with open(self.log_filename, "a", encoding="utf-8") as f:
                f.write(summary)
                
            self.logger.info("🏁 Pipeline Finished")

        except Exception as e:
            self.logger.error(f"❌ Critital error: {e}", exc_info=True)
            with open(self.log_filename, "a", encoding="utf-8") as f:
                f.write(f"\n## ❌ FATAL ERROR\n```\n{e}\n```\n")
                
    def resume_visual(self, normalized_filepath, multimodal_filepath):
        """
        Resumes Step 3 (Computer Vision) from where it stopped.
        1- Receives the normalized file path and the incomplete multimodal file path.
        2- Compares where it stopped.
        3- Continues processing.
        """
        self.logger.info(f"♻️ Resuming Visual Enrichment...")
        self.logger.info(f"   -> Base File: {os.path.basename(normalized_filepath)}")
        self.logger.info(f"   -> Incomplete File: {os.path.basename(multimodal_filepath)}")

        if not os.path.exists(normalized_filepath):
            self.logger.error("Normalized file not found for resume.")
            return

        # 1. Identify where it stopped
        processed_ids = set()
        if os.path.exists(multimodal_filepath):
            with open(multimodal_filepath, 'r', encoding='utf-8') as f_multi:
                for line in f_multi:
                    try:
                        record = json.loads(line)
                        processed_ids.add(record['id'])
                    except json.JSONDecodeError:
                        pass
        
        self.logger.info(f"🔍 Found {len(processed_ids)} previously processed registries.")

        # 2. Continue from where it stopped (Append Mode)
        t0 = time.time()

        # Reset global counts in processor
        processor_module.media_processed_count = len(processed_ids)
        processor_module.media_count = 0 # Count only *new* enrichments

        with open(normalized_filepath, 'r', encoding='utf-8') as f_in, \
             open(multimodal_filepath, 'a', encoding='utf-8') as f_out: # Mode 'a' (Append)
            
            for line in f_in:
                try:
                    record = json.loads(line)
                except json.JSONDecodeError:
                    continue

                # Skip if already processed
                if record.get('type') == 'metadata_footer' or 'id' not in record:
                    continue
                if record['id'] in processed_ids:
                    continue

                # 3. Process new registry
                original_body = record.get('body', '')

                # "Fast Fail" optimization
                if "http" in original_body or "![" in original_body:
                    enriched_body = processor_module.process_visual_content(original_body)
                    
                    if enriched_body != original_body:
                        record['body'] = enriched_body
                        processor_module.media_count += 1
                
                f_out.write(json.dumps(record, ensure_ascii=False) + "\n")
                f_out.flush() # Force write to disk immediately!
                processor_module.media_processed_count += 1
                
                # Feedback every 10 new registries processed
                new_processed = processor_module.media_processed_count - len(processed_ids)
                if new_processed % 10 == 0 and new_processed > 0:
                     self.logger.info(f"   ... Processed {new_processed} more registries. (Images: {processor_module.media_count})")

        dur = time.time() - t0
        
        # Collect resume telemetry
        described_images = processor_module.media_count
        vision_telemetry = processor_module.get_vision_telemetry()
        
        resume_metrics = {
            "Previously Processed Registries": len(processed_ids),
            "New Intercepted/Described Images": described_images,
            "Updated File": os.path.basename(multimodal_filepath)
        }
        resume_metrics.update(vision_telemetry)
        
        self._append_md_stage("RESUME", "Step 3: Vision AI (Resume)", resume_metrics, dur)
        self.logger.info(f"✅ Resume finished in {dur:.2f}s. New Images: {described_images}")
        
        return multimodal_filepath

if __name__ == "__main__":
    if sys.platform == "win32":
        prevent_sleep_windows(enable=True)
    RESUME_FROM_STEP_3 = True
    try:
        if not RESUME_FROM_STEP_3:
            SUBS = ["brasil"] 
            LIMIT = 1000
            CATEGORY = "top"
            TIMEFRAME = "year" # today, week, month, year, all
            
            builder = RakedditDatabaseBuilder(subreddits=SUBS, limit=LIMIT, category=CATEGORY, timeframe=TIMEFRAME)
            builder.run()
        else:
            builder = RakedditDatabaseBuilder(subreddits=[], limit=0)
            
            # --- FILL WITH YOUR REAL PATHS ---
            NORMALIZED_FILE = "./DATA/2-aggregates/BRASIL_data_normalized_2026-05-08_01-07-11.jsonl"
            INCOMPLETE_MULTI_FILE = "./DATA/3-vision_processing/MULTIMODAL_BRASIL_data_normalized_2026-05-08_01-07-11.jsonl"
            
            builder.resume_visual(NORMALIZED_FILE, INCOMPLETE_MULTI_FILE)
    except Exception as e:
        print(f"Error: {e}")
    finally:
        prevent_sleep_windows(enable=False)
