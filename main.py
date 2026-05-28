import datetime
import json
import sys
import time
import os
import logging
import platform

from modules.config_loader import config, prevent_sleep_windows
from modules.json_harvester import harvest_subreddit
from modules.processor import (
    extract_from_post, 
    process_media, 
    get_processed_count, 
    media_get_media_count, 
    get_vision_telemetry, 
    process_visual_content, 
    write_metadata_footer, 
    apply_youtube_cleanup_only
)
import modules.processor as processor_module
from modules.infer_engine import orchestrate_full_inference 

class RakedditDatabaseBuilder:
    """Orchestrator focused on building, enriching, and state-managing a dataset pipeline."""
    
    def __init__(self, subreddits, limit=100, category="top", timeframe="all"):
        self.subreddits = subreddits
        self.limit = limit
        self.category = category
        self.timeframe = timeframe
        self.start_time = datetime.datetime.now()
        
        # Paths Setup
        self.base_path = config.get_path('PATHS', 'BASE_PATH')
        self.logs_path = config.get_path('PATHS', 'LOGGING_PATH', fallback="./DATA/logs") 
        os.makedirs(self.logs_path, exist_ok=True)
        
        # Output Files
        self.log_filename = os.path.join(
            self.logs_path, 
            f"DATASET_BUILD_{self.start_time.strftime('%Y-%m-%d_%H-%M-%S')}.md"
        )
        self.system_log_filename = os.path.join(
            self.logs_path, 
            f"SYSTEM_{self.start_time.strftime('%Y-%m-%d_%H-%M-%S')}.log"
        )
        
        # State Management Setup
        self.state_file = config.get_path('PATHS', 'STATE_PATH', fallback="./DATA/pipeline_state.json")
        
        self._init_logging()
        self.pipeline_state = self._load_state()
        self._write_md_header()

    def _init_logging(self):
        """Sets up dual-channel logging (Terminal + File) with descriptive formatting."""
        self.logger = logging.getLogger("RakedditBuilder")
        self.logger.setLevel(logging.INFO)
        
        # Formatting
        formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(message)s', datefmt='%H:%M:%S')
        
        # 1. Stream Handler (Console)
        stream_handler = logging.StreamHandler()
        stream_handler.setFormatter(formatter)
        
        # 2. File Handler (System Log)
        file_handler = logging.FileHandler(self.system_log_filename, encoding='utf-8')
        file_handler.setFormatter(formatter)
        
        # Avoid duplicate logs if instantiated multiple times
        if not self.logger.handlers:
            self.logger.addHandler(stream_handler)
            self.logger.addHandler(file_handler)

    def _load_state(self):
        """Loads the checkpoint file if it exists, enabling resume capabilities."""
        if os.path.exists(self.state_file):
            self.logger.info(f"📂 Found existing pipeline state at {self.state_file}")
            with open(self.state_file, 'r', encoding='utf-8') as f:
                return json.load(f)
        return {}

    def _save_state(self, sub, phase, paths=None):
        """
        Saves current progress. 
        Phases: 1=Harvest, 2=Normalize, 3=Vision, 4=Done
        """
        if sub not in self.pipeline_state:
            self.pipeline_state[sub] = {}
            
        self.pipeline_state[sub]["phase"] = phase
        self.pipeline_state[sub]["updated_at"] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        if paths:
            # Keep track of file paths so subsequent steps know where to read from
            self.pipeline_state[sub]["paths"] = self.pipeline_state[sub].get("paths", {})
            self.pipeline_state[sub]["paths"].update(paths)
            
        with open(self.state_file, 'w', encoding='utf-8') as f:
            json.dump(self.pipeline_state, f, indent=4)
        
        self.logger.info(f"💾 State saved: r/{sub} reached Phase {phase}.")

    def _write_md_header(self):
        """Generates the human-readable Markdown report header."""
        header = f"""# 📊 Dataset Construction Report
**Start date:** {self.start_time.strftime('%d/%m/%Y')} | **Hour:** {self.start_time.strftime('%H:%M:%S')}

## ⚙️ Collection Parameters
* **Subreddits:** {', '.join(self.subreddits)}
* **Limits per Subreddit:** {self.limit} posts
* **Search Filter:** Category `{self.category}` | Period `{self.timeframe}`
* **System:** {platform.system()} {platform.release()}

"""
        with open(self.log_filename, "w", encoding="utf-8") as f:
            f.write(header)

    def _append_md_stage(self, sub, stage_name, metrics, duration):
        """Appends a completed stage block to the Markdown report."""
        content = f"### r/{sub} - {stage_name}\n"
        content += f"* **Runtime:** {duration:.2f}s\n"
        for key, value in metrics.items():
            content += f"* **{key}:** {value}\n"
        content += "\n"
        
        with open(self.log_filename, "a", encoding="utf-8") as f:
            f.write(content)

    def run(self):
        self.logger.info("🚀 INITIALIZING DATABASE BUILDER")
        self.logger.info(f"📄 MD Report routing to: {self.log_filename}")
        self.logger.info(f"🐛 System Logs routing to: {self.system_log_filename}")
        
        try:
            for sub in self.subreddits:
                self.logger.info(f"\n{'='*50}\n 🎯 PROCESSING SUBREDDIT: r/{sub}\n{'='*50}")
                
                # Check current state for resume capabilities
                current_state = self.pipeline_state.get(sub, {})
                current_phase = current_state.get("phase", 0)
                saved_paths = current_state.get("paths", {})
                
                path_norm = saved_paths.get("normalized_path")
                path_multi = saved_paths.get("multimodal_path")

                # ==========================================
                # STEP 1: Raw collection (Harvesting)
                # ==========================================
                if current_phase < 1:
                    self.logger.info(f"[PHASE 1/3] 📡 Harvesting raw JSONs from r/{sub}...")
                    t0 = time.time()
                    harvest_subreddit(sub, self.limit, self.category, self.timeframe)
                    dur1 = time.time() - t0
                    
                    self._append_md_stage(sub, "Step 1: Raw collection (Harvesting)", {
                        "Target Limit": self.limit, 
                        "Status": "Completed"
                    }, dur1)
                    
                    self.logger.info(f"✅ [PHASE 1] Completed in {dur1:.2f}s")
                    self._save_state(sub, phase=1)
                else:
                    self.logger.info(f"⏭️ [PHASE 1] Skipping Harvesting (State indicates already complete)")

                # ==========================================
                # STEP 2: Flattening and structuring (DFS)
                # ==========================================
                if current_phase < 2:
                    self.logger.info(f"[PHASE 2/3] 🧬 Normalizing and flattening comment trees...")
                    t0 = time.time()
                    path_norm = extract_from_post(self.base_path, sub)
                    dur2 = time.time() - t0
                    
                    total_registries = get_processed_count()
                    self._append_md_stage(sub, "Step 2: Flattening (DFS)", {
                        "Extracted nodes (Headers + Comments)": total_registries,
                        "Generated File": os.path.basename(path_norm)
                    }, dur2)
                    
                    self.logger.info(f"✅ [PHASE 2] Completed in {dur2:.2f}s. Extracted Nodes: {total_registries}")
                    self._save_state(sub, phase=2, paths={"normalized_path": path_norm})
                else:
                    self.logger.info(f"⏭️ [PHASE 2] Skipping Normalization (State indicates already complete)")

                # ==========================================
                # STEP 3: Enrichment (Vision AI & Ext Parsing)
                # ==========================================
                if current_phase < 3:
                    self.logger.info(f"[PHASE 3/3] 👁️ Running Vision AI and Multimodal Enrichment...")
                    
                    # Generate the target output path for Step 3
                    vision_dir = config.get_path('PATHS', 'VISION_PATH', fallback="./DATA/3-vision_processing")
                    os.makedirs(vision_dir, exist_ok=True)
                    
                    if not path_norm:
                        self.logger.error("❌ Cannot proceed to Phase 3: normalized_path is missing from state.")
                        continue
                        
                    target_multi = os.path.join(vision_dir, f"MULTIMODAL_{os.path.basename(path_norm)}")
                    
                    t0 = time.time()
                    # If the incomplete file already exists, trigger the built-in visual resume
                    if os.path.exists(target_multi):
                        self.logger.info(f"🔍 Found existing multimodal file. Triggering granular Vision Resume...")
                        self.resume_visual(path_norm, target_multi)
                        path_multi = target_multi
                    else:
                        path_multi = process_media(path_norm)
                        
                    dur3 = time.time() - t0
                    
                    # Collecting media metrics
                    described_images = media_get_media_count()
                    vision_telemetry = get_vision_telemetry()
                    
                    step_3_telemetry = {
                        "Total Images Processed": described_images,
                        "Average per Image": f"{vision_telemetry.get('AVERAGE TIME', 0)}s",
                        "Final Multimodal File": os.path.basename(path_multi)
                    }
                    
                    self._append_md_stage(sub, "Step 3: Vision AI", step_3_telemetry, dur3)
                    
                    self.logger.info(f"✅ [PHASE 3] Completed in {dur3:.2f}s. New AI Calls: {vision_telemetry.get('AI CALLS', 0)}")
                    self._save_state(sub, phase=3, paths={"multimodal_path": path_multi})
                else:
                    self.logger.info(f"⏭️ [PHASE 3] Skipping Vision AI (State indicates already complete)")

                self.logger.info(f"🎉 Pipeline for r/{sub} fully finished!")
                self._save_state(sub, phase=4) # Mark as fully complete

            # Finalizing Output
            total_time = time.time() - self.start_time.timestamp()
            summary = f"\n## 🏁 Final Report\n* **Total Orchestrator Runtime:** {total_time:.2f}s\n* **Status:** ✅ SUCCESS\n"
            with open(self.log_filename, "a", encoding="utf-8") as f:
                f.write(summary)
                
            self.logger.info(f"\n🏁 ALL TASKS COMPLETED. Total Runtime: {total_time:.2f}s")

        except Exception as e:
            self.logger.error(f"❌ CRITICAL FATAL ERROR in Orchestrator: {e}", exc_info=True)
            with open(self.log_filename, "a", encoding="utf-8") as f:
                f.write(f"\n## ❌ FATAL ERROR\n```\n{e}\n```\n")

    def resume_visual(self, normalized_filepath, multimodal_filepath):
        """
        Resumes Step 3 (Computer Vision) from where it stopped.
        Handles granular record-by-record checks to prevent duplicate API calls.
        """
        self.logger.info(f"♻️ RESUMING VISUAL ENRICHMENT...")
        self.logger.info(f"   -> Base Normal File: {os.path.basename(normalized_filepath)}")
        self.logger.info(f"   -> Incomplete Output File: {os.path.basename(multimodal_filepath)}")

        if not os.path.exists(normalized_filepath):
            self.logger.error("❌ Normalized base file not found for resume.")
            return

        # 1. Identify where it stopped
        processed_ids = set()
        if os.path.exists(multimodal_filepath):
            with open(multimodal_filepath, 'r', encoding='utf-8') as f_multi:
                for line in f_multi:
                    try:
                        record = json.loads(line)
                        if 'id' in record:
                            processed_ids.add(record['id'])
                    except json.JSONDecodeError:
                        pass
        
        self.logger.info(f"🔍 Found {len(processed_ids)} previously processed JSON registries. Skipping them...")

        # 2. Continue from where it stopped
        t0 = time.time()
        processor_module.media_processed_count = len(processed_ids)
        processor_module.media_count = 0 # Count only *new* enrichments

        with open(normalized_filepath, 'r', encoding='utf-8') as f_in, \
             open(multimodal_filepath, 'a', encoding='utf-8') as f_out:
            
            for line in f_in:
                try:
                    record = json.loads(line)
                except json.JSONDecodeError:
                    continue

                if record.get('type') == 'metadata_footer' or 'id' not in record:
                    continue
                if record['id'] in processed_ids:
                    continue

                # 3. Process new registry
                original_body = record.get('body', '')

                # Fast Fail logic including YouTube checks
                if "http" in original_body or "![" in original_body or "youtube" in original_body.lower() or "youtu.be" in original_body.lower():
                    enriched_body = processor_module.process_visual_content(original_body)
                    
                    if enriched_body != original_body:
                        record['body'] = enriched_body
                        processor_module.media_count += 1
                
                f_out.write(json.dumps(record, ensure_ascii=False) + "\n")
                f_out.flush() # Force write to disk immediately
                processor_module.media_processed_count += 1
                
                new_processed = processor_module.media_processed_count - len(processed_ids)
                if new_processed % 50 == 0 and new_processed > 0:
                     self.logger.info(f"   ... Processed {new_processed} more registries. (Intercepted/Described Media: {processor_module.media_count})")

        dur = time.time() - t0
        described_images = processor_module.media_count
        vision_telemetry = processor_module.get_vision_telemetry()
        
        resume_metrics = {
            "Previously Processed Registries": len(processed_ids),
            "New Intercepted/Described Images": described_images,
            "Updated File": os.path.basename(multimodal_filepath)
        }
        resume_metrics.update(vision_telemetry)
        
        self._append_md_stage("RESUME", "Step 3: Vision AI (Resume Phase)", resume_metrics, dur)
        self.logger.info(f"✅ Granular Resume finished in {dur:.2f}s. New Media Captured: {described_images}")
        
        return multimodal_filepath


# ==========================================
# HELPER DE MENU (OTIMIZAÇÃO)
# ==========================================
def pick_file_from_dir(directory):
    """Função reutilizável para listar e selecionar arquivos .jsonl em um diretório."""
    if not os.path.exists(directory):
        print(f"❌ Diretório não encontrado: {directory}")
        return None
    
    files = [f for f in os.listdir(directory) if f.endswith('.jsonl')]
    
    if not files:
        print(f"❌ Nenhum arquivo .jsonl encontrado em {directory}.")
        return None
        
    print("\n📂 Arquivos disponíveis:")
    for i, f in enumerate(files):
        print(f"  [{i}] {f}")
        
    try:
        f_idx = int(input("\n⌨️ Digite o número do arquivo: ").strip())
        return os.path.join(directory, files[f_idx])
    except (ValueError, IndexError):
        print("❌ Seleção inválida.")
        return None


if __name__ == "__main__":
    if sys.platform == "win32":
        prevent_sleep_windows(enable=True)
        
    try:
        print("\n" + "="*55)
        print(" 🚀 RAKEDDIT ORCHESTRATOR - STATEFUL PIPELINE ")
        print("="*55)
        print(" [1] INICIAR/RETOMAR COLETA AUTOMÁTICA (State Manager)")
        print(" [2] RETOMAR VISION AI MANUALMENTE (Escolher arquivo)")
        print(" [3] UNIFICAR TUDO (Junta aggregates e roda Vision)")
        print(" [4] YOUTUBE CLEANUP (Retroativo em arquivo)")
        print(" [5] ESCREVER METADATA FOOTER (Recalcular arquivo)")
        print(" [6] INFERÊNCIA DE SENTIMENTO")
        print("="*55)
        
        choice = input("\n⌨️ Selecione uma opção (1-6): ").strip()
        
        if choice == '1':
            print("\n--- Configuração de Coleta ---")
            subs_input = input("Subreddits (separados por vírgula): ").strip()
            SUBS = [s.strip() for s in subs_input.split(",") if s.strip()]
            
            try:
                LIMIT = int(input("Limite de posts por sub (ex: 1000): ").strip())
            except ValueError:
                print("⚠️ Valor inválido. Usando padrão (100).")
                LIMIT = 100
                
            CATEGORY = input("Categoria (top/new/hot) [Padrão: top]: ").strip() or "top"
            TIMEFRAME = input("Período (day/week/month/year/all) [Padrão: year]: ").strip() or "year"
            
            builder = RakedditDatabaseBuilder(subreddits=SUBS, limit=LIMIT, category=CATEGORY, timeframe=TIMEFRAME)
            builder.run()

        elif choice == '2':
            print("\n--- Retomar Processamento Vision AI ---")
            aggs_dir = config.get_path('PATHS', 'AGGREGATES_PATH', fallback="./DATA/2-aggregates")
            selected_file = pick_file_from_dir(aggs_dir)
            
            if selected_file:
                vision_dir = config.get_path('PATHS', 'VISION_PATH', fallback="./DATA/3-vision_processing")
                os.makedirs(vision_dir, exist_ok=True)
                INCOMPLETE_MULTI_FILE = os.path.join(vision_dir, f"MULTIMODAL_{os.path.basename(selected_file)}")
                
                builder = RakedditDatabaseBuilder(subreddits=[], limit=0)
                builder.resume_visual(selected_file, INCOMPLETE_MULTI_FILE)

        elif choice == '3':
            print("\n--- Unificação de Dataset ---")
            aggs_dir = config.get_path('PATHS', 'AGGREGATES_PATH', fallback="./DATA/2-aggregates")
            vision_dir = config.get_path('PATHS', 'VISION_PATH', fallback="./DATA/3-vision_processing")
            os.makedirs(vision_dir, exist_ok=True)
            
            temp_file = os.path.join(vision_dir, "MULTIMODAL_TEMP.jsonl")
            final_file = os.path.join(vision_dir, "MULTIMODAL_FINAL.jsonl")
            
            print(f"[*] Unificando arquivos de {aggs_dir}...")
            files = [f for f in os.listdir(aggs_dir) if f.endswith('.jsonl')]
            
            total_records = 0
            with open(temp_file, 'w', encoding='utf-8') as f_out:
                for file_name in files:
                    filepath = os.path.join(aggs_dir, file_name)
                    with open(filepath, 'r', encoding='utf-8') as f_in:
                        for line in f_in:
                            if '"type": "metadata_footer"' not in line:
                                f_out.write(line)
                                total_records += 1
                                
            print(f"[+] Unificação concluída: {total_records} registros em {temp_file}")
            
            print("\n[*] Iniciando processamento de Mídia (Qwen e Parsers Externos)...")
            builder = RakedditDatabaseBuilder(subreddits=[], limit=0)
            builder.resume_visual(temp_file, final_file)
            write_metadata_footer(final_file) # Ensure footer is placed at the end

        elif choice == '4':
            print("\n--- YouTube Cleanup Retroativo ---")
            vision_dir = config.get_path('PATHS', 'VISION_PATH', fallback="./DATA/3-vision_processing")
            selected_file = pick_file_from_dir(vision_dir)
            
            if selected_file:
                output_file = selected_file.replace(".jsonl", "_YT_CLEANED.jsonl")
                apply_youtube_cleanup_only(selected_file, output_file)

        elif choice == '5':
            print("\n--- Escrever/Recalcular Metadata Footer ---")
            vision_dir = config.get_path('PATHS', 'VISION_PATH', fallback="./DATA/3-vision_processing")
            selected_file = pick_file_from_dir(vision_dir)
            
            if selected_file:
                write_metadata_footer(selected_file)

        elif choice == '6':
            print("\n--- Inferência de Sentimento ---")
            vision_dir = config.get_path('PATHS', 'VISION_PATH', fallback="./DATA/3-vision_processing")
            selected_file = pick_file_from_dir(vision_dir)
            
            if selected_file:
                orchestrate_full_inference(selected_file)

        else:
            print("❌ Opção inválida. Encerrando.")

    except ValueError:
        print("❌ Erro de formatação no input (você digitou texto onde era número?). Encerrando.")
    except KeyboardInterrupt:
        print("\n⚠️ Interrupção manual (Ctrl+C). O estado foi salvo e pode ser retomado.")
    except Exception as e:
        print(f"❌ Erro Crítico no Orquestrador: {e}")
    finally:
        prevent_sleep_windows(enable=False)
        print("\n👋 Sessão Encerrada.")