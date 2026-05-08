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
        content += f"* **Processing Time:** {duration:.2f}s\n"
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
                    "Status": "Completed"
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
                
                # Montando o dicionário para o relatório MD
                step_3_telemetry = {
                    "Total de Comentários Modificados": described_images,
                    "Arquivo Final do Dataset": os.path.basename(path_multi)
                }
                # Fundindo os dicionários (adiciona a telemetria avançada)
                step_3_telemetry.update(vision_telemetry)
                
                self._append_md_stage(sub, "Step 3: Computer Vision", step_3_telemetry, dur3)
                
                self.logger.info(f"[3/3] Finished in {dur3:.2f}s. AI Calls: {vision_telemetry['AI CALLS']}")
                self.logger.info(f"[3/3] Finished in {dur3:.2f}s. Total Inference Time: {vision_telemetry['TOTAL INF TIME']}s")
                self.logger.info(f"[3/3] Finished in {dur3:.2f}s. Average per image: {vision_telemetry['AVERAGE TIME']}s")
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
        Retoma a Etapa 3 (Visão Computacional) de onde parou.
        1- Recebe o caminho do arquivo normalizado e do multimodal incompleto.
        2- Compara onde parou.
        3- Continua o processamento.
        """
        self.logger.info(f"♻️ Retomando Enriquecimento Visual...")
        self.logger.info(f"   -> Arquivo Base: {os.path.basename(normalized_filepath)}")
        self.logger.info(f"   -> Arquivo Incompleto: {os.path.basename(multimodal_filepath)}")

        if not os.path.exists(normalized_filepath):
            self.logger.error("Arquivo normalizado não encontrado para resume.")
            return

        # 1. Identificar onde parou
        processed_ids = set()
        if os.path.exists(multimodal_filepath):
            with open(multimodal_filepath, 'r', encoding='utf-8') as f_multi:
                for line in f_multi:
                    try:
                        record = json.loads(line)
                        processed_ids.add(record['id'])
                    except json.JSONDecodeError:
                        pass
        
        self.logger.info(f"🔍 Encontrados {len(processed_ids)} registros já processados.")

        # 2. Continuar de onde parou (Modo Append)
        t0 = time.time()

        # Resetar as contagens globais no processor
        processor_module.media_processed_count = len(processed_ids)
        processor_module.media_count = 0 # Contaremos apenas os *novos* enriquecimentos

        with open(normalized_filepath, 'r', encoding='utf-8') as f_in, \
             open(multimodal_filepath, 'a', encoding='utf-8') as f_out: # Modo 'a' (Append)
            
            for line in f_in:
                try:
                    record = json.loads(line)
                except json.JSONDecodeError:
                    continue

                # Pular se já foi processado
                if record.get('type') == 'metadata_footer' or 'id' not in record:
                    continue
                if record['id'] in processed_ids:
                    continue

                # 3. Processar o novo registro
                original_body = record.get('body', '')

                # "Fast Fail" optimization
                if "http" in original_body or "![" in original_body:
                    enriched_body = processor_module.process_visual_content(original_body)
                    
                    if enriched_body != original_body:
                        record['body'] = enriched_body
                        processor_module.media_count += 1
                
                f_out.write(json.dumps(record, ensure_ascii=False) + "\n")
                f_out.flush() # Forçar a gravação no disco imediatamente!
                processor_module.media_processed_count += 1
                
                # Feedback a cada 10 registros novos processados
                novos_processados = processor_module.media_processed_count - len(processed_ids)
                if novos_processados % 10 == 0 and novos_processados > 0:
                     self.logger.info(f"   ... Processados mais {novos_processados} registros. (Imagens: {processor_module.media_count})")

        dur = time.time() - t0
        
        # Coletar telemetria do resume
        imagens_descritas = processor_module.media_count
        telemetria_visao = processor_module.get_vision_telemetry()
        
        metricas_resume = {
            "Registros Previamente Processados": len(processed_ids),
            "Novas Imagens Interceptadas/Descritas": imagens_descritas,
            "Arquivo Atualizado": os.path.basename(multimodal_filepath)
        }
        metricas_resume.update(telemetria_visao)
        
        # Como não sabemos o subreddit exato do resume sem olhar o JSON inteiro, usamos "RESUME"
        self._append_md_stage("RESUME", "Etapa 3: Visão Computacional (Retomada)", metricas_resume, dur)
        self.logger.info(f"✅ Resume concluído em {dur:.2f}s. Novas Imagens: {imagens_descritas}")
        
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
            
            # --- PREENCHA COM OS SEUS CAMINHOS REAIS ---
            NORMALIZED_FILE = "./DATA/2-aggregates/BRASIL_data_normalized_2026-05-08_01-07-11.jsonl"
            INCOMPLETE_MULTI_FILE = "./DATA/3-vision_processing/MULTIMODAL_BRASIL_data_normalized_2026-05-08_01-07-11.jsonl"
            
            builder.resume_visual(NORMALIZED_FILE, INCOMPLETE_MULTI_FILE)
    except Exception as e:
        print(f"Error: {e}")
    finally:
        prevent_sleep_windows(enable=False)