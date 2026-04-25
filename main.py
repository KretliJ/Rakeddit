import os
import logging
import platform
import datetime
import time
from pathlib import Path

# Módulos do Projeto
from modules.config_loader import config, prevent_sleep_windows
from modules.json_harvester import harvest_subreddit
from modules.processor import (
    extract_from_post, 
    process_media, 
    get_processed_count, 
    media_get_media_count,
    media_get_processed_count
)
from modules.bert_filter import apply_bert_filter, bert_model_name
from modules.infer_engine import orchestrate_full_inference

class RakedditOrchestrator:
    """
    Gerencia a pipeline Rakeddit com foco em rastreabilidade, métricas acadêmicas 
    e tolerância a falhas (resume).
    """
    
    def __init__(self, subreddits=None, limit_per_sub=100, category="top", timeframe="month"):
        self.subreddits = subreddits if subreddits else []
        self.limit = limit_per_sub
        self.category = category
        self.timeframe = timeframe
        self.start_time = datetime.datetime.now()
        
        # Ingestão de configurações do config.ini
        self.models = {
            "Main NLP": config.get('MODELS', 'MAIN_INFER'),
            "Vision": config.get('MODELS', 'IMAGE_READER'),
            "Triage": bert_model_name # Definido no bert_filter
        }
        
        self.paths = {
            "base": config.get_path('PATHS', 'BASE_PATH'),
            "logs": config.get_path('PATHS', 'LOGGING_PATH'),
            "reports": config.get_path('PATHS', 'LOGGING_PATH')
        }
        
        self.log_filename = os.path.join(
            self.paths["reports"], 
            f"REPORT_{self.start_time.strftime('%Y-%m-%d_%H-%M-%S')}.md"
        )
        
        self._init_system()
        self._write_md_header()

    def _init_system(self):
        """Prepara o ambiente e previne hibernação."""
        for path in self.paths.values():
            os.makedirs(path, exist_ok=True)
        if platform.system() == "Windows":
            prevent_sleep_windows(enable=True)
        
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s [%(levelname)s] %(message)s',
            handlers=[logging.StreamHandler()]
        )
        self.logger = logging.getLogger("Rakeddit")

    def _write_md_header(self):
        """Cria o cabeçalho descritivo do relatório acadêmico."""
        header = f"""# 📊 Relatório de Execução Pipeline Rakeddit
**Data:** {self.start_time.strftime('%d/%m/%Y')} | **Início:** {self.start_time.strftime('%H:%M:%S')}

## 🛠 Configuração do Ambiente
* **Sistema Operacional:** {platform.system()} {platform.release()}
* **Modelo NLP Principal:** `{self.models['Main NLP']}`
* **Modelo de Visão:** `{self.models['Vision']}`
* **Modelo de Triagem (BERT):** `{self.models['Triage']}`

## 🎯 Parâmetros de Coleta
* **Subreddits:** {', '.join(self.subreddits) if self.subreddits else 'N/A (Resume Mode)'}
* **Limite por Sub:** {self.limit} posts
* **Filtro Temporal:** {self.category} / {self.timeframe}

---
"""
        with open(self.log_filename, "w", encoding="utf-8") as f:
            f.write(header)

    def _append_md_stage(self, title, stats_dict, duration):
        """Adiciona uma seção de etapa ao arquivo .md em tempo real."""
        content = f"### {title}\n"
        content += f"* **Duração da Etapa:** {duration}\n"
        for key, value in stats_dict.items():
            content += f"* **{key}:** {value}\n"
        content += "\n"
        
        with open(self.log_filename, "a", encoding="utf-8") as f:
            f.write(content)

    # --- MÉTODOS DE ETAPAS ISOLADAS ---
    
    def _run_step_3_5(self, path_multi):
        """Executa a triagem com BERT, mas checa antes se já foi concluída."""
        # Calcula qual seria o nome do arquivo de saída do BERT
        base_dir = os.path.dirname(path_multi)
        expected_filename = os.path.basename(path_multi).replace("MULTIMODAL_", "FILTERED_")
        expected_path = os.path.join(base_dir, expected_filename)
        
        # --- CHECAGEM DE ESTADO: O trator já passou? ---
        if os.path.exists(expected_path):
            self.logger.info(f"🔍 Verificando integridade do cache BERT para: {expected_filename}")
            
            # Conta as linhas de forma eficiente
            with open(path_multi, 'r', encoding='utf-8') as f_in, \
                 open(expected_path, 'r', encoding='utf-8') as f_out:
                linhas_in = sum(1 for _ in f_in)
                linhas_out = sum(1 for _ in f_out)
                
            # Se o número de linhas bater, o arquivo já está 100% triado
            if linhas_in == linhas_out and linhas_in > 0:
                self.logger.info(f"✅ BERT já processou este arquivo ({linhas_out}/{linhas_in} registros). Pulando etapa 3.5!")
                
                # Registra no relatório MD que usamos o cache
                self._append_md_stage("3.5 Triagem em Cascata (BERTimbau) [CACHED]", {
                    "Status": "Pulado (Já processado em execução anterior)",
                    "Arquivo Reutilizado": expected_filename
                }, "0:00:00")
                
                return expected_path
            else:
                self.logger.warning(f"⚠️ Arquivo filtrado corrompido ou incompleto ({linhas_out}/{linhas_in} registros). O trator do BERT vai passar de novo.")

        # Se não existe ou estava incompleto, roda a inferência normalmente
        t0 = time.time()
        path_filtered = apply_bert_filter(path_multi)
        dur = datetime.timedelta(seconds=time.time()-t0)
        self._append_md_stage("3.5 Triagem em Cascata (BERTimbau)", {
            "Status": "Triagem Concluída",
            "Ação": "Marcação da flag 'needs_llama'",
            "Arquivo Gerado": os.path.basename(path_filtered)
        }, dur)
        
        return path_filtered

    def _run_step_4(self, path_filtered):
        t0 = time.time()
        orchestrate_full_inference(path_filtered)
        dur = datetime.timedelta(seconds=time.time()-t0)
        self._append_md_stage("4. Inferência Cognitiva (Llama-3)", {
            "Status": "Processamento Finalizado",
            "Score Normativo": "Calculado e Injetado"
        }, dur)

    # --- MÉTODOS DE EXECUÇÃO PRINCIPAL ---

    def run(self):
        """Executa a pipeline completa do zero iterando pelos subreddits."""
        try:
            for sub in self.subreddits:
                self.logger.info(f"🚀 Iniciando processamento de r/{sub}")
                
                # --- ETAPA 1: HARVESTING ---
                t0 = time.time()
                harvest_subreddit(sub, self.limit, self.category, self.timeframe)
                dur = datetime.timedelta(seconds=time.time()-t0)
                self._append_md_stage(f"1. Harvesting r/{sub}", {"Status": "Concluído"}, dur)

                # --- ETAPA 2: EXTRACTION & FLATTENING ---
                t0 = time.time()
                path_norm = extract_from_post(self.paths["base"], sub)
                dur = datetime.timedelta(seconds=time.time()-t0)
                self._append_md_stage("2. Extração e Achatamento (DFS)", {
                    "Registros Gerados": get_processed_count(),
                    "Arquivo de Saída": os.path.basename(path_norm)
                }, dur)

                # --- ETAPA 3: MULTIMODAL PROCESSING ---
                t0 = time.time()
                path_multi = process_media(path_norm)
                dur = datetime.timedelta(seconds=time.time()-t0)
                self._append_md_stage("3. Enriquecimento Multimodal (Vision AI)", {
                    "Total Comentários": media_get_processed_count(),
                    "Imagens Encontradas": media_get_media_count(),
                    "Arquivo Gerado": os.path.basename(path_multi)
                }, dur)

                # Etapas finais
                path_filtered = self._run_step_3_5(path_multi)
                self._run_step_4(path_filtered)

            self.finalize()
        except Exception as e:
            self._handle_error(e)

    def resume_pipeline(self, filepath):
        """Retoma a execução a partir de um arquivo já processado parcialmente."""
        self.logger.info(f"♻️ Retomando pipeline a partir do arquivo: {os.path.basename(filepath)}")
        try:
            filename = os.path.basename(filepath)
            
            if filename.startswith("MULTIMODAL_"):
                self.logger.info("Iniciando a partir da Etapa 3.5 (BERT Filter)...")
                path_filtered = self._run_step_3_5(filepath)
                self._run_step_4(path_filtered)
                
            elif filename.startswith("FILTERED_"):
                self.logger.info("Iniciando a partir da Etapa 4 (Llama-3 Inference)...")
                self._run_step_4(filepath)
                
            else:
                self.logger.error("Prefixo do arquivo não reconhecido para Resume. Use um arquivo MULTIMODAL_ ou FILTERED_.")
                return

            self.finalize()
        except Exception as e:
            self._handle_error(e)

    def _handle_error(self, e):
        self.logger.error(f"Erro Crítico: {e}", exc_info=True)
        with open(self.log_filename, "a", encoding="utf-8") as f:
            f.write(f"\n# ❌ ERRO FATAL\n{e}")

    def finalize(self):
        """Encerra a execução e gera o sumário final."""
        end_time = datetime.datetime.now()
        total_dur = end_time - self.start_time
        summary = f"""---
## 🏁 Pipeline Concluída
* **Hora de Término:** {end_time.strftime('%H:%M:%S')}
* **Tempo Total de Operação:** {total_dur}
* **Status Final:** ✅ SUCESSO
"""
        with open(self.log_filename, "a", encoding="utf-8") as f:
            f.write(summary)
        
        self.logger.info(f"Processamento concluído. Relatório disponível em: {self.log_filename}")
        if platform.system() == "Windows":
            prevent_sleep_windows(enable=False)

# --- BLOCO DE EXECUÇÃO ---

if __name__ == "__main__":
    
    # MODO 1: Rodar a pipeline do zero
    MODO_RESUME = True
    
    if not MODO_RESUME:
        LISTA_SUBS = ["opiniaoburra"] 
        orchestrator = RakedditOrchestrator(subreddits=LISTA_SUBS, limit_per_sub=100)
        orchestrator.run()
        
    # MODO 2: Retomar a partir de um arquivo de checkpoint (MULTIMODAL_ ou FILTERED_)
    else:
        # Substitua pelo caminho real gerado na sua pasta de logs/dados
        ARQUIVO_ALVO = r"DATA\3-vision_processing\MULTIMODAL_OPINIAOBURRA_data_normalized_2026-04-24_17-52-21.jsonl"
        
        orchestrator = RakedditOrchestrator()
        orchestrator.resume_pipeline(ARQUIVO_ALVO)