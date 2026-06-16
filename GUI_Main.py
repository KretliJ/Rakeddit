import tkinter as tk
from tkinter import ttk, scrolledtext, filedialog, messagebox
import threading
import sys
import os
import json
import time

# Importações originais do seu main.py
from modules.config_loader import config, prevent_sleep_windows
from main import RakedditDatabaseBuilder # Reaproveita a classe do main.py
import modules.processor as processor_module
from modules.processor import (
    write_metadata_footer, 
    apply_youtube_cleanup_only
)
from modules.infer_engine import orchestrate_full_inference

class RedirectText:
    def __init__(self, text_ctrl):
        self.output = text_ctrl

    def write(self, string):
        self.output.insert(tk.END, string)
        self.output.see(tk.END)

    def flush(self):
        pass

class HarvesterGUI:
    def __init__(self, root):
        self.root = root
        self.root.title("Rakeddit - Harvester & Processing Orchestrator")
        self.root.geometry("950x700")
        
        # Prevenir que o Windows durma durante processamentos longos
        if sys.platform == "win32":
            prevent_sleep_windows(enable=True)
            
        self.root.protocol("WM_DELETE_WINDOW", self.on_closing)
        
        self.setup_ui()
        sys.stdout = RedirectText(self.console)
        
        print("🚀 Welcome to Rakeddit Harvester Orchestrator.\nReady for data collection and processing...")

    def setup_ui(self):
        style = ttk.Style()
        style.configure("TButton", font=("Helvetica", 10), padding=6)
        style.configure("Header.TLabel", font=("Helvetica", 12, "bold"))
        
        # --- Layout Principal ---
        left_frame = ttk.Frame(self.root, padding=10, width=350)
        left_frame.pack(side=tk.LEFT, fill=tk.Y)
        left_frame.pack_propagate(False) # Força a manter a largura
        
        right_frame = ttk.Frame(self.root, padding=10)
        right_frame.pack(side=tk.RIGHT, fill=tk.BOTH, expand=True)

        # =========================================================
        # PAINEL ESQUERDO: CONTROLES
        # =========================================================
        
        # --- SECÇÃO 1: STATEFUL PIPELINE ---
        ttk.Label(left_frame, text="1. Automated Collection Pipeline", style="Header.TLabel").pack(anchor=tk.W, pady=(0, 10))
        
        ttk.Label(left_frame, text="Subreddits (comma separated):").pack(anchor=tk.W)
        self.ent_subs = ttk.Entry(left_frame)
        self.ent_subs.pack(fill=tk.X, pady=(0, 5))
        
        ttk.Label(left_frame, text="Limit per Subreddit:").pack(anchor=tk.W)
        self.ent_limit = ttk.Entry(left_frame)
        self.ent_limit.insert(0, "100")
        self.ent_limit.pack(fill=tk.X, pady=(0, 5))
        
        ttk.Label(left_frame, text="Category:").pack(anchor=tk.W)
        self.combo_cat = ttk.Combobox(left_frame, values=["top", "new", "hot"], state="readonly")
        self.combo_cat.current(0)
        self.combo_cat.pack(fill=tk.X, pady=(0, 5))
        
        ttk.Label(left_frame, text="Timeframe:").pack(anchor=tk.W)
        self.combo_time = ttk.Combobox(left_frame, values=["all", "year", "month", "week", "day"], state="readonly")
        self.combo_time.current(1) # year por padrão
        self.combo_time.pack(fill=tk.X, pady=(0, 10))
        
        self.btn_run_pipeline = ttk.Button(left_frame, text="▶ Run / Resume Pipeline", command=self.run_pipeline)
        self.btn_run_pipeline.pack(fill=tk.X, pady=(0, 15))
        
        ttk.Separator(left_frame, orient=tk.HORIZONTAL).pack(fill=tk.X, pady=10)
        
        # --- SECÇÃO 2: FERRAMENTAS AVULSAS ---
        ttk.Label(left_frame, text="2. Standalone Operations", style="Header.TLabel").pack(anchor=tk.W, pady=(0, 10))
        
        self.btn_resume_vision = ttk.Button(left_frame, text="Resume Vision AI (Select File)", command=self.resume_vision)
        self.btn_resume_vision.pack(fill=tk.X, pady=3)
        
        self.btn_unify = ttk.Button(left_frame, text="Unify Aggregates & Run Vision", command=self.unify_and_vision)
        self.btn_unify.pack(fill=tk.X, pady=3)
        
        self.btn_yt_cleanup = ttk.Button(left_frame, text="YouTube Cleanup (Select File)", command=self.youtube_cleanup)
        self.btn_yt_cleanup.pack(fill=tk.X, pady=3)
        
        self.btn_footer = ttk.Button(left_frame, text="Write Metadata Footer (Select File)", command=self.write_footer)
        self.btn_footer.pack(fill=tk.X, pady=3)
        
        self.btn_inference = ttk.Button(left_frame, text="Sentiment Inference (Select File)", command=self.run_inference)
        self.btn_inference.pack(fill=tk.X, pady=3)
        
        # Lista de todos os botões para desativar durante a execução
        self.all_buttons = [
            self.btn_run_pipeline, self.btn_resume_vision, self.btn_unify, 
            self.btn_yt_cleanup, self.btn_footer, self.btn_inference
        ]

        # =========================================================
        # PAINEL DIREITO: CONSOLA
        # =========================================================
        ttk.Label(right_frame, text="System Console", style="Header.TLabel").pack(anchor=tk.W, pady=(0, 5))
        self.console = scrolledtext.ScrolledText(right_frame, wrap=tk.WORD, font=("Consolas", 9), bg="#1e1e1e", fg="#d4d4d4")
        self.console.pack(fill=tk.BOTH, expand=True)

    def on_closing(self):
        if sys.platform == "win32":
            prevent_sleep_windows(enable=False)
        self.root.destroy()

    def set_gui_state(self, state):
        for btn in self.all_buttons:
            btn.config(state=state)
        self.ent_subs.config(state=state)
        self.ent_limit.config(state=state)
        self.combo_cat.config(state="readonly" if state == tk.NORMAL else tk.DISABLED)
        self.combo_time.config(state="readonly" if state == tk.NORMAL else tk.DISABLED)

    def execute_in_thread(self, target_func, *args):
        """Bloqueia a interface, corre a função na thread, desbloqueia no fim."""
        def wrapper():
            self.root.after(0, self.set_gui_state, tk.DISABLED)
            try:
                target_func(*args)
            except Exception as e:
                print(f"\n❌ ERRO CRÍTICO: {e}")
            finally:
                self.root.after(0, self.set_gui_state, tk.NORMAL)
                print("\n[✔] Operation finished.")
                
        threading.Thread(target=wrapper, daemon=True).start()

    def pick_file(self, fallback_dir):
        directory = config.get_path('PATHS', fallback_dir, fallback=f"./DATA/{fallback_dir}")
        if not os.path.exists(directory):
            os.makedirs(directory, exist_ok=True)
            
        file_path = filedialog.askopenfilename(
            initialdir=directory,
            title="Select a JSONL file",
            filetypes=[("JSON Lines", "*.jsonl"), ("All Files", "*.*")]
        )
        return file_path

    # =========================================================
    # FUNÇÕES DE EXECUÇÃO (Mapeadas das opções do terminal)
    # =========================================================
    def run_pipeline(self):
        subs_raw = self.ent_subs.get()
        if not subs_raw:
            messagebox.showwarning("Warning", "Please enter at least one subreddit.")
            return
            
        subs = [s.strip() for s in subs_raw.split(",") if s.strip()]
        
        try:
            limit = int(self.ent_limit.get().strip())
        except ValueError:
            messagebox.showwarning("Warning", "Limit must be an integer.")
            return
            
        category = self.combo_cat.get()
        timeframe = self.combo_time.get()
        
        def task():
            builder = RakedditDatabaseBuilder(subreddits=subs, limit=limit, category=category, timeframe=timeframe)
            builder.run()
            
        self.execute_in_thread(task)

    def resume_vision(self):
        filepath = self.pick_file('AGGREGATES_PATH')
        if not filepath: return
        
        def task():
            print(f"\n--- Retomar Processamento Vision AI ---")
            vision_dir = config.get_path('PATHS', 'VISION_PATH', fallback="./DATA/3-vision_processing")
            os.makedirs(vision_dir, exist_ok=True)
            incomplete_file = os.path.join(vision_dir, f"MULTIMODAL_{os.path.basename(filepath)}")
            
            builder = RakedditDatabaseBuilder(subreddits=[], limit=0)
            builder.resume_visual(filepath, incomplete_file)
            
        self.execute_in_thread(task)

    def unify_and_vision(self):
        def task():
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
            write_metadata_footer(final_file)
            
        self.execute_in_thread(task)

    def youtube_cleanup(self):
        filepath = self.pick_file('VISION_PATH')
        if not filepath: return
        
        def task():
            print(f"\n--- YouTube Cleanup Retroativo ---")
            output_file = filepath.replace(".jsonl", "_YT_CLEANED.jsonl")
            apply_youtube_cleanup_only(filepath, output_file)
            
        self.execute_in_thread(task)

    def write_footer(self):
        filepath = self.pick_file('VISION_PATH')
        if not filepath: return
        
        def task():
            print(f"\n--- Escrever/Recalcular Metadata Footer ---")
            write_metadata_footer(filepath)
            
        self.execute_in_thread(task)

    def run_inference(self):
        filepath = self.pick_file('VISION_PATH')
        if not filepath: return
        
        def task():
            print(f"\n--- Inferência de Sentimento ---")
            orchestrate_full_inference(filepath)
            
        self.execute_in_thread(task)


if __name__ == "__main__":
    root = tk.Tk()
    app = HarvesterGUI(root)
    root.mainloop()