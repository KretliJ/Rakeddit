import tkinter as tk
from tkinter import ttk, scrolledtext, messagebox
import threading
import sys
import os
import subprocess
from Methods import AnalyticsEngine
from Utilities import Config

class RedirectText:
    def __init__(self, text_ctrl, root):
        self.output = text_ctrl
        self.root = root

    def write(self, string):
        # Dispatch text draw to Tkinter main thread
        self.root.after(0, self._sync_write, string)
        
    def _sync_write(self, string):
        self.output.insert(tk.END, string)
        self.output.see(tk.END)

    def flush(self):
        pass

class AppGUI:
    def __init__(self, root):
        self.root = root
        self.root.title("Unified Analytics Orchestrator (Network & Stats)")
        self.root.geometry("900x700")
        self.engine = AnalyticsEngine()
        
        # Safety lock: prevents concurrent task execution
        self.is_running = False

        self.analysis_tasks = {
            "1. Structural Analysis CCDFs": self.engine.plot_structural_ccdfs,
            "2. Motifs Analysis": self.engine.run_motif_analysis,
            "3. Average Score (Fig 3)": self.engine.run_figure3_average_score,
            "4. Triadic Analysis": self.engine.run_triadic_analysis,
            "5. Taxonomy Analysis": self.engine.run_taxonomy_analysis,
            "6. Taxonomy Trendline Graph": self.engine.run_rq3_analysis,
            "7. Statistical Report": self.engine.run_statistical_reports,
            "8. Homophily Analysis": self.engine.run_user_homophily_analysis,
            "9. Ablation Blind vs Multimodal": self.engine.run_ablation_matrix_analysis
        }
        
        self.setup_ui()
        sys.stdout = RedirectText(self.console, self.root)

    def setup_ui(self):
        style = ttk.Style()
        style.configure("TButton", font=("Helvetica", 10), padding=6)
        style.configure("Header.TLabel", font=("Helvetica", 14, "bold"))
        style.configure("Switch.TButton", font=("Helvetica", 10, "bold"), foreground="#2980b9")
        
        left_frame = ttk.Frame(self.root, padding=10)
        left_frame.pack(side=tk.LEFT, fill=tk.Y)
        
        right_frame = ttk.Frame(self.root, padding=10)
        right_frame.pack(side=tk.RIGHT, fill=tk.BOTH, expand=True)

        ttk.Label(left_frame, text="Control Panel", style="Header.TLabel").pack(pady=(0, 10))
        
        # Panel switch button
        self.btn_switch = ttk.Button(left_frame, text="🔄 Switch to NLP & GPU Panel", style="Switch.TButton", command=self.switch_to_nlp)
        self.btn_switch.pack(fill=tk.X, pady=(0, 15), ipady=5)

        # 1. Data load button
        self.btn_load = ttk.Button(left_frame, text="Load / Extract Data", command=self.thread_load_data)
        self.btn_load.pack(fill=tk.X, pady=5)

        ttk.Separator(left_frame, orient=tk.HORIZONTAL).pack(fill=tk.X, pady=15)

        # 2. Analysis module selector
        ttk.Label(left_frame, text="Select Analysis Module:", font=("Helvetica", 10, "italic")).pack(anchor=tk.W, pady=(0, 5))
        self.combo_var = tk.StringVar()
        self.combo = ttk.Combobox(left_frame, textvariable=self.combo_var, state="disabled")
        self.combo['values'] = list(self.analysis_tasks.keys())
        if self.analysis_tasks:
            self.combo.current(0)
        self.combo.pack(fill=tk.X, pady=5)

        # 3. Grouping strategy selector
        ttk.Label(left_frame, text="Grouping Strategy:", font=("Helvetica", 10, "italic")).pack(anchor=tk.W, pady=(10, 5))
        self.grouping_var = tk.StringVar()
        self.grouping_combo = ttk.Combobox(left_frame, textvariable=self.grouping_var, state="disabled", values=["Quartiles", "Categories", "Sentiments"])
        self.grouping_combo.current(0) 
        self.grouping_combo.pack(fill=tk.X, pady=5)

        self.interactive_var = tk.BooleanVar(value=False)
        self.chk_interactive = ttk.Checkbutton(left_frame, text="Filter: Interactive Cascades Only", variable=self.interactive_var, state=tk.DISABLED)
        self.chk_interactive.pack(fill=tk.X, pady=(5, 15))

        # 4. Execution buttons
        btn_frame = ttk.Frame(left_frame)
        btn_frame.pack(fill=tk.X, pady=15)
        
        self.btn_run = ttk.Button(btn_frame, text="Run Selected", state=tk.DISABLED, command=self.trigger_selected_analysis)
        self.btn_run.pack(side=tk.LEFT, fill=tk.X, expand=True, padx=(0, 5))

        self.btn_run_all = ttk.Button(btn_frame, text="Run All", state=tk.DISABLED, command=self.trigger_all_analyses)
        self.btn_run_all.pack(side=tk.RIGHT, fill=tk.X, expand=True, padx=(5, 0))

        ttk.Separator(left_frame, orient='horizontal').pack(fill=tk.X, pady=15)
        
        # 5. Cache management (danger zone)
        ttk.Label(left_frame, text="Cache Management", font=("Helvetica", 10, "bold")).pack(anchor=tk.W, pady=(0, 5))
        self.btn_nuke_network = ttk.Button(left_frame, text="☢ Nuke Network Cache", command=self.nuke_network_cache)
        self.btn_nuke_network.pack(fill=tk.X, pady=2)

        # Right-side execution console
        ttk.Label(right_frame, text="Execution Console", style="Header.TLabel").pack(anchor=tk.W, pady=(0, 5))
        self.console = scrolledtext.ScrolledText(right_frame, wrap=tk.WORD, font=("Consolas", 9), bg="#1e1e1e", fg="#d4d4d4")
        self.console.pack(fill=tk.BOTH, expand=True)

        print("[SYSTEM] Welcome to the Network Analytics Suite.\n[SYSTEM] Ready. Awaiting Data Extraction...")

    def switch_to_nlp(self):
        if self.is_running:
            messagebox.showwarning("Atenção", "Uma análise está atualmente em execução!\nAguarde o término do processo antes de trocar de painel.")
            return
        
        # Launch the other panel script and close this window
        subprocess.Popen([sys.executable, "GUI_NLP.py"])
        self.root.destroy()

    def thread_load_data(self):
        self.btn_load.config(state=tk.DISABLED)
        self.is_running = True
        threading.Thread(target=self._process_load, daemon=True).start()

    def _process_load(self):
        success = self.engine.load_or_extract_data()
        self.is_running = False
        if success:
            self.root.after(0, self.enable_analysis_controls)
            print("\n[SUCCESS] Data successfully loaded into RAM. Select a module and hit 'Run Analysis' or 'Run All'.")
        else:
            self.root.after(0, lambda: self.btn_load.config(state=tk.NORMAL))
            print("\n[ERROR] Failed to load or extract data.")

    def enable_analysis_controls(self):
        self.combo.config(state="readonly")
        self.grouping_combo.config(state="readonly")
        self.chk_interactive.config(state=tk.NORMAL)
        self.btn_run.config(state=tk.NORMAL)
        self.btn_run_all.config(state=tk.NORMAL)

    def trigger_selected_analysis(self):
        selected_task_name = self.combo_var.get()
        selected_grouping = self.grouping_var.get()
        is_interactive = self.interactive_var.get()
        if selected_task_name in self.analysis_tasks:
            target_function = self.analysis_tasks[selected_task_name]
            self.run_task_in_thread(target_function, selected_grouping, is_interactive)

    def trigger_all_analyses(self):
        selected_grouping = self.grouping_var.get()
        is_interactive = self.interactive_var.get()
        self.run_all_tasks_in_thread(selected_grouping, is_interactive)

    def run_task_in_thread(self, task_func, grouping, is_interactive):
        def wrapper():
            self.is_running = True
            self.root.after(0, self._set_controls_state, tk.DISABLED)
            task_func(grouping=grouping, interactive_only=is_interactive)
            print("\n[✔] Task completed successfully.")
            self.root.after(0, self._set_controls_state, tk.NORMAL)
            self.is_running = False
        threading.Thread(target=wrapper, daemon=True).start()

    def run_all_tasks_in_thread(self, grouping, is_interactive):
        def wrapper():
            self.is_running = True
            self.root.after(0, self._set_controls_state, tk.DISABLED)
            print(f"\n[🚀] Starting FULL PIPELINE for {grouping} (Interactive Only: {is_interactive})...")
            
            for task_name, task_func in self.analysis_tasks.items():
                print(f"\n---> Running module: {task_name}")
                try:
                    task_func(grouping=grouping, interactive_only=is_interactive)
                except Exception as e:
                    print(f"  [ERROR] Failed to run {task_name}: {e}")
            
            print("\n[✔✔✔] ALL TASKS COMPLETED SUCCESSFULLY.")
            self.root.after(0, self._set_controls_state, tk.NORMAL)
            self.is_running = False
        threading.Thread(target=wrapper, daemon=True).start()

    def _set_controls_state(self, state):
        self.btn_run.config(state=state)
        self.btn_run_all.config(state=state)
        self.btn_switch.config(state=state)
        self.combo.config(state="readonly" if state == tk.NORMAL else tk.DISABLED)
        self.grouping_combo.config(state="readonly" if state == tk.NORMAL else tk.DISABLED)

    def nuke_network_cache(self):
        parquet_path = Config.CACHE_PATH
        triads_path = Config.CACHE_PATH.replace('.parquet', '_triads.json')
        homophily_path = Config.CACHE_PATH.replace('.parquet', '_homophily.json')
        
        files_deleted = 0
        deleted_names = []
        
        for path in [parquet_path, triads_path, homophily_path]:
            if os.path.exists(path):
                try:
                    os.remove(path)
                    files_deleted += 1
                    deleted_names.append(os.path.basename(path))
                except Exception as e:
                    print(f"  [ERROR] Não foi possível apagar {path}: {e}")

        if files_deleted > 0:
            msg = f"BOOM! {files_deleted} network cache file(s) deleted:\n" + "\n".join([f"- {name}" for name in deleted_names])
            print(f"\n[☢] {msg}")
            messagebox.showinfo("Network Cache Nuked", msg)
        else:
            print("[INFO] ℹ️ Network cache is already empty.")
            messagebox.showinfo("ℹ️ Cache empty ℹ️", "No parquet found")

if __name__ == "__main__":
    root = tk.Tk()
    app = AppGUI(root)
    root.mainloop()