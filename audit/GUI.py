import tkinter as tk
from tkinter import ttk, scrolledtext
import threading
import sys
from Methods import AnalyticsEngine

class RedirectText:
    def __init__(self, text_ctrl):
        self.output = text_ctrl

    def write(self, string):
        self.output.insert(tk.END, string)
        self.output.see(tk.END)

    def flush(self):
        pass

class AppGUI:
    def __init__(self, root):
        self.root = root
        self.root.title("Unified Analytics Orchestrator")
        self.root.geometry("900x650")
        self.engine = AnalyticsEngine()
        
        self.analysis_tasks = {
            "1. Structural Analysis (Fig 1)": self.engine.plot_structural_ccdfs,
            "2. Motifs Analysis (Fig 2)": self.engine.run_motif_analysis,
            "3. Average Score (Fig 3)": self.engine.run_figure3_average_score,
            "4. Triadic Analysis (RQ2)": self.engine.run_triadic_analysis,
            "5. Taxonomy Analysis (BCC)": self.engine.run_taxonomy_analysis,
            "6. Virality vs Sentiment (RQ3)": self.engine.run_rq3_analysis,
            "7. Statistical Report": self.engine.run_statistical_reports
        }
        
        self.setup_ui()
        sys.stdout = RedirectText(self.console)

    def setup_ui(self):
        style = ttk.Style()
        style.configure("TButton", font=("Helvetica", 10), padding=6)
        style.configure("Header.TLabel", font=("Helvetica", 14, "bold"))
        
        left_frame = ttk.Frame(self.root, padding=10)
        left_frame.pack(side=tk.LEFT, fill=tk.Y)
        
        right_frame = ttk.Frame(self.root, padding=10)
        right_frame.pack(side=tk.RIGHT, fill=tk.BOTH, expand=True)

        ttk.Label(left_frame, text="Control Panel", style="Header.TLabel").pack(pady=(0, 10))

        # --- 1. Botão de Carga ---
        self.btn_load = ttk.Button(left_frame, text="Load / Extract Data", command=self.thread_load_data)
        self.btn_load.pack(fill=tk.X, pady=5)

        ttk.Separator(left_frame, orient=tk.HORIZONTAL).pack(fill=tk.X, pady=15)

        # --- 2. Module Dropdown ---
        ttk.Label(left_frame, text="Select Analysis Module:", font=("Helvetica", 10, "italic")).pack(anchor=tk.W, pady=(0, 5))
        self.combo_var = tk.StringVar()
        self.combo = ttk.Combobox(left_frame, textvariable=self.combo_var, state="disabled")
        self.combo['values'] = list(self.analysis_tasks.keys())
        if self.analysis_tasks:
            self.combo.current(0)
        self.combo.pack(fill=tk.X, pady=5)

        # --- 3. Grouping Strategy Dropdown ---
        ttk.Label(left_frame, text="Grouping Strategy:", font=("Helvetica", 10, "italic")).pack(anchor=tk.W, pady=(10, 5))
        self.grouping_var = tk.StringVar()
        self.grouping_combo = ttk.Combobox(left_frame, textvariable=self.grouping_var, state="disabled", values=["Categories", "Quartiles", "Sentiments"])
        self.grouping_combo.current(0) 
        self.grouping_combo.pack(fill=tk.X, pady=5)

        # --- 4. Botões de Execução ---
        btn_frame = ttk.Frame(left_frame)
        btn_frame.pack(fill=tk.X, pady=15)
        
        self.btn_run = ttk.Button(btn_frame, text="Run Selected", state=tk.DISABLED, command=self.trigger_selected_analysis)
        self.btn_run.pack(side=tk.LEFT, fill=tk.X, expand=True, padx=(0, 5))

        self.btn_run_all = ttk.Button(btn_frame, text="Run All", state=tk.DISABLED, command=self.trigger_all_analyses)
        self.btn_run_all.pack(side=tk.RIGHT, fill=tk.X, expand=True, padx=(5, 0))

        # --- Console Lateral ---
        ttk.Label(right_frame, text="Execution Console", style="Header.TLabel").pack(anchor=tk.W, pady=(0, 5))
        self.console = scrolledtext.ScrolledText(right_frame, wrap=tk.WORD, font=("Consolas", 9), bg="#1e1e1e", fg="#d4d4d4")
        self.console.pack(fill=tk.BOTH, expand=True)

        print("Welcome to the Unified Analytics Suite.\nReady. Awaiting Data Extraction...")

    def thread_load_data(self):
        self.btn_load.config(state=tk.DISABLED)
        threading.Thread(target=self._process_load, daemon=True).start()

    def _process_load(self):
        success = self.engine.load_or_extract_data()
        if success:
            self.root.after(0, self.enable_analysis_controls)
            print("\n[SUCCESS] Data successfully loaded into RAM. Select a module and hit 'Run Analysis' or 'Run All'.")
        else:
            self.root.after(0, lambda: self.btn_load.config(state=tk.NORMAL))
            print("\n[ERROR] Failed to load or extract data.")

    def enable_analysis_controls(self):
        self.combo.config(state="readonly")
        self.grouping_combo.config(state="readonly")
        self.btn_run.config(state=tk.NORMAL)
        self.btn_run_all.config(state=tk.NORMAL)

    def trigger_selected_analysis(self):
        selected_task_name = self.combo_var.get()
        selected_grouping = self.grouping_var.get()
        if selected_task_name in self.analysis_tasks:
            target_function = self.analysis_tasks[selected_task_name]
            self.run_task_in_thread(target_function, selected_grouping)

    def trigger_all_analyses(self):
        selected_grouping = self.grouping_var.get()
        self.run_all_tasks_in_thread(selected_grouping)

    def run_task_in_thread(self, task_func, grouping):
        def wrapper():
            self.root.after(0, self._set_controls_state, tk.DISABLED)
            task_func(grouping=grouping)
            print("\n[✔] Task completed successfully.")
            self.root.after(0, self._set_controls_state, tk.NORMAL)

        threading.Thread(target=wrapper, daemon=True).start()

    def run_all_tasks_in_thread(self, grouping):
        def wrapper():
            self.root.after(0, self._set_controls_state, tk.DISABLED)
            print(f"\n[🚀] Starting FULL PIPELINE for {grouping}...")
            
            for task_name, task_func in self.analysis_tasks.items():
                print(f"\n---> Running module: {task_name}")
                try:
                    task_func(grouping=grouping)
                except Exception as e:
                    print(f"  [ERROR] Failed to run {task_name}: {e}")
            
            print("\n[✔✔✔] ALL TASKS COMPLETED SUCCESSFULLY.")
            self.root.after(0, self._set_controls_state, tk.NORMAL)

        threading.Thread(target=wrapper, daemon=True).start()

    def _set_controls_state(self, state):
        self.btn_run.config(state=state)
        self.btn_run_all.config(state=state)
        self.combo.config(state="readonly" if state == tk.NORMAL else tk.DISABLED)
        self.grouping_combo.config(state="readonly" if state == tk.NORMAL else tk.DISABLED)

if __name__ == "__main__":
    root = tk.Tk()
    app = AppGUI(root)
    root.mainloop()