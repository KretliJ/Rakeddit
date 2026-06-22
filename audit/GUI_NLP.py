import tkinter as tk
from tkinter import ttk, scrolledtext, messagebox
import threading
import sys
import os
import subprocess
from datetime import datetime
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

class NLPGUI:
    def __init__(self, root):
        self.root = root
        self.root.title("NLP & Psycholinguistics Orchestrator (NVIDIA GPU)")
        self.root.geometry("850x600")
        
        self.container_name = "rakeddit_gpu_container"
        self.is_running = False
        
        self.setup_ui()
        # Redirect stdout through self.root to protect Tkinter threads
        sys.stdout = RedirectText(self.console, self.root)

    def setup_ui(self):
        style = ttk.Style()
        style.configure("TButton", font=("Helvetica", 10), padding=6)
        style.configure("Header.TLabel", font=("Helvetica", 14, "bold"), foreground="#2c3e50")
        style.configure("Danger.TButton", font=("Helvetica", 10, "bold"), foreground="red")
        style.configure("Switch.TButton", font=("Helvetica", 10, "bold"), foreground="#27ae60")
        
        left_frame = ttk.Frame(self.root, padding=15)
        left_frame.pack(side=tk.LEFT, fill=tk.Y)
        
        right_frame = ttk.Frame(self.root, padding=15)
        right_frame.pack(side=tk.RIGHT, fill=tk.BOTH, expand=True)

        # GPU control panel header
        ttk.Label(left_frame, text="GPU Control Panel", style="Header.TLabel").pack(pady=(0, 10))
        self.btn_run_all = ttk.Button(left_frame, text="🚀 Run Full Pipeline", command=lambda: self.run_selected_task("4. Full Pipeline"))
        self.btn_run_all.pack(fill=tk.X, pady=10, ipady=10)

        # Panel switch button
        self.btn_switch = ttk.Button(left_frame, text="🔄 Switch to Network Panel", style="Switch.TButton", command=self.switch_to_network)
        self.btn_switch.pack(fill=tk.X, pady=(0, 20), ipady=5)

        # Specific task selector
        ttk.Label(left_frame, text="Specific Tasks:", font=("Helvetica", 9, "bold")).pack(anchor=tk.W)
        self.task_combo = ttk.Combobox(left_frame, values=["1. BERTopic Inference", "2. Cascade Extraction", "3. LIWC Analysis"], state="readonly")
        self.task_combo.pack(fill=tk.X, pady=5)
        
        self.btn_run_selected = ttk.Button(left_frame, text="Run Selected", command=self.run_from_combo)
        self.btn_run_selected.pack(fill=tk.X, pady=5)

        ttk.Separator(left_frame, orient='horizontal').pack(fill=tk.X, pady=15)
        
        # Data management (danger zone)
        ttk.Label(left_frame, text="Data Management", font=("Helvetica", 10, "bold")).pack(anchor=tk.W, pady=(0, 5))
        self.btn_nuke_nlp = ttk.Button(left_frame, text="☢ Nuke NLP Cache", style="Danger.TButton", command=self.nuke_nlp_cache)
        self.btn_nuke_nlp.pack(fill=tk.X, pady=5)
        
        self.btn_stop_docker = ttk.Button(left_frame, text="⏹ Force Stop Container", command=self.force_stop_container)
        self.btn_stop_docker.pack(fill=tk.X, pady=5)

        # Right-side Docker/GPU console
        ttk.Label(right_frame, text="Docker / GPU Console", style="Header.TLabel").pack(anchor=tk.W, pady=(0, 10))
        self.console = scrolledtext.ScrolledText(right_frame, wrap=tk.WORD, font=("Consolas", 10), bg="#1e1e1e", fg="#00ff00")
        self.console.pack(fill=tk.BOTH, expand=True)

        self._log("=======================================================", "SYSTEM")
        self._log("ℹ️ NLP & Psycholinguistics Module (GPU Accelerated) ℹ️", "SYSTEM")
        self._log("=======================================================", "SYSTEM")
        self._log("ℹ️ Status: Waiting for commands...\n", "INFO")

    def set_ui_state(self, state=tk.DISABLED):
        """Enable or disable main control buttons."""
        self.btn_run_all.config(state=state)
        self.btn_run_selected.config(state=state)
        self.task_combo.config(state=state)
        self.btn_nuke_nlp.config(state=state)

    def run_selected_task(self, task):
        self.set_ui_state(tk.DISABLED) # Bloqueia antes de começar
        self.is_running = True
        threading.Thread(target=self._execute_logic, args=(task,), daemon=True).start()

    def run_from_combo(self):
        self.run_selected_task(self.task_combo.get())

    def _execute_logic(self, task):
        task_map = {
            "1. BERTopic Inference": "bertopic",
            "2. Cascade Extraction": "cascades",
            "3. LIWC Analysis": "liwc",
            "4. Full Pipeline": "full"
        }
        arg = task_map.get(task, "full")
        
        # 1. Ensure container is awake before running
        self._ensure_container_is_ready()
        
        self._log(f"ℹ️ Starting task: {task} (arg: {arg})", "INFO")
        
        # 2. Working dir is /app/audit, so script name alone is sufficient
        cmd = ["docker", "exec", self.container_name, "python", "Analytical_NLP_Engine.py", arg]
        
        # 3. Stream logs to console
        self._stream_docker_logs(cmd)

    def _log(self, message, level="INFO"):
        """Motor de log customizado para facilitar o debug"""
        timestamp = datetime.now().strftime("%H:%M:%S")
        if level == "SYSTEM":
            print(f"{message}")
        else:
            print(f"[{timestamp}] [{level}] {message}")

    def switch_to_network(self):
        if self.is_running:
            messagebox.showwarning("Warning", "A GPU analysis is currently running!\nWait for it to finish before switching panels.")
            return
        subprocess.Popen([sys.executable, "GUI.py"])
        self.root.destroy()

    def trigger_nlp_pipeline(self):
        self.btn_run_all.config(state=tk.DISABLED)
        self.btn_switch.config(state=tk.DISABLED)
        self.is_running = True
        threading.Thread(target=self.run_docker_process, daemon=True).start()

    def _stream_docker_logs(self, cmd_list):
        """Execute a system command and stream output in real time to the GUI console."""
        process = subprocess.Popen(
            cmd_list,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
            encoding='utf-8',
            errors='replace',
            universal_newlines=True
        )
        for line in process.stdout:
            print(f"[DOCKER-BUILD] {line}", end="")
            
        process.wait()
        self.root.after(0, lambda: self.set_ui_state(tk.NORMAL))  # Re-enable UI when done
        self.is_running = False

    def _ensure_container_is_ready(self):
        """Manage full Docker container lifecycle: create, start, or unpause as needed."""
        self._log("ℹ️ Checking for docker-compose.yml...", "DEBUG")
        if not os.path.exists("docker-compose.yml"):
            self._log("❌ 'docker-compose.yml' not found in current folder !!!", "ERROR")
            return False

        self._log(f"ℹ️ Sending 'docker inspect {self.container_name}'...", "DEBUG")
        try:
            # Verifica se o contêiner existe (COM TIMEOUT DE 10 SEGUNDOS)
            inspect = subprocess.run(
                ["docker", "inspect", "-f", "{{.State.Status}}", self.container_name], 
                capture_output=True, text=True, timeout=10
            )
            
            self._log(f"✅ 'docker inspect' response received. Code: {inspect.returncode}", "DEBUG")
            
            if inspect.returncode != 0:
                self._log(f"⚠️ Container '{self.container_name}' does not exist. Creating from image...", "WARN")
                self._log("✅ Environment build initiated !!!", "INFO")
                
                print("\n" + "="*50)
                # Em vez de um subprocess.run cego, usamos o nosso stream em tempo real
                up_return_code = self._stream_docker_logs(["docker", "compose", "up", "-d"])
                print("="*50 + "\n")
                
                if up_return_code != 0:
                    self._log("❌ Failed to create container with docker compose.", "ERROR")
                    return False
                self._log("✅ CONTAINER CREATED SUCCESSFULLY", "INFO")
                return True
            
            status = inspect.stdout.strip()
            self._log(f"ℹ️ Current state of container: {status.upper()}", "INFO")

            if status == "paused":
                self._log("ℹ️ Resuming processes (Unpause)...", "INFO")
                subprocess.run(["docker", "unpause", self.container_name], check=True, timeout=15)
            elif status in ["exited", "created"]:
                self._log("ℹ️ Container was stopped. Starting...", "INFO")
                subprocess.run(["docker", "start", self.container_name], check=True, timeout=15)
            elif status == "running":
                self._log("✅ Container already running and ready.", "INFO")
            else:
                self._log(f"⚠️ Unexpected state ({status}). Attempting forced start...", "WARN")
                subprocess.run(["docker", "start", self.container_name], check=False, timeout=15)
                
            return True

        except subprocess.TimeoutExpired as e:
            self._log(f"❌ TIMEOUT: Docker took too long to respond!", "ERROR")
            self._log(" Your Docker Desktop might have had a bad time in the background.", "ERROR")
            self._log(" Restart docker and try again.", "ERROR")
            return False
        except Exception as e:
            self._log(f"❌ Unexpected error during docker check: {e}", "ERROR")
            return False

    def run_docker_process(self):
        self._log("ℹ️ Received run request. Checking in on GPU...", "INFO")
        
        try:
            is_ready = self._ensure_container_is_ready()
            if not is_ready:
                raise RuntimeError("❌ Docker environment initialization failed ")

            self._log("---------------------", "SYSTEM")
            self._log("✅ Running inference", "SYSTEM")
            self._log("---------------------\n", "SYSTEM")
            
            process = subprocess.Popen(
                ["docker", "exec", self.container_name, "python", "Analytical_NLP_Engine.py"],
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1,
                encoding='utf-8',
                errors='replace',
                universal_newlines=True
            )
            
            for line in process.stdout:
                # Clean print: the internal script already formats its own output
                print(line, end="")
                
            process.wait()
            
            if process.returncode == 0:
                self._log("✅ NLP PIPELINE COMPLETED", "INFO")
            else:
                self._log(f"❌ NLP script failed internally. Code: {process.returncode}", "ERROR")
                
        except FileNotFoundError:
            self._log("❌ 'docker' not found in PATH. Is Docker Desktop running?", "ERROR")
        except subprocess.CalledProcessError as e:
            self._log(f"❌ Docker command failed: {e}", "ERROR")
        except Exception as e:
            self._log(f"❌ Unexpected orchestrator error: {e}", "ERROR")
        finally:
            self.pause_container()
            self.is_running = False
            self.root.after(0, lambda: self.btn_run_all.config(state=tk.NORMAL))
            self.root.after(0, lambda: self.btn_switch.config(state=tk.NORMAL))

    def pause_container(self):
        try:
            status_cmd = subprocess.run(["docker", "inspect", "-f", "{{.State.Status}}", self.container_name], capture_output=True, text=True)
            if status_cmd.returncode == 0 and status_cmd.stdout.strip() == "running":
                self._log("ℹ️ Freeing resources...", "INFO")
                subprocess.run(["docker", "pause", self.container_name], check=True)
                self._log("ℹ️ Container hibernating.", "INFO")
        except Exception as e:
            self._log(f"⚠️ Could not stop container: {e}", "WARN")

    def force_stop_container(self):
        if self.is_running:
            messagebox.showwarning("⚠️ Warning ⚠️", "Interrupting the container will discard unsaved progress")
        threading.Thread(target=self._stop_docker_thread, daemon=True).start()

    def _stop_docker_thread(self):
        self._log("⚠️ Sending SIGKILL to Docker container...", "WARN")
        try:
            subprocess.run(["docker", "stop", self.container_name], check=True)
            self._log("Contêiner encerrado e memória de vídeo (VRAM) liberada.", "INFO")
            self.is_running = False
            self.root.after(0, lambda: self.btn_run_all.config(state=tk.NORMAL))
            self.root.after(0, lambda: self.btn_switch.config(state=tk.NORMAL))
        except Exception as e:
            self._log(f"❌ Could not stop container: {e}", "ERROR")

    def nuke_nlp_cache(self):
        nlp_cache_path = os.path.join(Config.RESULTS_DIR, "nlp_dataframe_cache.parquet")
        if os.path.exists(nlp_cache_path):
            try:
                os.remove(nlp_cache_path)
                msg = f"☢️ BOOM! File deleted:\n- {os.path.basename(nlp_cache_path)}\n\nNext run will rebuild embeddings from scratch on the GPU."
                self._log("☢️ NLP cache destroyed successfully.", "WARN")
                messagebox.showinfo("☢️ NLP Cache Nuked", msg)
            except Exception as e:
                self._log(f"❌ Could not delete {nlp_cache_path}: {e}", "ERROR")
        else:
            self._log("ℹ️ NLP cache is already empty.", "INFO")
            messagebox.showinfo("ℹ️ Cache empty ℹ️", "No parquet found")

if __name__ == "__main__":
    root = tk.Tk()
    app = NLPGUI(root)
    root.mainloop()