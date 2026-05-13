import json
import os
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from collections import defaultdict
from glob import glob
# from modules.config_loader import config # Descomente na sua versão
import tkinter as tk
from tkinter import ttk, messagebox, colorchooser # Adicionado colorchooser
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg, NavigationToolbar2Tk
import matplotlib.pyplot as plt # Certifique-se de que o plt está importado

# ==========================================
# SIMULAÇÃO DE DADOS 
# ==========================================
def generate_cascade_stats(jsonl_path):
    print("[*] Extraindo dados enriquecidos para o Analytics...")
    timestamps = {}
    records = []

    # Passo 1: Mapear timestamps para calcular a aceleração
    with open(jsonl_path, 'r', encoding='utf-8') as f:
        for line in f:
            try:
                record = json.loads(line)
                if record.get('type') == 'metadata_footer' or not record.get('id'): continue
                
                r_id = record['id']
                ts = record.get('timestamp') or record.get('created_utc') or 0
                timestamps[r_id] = float(ts)
                
                if record.get('is_valid_text'):
                    records.append(record)
            except json.JSONDecodeError: pass

    # Passo 2: Construir DataFrame com Features Estruturais
    enriched_data = []
    for r in records:
        r_id = r['id']
        parent_id = r.get('parent_id')
        
        # Puxa o score da IA (com fallback seguro para 0.0)
        tox = r.get('toxicity_score', 0.0)
        
        # Calcula o Delta de Tempo (em minutos)
        ts = timestamps.get(r_id, 0)
        parent_ts = timestamps.get(parent_id, ts)
        time_delta = abs(ts - parent_ts) / 60.0
        
        enriched_data.append({
            'subreddit': r.get('subreddit', 'unknown'),
            'depth': r.get('depth', 0),
            'toxicity_score': tox,
            'time_delta_mins': time_delta,
            'is_post': r.get('type') == 'post_header'
        })

    df = pd.DataFrame(enriched_data)
    
    # Agrupamento Estatístico
    posts_per_sub = df[df['is_post']].groupby('subreddit').size().to_dict()
    
    # Agrupamos calculando Média de Toxicidade e Mediana de Tempo (para evitar distorção de outliers)
    stats = df.groupby(['subreddit', 'depth']).agg(
        node_count=('depth', 'count'),
        avg_toxicity=('toxicity_score', 'mean'),
        median_time_delta=('time_delta_mins', 'median')
    ).reset_index()
    
    stats['avg_breadth'] = stats.apply(
        lambda x: x['node_count'] / posts_per_sub.get(x['subreddit'], 1), axis=1
    )
    
    return stats

class StructuralSignatureApp(tk.Tk):
    def __init__(self, breadth_stats):
        super().__init__()
        self.title("MultimodalBrasil: Visualizador Estrutural")
        self.geometry("1280x800")
        
        self.stats = breadth_stats
        self.subreddits = sorted(self.stats['subreddit'].unique())
        
        # Estado do aplicativo
        self.is_log_scale = True
        self.visible_subs = {sub: True for sub in self.subreddits}
        
        # Estilo das linhas (linhas originais contínuas, médias tracejadas)
        self.line_styles = {sub: '-' for sub in self.subreddits}
        
        # --- PALETA ARCO-ÍRIS SATURADA ---
        cmap = plt.get_cmap('hsv')
        colors = [cmap(i) for i in np.linspace(0, 0.85, len(self.subreddits))]
        self.color_map = dict(zip(self.subreddits, colors))

        self._build_ui()
        self.auto_categorize()

    def _build_ui(self):
        # Painel Superior (Controles)
        control_frame = ttk.Frame(self, padding="10")
        control_frame.pack(side=tk.TOP, fill=tk.X)

        self.btn_scale = ttk.Button(
            control_frame, 
            text="Exibindo: Escala Logarítmica (Clique para Real)", 
            command=self.toggle_scale,
            width=50
        )
        self.btn_scale.pack(side=tk.LEFT, padx=5)
        
        # NOVO BOTÃO DE MÉDIA
        self.btn_avg = ttk.Button(
            control_frame,
            text="➕ Criar Linha de Média",
            command=self.open_average_dialog,
            width=25
        )
        self.btn_avg.pack(side=tk.LEFT, padx=5)
        
        self.metric_var = tk.StringVar(value='avg_breadth')
        ttk.Label(control_frame, text=" | Analisar Métrica:").pack(side=tk.LEFT, padx=(10, 2))
        self.metric_combo = ttk.Combobox(
            control_frame, 
            textvariable=self.metric_var, 
            state="readonly",
            values=['avg_breadth', 'avg_toxicity', 'median_time_delta'],
            width=20
        )
        self.metric_combo.pack(side=tk.LEFT, padx=5)
        self.metric_combo.bind("<<ComboboxSelected>>", lambda e: self.draw_plot())

        ttk.Label(control_frame, text="Dica: Clique nos itens da legenda para ocultar/exibir.").pack(side=tk.RIGHT, padx=10)

        # Matplotlib Canvas
        self.fig, self.ax = plt.subplots(figsize=(12, 7))
        self.fig.subplots_adjust(left=0.08, right=0.98, top=0.92, bottom=0.1)
        
        self.canvas = FigureCanvasTkAgg(self.fig, master=self)
        self.canvas.get_tk_widget().pack(side=tk.TOP, fill=tk.BOTH, expand=True)
        
        toolbar = NavigationToolbar2Tk(self.canvas, self)
        toolbar.update()
        
        self.fig.canvas.mpl_connect('pick_event', self.on_pick)

    def open_average_dialog(self):
        dialog = tk.Toplevel(self)
        dialog.title("Criar Linha de Média Combinada")
        dialog.geometry("400x600") # Janela um pouco maior
        dialog.transient(self) 
        dialog.grab_set() 

        # --- CORREÇÃO UI: O BOTÃO FICA FIXO NO FUNDO ---
        # Empacotamos o botão PRIMEIRO, colado no BOTTOM.
        btn_frame = ttk.Frame(dialog)
        btn_frame.pack(side=tk.BOTTOM, fill=tk.X, pady=15)
        
        def confirm_action():
            name = name_var.get().strip()
            selected = [s for s, var in sub_vars.items() if var.get()]
            if not name or name in self.subreddits or not selected:
                messagebox.showwarning("Aviso", "Nome inválido, duplicado ou nenhum sub selecionado.", parent=dialog)
                return
            self.add_average_line(name, selected, self.chosen_avg_color)
            dialog.destroy()

        ttk.Button(btn_frame, text="Confirmar e Plotar", command=confirm_action).pack()

        # --- AGORA EMPACOTAMOS O RESTO NO TOPO ---
        top_frame = ttk.Frame(dialog)
        top_frame.pack(side=tk.TOP, fill=tk.BOTH, expand=True)

        ttk.Label(top_frame, text="Nome da Nova Linha:", font=("Arial", 10, "bold")).pack(pady=(15, 5))
        name_var = tk.StringVar()
        ttk.Entry(top_frame, textvariable=name_var, font=("Arial", 10)).pack(pady=5, fill=tk.X, padx=20)

        ttk.Label(top_frame, text="Cor da Linha:", font=("Arial", 10, "bold")).pack(pady=(10, 5))
        frame_color = ttk.Frame(top_frame)
        frame_color.pack(fill=tk.X, padx=20, pady=5)
        
        self.chosen_avg_color = "#FF00FF" 
        lbl_color_preview = tk.Label(frame_color, text="      ", bg=self.chosen_avg_color, relief="solid", borderwidth=1)
        lbl_color_preview.pack(side=tk.LEFT, padx=(0, 10))
        
        def pick_color():
            color_tuple = colorchooser.askcolor(title="Escolha a cor da média", initialcolor=self.chosen_avg_color)
            if color_tuple[1]: 
                self.chosen_avg_color = color_tuple[1]
                lbl_color_preview.config(bg=self.chosen_avg_color)

        ttk.Button(frame_color, text="🎨 Selecionar Cor...", command=pick_color).pack(side=tk.LEFT)

        ttk.Label(top_frame, text="Selecione os Subreddits para Agrupar:", font=("Arial", 10, "bold")).pack(pady=(15, 5))
        
        # O frame_subs ocupa o espaço que "sobrou" no meio
        frame_subs = ttk.Frame(top_frame)
        frame_subs.pack(fill=tk.BOTH, expand=True, padx=20, pady=5)
        
        sub_vars = {}
        for sub in self.subreddits:
            var = tk.BooleanVar(value=False)
            sub_vars[sub] = var
            ttk.Checkbutton(frame_subs, text=sub, variable=var).pack(anchor=tk.W, pady=2)

    def add_average_line(self, name, selected_subs, line_color):
        """Calcula a média de TODAS as métricas e injeta a nova linha no dataframe"""
        # Filtra os dados apenas dos subs selecionados
        df_filtered = self.stats[self.stats['subreddit'].isin(selected_subs)]
        
        # Agrupa por profundidade e calcula a média para todas as métricas estruturais
        df_avg = df_filtered.groupby('depth', as_index=False).agg({
            'avg_breadth': 'mean',
            'avg_toxicity': 'mean',
            'median_time_delta': 'mean' # A média das medianas para manter a escala suave
        })
        
        df_avg['subreddit'] = name
        
        # Concatena a nova linha ao dataframe original
        self.stats = pd.concat([self.stats, df_avg], ignore_index=True)
        
        # Atualiza os estados para renderização
        self.subreddits.append(name)
        self.visible_subs[name] = True
        self.line_styles[name] = '--' 
        
        # USA A COR ESCOLHIDA NO SELETOR!
        self.color_map[name] = line_color 
        
        self.draw_plot()

    def toggle_scale(self):
        self.is_log_scale = not self.is_log_scale
        txt = "Exibindo: Escala Logarítmica (Clique para Real)" if self.is_log_scale else "Exibindo: Escala Real (Clique para Logarítmica)"
        self.btn_scale.config(text=txt)
        self.draw_plot()

    def draw_plot(self):
        self.ax.clear()
        sns.set_theme(style="whitegrid")
        current_metric = self.metric_var.get()
        self.plot_objects = {}
        
        for sub in self.subreddits:
            sub_data = self.stats[self.stats['subreddit'] == sub]
            alpha_val = 0.8 if self.visible_subs[sub] else 0.0
            
            # Recupera o estilo (tracejado ou contínuo) e deixa a linha de média mais grossa
            l_style = self.line_styles.get(sub, '-')
            l_width = 3.5 if l_style == '--' else 2
            
            line, = self.ax.plot(
                sub_data['depth'], 
                sub_data[current_metric], 
                marker='o', 
                markersize=8, 
                linewidth=l_width,
                linestyle=l_style,
                label=sub, 
                color=self.color_map[sub],
                alpha=alpha_val
            )
            self.plot_objects[sub] = {'line': line, 'anns': []}

        depths = self.stats['depth'].unique()
        
        for d in depths:
            points_at_d = self.stats[(self.stats['depth'] == d) & 
                                     (self.stats['subreddit'].map(self.visible_subs))]
            points_at_d = points_at_d.sort_values('avg_breadth')
            
            offsets = [0] * len(points_at_d)
            if len(points_at_d) > 1:
                spread_factor = 15
                for i in range(len(points_at_d)):
                    offsets[i] = (i - len(points_at_d)/2 + 0.5) * spread_factor

            for i, (_, row) in enumerate(points_at_d.iterrows()):
                sub = row['subreddit']
                current_metric = self.metric_var.get()
                x, y = row['depth'], row[current_metric]

                if current_metric == 'avg_breadth':
                    txt_label = f'{y:.0f}'  # Nós inteiros
                elif current_metric == 'avg_toxicity':
                    txt_label = f'{y:.3f}'  # 3 casas para o Score
                else:
                    txt_label = f'{y:.1f}'
                ann = self.ax.annotate(
                    text=txt_label, 
                    xy=(x, y),
                    xytext=(0, offsets[i] * 1.5), 
                    textcoords='offset points',
                    ha='center',
                    va='center',
                    fontsize=8,
                    weight='bold',
                    bbox=dict(facecolor=self.color_map[sub], alpha=0.6, edgecolor='black', boxstyle='round,pad=0.3')
                )
                self.plot_objects[sub]['anns'].append(ann)

        self.ax.set_xticks(depths)
        if self.is_log_scale:
            self.ax.set_yscale('log')
        else:
            self.ax.set_yscale('linear')
            
        self.ax.set_title('MultimodalBrasil: Assinatura Estrutural Dinâmica', fontsize=16, weight='bold')
        self.ax.set_xlabel('Profundidade da Cascata (Depth)', fontsize=12)
        metric_labels = {
            'avg_breadth': 'Largura Média (Nós)',
            'avg_toxicity': 'Toxicidade Média (Score de 0 a 1)',
            'median_time_delta': 'Tempo de Resposta (Mediana em Minutos)'
        }
        ylabel_text = metric_labels.get(current_metric, 'Valor')
        self.ax.set_ylabel(ylabel_text, fontsize=12)

        leg = self.ax.legend(title='Subreddits (Toggle)', loc='upper right', framealpha=0.9)
        self.legend_map = {}
        
        for leg_line, leg_text, sub in zip(leg.get_lines(), leg.get_texts(), self.subreddits):
            leg_line.set_picker(True)
            leg_line.set_pickradius(10)
            leg_text.set_picker(True)
            
            self.legend_map[leg_line] = sub
            self.legend_map[leg_text] = sub
            
            alpha = 1.0 if self.visible_subs[sub] else 0.3
            leg_line.set_alpha(alpha)
            leg_text.set_alpha(alpha)

        self.canvas.draw()

    def on_pick(self, event):
        sub_clicked = self.legend_map.get(event.artist)
        if not sub_clicked: return

        self.visible_subs[sub_clicked] = not self.visible_subs[sub_clicked]
        self.draw_plot()

    def auto_categorize(self):
        """Classifica os subreddits originais nas 4 categorias sociológicas automaticamente."""
        print("[*] Executando Algoritmo de Auto-Categorização...")
        
        # Cores puras e saturadas para as 4 categorias
        cat_colors = {
            "Conflito Cronico": "#FF0000",          # Vermelho Vivo
            "Virais e Entretenimento": "#00FF00",   # Verde Limão
            "Camaras de Eco": "#0000FF",            # Azul Puro
            "Ecossistemas Resilientes": "#FFFF00"   # Amarelo Puro
        }

        categories = {k: [] for k in cat_colors.keys()}
        subs_originais = list(self.subreddits)

        for sub in subs_originais:
            sub_data = self.stats[self.stats['subreddit'] == sub]
            if sub_data.empty: continue

            # Extrai os dados do nível 1 e da profundidade máxima (geralmente 10)
            try:
                tox_1 = sub_data[sub_data['depth'] == 1]['avg_toxicity'].values[0]
                max_depth = sub_data['depth'].max()
                tox_max = sub_data[sub_data['depth'] == max_depth]['avg_toxicity'].values[0]
                breadth_max = sub_data[sub_data['depth'] == max_depth]['avg_breadth'].values[0]
            except IndexError:
                continue # Pula se faltarem dados

            # --- CRITÉRIOS MATEMÁTICOS DE CLASSIFICAÇÃO ---
            if tox_max <= 0.615:
                # Resilientes: Toxicidade final permanece baixa (ex: FilosofiaBAR)
                categories["Ecossistemas Resilientes"].append(sub)
                
            elif tox_1 >= 0.65 and breadth_max >= 300:
                # Conflito Crônico: Já nasce tóxico (>0.65) e retém muita gente no fundo (>300 nós)
                categories["Conflito Cronico"].append(sub)
                
            elif tox_1 < 0.63 and breadth_max <= 200:
                # Virais: Nasce pacífico (<0.63), morre rápido no fundo (pouca retenção)
                categories["Virais e Entretenimento"].append(sub)
                
            else:
                # Câmaras de Eco: O que sobra (Tox alta/média com gatilho no meio e retenção mediana)
                categories["Camaras de Eco"].append(sub)

        # Injeta as médias no gráfico automaticamente
        for cat_name, subs_in_cat in categories.items():
            if len(subs_in_cat) > 0:
                print(f" -> {cat_name}: {subs_in_cat}")
                self.add_average_line(cat_name, subs_in_cat, cat_colors[cat_name])

if __name__ == "__main__":
    df_stats = generate_cascade_stats('./DATA/results/with_vision/INFERRED_MULTIMODAL_FINAL.jsonl')
    app = StructuralSignatureApp(df_stats)
    app.mainloop()