import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.ticker as mtick
import seaborn as sns

class QuartileNegativityAnalytics:
    def __init__(self, df_cascades, results_dir="results/RQ1/Sentiments"):
        print("[*] Inicializando Análise Estrutural por Quartis de Negatividade...")
        self.df = df_cascades.copy()
        self.RESULTS_DIR = results_dir
        os.makedirs(self.RESULTS_DIR, exist_ok=True)
        
        # Estética global
        sns.set_theme(style="ticks", rc={"axes.grid": True, "grid.alpha": 0.5, "grid.linestyle": "--"})
        
        # Garante que a coluna de horas exista
        if 'duration_hours' not in self.df.columns and 'duration_minutes' in self.df.columns:
            self.df['duration_hours'] = self.df['duration_minutes'] / 60.0
            
        self._calculate_quartiles()
        
        # Paleta divergente: Azul (Q1 - Frio) para Vermelho (Q4 - Quente)
        self.palette = sns.color_palette("coolwarm", len(self.categories)).as_hex()

    def _calculate_quartiles(self):
        """Divide as cascatas em 4 quartis baseados no percentual de mensagens negativas."""
        print("  -> Calculando quartis de negatividade...")
        
        # Utilizamos duplicates='drop' caso haja muitas cascatas com exatamente 0% de negatividade
        # o que poderia quebrar o limite matemático dos quartis.
        try:
            self.df['negativity_quartile'] = pd.qcut(
                self.df['perc_negative'], 
                q=4, 
                labels=['Q1 (Lowest Negativity)', 'Q2 (Low-Mid)', 'Q3 (Mid-High)', 'Q4 (Highest Negativity)']
            )
        except ValueError:
            print("  [!] Aviso: Distribuição muito concentrada. Usando quantis com duplicates='drop'.")
            self.df['negativity_quartile'], bins = pd.qcut(
                self.df['perc_negative'], 
                q=4, 
                retbins=True,
                duplicates='drop'
            )
            # Gera labels dinâmicas baseadas nos bins que restaram
            labels = [f"Q{i+1}" for i in range(len(bins)-1)]
            self.df['negativity_quartile'] = pd.cut(self.df['perc_negative'], bins=bins, labels=labels, include_lowest=True)

        self.categories = self.df['negativity_quartile'].cat.categories
        
        # Print de log para a banca/relatório
        print("\n  -> Distribuição de cascatas por quartil:")
        print(self.df['negativity_quartile'].value_counts().sort_index())
        print("-" * 50)

    def plot_all_ccdfs(self):
        """Gera todos os gráficos CCDF solicitados na Figura 1 do artigo."""
        print("\n[*] Gerando gráficos CCDF...")
        
        # Tupla: (coluna_dataframe, Nome_Eixo_X, Nome_Arquivo)
        metrics = [
            ('structural_virality', 'STRUCTURAL VIRALITY (WIENER)', 'Structural_RQ1_CCDF_Structural_Virality.pdf'),
            ('max_depth', 'MAX CASCADE DEPTH', 'Structural_RQ1_CCDF_Max_Depth.pdf'),
            ('max_breadth', 'MAX CASCADE BREADTH', 'Structural_RQ1_CCDF_Max_Breadth.pdf'),
            ('unique_users', 'UNIQUE PARTICIPATING USERS', 'Structural_RQ1_CCDF_Unique_Users.pdf'),
            ('cascade_size', 'TOTAL VOLUME OF MESSAGES', 'Structural_RQ1_CCDF_Number_Messages.pdf'),
            ('duration_hours', 'CASCADE LIFESPAN (HOURS)', 'Structural_RQ1_CCDF_Duration_Hours.pdf')
        ]
        
        # Adiciona Inter-Arrival Time apenas se existir no DataFrame
        if 'mean_iat_minutes' in self.df.columns:
            metrics.append(('mean_iat_minutes', 'MEAN INTER-ARRIVAL TIME (MIN)', 'Structural_RQ1_CCDF_Mean_IAT_Min.pdf'))
        
        for col, xlabel, filename in metrics:
            if col not in self.df.columns:
                print(f"  [-] Aviso: Coluna '{col}' não encontrada. Pulando...")
                continue
                
            fig, ax = plt.subplots(figsize=(8, 6))
            
            for i, cat in enumerate(self.categories):
                # Filtra os dados daquele quartil e remove NaNs
                data = self.df[self.df['negativity_quartile'] == cat][col].dropna().values
                if len(data) == 0: 
                    continue
                
                # Cálculo do Complementary Cumulative Distribution Function (CCDF)
                sorted_data = np.sort(data)
                y = (1.0 - np.arange(len(sorted_data)) / len(sorted_data)) * 100
                
                # Plot da linha
                ax.plot(sorted_data, y, color=self.palette[i], linewidth=3.5, label=cat)

            # Estilização
            ax.set_xscale('log')
            ax.set_xlabel(xlabel, fontsize=16, fontweight='bold')
            ax.set_ylabel('CCDF (% OF CASCADES)', fontsize=16, fontweight='bold')
            ax.yaxis.set_major_formatter(mtick.PercentFormatter(decimals=0))
            ax.tick_params(labelsize=14)
            sns.despine()
            
            filepath = os.path.join(self.RESULTS_DIR, filename)
            plt.savefig(filepath, dpi=300, bbox_inches='tight')
            plt.close()
            print(f"  -> Salvo: {filename}")

    def plot_master_legend(self):
        """Gera um PDF contendo APENAS a legenda, para colocar no topo da Figura 1 no LaTeX."""
        print("\n[*] Gerando Legenda Mestra...")
        fig, ax_leg = plt.subplots(figsize=(12, 0.8)) 
        ax_leg.axis('off')
        
        dummy_handles = []
        for i, cat in enumerate(self.categories):
            line, = ax_leg.plot([], [], color=self.palette[i], linewidth=5.0, label=cat)
            dummy_handles.append(line)
            
        ax_leg.legend(handles=dummy_handles, loc='center', ncol=len(self.categories), 
                      fontsize=14, framealpha=1.0, edgecolor='black', 
                      handlelength=3.0, handletextpad=1.0)
        
        filepath = os.path.join(self.RESULTS_DIR, "Structural_RQ1_Top_Legend.pdf")
        plt.savefig(filepath, dpi=300, bbox_inches='tight')
        plt.close()
        print(f"  -> Salvo: Structural_RQ1_Top_Legend.pdf")

# ==========================================
# INTEGRAÇÃO COM O JSONL REAL
# ==========================================
if __name__ == "__main__":
    import sys
    print("="*50)
    print(" QUARTILE ANALYTICS - PROCESSAMENTO DO JSONL ")
    print("="*50)
    
    try:
        # Importa o orquestrador que sabe como ler o JSONL e montar os grafos
        from sixth_wave_analytics import SixthWaveAnalyticsOrchestrator
        
        app = SixthWaveAnalyticsOrchestrator()
        print(f"[*] Lendo e reconstruindo cascatas a partir de: {app.MULTIMODAL_PATH}")
        
        # Constrói as árvores em RAM e extrai max_depth, virality, perc_negative, etc.
        sucesso = app.extract_and_compute_all()
        
        if sucesso and app.df_cascades is not None and not app.df_cascades.empty:
            print("\n[*] Extração concluída. Iniciando a plotagem por Quartis de Negatividade...")
            
            # Instancia a nossa classe de quartis passando o DataFrame real
            analytics = QuartileNegativityAnalytics(app.df_cascades, results_dir="results/RQ1/Sentiments")
            analytics.plot_all_ccdfs()
            analytics.plot_master_legend()
            
            print("\n[SUCESSO] Pipeline concluído com dados reais do Reddit.")
            print("Verifique os PDFs na pasta: results/RQ1/Sentiments/")
        else:
            print("\n[-] Erro: O DataFrame de cascatas está vazio ou a extração falhou.")
            print("Verifique se o caminho do JSONL está correto no sixth_wave_analytics.py.")
            
    except ImportError:
        print("\n[-] Erro de Importação: O arquivo 'sixth_wave_analytics.py' não foi encontrado.")
        print("Certifique-se de que ambos os scripts estão na mesma pasta.")
        print("Ele é necessário para reconstruir a topologia de grafos a partir do JSONL bruto.")
        sys.exit(1)