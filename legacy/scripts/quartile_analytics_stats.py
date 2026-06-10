import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.ticker as mtick
import seaborn as sns
from scipy import stats

class QuartileNegativityAnalytics:
    def __init__(self, df_cascades, results_dir="results/RQ1/Sentiments"):
        print("[*] Inicializando Análise Estrutural por Quartis com Rigor Estatístico...")
        self.df = df_cascades.copy()
        self.RESULTS_DIR = results_dir
        os.makedirs(self.RESULTS_DIR, exist_ok=True)
        
        sns.set_theme(style="ticks", rc={"axes.grid": True, "grid.alpha": 0.5, "grid.linestyle": "--"})
        
        if 'duration_hours' not in self.df.columns and 'duration_minutes' in self.df.columns:
            self.df['duration_hours'] = self.df['duration_minutes'] / 60.0
            
        self._calculate_quartiles()
        self.palette = sns.color_palette("coolwarm", len(self.categories)).as_hex()
        
        # Dicionário para armazenar os resultados estatísticos que irão para o PDF final
        self.statistical_results = []

    def _calculate_quartiles(self):
        """Divide as cascatas em 4 quartis baseados na negatividade."""
        try:
            self.df['negativity_quartile'] = pd.qcut(
                self.df['perc_negative'], q=4, 
                labels=['Q1', 'Q2', 'Q3', 'Q4']
            )
        except ValueError:
            self.df['negativity_quartile'], bins = pd.qcut(
                self.df['perc_negative'], q=4, retbins=True, duplicates='drop'
            )
            labels = [f"Q{i+1}" for i in range(len(bins)-1)]
            self.df['negativity_quartile'] = pd.cut(self.df['perc_negative'], bins=bins, labels=labels, include_lowest=True)

        self.categories = self.df['negativity_quartile'].cat.categories

    def _cliffs_delta(self, lst1, lst2):
        """
        Calcula o Cliff's Delta usando a estatística U de Mann-Whitney.
        d = (2 * U) / (m * n) - 1
        """
        m, n = len(lst1), len(lst2)
        if m == 0 or n == 0: return 0.0
        u, _ = stats.mannwhitneyu(lst1, lst2, alternative='two-sided')
        delta = (2 * u) / (m * n) - 1
        return delta

    def plot_all_ccdfs_and_test(self):
        """Gera CCDFs em escala decimal e roda Kruskal-Wallis, KS e Cliff's Delta."""
        print("\n[*] Gerando gráficos CCDF (Escala Decimal) e calculando estatísticas...")
        
        metrics = [
            ('structural_virality', 'STRUCTURAL VIRALITY (WIENER)', 'Structural_RQ1_CCDF_Structural_Virality.pdf'),
            ('max_depth', 'MAX CASCADE DEPTH', 'Structural_RQ1_CCDF_Max_Depth.pdf'),
            ('max_breadth', 'MAX CASCADE BREADTH', 'Structural_RQ1_CCDF_Max_Breadth.pdf'),
            ('unique_users', 'UNIQUE PARTICIPATING USERS', 'Structural_RQ1_CCDF_Unique_Users.pdf'),
            ('cascade_size', 'TOTAL VOLUME OF MESSAGES', 'Structural_RQ1_CCDF_Number_Messages.pdf'),
            ('duration_hours', 'CASCADE LIFESPAN (HOURS)', 'Structural_RQ1_CCDF_Duration_Hours.pdf')
        ]
        
        if 'mean_iat_minutes' in self.df.columns:
            metrics.append(('mean_iat_minutes', 'MEAN INTER-ARRIVAL TIME (MIN)', 'Structural_RQ1_CCDF_Mean_IAT_Min.pdf'))
        
        for col, xlabel, filename in metrics:
            if col not in self.df.columns: continue
                
            fig, ax = plt.subplots(figsize=(10, 7))
            
            # Armazena os dados de cada quartil para testes estatísticos
            group_data = []
            q1_data, q4_data = [], []
            
            for i, cat in enumerate(self.categories):
                data = self.df[self.df['negativity_quartile'] == cat][col].dropna().values
                if len(data) == 0: continue
                
                group_data.append(data)
                if 'Q1' in cat: q1_data = data
                if 'Q4' in cat: q4_data = data
                
                # Média e Desvio Padrão para a Legenda
                mean_val = np.mean(data)
                std_val = np.std(data)
                label_text = f"{cat} (μ={mean_val:.2f}, σ={std_val:.2f})"
                
                # CCDF
                sorted_data = np.sort(data)
                y = (1.0 - np.arange(len(sorted_data)) / len(sorted_data)) * 100
                ax.plot(sorted_data, y, color=self.palette[i], linewidth=3.5, label=label_text)

            # --- TESTES ESTATÍSTICOS ---
            # 1. Kruskal-Wallis (Todas as 4 distribuições)
            if len(group_data) == 4:
                kw_stat, kw_p = stats.kruskal(*group_data)
            else:
                kw_stat, kw_p = float('nan'), float('nan')
                
            # 2. Kolmogorov-Smirnov e Cliff's Delta (Q1 vs Q4)
            if len(q1_data) > 0 and len(q4_data) > 0:
                ks_stat, ks_p = stats.ks_2samp(q1_data, q4_data)
                c_delta = self._cliffs_delta(q1_data, q4_data)
            else:
                ks_stat, ks_p, c_delta = float('nan'), float('nan'), float('nan')
                
            self.statistical_results.append({
                'Metric': col,
                'Kruskal H': kw_stat, 'Kruskal p': kw_p,
                'KS D-value': ks_stat, 'KS p': ks_p,
                "Cliff's Delta (Q1vQ4)": c_delta
            })

            # --- ESTILIZAÇÃO DO GRÁFICO ---
            # Escala DECIMAL (Removido o log)
            ax.set_xlabel(xlabel, fontsize=16, fontweight='bold')
            ax.set_ylabel('CCDF (% OF CASCADES)', fontsize=16, fontweight='bold')
            ax.yaxis.set_major_formatter(mtick.PercentFormatter(decimals=0))
            
            # Legenda interna com Média e Desvio Padrão
            ax.legend(fontsize=12, loc='upper right', framealpha=0.9, edgecolor='black')
            ax.tick_params(labelsize=14)
            sns.despine()
            
            filepath = os.path.join(self.RESULTS_DIR, filename)
            plt.savefig(filepath, dpi=300, bbox_inches='tight')
            plt.close()
            print(f"  -> Salvo: {filename}")

    def compute_spearman(self):
        """Calcula Spearman entre Negatividade e Viralidade Estrutural."""
        print("\n[*] Calculando Spearman Correlation...")
        if 'perc_negative' in self.df.columns and 'structural_virality' in self.df.columns:
            clean_df = self.df[['perc_negative', 'structural_virality']].dropna()
            corr, p_value = stats.spearmanr(clean_df['perc_negative'], clean_df['structural_virality'])
            
            self.statistical_results.append({
                'Metric': 'Spearman: Negativity vs Virality',
                'Kruskal H': float('nan'), 'Kruskal p': p_value, # Usando a coluna 'p' para o p-value
                'KS D-value': corr, 'KS p': float('nan'), # Usando D-value para a Correlação
                "Cliff's Delta (Q1vQ4)": float('nan')
            })
            print(f"  -> Spearman Rho: {corr:.4f} (p-value: {p_value:.4e})")

    def export_statistical_report_pdf(self):
        """Gera um PDF contendo uma tabela com todos os resultados estatísticos."""
        print("\n[*] Exportando Relatório Estatístico em PDF...")
        
        df_stats = pd.DataFrame(self.statistical_results)
        
        # Formatação de exibição das colunas
        df_stats['Kruskal H'] = df_stats['Kruskal H'].apply(lambda x: f"{x:.2f}" if pd.notna(x) else "-")
        df_stats['Kruskal p'] = df_stats['Kruskal p'].apply(lambda x: f"{x:.2e}" if pd.notna(x) else "-")
        df_stats['KS D-value'] = df_stats['KS D-value'].apply(lambda x: f"{x:.4f}" if pd.notna(x) else "-")
        df_stats['KS p'] = df_stats['KS p'].apply(lambda x: f"{x:.2e}" if pd.notna(x) else "-")
        df_stats["Cliff's Delta (Q1vQ4)"] = df_stats["Cliff's Delta (Q1vQ4)"].apply(lambda x: f"{x:.4f}" if pd.notna(x) else "-")

        fig, ax = plt.subplots(figsize=(14, 4))
        ax.axis('off')
        
        table = ax.table(
            cellText=df_stats.values,
            colLabels=df_stats.columns,
            cellLoc='center',
            loc='center',
            colColours=['#f2f2f2'] * len(df_stats.columns)
        )
        table.auto_set_font_size(False)
        table.set_fontsize(10)
        table.scale(1.2, 1.8)
        
        plt.title("Statistical Validation Report (Kruskal-Wallis, KS, Cliff's Delta, Spearman)", 
                  fontsize=14, fontweight='bold', pad=20)
        
        filepath = os.path.join(self.RESULTS_DIR, "Statistical_Report_Summary.pdf")
        plt.savefig(filepath, dpi=300, bbox_inches='tight')
        plt.close()
        print(f"  -> Salvo: Statistical_Report_Summary.pdf")

# ==========================================
# MOCK DE TESTE E INTEGRAÇÃO
# ==========================================
if __name__ == "__main__":
    print("="*50)
    print(" TESTE: ESTATÍSTICA DE QUARTIS ")
    print("="*50)
    
    # Mock simulando as cascatas reais
    np.random.seed(42)
    n_samples = 10000
    perc_neg = np.random.uniform(0, 100, n_samples)
    depth_multiplier = 1 + (perc_neg / 50.0) 
    
    df_mock = pd.DataFrame({
        'perc_negative': perc_neg,
        'structural_virality': np.random.lognormal(mean=1.0, sigma=0.5, size=n_samples) * depth_multiplier,
        'max_depth': (np.random.exponential(scale=3, size=n_samples) * depth_multiplier).astype(int) + 1,
        'max_breadth': np.random.exponential(scale=5, size=n_samples).astype(int) + 1,
        'unique_users': (np.random.exponential(scale=10, size=n_samples) * depth_multiplier).astype(int) + 1,
        'cascade_size': (np.random.exponential(scale=15, size=n_samples) * depth_multiplier).astype(int) + 2,
        'duration_minutes': np.random.exponential(scale=300, size=n_samples) * depth_multiplier,
    })
    
    # Execução
    analytics = QuartileNegativityAnalytics(df_mock, results_dir="results/RQ1/Sentiments")
    analytics.plot_all_ccdfs_and_test()
    analytics.compute_spearman()
    analytics.export_statistical_report_pdf()
    
    print("\n[SUCESSO] Pipeline estatístico executado. Verifique os PDFs.")