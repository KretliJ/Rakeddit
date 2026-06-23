import os
import seaborn as sns # type: ignore

class Config:
    # 1. Deteta dinamicamente o diretório base do projeto (Agnóstico a SO)
    # Como o Utilities.py está dentro da pasta 'audit', subimos um nível para encontrar a raiz
    CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
    BASE_DIR = os.path.dirname(CURRENT_DIR)

    # 2. File Paths absolutos construídos dinamicamente (Usa \ no Windows e / no Docker automaticamente)
    MULTIMODAL_PATH = os.path.join(BASE_DIR, "DATA", "4-inferred", "INFERRED_MULTIMODAL_FINAL.jsonl")
    CACHE_PATH = os.path.join(BASE_DIR, "DATA", "4-inferred", "cascades_dataframe_cache.parquet")
    BLIND_PATH = os.path.join(BASE_DIR, "DATA", "4-inferred", "INFERRED_BLIND_DATASET.jsonl")
    RESULTS_DIR = os.path.join(CURRENT_DIR, "results", "unified_analytics")
    
    # Taxonomies
    VALID_SENTIMENTS = {'POSITIVE', 'NEUTRAL', 'NEGATIVE'}
    MODERATION_LABELS = {'REMOVED_BY_MOD', 'USER_DELETED', 'AUTOMOD_WARNING'}
    
    CATEGORIES = ['PUBLIC ARENAS', 'HUMOR', 'SOCIOCULTURAL', 'HOBBIES']
    CATEGORY_MAP = {
        'brasil': 'PUBLIC ARENAS', 'brasilivre': 'PUBLIC ARENAS', 'brasildob': 'PUBLIC ARENAS', 'debatesbr': 'PUBLIC ARENAS', 'noticiasbr': 'PUBLIC ARENAS',
        'botecodoreddit': 'HUMOR', 'farialimabets': 'HUMOR', 'memesbr': 'HUMOR', 'shitpostbr': 'HUMOR',
        'antitrampo': 'SOCIOCULTURAL', 'opiniaoburra': 'SOCIOCULTURAL', 'opiniaoimpopular': 'SOCIOCULTURAL', 'filosofiabar': 'SOCIOCULTURAL', 'infernosocial': 'SOCIOCULTURAL',
        'futebol': 'HOBBIES', 'gamesecultura': 'HOBBIES', 'videogamesbrasil': 'HOBBIES', 'carros': 'HOBBIES', 'computadores': 'HOBBIES', 'saopaulo': 'HOBBIES'
    }

    # Triad Mapping
    TRIAD_MAPPING = {
        ('NEGATIVE', 'NEGATIVE', 'NEGATIVE'): 'Negative Persistence',
        ('POSITIVE', 'POSITIVE', 'POSITIVE'): 'Positive Persistence',
        ('POSITIVE', 'NEGATIVE', 'NEGATIVE'): 'Negative Convergence (from pos)',
        ('NEUTRAL', 'NEGATIVE', 'NEGATIVE'): 'Negative Convergence (from neu)',
        ('NEGATIVE', 'POSITIVE', 'POSITIVE'): 'Positive Convergence (from neg)',
        ('NEUTRAL', 'POSITIVE', 'POSITIVE'): 'Positive Convergence (from neu)',
        ('POSITIVE', 'POSITIVE', 'NEGATIVE'): 'Shift (to neg)',
        ('NEGATIVE', 'NEGATIVE', 'POSITIVE'): 'Shift (to pos)',
        ('POSITIVE', 'NEGATIVE', 'POSITIVE'): 'Oscillation (a)',
        ('NEGATIVE', 'POSITIVE', 'NEGATIVE'): 'Oscillation (b)',
        ('POSITIVE', 'NEUTRAL', 'NEGATIVE'):  'Mixed Transition (to neg)',
        ('NEGATIVE', 'NEUTRAL', 'POSITIVE'):  'Mixed Transition (to pos)'
    }

    # Ordered Triads for Heatmaps
    ORDERED_TRIADS = [
        'Negative Persistence', 'Positive Persistence',
        'Negative Convergence (from pos)', 'Negative Convergence (from neu)',
        'Positive Convergence (from neg)', 'Positive Convergence (from neu)',
        'Shift (to neg)', 'Shift (to pos)',
        'Oscillation (a)', 'Oscillation (b)',
        'Mixed Transition (to neg)', 'Mixed Transition (to pos)'
    ]

    @staticmethod
    def get_colors():
        color = "inferno"
        cmap = color + "_r"
        color_scheme = sns.color_palette(color, 4).as_hex()
        return {
            'CATEGORIES': {cat: color for cat, color in zip(Config.CATEGORIES, color_scheme)},
            'SENTIMENTS': {'POSITIVE': '#3B0F70', 'NEUTRAL': '#CA3E72', 'NEGATIVE': '#FECF92'},
            'COLOR_SCHEME': color_scheme,
            'CMAP': cmap,
            'LINESTYLES': ['-', '--', '-.', ':']
        }

    @staticmethod
    def setup_directories():
        os.makedirs(Config.RESULTS_DIR, exist_ok=True)

    @staticmethod
    def set_sns_theme():
        sns.set_theme(style="ticks", rc={"axes.grid": True, "grid.alpha": 0.5, "grid.linestyle": "--"})