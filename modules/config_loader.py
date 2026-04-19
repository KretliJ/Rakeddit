# modules/config_loader.py
import os
import configparser
from pathlib import Path

class ConfigLoader:
    # Singleton
    _instance = None
    _config = None
    _config_path = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._load_config()
        return cls._instance
    
    def _load_config(self):

        # Fings config.ini from root
        # IMPORTANT: Assumes config.ini shares a folder with main.py
        possible_paths = [
            Path(__file__).parent.parent / 'config.ini',  # ../config.ini
            Path.cwd() / 'config.ini',                    # ./config.ini
        ]
        
        for path in possible_paths:
            if path.exists():
                self._config_path = path
                break
        
        if not self._config_path:
            raise FileNotFoundError("config.ini não encontrado!")
        
        self._config = configparser.ConfigParser()
        self._config.read(self._config_path)
        print(f"[ConfigLoader] Config carregada de: {self._config_path}")
    
    def get(self, section, key, fallback=None):
        # Returns requested config value
        try:
            return self._config.get(section, key, fallback=fallback)
        except:
            return fallback
    
    def get_int(self, section, key, fallback=0):
        # Returns INT
        try:
            return self._config.getint(section, key, fallback=fallback)
        except:
            return fallback
    
    def get_float(self, section, key, fallback=0.0):
        # Returns FLOAT
        try:
            return self._config.getfloat(section, key, fallback=fallback)
        except:
            return fallback
    
    def get_boolean(self, section, key, fallback=False):
        # Returns BOOL
        try:
            return self._config.getboolean(section, key, fallback=fallback)
        except:
            return fallback
    
    def get_path(self, section, key, fallback=None):
        # Returns absolute path
        path = self.get(section, key, fallback)
        if path:
            return os.path.abspath(path)
        return fallback
    
    @property
    def config(self):
        # (INTERNAL USE) Direct access to config object
        return self._config

# Exposed global instance
config = ConfigLoader()