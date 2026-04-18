from modules.json_harvester import *
from modules.processor import *
# Global configs
import configparser
config.read('config.ini')

HEADERS = {'User-Agent': config.get('HEADERS', 'User-Agent')}
BASE_PATH = config.get('PATHS', 'BASE_PATH')


if __name__  == "__main__":
    # To rake multiple subs use:
    # for sub subreddit_name in ["brasil", "portugal", "askacademico"]:

        subreddit_name = "brasil" # -> Comment this off if running loop
        # harvest_subreddit(subreddit_name, "top", limit=10)
        extract_from_post(BASE_PATH, limit="brasil")