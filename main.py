from aggregator import generate_rankings
from json_harvester import *
if __name__  == "__main__":
    # To rake multiple subs use:
    # for sub subreddit_name in ["brasil", "portugal", "askacademico"]:

        subreddit_name = "brasil" # -> Comment this off if running loop
        harvest_subreddit(subreddit_name, "hot", limit=100)
        generate_rankings(subreddit_name)
        harvest_user(subreddit_name, limit_users = 1000)