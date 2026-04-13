# Rakeddit: A Systematic Reddit Harvester

### [ ARCHITECTURE OVERVIEW ]

Rakeddit is a multi-stage Python pipeline designed for high-integrity behavioral data extraction from Reddit. It prioritizes "Gentle Harvesting" through randomized jittering and automated rate-limit handling, ensuring long-term stability for large-scale datasets. It does not require API access.

⚠️WARNING:
* Strictly for research purposes. This is designed to handle up to 10,000 user profiles (up to 1,000,000 data points).
* This is designed to run over extended periods of time.
* All data collection follows a gentle approach to respect platform infrastructure and privacy guidelines.
    * This will not collect email, phone numbers, real names, social security numbers, or check when you used the bathroom.
    
### [ EXECUTION SCRIPT ]

#### STAGE 1: Subreddit Ingestion
The system identifies active threads and extracts the raw JSON structure of the community's front page.
* **Mechanism:** Recursive `GET` requests to Reddit's `.json` endpoints.
* **Compliance:** Randomized jitter (3.0s - 7.0s) and exponential back-off for HTTP 429 (Too Many Requests).
* **Storage:** Localized storage in `./json_dumps/[subreddit_name]/`.

#### STAGE 2: Data Aggregation & Ranking
Raw JSON dumps are processed to map the social landscape of the targeted subreddit.
* **Processing:** Deep recursive traversal of comment trees (`kind: t1`) and post data (`kind: t3`).
* **Metric:** Generation of User Rankings based on Post Karma and Comment Karma.
* **Output:** JSON manifests in `./aggregates/` serving as target lists for Phase 3.

#### STAGE 3: Unified User Profiling
Extraction of the "Social Footprint" of individual users identified in Stage 2.
* **Historical Depth:** Up to 100 recent interactions per user.
* **Data Merging:** Smart-merge logic that appends new interactions to existing local profiles, preventing data duplication and maintaining a chronological history.

---

### [ USAGE ]

```python
# main.py entry point
from aggregator import generate_rankings
from json_harvester import *

if __name__ == "__main__":
    # Harvest from. Only the subreddit's name, no "r/"
    target = "subreddit"
    
    # 1. Harvest the 'Hot' section
    harvest_subreddit(target, "hot", limit=100)
    
    # 2. Build the user frequency/karma map
    generate_rankings(target)
    
    # 3. Harvest individual histories for the top 1000 users
    harvest_user(target, limit_users=1000)
```

### [ DISCLAIMER ]

Rakeddit is provided "as is" for educational and research purposes only. 
- The author assumes no liability for misuse or rate-limit violations.
- Users are responsible for complying with Reddit's ToS and 
  applicable data protection laws (GDPR, LGPD, CCPA, etc.).
- "Rakeddit" is a name. Not legal advice.
     - Because calling it a scraper isn't legally defensible (nor particularly truthful)
