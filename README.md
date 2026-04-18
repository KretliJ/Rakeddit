# Rakeddit: A Systematic Social Media Harvester

### [ ARCHITECTURE OVERVIEW ]

Rakeddit is a multi-stage Python pipeline designed for high-integrity behavioral data extraction from Reddit. It prioritizes "Gentle Harvesting" through randomized jittering and automated rate-limit handling, ensuring long-term stability for large-scale datasets. It does not require API access.

   * LLM & Graph Ready: Outputs normalized "Tidy Data", perfectly structured for local LLM context windows (Stance Detection) and Graph Theory analysis (NetworkX, Gephi).

WARNING:

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

#### STAGE 2: Data Normalization (Tree Flattening)
Transforms the highly nested, raw JSON comment trees into a flat, relational structure (Tidy Data) without losing conversational context.
* **Mechanism:** Iterative Depth-First Search using a stack, preventing Python recursion limits on deep threads.
* **Context Mapping:** Preserves referential integrity (`parent_id` and `post_id`) to allow reconstruction and reference of conversation paths for Context-Aware LLM Inference.
* **Storage:** Streams directly to JSON Lines (`.jsonl`) format, guaranteeing reduced memory footpring regardless of the dataset size.

---

### [ USAGE ]

#### [Functions](functions.md)

### [ DISCLAIMER ]

Rakeddit is provided "as is" for educational and research purposes only. 
- The author assumes no liability for misuse or rate-limit violations.
- Users are responsible for complying with Reddit's ToS and 
  applicable data protection laws (GDPR, LGPD, CCPA, etc.).
- "Rakeddit" is a name. Not legal advice.
     - Because calling it a scraper isn't legally defensible (nor particularly truthful)
