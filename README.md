# Rakeddit: A Systematic Social Media Harvester

### [ ARCHITECTURE OVERVIEW ]

Rakeddit is a multi-stage Python pipeline designed for high-integrity behavioral data extraction and local multimodal AI analysis from Reddit. It prioritizes "Gentle Harvesting" through randomized jittering and automated rate-limit handling and Zero-Cloud Dependency for its cognitive engine ensuring long-term stability for large-scale datasets. It does not require API access.

   * LLM & Graph Ready: Outputs normalized "Tidy Data", perfectly structured for local LLM context windows (Stance Detection) and Graph Theory analysis (NetworkX, Gephi).

WARNING:

    * Strictly for research purposes. This is designed to handle up to 10,000 user profiles (up to 1,000,000 data points).
    * This is designed to run over extended periods of time.
    * All data collection follows a gentle approach to respect platform infrastructure and privacy guidelines.
        * This will not collect email, phone numbers, real names, social security numbers, or check when you last used the bathroom.
    
### [ EXECUTION SCRIPT ]

#### STAGE 1: Subreddit Ingestion
The system identifies active threads and extracts the raw JSON structure of the community's front page.
* **Mechanism:** Recursive `GET` requests to Reddit's `.json` endpoints.
* **Compliance:** Randomized jitter (3.0s - 7.0s) and exponential back-off for HTTP 429 (Too Many Requests).
* **Storage:** Localized storage in `./json_dumps/[subreddit_name]/`.

#### STAGE 2: Data Normalization (Tree Flattening)
Transforms the highly nested, raw JSON comment trees into a flat, relational structure without losing conversational context.
* **Mechanism:** Iterative Depth-First Search using a stack, preventing Python recursion limits on deep threads.
* **Context Mapping:** Preserves referential integrity (`parent_id` and `post_id`) to allow reconstruction and reference of conversation paths for Context-Aware LLM Inference.
* **Storage:** Streams directly to JSON Lines (`.jsonl`) format, guaranteeing a reduced memory footprint regardless of dataset size.

#### STAGE 3: Multimodal Context Enrichment
Sanitizes and processes visual media (JPEGs, PNGs, GIFs) into textual descriptions to capture rhetorical intent and OCR data without breaking the text-based LLM pipeline.
* **Mechanism:** in-RAM image interceptor (Pillow) flattens alpha channels and animated frames, resizing media to prevent CUDA Out-of-Memory (OOM) errors.
* **Vision Engine:** Connects to local Small Vision Language Models (e.g., Qwen2.5-VL via Ollama) via strict protocol validation to extract textual content and visual context.

#### STAGE 3.5: Cascade Inference (BERT Pre-filtering)
Acts as a high-speed, low-cost syntactic filter to prune non-toxic comments, acting as a triage layer to save massive LLM inference time and VRAM.
* **Mechanism:** Utilizes lightweight HuggingFace transformers (e.g., Multilingual Toxicity BERT) to perform a binary classification (Safe vs. Suspicious).
* **Routing:** Safely ignores harmless chatter (True Negatives) and flags any sarcastic, profane, or ambiguous text for deeper semantic auditing in Stage 4.

#### STAGE 4: Cognitive Inference & Toxicity Scoring
Evaluates the flattened dataset dynamically, computing hostility, sarcasm, and ad hominem vectors using local Large Language Models (e.g., Llama-3 8B).
* **Mechanism:** Reconstructs the conversational path top-down. The engine passes the Original Post Context and the exact conversation thread to the LLM to ensure precise contextual awareness and metalinguistic analysis.
* **Output:** Appends AI-generated JSON analysis directly to the `.jsonl` records, scoring normative severity based on predefined sociological and legal prompts.
---

### [ USAGE ]

#### [Functions](functions.md)
  
### [ REQUIREMENTS & STARTING UP ]

#### 1. System Requirements
* **OS:** Windows or Linux (Debian-based distributions recommended for Python integration).
* **Python:** Version 3.10 or higher (Python 3.13 verified for stable `Pillow` compilation).
* **Backend:** [Ollama](https://ollama.com/) must be installed and running locally as a background service.
* **Hardware:** A dedicated GPU is highly recommended (e.g., 8GB VRAM or higher) for stable multimodal parsing and text generation.
 
#### 2. Environment Setup
Clone the repository and install the required Python packages. The pipeline relies on `requests` for local API interactions, `Pillow` for in-RAM image sanitization, and `transformers`/`torch` for cascade filtering.

```bash
git clone [https://github.com/KretliJ/Rakeddit.git](https://github.com/KretliJ/Rakeddit.git)
cd Rakeddit
pip install -r requirements.txt
```

#### 3. Local AI Provisioning (Ollama)

```bash
# Pull the Main Inference Engine (Text / NLP)
ollama pull llama3:8b-instruct-q8_0

# Pull the Image Reader Engine (Vision / OCR)
# Note: The 3B parameter version is optimized to leave VRAM headroom on 8GB GPUs.
ollama pull qwen2.5vl:3b
```
(Note: The BERTimbau / Toxicity model will be downloaded automatically via HuggingFace on the first run).

#### 4. Configuration (config.ini)
Ensure your `config.ini` is set up correctly in the root directory. Do not use quotes around the model names, as the strict protocol validation will reject the payload with an HTTP 400 Bad Request.
```Ini, TOML
[HEADERS]
User-Agent = Research_Gentle_Harvesting_With_Jitter (contact: [YOUR EMAIL])
# CHANGE THIS TO REFLECT YOUR USE CASE

[PATHS]
# CHANGE BASE_PATH IF YOU WANT TO DUMP FILES ELSEWHERE
BASE_PATH = ./DATA/json_dumps/
AGGREGATES_PATH = ./DATA/aggregates/
MEDIA_PATH = ./DATA/temp_media/
MULTIMODAL_PATH = ./DATA/vision_processing/

[MODELS]
# CHANGE THESE TO USE DIFFERENT AGENTS
MAIN_INFER = llama3
IMAGE_READER = qwen3-vl:2b-instruct
```

#### 5. Execution
With backend running and dependencies installed, trigger the orchestrator with:

```bash
python main.py
```

### [ VERSION HISTORY ]

* version 1.3.0 (Cascade Architecture & OOP Refactoring)

  * Implemented Model Routing (BERT to Llama-3) to optimize VRAM and inference time.
  * Refactored main.py into an Object-Oriented pipeline.
  * Integrated native Python logging for robust error tracking and monitoring.
  * Separated POST CONTEXT from CONVERSATION HISTORY in LLM prompts to fix orphan comment hallucinations.

* version 1.2.1 (model adjustment)
  * Testing how local inference works around toxicity detection
  * Migrate from llama-3 8B Q4_0 to Q8_0
  * Added a script to extract hallucinated rows
    
* version: 1.2.0 (Multimodal Update)
  * off-grid local AI inference (Ollama integration).
  * Media interceptor & visual context enrichment for images/GIFs.
  * Context-aware DFS dynamic prompt orchestration.

* version: 1.1.0
  * Tidy Data format
  * DFS processing of raw json endpoint responses 

### [ DISCLAIMER ]

Rakeddit is provided "as is" for educational and research purposes only. 
- The author assumes no liability for misuse or rate-limit violations.
- Users are responsible for complying with Reddit's ToS and 
  applicable data protection laws (GDPR, LGPD, CCPA, etc.).
- "Rakeddit" is a name. Not legal advice.
     - Because calling it a scraper isn't legally defensible (nor particularly truthful)
