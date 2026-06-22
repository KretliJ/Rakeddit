# Rakeddit: A Systematic Social Media Harvester

### [ ARCHITECTURE OVERVIEW ]

Rakeddit is a multi-stage Python pipeline designed for high-integrity behavioral data extraction, structural graph analysis, and local multimodal AI analysis from Reddit. It prioritizes "Gentle Harvesting" through randomized jittering and automated rate-limit handling, alongside a Zero-Cloud Dependency architecture ensuring long-term structural data integrity without reliance on external commercial APIs.

* **LLM & Graph Ready:** Outputs normalized "Tidy Data", optimized for local transformer context windows (Stance and Polarity Detection) and topological Network Theory computations (`networkx`).
* **Cascade Analytics Orchestrator:** Features a unified, multi-threaded Model-View-Controller (MVC) pipeline that aggregates and computes multi-platform graph dynamics, structural cascades, behavior profiles, and sequential triadic motifs over a massive 1.1M node dataset.

WARNING:

* Strictly for research purposes. This is designed to realistically collect up to 1,000,000 data points.
* This is designed to run over extended periods of time.
* All data collection follows a gentle approach to respect platform infrastructure and privacy guidelines.
  * This will not collect email, phone numbers, real names, social security numbers, or check when you last used the bathroom.
* This caused my IP to get a long-term cooldown from reddit's servers, so use it with caution (or not, if you expect to finish in under a week).

### [ PIPELINE STAGES ]

#### STAGE 1: Subreddit Ingestion

The system identifies active threads and extracts the raw JSON structure of a community's front page.

* **Mechanism:** Recursive `GET` requests to native `.json` endpoints.
* **Compliance:** Randomized jitter (3.0s - 7.0s) and exponential back-off for HTTP 429 exceptions.

#### STAGE 2: Relational Flattening (Tree Processing)

Transforms nested, conversational JSON reply hierarchies into flat, highly connected relational rows without breaking structural cascade vectors.

* **Mechanism:** Stack-based Depth-First Search (DFS) execution bypassing Python standard stack recursion depth errors on long-tail debate layers.
* **Graph Integrity:** Injects a `post_header` as root anchor (`depth: 0`). Sanitizes AutoMod and deleted accounts to retain path connectivity, computing exact temporal delta properties and human-readable Unix timestamps.

#### STAGE 3: Multimodal Context Enrichment

Sanitizes and processes visual elements (JPEGs, PNGs, GIFs) into semantic annotations to capture rhetorical intent and OCR data without interrupting the main text-based pipeline.

* **Mechanism:** In-RAM image interceptor (`Pillow`) flattens alpha channels, rescaling dimensions to safeguard VRAM limits.
* **Vision Engine:** Standardized schemas connecting to local Small Vision Language Models (e.g., `Qwen2.5-VL` via `Ollama`) to handle visual extraction protocols.

#### STAGE 4: Analytics Engine & Unified Orchestration

Computes behavioral features, user structures, and validation curves across different subreddits.

* **Structural Suite (Figure 1):** Renders high-fidelity Complementary Cumulative Distribution Functions (CCDFs) spanning Structural Virality (Wiener index mappings), Max Depth, Max Breadth, Total Message Volumes, and Participant Scales. Includes bivariant temporal trendlines tracking velocity dynamics over depth and size boundaries.
* **User Interaction Motifs (Figure 2):** Extracts directed structural configurations (Dyads, Mutual Pairs, Chains, Fan-In, Fan-Out, Triangles) and executes multi-group Kruskal-Wallis non-parametric significance testing.
* **Platform Interaction Reactions (Figure 3):** Computes CCDFs tracking Average Cascade Scores (Upvotes minus Downvotes), mapping algorithmic reinforcement behavior against group toxicity levels.
* **Triadic Sentiments (RQ2):** Performs sequential time-series scanning across grandfather-father-son structures to trace emotional transitions (Persistence, Convergence, Shifts, Oscillations).
* **Taxonomy & Intersections (BCC / RQ3):** Generates structural regression curves mapping conflict indexes against platform virality boundaries, integrating global multi-marker master legends for dense point structures.

---

### [ USAGE ]

#### [Functions](functions.md)

### [ REQUIREMENTS & STARTING UP ]

#### 1. System Requirements

* **OS:** Windows or Linux (Debian-based distributions recommended for Python integration).
* **Python:** Version 3.11 or higher.
* **Backend:** [Ollama](https://ollama.com/) must be installed and running locally as a background service.
* **Hardware:** A dedicated GPU is highly recommended (e.g., 8GB VRAM or higher) for stable multimodal parsing and NLP inference.

#### 2. Environment Setup

Clone the repository and install the required Python packages. The pipeline relies on `requests` for local API interactions, `Pillow` for in-RAM image sanitization, and `transformers`/`torch` for cascade filtering.

```bash
git clone https://github.com/KretliJ/Rakeddit
cd Rakeddit
pip install -r requirements.txt
```

#### 3. Local AI Provisioning (Ollama)

```bash
# Pull the Sentiment Polarity Classifier Model
ollama pull cardiffnlp/twitter-xlm-roberta-base-sentiment

# Pull the Image Reader Engine (Vision / OCR)
ollama pull qwen3-vl:2b-instruct
```

#### 4. Configuration (config.ini)

Ensure your `config.ini` is set up correctly in the root directory. Do not use quotes around the model names, as the strict protocol validation will reject the payload with an HTTP 400 Bad Request.

```Ini,
[HEADERS]
User-Agent = Research_Gentle_Harvesting_With_Jitter (contact: [YOUR EMAIL])
# CHANGE THIS TO REFLECT YOUR USE CASE

[PATHS]
# CHANGE BASE_PATH IF YOU WANT TO DUMP FILES ELSEWHERE
BASE_PATH = ./DATA/json_dumps/
AGGREGATES_PATH = ./DATA/aggregates/
MEDIA_PATH = ./DATA/temp_media/
MULTIMODAL_PATH = ./DATA/vision_processing/
LOGGING_PATH = ./logging/

[MODELS]
# CHANGE THESE TO USE DIFFERENT AGENTS
MAIN_INFER = cardiffnlp/twitter-xlm-roberta-base-sentiment
IMAGE_READER = qwen3-vl:2b-instruct
```

#### 5. Execution

With backend running and dependencies installed, trigger the orchestrator with:

```bash
python main.py
```

### [ VERSION HISTORY ]

* Version 4.3.0 (Docker & Modular NLP Update)
  * Introduced `GUI_NLP.py` for modular, GPU-accelerated NLP and Psycholinguistics pipeline execution.
  * Integrated Docker containerization with automatic state management and dynamic volume mapping.
  * Resolved Pandas Series typing conflicts and deadlock issues in the BERTopic and WordCloud pipeline.
  * Isolated LIWC processing for standalone execution, improving inference cache efficiency.

<details>
<summary>Version History  </summary>

* Version 4.2.2 (Stability)
  * Improved text validation
  * Refactored homophilia method
  * Refactored reports
* Version 4.2.0 (NLP Engine Update)
  * BERTopic and wordcloud analysis integration
* Version 4.1.0 (Unified Analysis Update)
  * Unified legacy analytic pipelines into an optimized MVC model (GUI.py, Methods.py, Utilities.py).
  * Implemented structured multi-folder routing based on user-selected grouping strategies.
  * Engineered multi-threaded "Run All" pipeline runner to optimize bulk processing loops.
  * Standardized file outputs to PDF
* Version 3.1.0 (Analysis Update)
  * Final analytics pipeline build
  * Replaced CSVs with RAM DataFrames
  * Direct export to PNG & PDF
  * Improved resume state to main pipeline in main.py
* Version 2.2.0 (Thesis Milestone Update)
  * Analytics GUI automatic categories (4 sociologic types)
  * Infer engine with batching and resume inteligente
  * GNN GraphSAGE with structural features (degree, time, depth)
  * Interactive orchestrator menu (6 operation modes)
* Version 2.0.0 (Graph Structure & Telemetry Update)
  * Objective changes made on main project pipeline
  * Introduced native depth tracking and post_header anchoring (depth: 0) for advanced cascade analysis.
  * Preserved graph integrity by sanitizing [AutoModerator] and [deleted] nodes while maintaining their parent_id links.
  * Implemented is_valid_text flags for NLP token optimization.
  * Added metadata_footer for exact Unix-to-Human temporal windows at the EOF of datasets.
  * Added automated Markdown telemetry generation during database building.
  * Created analytics.py for calculating and plotting Average Breadth and Max Depth structural signatures.
* Version 1.3.0 (Cascade Architecture & OOP Refactoring)
  * Implemented Model Routing (BERT to Llama-3) to optimize VRAM and inference time.
  * Refactored main.py into an Object-Oriented pipeline.
  * Integrated native Python logging for robust error tracking and monitoring.
  * Separated POST CONTEXT from CONVERSATION HISTORY in LLM prompts to fix orphan comment hallucinations.
* Version 1.2.1 (model adjustment)
  * Testing how local inference works around toxicity detection
  * Migrate from llama-3 8B Q4_0 to Q8_0
  * Added a script to extract hallucinated rows
* Version: 1.2.0 (Multimodal Update)
  * off-grid local AI inference (Ollama integration).
  * Media interceptor & visual context enrichment for images/GIFs.
  * Context-aware DFS dynamic prompt orchestration.
* Version: 1.1.0
  * Tidy Data format
  * DFS processing of raw json endpoint responses

</details>



### [ DISCLAIMER ]

Rakeddit and related modules are provided "as is" for educational, scientific research, and result reproduction purposes only.

- The authors and developer assume zero liability for platform rate-limit compliance infractions.
- Users are responsible for complying with Reddit's ToS and
  applicable data protection laws (GDPR, LGPD, CCPA, etc.).
- "Rakeddit" is a name. Not legal advice.
  - Because calling it a scraper isn't particularly truthful.
