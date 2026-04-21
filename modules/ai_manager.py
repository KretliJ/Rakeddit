import os
import json
import base64
import time
import requests
from io import BytesIO
from PIL import Image
from collections import defaultdict

# Global configs
from modules.config_loader import config

AGGREGATES = config.get('PATHS', 'AGGREGATES_PATH')
IMAGE_READER = config.get('MODELS', 'IMAGE_READER')

def build_context_chain(target_id, data_dict, post_data):
    # Climbs tree from target_id using parent_id to assemble context.
    # data_dict: normalized JSONL dictionary { 'id': { ... } }
    # post_data: dictionary with original post data
    
    chain = []
    current_id = target_id
    
    # climbs tree and fetches parents
    while current_id in data_dict:
        # Skips target (already goes in prompt)
        if current_id != target_id:
            record = data_dict[current_id]
            chain.append(f"[{record['author']}]: {record['body']}")
        
        current_id = data_dict[current_id].get('parent_id')
        
        # If parent_id equals post_id, we are at root
        if current_id == post_data['id']:
            break
            
    # Appends original Post on top
    chain.append(f"[ORIGINAL POST - {post_data['author']}]: {post_data['body']}")
    
    # Inverts list for choronological order (Post -> Parent -> Children)
    return "\n".join(chain[::-1])

# ______________________________________________________________________________________________

def prompt_maker(context_chain, target_comment_author, target_comment_body):
    
    prompt = f"""You are a linguistic analyst of Brazilian Portuguese online discourse.
Evaluate ONLY the TARGET COMMENT based on the context. Output ONLY a valid JSON object. No markdown, no explanations.

FLAGS (Output 0 or 1):
- f1 (Incivility): 1 if hostile, condescending, passive-aggressive, or mocking. 0 if objective, or if attacking an abstract IDEA rather than a person ("ideia burra").
- f2 (Fallacy): 1 if deliberately using logical fallacies (e.g., strawman, whataboutism) to distort arguments.
- f3 (Ad Hominem): 1 EXCLUSIVELY if attacking the character/intelligence of the DIRECT INTERLOCUTOR. 0 if insulting politicians, third parties, or news subjects.
- f4 (Hate Speech): 1 if promoting violence, dehumanizing protected demographics, OR using graphic genital/sexual analogies involving minors in ideological debates.
- f5 (Sarcasm): 1 if stating the exact opposite of the truth to ridicule. 0 for mere exaggerations or rhetorical anger.

AGGRO (Output 0 to 3):
- 0 (Neutral): Civil, objective. Includes enthusiastic or positive swearing ("Caralho, muito bom!").
- 1 (Cynical): Passive-aggressive, mild frustration, sarcastic bite.
- 2 (Hostile): Direct insults, clear anger, name-calling, shouting (without threats).
- 3 (Extreme): Threats of violence, extreme rage, wishes of harm.

### CONVERSATION HISTORY (FOR CONTEXT ONLY):
{context_chain}

### TARGET COMMENT TO ANALYZE:
[{target_comment_author}]: "{target_comment_body}"

{{
  "f1": int,
  "f2": int,
  "f3": int,
  "f4": int,
  "f5": int,
  "aggro": int
}}"""
    
    print(f"[INFO] Prompt formatted")
    return prompt.strip()

# ______________________________________________________________________________________________

def calculate_toxicity(ai_response_dict):
    if not isinstance(ai_response_dict, dict):
        return 0.0

    try:
        f1 = float(ai_response_dict.get('f1', 0))
        f2 = float(ai_response_dict.get('f2', 0))
        f3 = float(ai_response_dict.get('f3', 0))
        f4 = float(ai_response_dict.get('f4', 0))
        f5 = float(ai_response_dict.get('f5', 0))
        aggro = float(ai_response_dict.get('aggro', 0))
    except (ValueError, TypeError):
        return 0.0

    w1, w2, w3, w4, w5 = 0.5, 0.75, 1.0, 2.0, 0.3
    weighted_sum = (f1 * w1) + (f2 * w2) + (f3 * w3) + (f4 * w4) + (f5 * w5)
    max_score = 4.55
    
    base_toxicity = weighted_sum / max_score
    m_aggro = {0.0: 0.5, 1.0: 0.75, 2.0: 1.0, 3.0: 1.0}.get(aggro, 0.5)
    
    # Function collapse into toxic/non-toxic: Aggro as multiplier
    final_toxicity = base_toxicity * m_aggro
    
    # Toxicity Floor:
    # Protects against false negativs (f_sum = 0) or obfuscated flags 
    # where message is explicitly hostile or extreme (aggro >= 2)
    if aggro >= 2.0:
        floor = aggro * 0.05 # aggro 2 -> 0.1000 | aggro 3 -> 0.1500
        # Score will be biggest value between normal calculation and safety floor
        final_toxicity = max(final_toxicity, floor)
        
    # Roof (Normalizes to a maximum of 1.0)
    return round(min(final_toxicity, 1.0), 4)
# ______________________________________________________________________________________________


def call_vision_ai(image_path, extension, model_name=IMAGE_READER):
    # Uses AI defined in IMAGE_READER as a media interceptor

    if not image_path or not os.path.exists(image_path):
        return f"({extension}) File not found."

    try:
        # --- interception ---
        with Image.open(image_path) as img:
            # If GIF, strikes initial frame
            if getattr(img, "is_animated", False):
                img.seek(0)
            
            # Removes alpha to avoid breaking tensors
            img = img.convert("RGB")
            
            # Being mindful of context window - pun intended
            img.thumbnail((1024, 1024))
            
            # Saves in a virtual RAM buffer
            buffered = BytesIO()
            img.save(buffered, format="JPEG")
            imagem_base64 = base64.b64encode(buffered.getvalue()).decode('utf-8')
            
    except Exception as e:
        return f"({extension}) Error sanitizing image: {e}"

    # --- local API call ---
    url = "http://localhost:11434/api/chat"
    
    # Initial complex prompt for maximum extraction
    current_prompt = (
        "Extract all visible text from top-left to bottom-right. "
        "Describe the main subjects, focusing on their facial expressions and body language. "
        "Explicitly identify any recognizable public figures, politicians, or pop-culture characters. "
        "If the image is a known internet meme format, state its name or usual context. "
        "Be concise and objective."
    )
    max_attempts = 3
    for attempt in range(max_attempts):
        # Payload defined per attemptto allow for change
        payload = {
            "model": model_name,
            "messages": [
                {
                    "role": "user",
                    "content": current_prompt,
                    "images": [imagem_base64]
                }
            ],
            "stream": False,
            "options": {
                "temperature": 0.0,  # Factual/Deterministic
                "num_predict": 150,
                "num_ctx": 4096,
                "top_p": 0.1
            }
        }

        try:
            # Timeout 3 minutes
            response = requests.post(url, json=payload, timeout=180)
            
            if response.status_code == 200:
                description = response.json().get("message", {}).get("content", "").strip()
                if description:
                    return description
            
        except requests.exceptions.Timeout:
            print(f"⏰ Timeout na tentativa #{attempt+1}/{max_attempts}")
            
            # Fallback: Simplifies prompt for next attempt - After all, it already failed
            current_prompt = "Describe the image"
            
            if attempt < max_attempts - 1:
                # Delay for backend/VRAM to breathe
                time.sleep(30)
            else:
                print(f"({extension}) Persistent failure after {max_attempts} attempts.")
                return "Generic image for reaction or enriching explanation"
        
        except Exception as e:
            return f"({extension}) Error: {e}"

    return "Generic image for reaction or enriching explanation"