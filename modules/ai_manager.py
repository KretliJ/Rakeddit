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

def prompt_maker(context_chain, target_comment_author, target_comment_body, post_title, post_content):
    prompt = f"""You are a Digital Forensic Auditor specializing in Brazilian Penal Law and online discourse.
Your task is to analyze the TARGET COMMENT based on the provided context and output a normative severity score.
Output ONLY a valid JSON object. Do not add explanations.

FLAGS (Output 0 or 1):
- f1 (Profanity): 1 for explicit swear words, obscenities, or profane slurs. 
- f2 (Threats): 1 for an actionable promise to inflict physical or unjust damage.
- f3 (Insult/Ad Hominem): 1 STRICTLY for direct attacks on an interlocutor's honor/character (e.g., "você é um idiota"). 0 for attacking abstract ideas.
- f4 (Identity Hate): 1 for dehumanizing or segregating protected groups. 0 for generic political polarization, citing historical events, or satirical strawman arguments.
- f5 (Perturbation): 1 for functionally obstructive speech (spam, pure rage-bait, harassment). 0 for casual slang, memes, or intense but functional debate. 0 if in doubt.

AGGRO (Output 0 to 3):
- 0: No hostility, functional speech, abstract debate.
- 1: Sarcasm, cynical tone, passive-aggressive.
- 2: Combative, anger, clear intent to offend.
- 3: Explicit hostility, violence, dehumanization.

GENERAL AUDIT RULE: Do not mistake informal internet slang or intense political disagreement for toxicity. HOWEVER, internet culture NEVER excuses direct personal attacks (f3) or explicit profanity (f1). A targeted insult is always an insult. Abstract debate is safe; targeted aggression is toxic.

### ORIGINAL POST CONTEXT:
Title: "{post_title}"
Content: "{post_content}"

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

    # Juridical proportionality weights (Coarse Tuning)
    w1, w2, w3, w4, w5 = 0.5, 1.0, 1.0, 2.0, 0.25
    
    weighted_sum = (f1 * w1) + (f2 * w2) + (f3 * w3) + (f4 * w4) + (f5 * w5)
    max_score = w1 + w2 + w3 + w4 + w5  # 4.75
    
    base_toxicity = weighted_sum / max_score
    
    # Dosimetry (Fine Tuning)
    aggro_multipliers = {
        0.0: 0.875,  # Attenuant
        1.0: 1.000,  # Generic Intent
        2.0: 1.125,  # Aggravating Circumstances
        3.0: 1.250   # Qualifying Circumstances)
    }
    
    m_aggro = aggro_multipliers.get(aggro, 1.0)
    
    final_toxicity = base_toxicity * m_aggro
    
    # Roof: Guarantees integrity of interval [0.0, 1.0]
    return round(min(final_toxicity, 1.0), 2)
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
                "num_ctx": 8192,
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