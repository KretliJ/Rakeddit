import os
import json
import base64
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
    
    topics = {
        'f1': 'Incivility (rude, disrespectful tone, or insults)',
        'f2': 'Deliberate Fallacies (intentional strawman, loaded question, or false dilemma — not honest mistakes)',
        'f3': 'Ad Hominem (attacking the person instead of the argument), ONLY when attacking the specific commenter being replying to. Attacking a political figure or group belongs in f4 (Hate Speech) if dehumanizing, or nowhere if just criticism.',
        'f4': 'Hate Speech (attacks on groups/identity or dehumanization)',
        'f5': 'Sarcasm/Irony (saying the opposite of what you mean for effect, e.g., "Great job..." after a failure. Does NOT include: analogies, ellipses, rhetorical questions, or simple disagreement.)'
    }

    aggro_levels = {
        '0': 'Neutral or Gentle: Civil disagreement, objective statements, questions, laughter ("kkkk", "hahaha", "ksksks", "rsrs"). In Brazilian Portuguese, "kkk" is laughter, NOT related to hate groups.',
        '1': 'Assertive: Mild frustration or emphasis (e.g., "MDS" = "my god", "nossa" = "wow", "caramba"). Slightly heated but not attacking.',
        '2': 'Hostile: Direct insults, name-calling, clear anger, shouting ("SEU IDIOTA", "CALA BOCA"). Personal attacks without threats.',
        '3': 'Extremely Aggressive: Threats of violence, extreme rage, dehumanizing language, wishes of harm ("devia morrer", "vou te quebrar").'
    }

    system_instruction = (
        "You are a linguistic analyst specializing in online political discourse. "
        "Your task is to classify a SPECIFIC Reddit comment based on its negative traits. "
        "You will be provided with the conversation history for context, but you MUST ONLY evaluate the 'TARGET COMMENT'."
    )

    task_details = (
        "Evaluate the TARGET COMMENT for the following traits (1 if present, 0 if absent):\n"
        f"   - f1: {topics['f1']}\n"
        f"   - f2: {topics['f2']}\n"
        f"   - f3: {topics['f3']}\n"
        f"   - f4: {topics['f4']}\n"
        f"   - f5: {topics['f5']}\n"
        "Rate 'aggro' (Aggressiveness) from 0 to 3:\n"
        f"   - 0: {aggro_levels['0']} | 1: {aggro_levels['1']} | 2: {aggro_levels['2']} | 3: {aggro_levels['3']}\n"
        "\nOutput ONLY a valid JSON object."
    )

    prompt = f"""
{system_instruction}

{task_details}

### CONVERSATION HISTORY (FOR CONTEXT ONLY):
{context_chain}

### TARGET COMMENT TO ANALYZE:
[{target_comment_author}]: "{target_comment_body}"

"f3 (Ad Hominem) = 1 ONLY when attacking the PERSON making the argument.
f3 = 0 for:
- Rhetorical questions that mock an ARGUMENT 
- Jokes or nonsense comments without a clear personal target
- Attacking a third party not in the conversation
"

"f4 (Hate Speech) = 1 ONLY IF:
- Attacks a PROTECTED GROUP (race, religion, gender, disability, sexual orientation)
- Uses slurs or calls for violence

f4 = 0 for:
- Swear words directed at objects or situations ("merda", "porra", "caralho")
- General frustration expressions
- Insults about someone's character (that's f1 or f3)
"
"f5 (Sarcasm/Irony) = 1 ONLY if:
- Unmistakable  — e.g., "Great job..." after a clear failure.
f5 = 0 for:
- If you have to ask whether it is sarcasm. Subtle or ambiguous sarcasm is 0.
"
"CRITICAL: The target comment's f1-f5 and aggro scores should be based solely on its own words and tone. Do not deduct points because it agrees/disagrees with a previous comment, and do not add points because a previous comment was hostile."

### OUTPUT FORMAT:
{{
  "f1": int,
  "f2": int,
  "f3": int,
  "f4": int,
  "f5": int,
  "aggro": int
}}
"""
    print(f"[INFO] Prompt formatted")
    return prompt.strip()

# ______________________________________________________________________________________________

def calculate_toxicity(ai_response_dict):
    # Calculates normalized toxicity index based on binary classification.
    # Weights: f1=0.5, f2=0.75, f3=1.0, f4=2.0, f5=0.3
   
    if not isinstance(ai_response_dict, dict):
        return 0.0  # Fallback caso a IA não retorne um dicionário válido

    # Safe extraction with float conversion (if AI decides it wants to return "1" rather than ')
    try:
        f1 = float(ai_response_dict.get('f1', 0))
        f2 = float(ai_response_dict.get('f2', 0))
        f3 = float(ai_response_dict.get('f3', 0))
        f4 = float(ai_response_dict.get('f4', 0))
        f5 = float(ai_response_dict.get('f5', 0))
    except (ValueError, TypeError):
        return 0.0

    # defined weights
    w1, w2, w3, w4, w5 = 0.5, 0.75, 1.0, 2.0, 0.3
    
    weighted_sum = (f1 * w1) + (f2 * w2) + (f3 * w3) + (f4 * w4) + (f5 * w5)
    max_score = w1 + w2 + w3 + w4 + w5 # 4.55 currently
    
    toxicity = weighted_sum / max_score
    
    return round(toxicity, 4) # Round to 4 decimal places

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
    
    prompt = "Transcribe EVERY writing from top-left to bottom-right. Then, describe the characters' expressions. If there is a relevant character in the image, point them out. If the image is a known meme, point it out (else, say nothing about it)."

    payload = {
        "model": model_name,
        "messages": [
            {
                "role": "user",
                "content": prompt,
                "images": [imagem_base64]
            }
        ],
        "stream": False,
        "options": {
            "temperature": 0.0 # Forçe factual/deterministic responses
        }
    }

    try:
        response = requests.post(url, json=payload, timeout=120)
        
        if response.status_code != 200:
            return f"({extension}) Error HTTP Ollama: {response.status_code}"
            
        result = response.json()
        description = result.get("message", {}).get("content", "").strip()
        
        if not description:
            return f"({extension}) IA collapsed (Void String)."
            
        return f"({extension}) {description}"

    except Exception as e:
        return f"({extension}) Connection failure: {e}"