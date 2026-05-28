import os
import base64
import time
import requests
from io import BytesIO
from PIL import Image

from modules.config_loader import config

IMAGE_READER = config.get('MODELS', 'IMAGE_READER', fallback="qwen3-vl:2b-instruct")
FALLBACK_TEXT = "Imagem de reação ou explicação"

def call_vision_ai(image_path, extension, model_name=IMAGE_READER):
    # 1. Catch missing files
    if not image_path or not os.path.exists(image_path):
        return FALLBACK_TEXT

    # 2. Catch corrupted images
    try:
        with Image.open(image_path) as img:
            if getattr(img, "is_animated", False):
                img.seek(0)
            
            img = img.convert("RGB")
            img.thumbnail((1024, 1024))
            
            buffered = BytesIO()
            img.save(buffered, format="JPEG")
            imagem_base64 = base64.b64encode(buffered.getvalue()).decode('utf-8')
            
    except Exception as e:
        print(f"   [AI ERROR] Image sanitization failed: {e}")
        return FALLBACK_TEXT

    url = "http://localhost:11434/api/chat"
    current_prompt = (
        "Transcreva todo o texto da imagem. Em seguida, identifique figuras públicas, "
        "políticos ou memes conhecidos. Descreva a ação e o tom visual usando poucas "
        "palavras-chave. Seja seco, direto e não use frases introdutórias."
    )
    
    max_attempts = 3
    for attempt in range(max_attempts):
        payload = {
            "model": model_name,
            "messages": [{"role": "user", "content": current_prompt, "images": [imagem_base64]}],
            "stream": False,
            "options": {
                "temperature": 0.0,
                "num_predict": 200,
                "num_ctx": 4096,
                "top_p": 0.1,
                "repeat_penalty": 1.2,
                "presence_penalty": 0.0,
                "stop": ["\n\n", "###"]
            }
        }

        try:
            response = requests.post(url, json=payload, timeout=180)
            if response.status_code == 200:
                description = response.json().get("message", {}).get("content", "").strip()
                if description:
                    return description
        except requests.exceptions.Timeout:
            print(f"⏰ Timeout na tentativa #{attempt+1}/{max_attempts}")
            current_prompt = "Descreva a imagem"
            if attempt < max_attempts - 1:
                time.sleep(30)
            else:
                return FALLBACK_TEXT
        except Exception as e:
            print(f"   [AI ERROR] Inference failed: {e}")
            return FALLBACK_TEXT

    return FALLBACK_TEXT