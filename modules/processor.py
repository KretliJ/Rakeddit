import os
import json
import configparser
import glob
import requests
import re
import subprocess
import time
from modules.json_harvester import downloader_function
from datetime import datetime, timedelta
from modules.ai_manager import call_vision_ai


from modules.config_loader import config
# Globals
processed_count = 0
media_processed_count = 0
media_count = 0
vision_ai_calls = 0
vision_ai_total_time = 0.0
AGGREGATES = config.get_path('PATHS', 'AGGREGATES_PATH')
IMAGES = config.get_path('PATHS', 'MEDIA_PATH')
MULTIMODAL = config.get_path('PATHS', 'MULTIMODAL_PATH')


def extract_from_post(folder_path, limit="none", aggregates_dir=AGGREGATES):
    global processed_count
    # Flattens a comment tree using Depth-First Search.
    # folder_path: Base path for subreddit folder (e.g. './json_dumps/')
    # limit: "none" to read all (default), or string with subreddit name (e.g. "anime")
    
    # Choose which folders to seek based on limit
    target_folders = []
    if limit.lower() == "none":
        target_folders = [os.path.join(folder_path, d) for d in os.listdir(folder_path) 
                          if os.path.isdir(os.path.join(folder_path, d))]
    else:
        target_folders = [os.path.join(folder_path, limit)]

    current_time = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    # Write new structured JSONL (JSON Lines)
    if limit != "none":
        out_file = f"{aggregates_dir}/{limit.upper()}_data_normalized_" + current_time + ".jsonl"
    else:
        out_file = f"{aggregates_dir}FULL_data_normalized_" + current_time + ".jsonl"

    print(f"\n[INFO] Data extraction started. Streaming to: {out_file}")

    processed_count = 0
    min_ts = float('inf')
    max_ts = float('-inf')
    # Open output file once, append line by line
    with open(out_file, 'w', encoding='utf-8') as f_out:
        for folder in target_folders:
            for filename in os.listdir(folder):
                if filename.endswith(".json"):
                    filepath = os.path.join(folder, filename)
                    with open(filepath, 'r', encoding='utf-8') as f_in:
                        try:
                            data = json.load(f_in)
                            post_data = data[0]['data']['children'][0]['data']
                            comments_data = data[1]['data']['children']
                        except: continue
                    ts = post_data.get('created_utc')
                    if ts:
                        min_ts = min(min_ts, ts)
                        max_ts = max(max_ts, ts)
                    # --- 1. POST HEADER INSERTION (DEPTH 0) ---
                    post_record = {
                        "type": "post_header",
                        "id": post_data['id'],
                        "parent_id": None,
                        "post_id": post_data['id'],
                        "subreddit": post_data['subreddit'],
                        "author": post_data['author'],
                        "timestamp": post_data['created_utc'],
                        "title": post_data.get('title', ''),
                        "body": post_data.get('selftext', ''),
                        "depth": 0, # Raiz da cascata
                        "metadata_score": post_data.get('score', 0)
                    }
                    f_out.write(json.dumps(post_record, ensure_ascii=False) + "\n")
                    processed_count += 1

                    # --- 2. DFS COM RASTREAMENTO DE PROFUNDIDADE ---
                    # O stack agora guarda (objeto_comentario, id_do_pai, profundidade)
                    stack = []
                    for c in reversed(comments_data):
                        if c.get('kind') == 't1':
                            stack.append((c, post_data['id'], 1))
                    
                    while stack:
                        current_comment, parent_id, current_depth = stack.pop()
                        c_data = current_comment['data']

                        c_author = c_data.get('author', '[deleted]')    
                        c_body = c_data.get('body', '[empty]')

                        # We don't need automod or removed comments, but they must stay not to break the graphs
                        if c_author.lower() == "automoderator":
                            c_body = "[AutoModerator]"

                        is_valid = c_body not in ["[deleted]", "[removed]", "[AutoModerator]", "[empty]"]

                        ts_c = c_data.get('created_utc')
                        if ts_c:
                            min_ts = min(min_ts, ts_c)
                            max_ts = max(max_ts, ts_c)

                        comment_record = {
                            "type": "comment",
                            "id": c_data.get('id'),
                            "parent_id": parent_id,
                            "post_id": post_data['id'],
                            "subreddit": post_data['subreddit'],
                            "author": c_author,
                            "timestamp": c_data.get('created_utc'),
                            "body": c_body,
                            "depth": current_depth,
                            "metadata_score": c_data.get('score', 0),
                            "is_valid_text": is_valid
                        }
                        f_out.write(json.dumps(comment_record, ensure_ascii=False) + "\n")
                        processed_count += 1
                        
                        replies = c_data.get('replies')
                        if replies and isinstance(replies, dict):
                            children = replies['data']['children']
                            for r in reversed(children):
                                if r.get('kind') == 't1':
                                    # Incrementa a profundidade para os filhos
                                    stack.append((r, c_data.get('id'), current_depth + 1))
                        
        if processed_count > 0:
            metadata_eof = {
                "type": "metadata_footer",
                "total_records": processed_count,
                "temporal_window": {
                    "unix_start": min_ts,
                    "unix_end": max_ts,
                    "human_start": datetime.fromtimestamp(min_ts).strftime('%Y-%m-%d %H:%M:%S'),
                    "human_end": datetime.fromtimestamp(max_ts).strftime('%Y-%m-%d %H:%M:%S'),
                    "duration_days": round((max_ts - min_ts) / 86400, 2)
                },
                "generated_at": datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }
            f_out.write(json.dumps(metadata_eof, ensure_ascii=False) + "\n")

    return out_file

def get_processed_count():
    return processed_count

# ______________________________________________________________________________________________

def process_media(jsonl_filepath):
    # 1. loops over normalized dataset
    # 2. intercepts comments with media
    # 3. enriches text body via AI visualization
    # 4. saves new multimodal JSONL
    
    # ==========================================
    # CONFIG AND PATHS
    # ==========================================
    global media_processed_count, media_count
    current_dir = os.path.dirname(os.path.abspath(__file__))
    config_path = os.path.join(current_dir, '..', 'config.ini')
    
    config = configparser.ConfigParser()
    config.read(config_path)
    AGGREGATES = config.get('PATHS', 'AGGREGATES_PATH', fallback='./aggregates')    
    
    base = os.path.basename(jsonl_filepath)
    file_name = f"MULTIMODAL_{base}"
    out_path = os.path.join(MULTIMODAL, file_name)

    print(f"\n[INFO] Initiating enriching pipeline.")
    print(f"[INFO] Reading: {base}")
    
    # ==========================================
    # ETA CALC PRESCAN
    # ==========================================
    is_potentially_media = 0
    with open(jsonl_filepath, 'r', encoding='utf-8') as f_scan:
        for line in f_scan:
            # Raw string lookup is faster
            if "http" in line or "![" in line:
                is_potentially_media += 1
                
    avg_time_per_image = 1.24  # For author system that's 1.24s
    eta_seconds = int((is_potentially_media * avg_time_per_image)*2) # 2 * accounts for image download time
    eta_formatted = str(timedelta(seconds=eta_seconds))
    
    print(f"[ETA] Detected ~{is_potentially_media} items with potential media.")
    print(f"[ETA] Estimated time for step 3: {eta_formatted} ({avg_time_per_image}s/req)\n")

    media_processed_count = 0
    media_count = 0
    # ==========================================
    # PROCESSING
    # ==========================================
    # Opens input file for reading and output for simultaneous manipulation
    with open(jsonl_filepath, 'r', encoding='utf-8') as f_in, \
         open(out_path, 'w', encoding='utf-8') as f_out:
        
        for line in f_in:
            record = json.loads(line)
            original_body = record.get('body', '')

            # "Fast Fail" optimization:
            # Only call process_visual_content if text has obvious image link or markdown formats
            if "http" in original_body or "![" in original_body:
                
                # AI call
                enriched_body = process_visual_content(original_body)
                
                # If text changed (processed media), updates the record
                if enriched_body != original_body:
                    record['body'] = enriched_body
                    media_count += 1
            
            # Writes registries (modified or not) in new JSONL
            f_out.write(json.dumps(record, ensure_ascii=False) + "\n")
            media_processed_count += 1

    # ==========================================
    # REPORT
    # ==========================================
    print(f"\n[SUCCESS] Visual processing completed.")
    print(f" -> Saved in: {out_path}")
    print(f" -> Total iterations: {media_processed_count}")
    print(f" -> Enriched iterations: {media_count}")
    
    return out_path

def media_get_processed_count():
    return media_processed_count

def media_get_media_count():
    return media_count
# ______________________________________________________________________________________________

def process_visual_content(body_text):
    global vision_ai_calls, vision_ai_total_time
    # Flattens reddit proprietary formatting, downloads media and evokes AI
    
    if not body_text:
        return body_text

# 1. Giphy treatment (UPDATED: Catches both ![gif] and ![img] disguised giphys)
    giphy_pattern = r'!\[(?:gif|img)\]\(giphy\|([a-zA-Z0-9]+)[^)]*\)'
    body_text = re.sub(giphy_pattern, r'https://i.giphy.com/\1.gif', body_text)

    # 1.5. Subreddit Emotes (NEW: Safely flags custom server emojis without crashing the downloader)
    emote_pattern = r'!\[(?:img|gif)\]\(emote\|[^)]+\)'
    body_text = re.sub(emote_pattern, '[Reddit Emote]', body_text)

    # ==========================================
    # ---> NATIVE REDDIT MARKDOWN <---
    # ==========================================
    # Converts ![img](mvv0yl8plgnf1) -> https://i.redd.it/mvv0yl8plgnf1.jpeg
    native_img_pattern = r'!\[img\]\(([a-zA-Z0-9]{10,20})\)'
    body_text = re.sub(native_img_pattern, r'https://i.redd.it/\1.jpeg', body_text)
    
    # Converts ![gif](mvv0yl8plgnf1) -> https://i.redd.it/mvv0yl8plgnf1.gif
    native_gif_pattern = r'!\[gif\]\(([a-zA-Z0-9]{10,20})\)'
    body_text = re.sub(native_gif_pattern, r'https://i.redd.it/\1.gif', body_text)
    # ==========================================

    # 2. Decompositon from preview.redd.it
    preview_pattern = r'https?://preview\.redd\.it/([a-zA-Z0-9_-]+\.(?:jpeg|jpg|png|gif))(?:\?[^\s\])]*)?'
    body_text = re.sub(preview_pattern, r'https://i.redd.it/\1', body_text)

    # 2.5. Youtube Cleanup (ADICIONADO)
    if "youtube" in body_text.lower() or "youtu.be" in body_text.lower():
        body_text = process_youtube_links(body_text)

    # 3. URLs extraction
    media_pattern = r'(https?://\S+\.(?:jpg|jpeg|png|gif|mp4))'
    links = re.findall(media_pattern, body_text)

    if not links:
        return body_text

    # 4. Image processing with text filter
    for link in set(links):
        ext = link.split('.')[-1].split('?')[0].upper()
        
        image_data = downloader_function(link)
        print(f"[IMAGE AI - ({ext})] Analyzing from: {link}")
        
        # --- TELEMETRIA: Medindo apenas o tempo de inferência ---
        t_start = time.time()
        description = call_vision_ai(image_data, extension=ext)
        t_end = time.time()
        
        vision_ai_total_time += (t_end - t_start)
        vision_ai_calls += 1
        # --------------------------------------------------------
        if re.search(r'(.{3,10})\1{3,}', description):
            description = "Imagem de reação ou explicação"
   
        replacement_tag = f"[CONTEUDO VISUAL: {description}]"
        body_text = body_text.replace(link, replacement_tag)
        
    return body_text.strip()

# ______________________________________________________________________________________________

def get_vision_telemetry():
    avg_time = (vision_ai_total_time / vision_ai_calls) if vision_ai_calls > 0 else 0.0
    return {
        "AI CALLS": vision_ai_calls,
        "TOTAL INF TIME": round(vision_ai_total_time, 2),
        "AVERAGE TIME": round(avg_time, 2)
    }

# ______________________________________________________________________________________________

def process_multimodal_dataset(
    aggregates_dir="DATA/2-aggregates", 
    temp_file="DATA/3-vision_processing/MULTIMODAL_TEMP.jsonl", 
    final_file="DATA/3-vision_processing/MULTIMODAL_FINAL.jsonl",
    base_media_path="DATA/1-raw" # Ajuste para a pasta onde as imagens originais foram salvas
):
    """
    Função End-to-End: Unifica, calcula metadados, processa Qwen (com resume) e salva.
    """
    
    # =========================================================
    # FASE 1 & 2: UNIFICAÇÃO E CÁLCULO DE METADADOS
    # =========================================================
    if not os.path.exists(temp_file):
        print(f"[*] Iniciando unificação. Lendo de '{aggregates_dir}'...")
        os.makedirs(os.path.dirname(temp_file), exist_ok=True)
        
        files = glob.glob(os.path.join(aggregates_dir, "**", "*.json*"), recursive=True)
        total_records = 0
        timestamps = []
        
        with open(temp_file, 'w', encoding='utf-8') as f_out:
            for file_path in files:
                with open(file_path, 'r', encoding='utf-8') as f_in:
                    try:
                        data = json.load(f_in)
                        if isinstance(data, dict): data = [data]
                    except json.JSONDecodeError:
                        f_in.seek(0)
                        data = [json.loads(line) for line in f_in if line.strip()]

                    for record in data:
                        if record.get('type') == 'metadata_footer':
                            continue
                            
                        f_out.write(json.dumps(record, ensure_ascii=False) + '\n')
                        total_records += 1
                        
                        ts = record.get('created_utc') 
                        if ts: timestamps.append(float(ts))

        if timestamps:
            unix_start, unix_end = min(timestamps), max(timestamps)
            footer = {
                "type": "metadata_footer",
                "total_records": total_records,
                "temporal_window": {
                    "unix_start": unix_start,
                    "unix_end": unix_end,
                    "human_start": datetime.fromtimestamp(unix_start).strftime("%Y-%m-%d %H:%M:%S"),
                    "human_end": datetime.fromtimestamp(unix_end).strftime("%Y-%m-%d %H:%M:%S"),
                    "duration_days": round((unix_end - unix_start) / 86400, 2)
                },
                "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            }
            with open(temp_file, 'a', encoding='utf-8') as f_out:
                f_out.write(json.dumps(footer, ensure_ascii=False) + '\n')
                
        print(f"[+] Unificação concluída: {total_records} registros salvos no TEMP.")
    else:
        print(f"[*] Arquivo temporário já existe. Pulando etapa de unificação.")


    # =========================================================
    # FASE 3 & 4: RESUME E PROCESSAMENTO MULTIMODAL (QWEN)
    # =========================================================
    print(f"\n[*] Iniciando processamento de Mídia (Qwen) para '{final_file}'...")
    
    processed_ids = set()
    if os.path.exists(final_file):
        with open(final_file, 'r', encoding='utf-8') as f_final:
            for line in f_final:
                try:
                    record = json.loads(line)
                    if 'id' in record: processed_ids.add(record['id'])
                except: continue
        print(f"[RESUME] {len(processed_ids)} registros já processados serão ignorados.")

    with open(temp_file, 'r', encoding='utf-8') as f_temp, \
         open(final_file, 'a', encoding='utf-8') as f_out:
        
        for line in f_temp:
            record = json.loads(line)
            
            if record.get('type') == 'metadata_footer':
                f_out.write(json.dumps(record, ensure_ascii=False) + '\n')
                continue
                
            record_id = record.get('id')
            if record_id in processed_ids:
                continue 
                
            # --- INTEGRAÇÃO DA SUA LÓGICA DO QWEN ---
            # Só fazemos o esforço de buscar imagem se for um post raiz (comentários raramente têm mídia acoplada dessa forma no raw)
            if record.get('type') == 'post_header' or 'url' in record:
                sub_name = record.get('subreddit', 'unknown')
                target_dir = os.path.join(base_media_path, sub_name)
                
                image_path = None
                file_ext = ""
                
                # Tenta localizar a mídia física baixada pelo scraper
                for ext in ['.jpg', '.png', '.jpeg', '.gif']:
                    test_path = os.path.join(target_dir, f"{record_id}{ext}")
                    if os.path.exists(test_path):
                        image_path = test_path
                        file_ext = ext
                        break
                
                if image_path:
                    print(f"   -> [QWEN VISION] Analisando imagem do post: {record_id}{file_ext}")
                    # Chama a sua função importada do ai_manager
                    description = call_vision_ai(image_path, file_ext)
                    
                    record['vision_description'] = description
                    record['has_media'] = True
                else:
                    record['vision_description'] = None
                    record['has_media'] = False
            else:
                record['vision_description'] = None
                record['has_media'] = False

            # Salva no arquivo final
            f_out.write(json.dumps(record, ensure_ascii=False) + '\n')
            f_out.flush() # Salva imediatamente no disco (Proteção contra crash)

    print("\n[+] Processamento Multimodal finalizado com sucesso!")

# ______________________________________________________________________________________________

def get_youtube_title(url):
    """Extrai o título de um vídeo do YouTube sem usar API oficial."""
    try:
        # User-agent para evitar bloqueios básicos
        headers = {'User-Agent': 'Mozilla/5.0'}
        r = requests.get(url, headers=headers, timeout=10)
        
        if r.status_code == 200:
            # Busca a tag <title>
            match = re.search(r'<title>(.*?)</title>', r.text, re.IGNORECASE)
            if match:
                title = match.group(1)
                # Limpa sufixos comuns do YouTube
                clean_title = title.replace(' - YouTube', '').strip()
                return clean_title
    except Exception as e:
        print(f"   [YT ERROR] Falha ao acessar {url}: {e}")
        
    return "Vídeo do YouTube (Título Indisponível)"

# ______________________________________________________________________________________________

def process_youtube_links(body_text):
    """Locates YouTube links and replaces them with the extracted title."""
    # Matches youtube.com, m.youtube.com, www.youtube.com, youtu.be, and shorts.
    yt_pattern = r'(https?://(?:m\.|www\.)?(?:youtube\.com/(?:watch\?.*v=|shorts/|embed/)|youtu\.be/)[a-zA-Z0-9_-]+(?:[^\s\])]*))'
    links = re.findall(yt_pattern, body_text)
    
    if not links:
        return body_text

    for link in set(links):
        title = get_youtube_title(link)
        replacement = f"[VIDEO: {title}]"
        body_text = body_text.replace(link, replacement)
        
    return body_text

# ______________________________________________________________________________________________

def write_metadata_footer(jsonl_path):
    import json
    from datetime import datetime
    import os
    
    total_records = 0
    timestamps = []
    
    if not os.path.exists(jsonl_path):
        print(f"[ERROR] Arquivo não encontrado para gerar footer: {jsonl_path}")
        return

    with open(jsonl_path, 'r', encoding='utf-8') as f:
        for line in f:
            try:
                record = json.loads(line)
                if record.get('type') == 'metadata_footer':
                    continue
                
                total_records += 1
                ts = record.get('timestamp') or record.get('created_utc')
                if ts:
                    timestamps.append(float(ts))
            except json.JSONDecodeError:
                continue

    if total_records == 0:
        return

    min_ts = min(timestamps) if timestamps else 0
    max_ts = max(timestamps) if timestamps else 0
    
    # Exact requested structure
    footer = {
        "type": "metadata_footer",
        "total_records": total_records,
        "temporal_window": {
            "unix_start": min_ts,
            "unix_end": max_ts,
            "human_start": datetime.fromtimestamp(min_ts).strftime('%Y-%m-%d %H:%M:%S') if min_ts else "N/A",
            "human_end": datetime.fromtimestamp(max_ts).strftime('%Y-%m-%d %H:%M:%S') if max_ts else "N/A"
        },
        "generated_at": datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    }
    
    with open(jsonl_path, 'a', encoding='utf-8') as f_out:
        f_out.write(json.dumps(footer, ensure_ascii=False) + "\n")
        
    print(f"[INFO] Metadata Footer injected ({total_records} records).")

# ______________________________________________________________________________________________

def apply_youtube_cleanup_only(input_path, output_path):
    """
    Passagem exclusiva para limpar links do YouTube em um dataset já processado.
    """
    import json
    import os
    from modules.processor import process_youtube_links # Importa a função que criamos

    print(f"[*] Iniciando Retro-Cleanup de YouTube: {os.path.basename(input_path)}")
    
    with open(input_path, 'r', encoding='utf-8') as f_in, \
         open(output_path, 'w', encoding='utf-8') as f_out:
        
        for line in f_in:
            record = json.loads(line)
            
            # Pula metadados (serão recalculados no final)
            if record.get('type') == 'metadata_footer':
                continue
                
            body = record.get('body', '')
            
            # Verifica gatilho de link do YouTube
            if "youtube" in body.lower() or "youtu.be" in body.lower():
                # Esta função faz o download do título e substitui o link
                record['body'] = process_youtube_links(body)
                print(f"\nLink encontrado! Baixado de:" + body)
            
            f_out.write(json.dumps(record, ensure_ascii=False) + "\n")
            
    # Ao final, gera o novo footer de metadados
    write_metadata_footer(output_path)
    print(f"[+] Retro-Cleanup concluído: {output_path}")

# ______________________________________________________________________________________________

def apply_native_image_cleanup(input_path, output_path):
    """
    Surgical pass to catch only the missed ![img]() and ![gif]() markdown tags,
    download them, send them to Qwen, and rewrite the final dataset.
    """
    import json
    import os

    print(f"[*] Initiating Retro-Vision for Native Reddit Images: {os.path.basename(input_path)}")
    
    with open(input_path, 'r', encoding='utf-8') as f_in, \
         open(output_path, 'w', encoding='utf-8') as f_out:
        
        for line in f_in:
            record = json.loads(line)
            
            if record.get('type') == 'metadata_footer':
                continue
                
            body = record.get('body', '')
            
            # Fast-Fail: Only trigger the pipeline if the raw markdown is present
            if "![img](" in body or "![gif](" in body:
                print(f"\n[RETRO-VISION] Found missed native media tag. Evoking AI...")
                # This will now successfully hit the new regex, download the image, and call Qwen
                record['body'] = process_visual_content(body)
            
            f_out.write(json.dumps(record, ensure_ascii=False) + "\n")
            
    # Recalculate the metadata footer at the end
    write_metadata_footer(output_path)
    print(f"\n[+] Retro-Vision complete: {output_path}")