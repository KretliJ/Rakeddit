import os
os.environ["CUBLAS_WORKSPACE_CONFIG"] = ":4096:8"
import json
import math
import torch
import torch.nn.functional as F
from torch_geometric.data import Data
from torch_geometric.nn import SAGEConv
from sklearn.preprocessing import LabelEncoder
from sklearn.metrics import f1_score, confusion_matrix
from collections import defaultdict
import random
import numpy as np

# ==========================================
# 1. CONSTRUTOR DO GRAFO (ETL + FEATURE ENGINEERING)
# ==========================================
def build_multimodal_graph(jsonl_path):
    print(f"[*] Extraindo topologia de: {os.path.basename(jsonl_path)}")
    
    nodes_map = {}
    x_features = []
    edges_source = []
    edges_target = []
    y_labels = []
    
    sub_encoder = LabelEncoder()
    subreddits_raw = []
    
    # Dicionários temporários para métricas estruturais (degree e tempo)
    timestamps = {}
    children_count = defaultdict(int)

    # --- PASSO 1: Mapeamento Global (O(N)) ---
    # Precisamos saber os tempos e quem responde a quem antes de montar os features
    print("[*] Passo 1/3: Mapeando graus e timestamps...")
    with open(jsonl_path, 'r', encoding='utf-8') as f:
        for line in f:
            try:
                record = json.loads(line)
                if record.get('type') == 'metadata_footer' or not record.get('id'): continue
                
                r_id = record['id']
                parent_id = record.get('parent_id')
                ts = record.get('timestamp') or record.get('created_utc') or 0
                
                timestamps[r_id] = float(ts)
                if parent_id:
                    children_count[parent_id] += 1
            except json.JSONDecodeError:
                continue

    # --- PASSO 2: Feature Engineering ---
    print("[*] Passo 2/3: Extraindo Features e Thresholds...")
    with open(jsonl_path, 'r', encoding='utf-8') as f:
        for idx, line in enumerate(f):
            try:
                record = json.loads(line)
            except json.JSONDecodeError: continue
            if record.get('type') == 'metadata_footer' or not record.get('id'): continue
                
            r_id = record['id']
            nodes_map[r_id] = idx
            
            # 1. Features base
            conf = record.get('ai_analysis', {}).get('confidence', 0.0)
            depth = record.get('depth', 0)
            subreddits_raw.append(record.get('subreddit', 'unknown'))
            
            # 2. Features Estruturais Anabolizadas
            # Degree: Quantos filhos esse nó tem? (Usamos log1p para suavizar outliers de virais)
            degree = children_count.get(r_id, 0)
            log_degree = math.log1p(degree)
            
            # Time Delta: Quão rápido responderam ao pai? (Em minutos)
            ts = timestamps.get(r_id, 0)
            parent_id = record.get('parent_id')
            parent_ts = timestamps.get(parent_id, ts) # Se não tiver pai, delta = 0
            time_delta_mins = abs(ts - parent_ts) / 60.0
            log_time_delta = math.log1p(time_delta_mins)
            
            x_features.append([conf, depth, log_degree, log_time_delta])
            
            # Alvo (y): Classificação Binária (Toxicidade >= 0.70)
            tox_score = record.get('toxicity_score', 0.0)
            y_labels.append(1 if tox_score >= 0.70 else 0)

    # Encode de subreddits
    sub_encoded = sub_encoder.fit_transform(subreddits_raw)
    for i in range(len(x_features)):
        x_features[i].append(float(sub_encoded[i]))

    # --- PASSO 3: Construção da Matriz de Adjacência ---
    print("[*] Passo 3/3: Construindo matriz de arestas (Edge Index)...")
    with open(jsonl_path, 'r', encoding='utf-8') as f:
        for line in f:
            try:
                record = json.loads(line)
            except json.JSONDecodeError: continue
            if record.get('type') == 'metadata_footer' or not record.get('id'): continue
                
            child_id = record['id']
            parent_id = record.get('parent_id')
            
            if child_id in nodes_map and parent_id in nodes_map:
                edges_source.append(nodes_map[parent_id])
                edges_target.append(nodes_map[child_id])

    # Tensores
    x = torch.tensor(x_features, dtype=torch.float)
    y = torch.tensor(y_labels, dtype=torch.long)
    edge_index = torch.tensor([edges_source, edges_target], dtype=torch.long)
    
    graph_data = Data(x=x, edge_index=edge_index, y=y)
    print(f"[+] Grafo Final: {graph_data}")
    return graph_data

# ==========================================
# 2. ARQUITETURA DA REDE NEURAL (GraphSAGE)
# ==========================================
class ToxicCascadeGNN(torch.nn.Module):
    def __init__(self, num_node_features, hidden_channels, num_classes=2):
        super(ToxicCascadeGNN, self).__init__()
        self.conv1 = SAGEConv(num_node_features, hidden_channels)
        self.conv2 = SAGEConv(hidden_channels, hidden_channels)
        self.out = torch.nn.Linear(hidden_channels, num_classes)

    def forward(self, x, edge_index):
        x = self.conv1(x, edge_index)
        x = F.relu(x)
        x = F.dropout(x, p=0.5, training=self.training)
        x = self.conv2(x, edge_index)
        x = F.relu(x)
        x = self.out(x)
        return F.log_softmax(x, dim=1)

# ==========================================
# 3. ROTINA DE TREINAMENTO COM MÉTRICAS REAIS
# ==========================================
def train_gnn(graph_data, epochs=100, hidden_dim=64):
    device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
    print(f"\n[*] Iniciando Treinamento GNN no dispositivo: {device}")
    
    graph_data = graph_data.to(device)
    num_features = graph_data.num_node_features
    model = ToxicCascadeGNN(num_node_features=num_features, hidden_channels=hidden_dim).to(device)
    
    # Pesos na Loss Function para lidar com classes desbalanceadas (ex: 90% pacífico, 10% tóxico)
    # Aqui damos um "peso" maior para o erro se o modelo errar a classe 1 (Tóxico)
    class_counts = torch.bincount(graph_data.y)
    total_samples = len(graph_data.y)
    class_weights = total_samples / (len(class_counts) * class_counts.float())
    class_weights = class_weights.to(device)
    
    optimizer = torch.optim.Adam(model.parameters(), lr=0.01, weight_decay=5e-4)
    
    num_nodes = graph_data.num_nodes
    indices = torch.randperm(num_nodes)
    train_mask = torch.zeros(num_nodes, dtype=torch.bool)
    test_mask = torch.zeros(num_nodes, dtype=torch.bool)
    
    split = int(num_nodes * 0.8)
    train_mask[indices[:split]] = True
    test_mask[indices[split:]] = True
    
    model.train()
    for epoch in range(epochs):
        optimizer.zero_grad()
        out = model(graph_data.x, graph_data.edge_index)
        # NLL Loss com pesos balanceados
        loss = F.nll_loss(out[train_mask], graph_data.y[train_mask], weight=class_weights)
        loss.backward()
        optimizer.step()
        
        if epoch % 10 == 0:
            print(f'   -> Epoch: {epoch:03d}, Loss: {loss:.4f}')
            
    # --- AVALIAÇÃO COM SCIKIT-LEARN ---
    model.eval()
    with torch.no_grad():
        out = model(graph_data.x, graph_data.edge_index)
        pred = out.argmax(dim=1)
        
        # Mover tensores da GPU de volta para a CPU para usar o sklearn
        y_true = graph_data.y[test_mask].cpu().numpy()
        y_pred = pred[test_mask].cpu().numpy()
        
        acc = int((pred[test_mask] == graph_data.y[test_mask]).sum()) / int(test_mask.sum())
        f1 = f1_score(y_true, y_pred, average='binary')
        cm = confusion_matrix(y_true, y_pred)
        
    print(f'\n[========== RESULTADOS FINAIS ==========]')
    print(f'[+] Accuracy Geral : {acc:.4f}')
    print(f'[+] F1-Score (Tóxico): {f1:.4f}  <-- Métrica Importante')
    print(f'[+] Matriz de Confusão:')
    print(f'    TN (Falso Pacífico): {cm[0][0]} | FP (Alarme Falso) : {cm[0][1]}')
    print(f'    FN (Ódio Não Visto): {cm[1][0]} | TP (Ódio Detectado): {cm[1][1]}')
    
    return model

def set_seed(seed):
    random.seed(seed)
    np.random.seed(seed)
    torch.manual_seed(seed)
    torch.cuda.manual_seed(seed)
    torch.cuda.manual_seed_all(seed)
    
    torch.use_deterministic_algorithms(True)

    torch.backends.cudnn.deterministic = True
    torch.backends.cudnn.benchmark = False

if __name__ == "__main__":
    set_seed(42)
    inferred_path = 'DATA/results/with_vision/'
    target_file = 'DATA/results/with_vision/INFERRED_MULTIMODAL_FINAL.jsonl'
    
    if os.path.exists(target_file):
        data = build_multimodal_graph(target_file)
        torch.save(data, os.path.join(inferred_path, 'graph_tensor.pt'))
        trained_model = train_gnn(data, epochs=100) # Aumentei para 100 epochs para ver a curva
    else:
        print(f"❌ Arquivo não encontrado: {target_file}")