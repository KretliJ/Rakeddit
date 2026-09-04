
FINE-TUNING REPORT: BERTabaporu on Human-Annotated Reddit Comments
==================================================================

1. TRAINING DATA

---

- Source: Human validation dataset (HUMAN_RANK_BASE_Q.jsonl + HUMAN_RANK_BASE_UQ.jsonl)
- Total annotated comments: 784 (filtered from 800 original)
- Label distribution:
  POS:  58 ( 7.4%)
  NEU: 546 (69.6%)
  NEG: 180 (23.0%)
- Train/validation split: 80/20 (627 train, 157 validation)
- Stratified sampling used to preserve label distribution

2. MODEL ARCHITECTURE

---

- Base model: pablocosta/bertabaporu-large-uncased
- Task: Sequence classification (3 labels: POS, NEU, NEG)
- Classification head: Randomly initialized (fine-tuned during training)
- Max sequence length: 256 tokens
- Number of parameters: ~1.48 GB (large model)

3. TRAINING CONFIGURATION

---

- Batch size (per device): 4
- Gradient accumulation steps: 2
- Effective batch size: 8
- Learning rate: 2e-5
- Optimizer: AdamW
- Scheduler: Linear decay with warmup
- Epochs: 5 (with early stopping, patience=2)
- Loss function: Cross-entropy
- Mixed precision (fp16): Disabled for stability
- Weight decay: 0.01
- Report to: None (wandb/mlflow disabled)

4. HARDWARE ENVIRONMENT

---

- GPU: NVIDIA GeForce RTX 4060 Ti (8 GB VRAM)
- PyTorch: 2.6.0+cu124
- Transformers: 5.16.1
- CUDA available: Yes
- GPU memory usage: ~4-5 GB during training

5. TRAINING PROGRESS (Per Epoch)

---

| Epoch | Train Loss | Val Loss | Accuracy | F1 POS | F1 NEU | F1 NEG |
| ----- | ---------- | -------- | -------- | ------ | ------ | ------ |
| 1     | 1.146      | 0.595    | 74.5%    | 0.000  | 0.845  | 0.364  |
| 2     | 0.662      | 0.439    | 84.1%    | 0.571  | 0.889  | 0.765  |
| 3     | 0.717      | 0.619    | 84.1%    | 0.632  | 0.893  | 0.710  |
| 4     | 0.329      | 0.786    | 84.7%    | 0.700  | 0.896  | 0.719  |

Best model: Epoch 2 (lowest validation loss, highest F1 NEG)

6. CONFUSION MATRIX (Best Model — Epoch 2 — Validation Set)

---

             Predicted
              POS   NEU   NEG
True POS       6     6     0   (n=12)
True NEU       3   100     6   (n=109)
True NEG       0    10    26   (n=36)

7. KEY METRICS (Best Model — Epoch 2)

---

- Validation accuracy: 84.1%
- Macro F1: 0.742
- Per-class metrics:
  POS: Precision=0.667, Recall=0.500, F1=0.571
  NEU: Precision=0.862, Recall=0.917, F1=0.889
  NEG: Precision=0.812, Recall=0.722, F1=0.765

8. OBSERVATIONS

---

- The model learned rapidly, with loss decreasing consistently through epoch 2
- NEU class performed best (F1 = 0.889), as expected (majority class)
- NEG class improved significantly from epoch 1 to 2 (F1: 0.364 → 0.765)
- POS class showed the most improvement (F1: 0.000 → 0.571), despite being the minority class
- Accuracy improved by 9.6 percentage points from epoch 1 to 2
- Early stopping triggered at epoch 3 (validation loss increased), loading epoch 2 as best model

9. DISCUSSION

---

The human-annotated dataset (784 comments) proved sufficient for effective fine-tuning.
Despite the under-representation of POS (7.4%), the model was able to learn this class
with reasonable performance (F1 = 0.571). The critical NEU-NEG distinction, which is
central to cascade analysis, is well-calibrated with F1 scores of 0.889 and 0.765,
respectively.

The fine-tuned model substantially outperforms the generic XLM-RoBERTa model
(validation accuracy: 84.1% vs. ~60% in previous human validation), demonstrating
the value of domain-specific fine-tuning for Brazilian Reddit text. The confusion
matrix shows that the model's main error pattern is confusing NEU with NEG
(6 out of 109 NEU comments misclassified as NEG) and NEG with NEU
(10 out of 36 NEG comments misclassified as NEU), which is consistent with
the known difficulty of distinguishing criticism from hostility in political
and social discussions.

10. NEXT STEPS

---

- Use fine-tuned model in infer_engine.py:
  'bertabaporu': './bertabaporu_finetuned'
- Run inference on full dataset
- Compare results with XLM-RoBERTa baseline
- Report final metrics in paper

11. FILE OUTPUTS

---

- Model saved to: ./bertabaporu_finetuned/
- Files: config.json, model.safetensors, tokenizer files
- Usage: Update infer_engine.py with:
  'bertabaporu': './bertabaporu_finetuned'
