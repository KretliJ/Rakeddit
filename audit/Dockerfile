# Use the official RAPIDS base image (CUDA 12.2, Python 3.11, cuML for GPU UMAP)
FROM rapidsai/base:24.04-cuda12.2-py3.11

# Set working directory inside the container
WORKDIR /app

# Copy dependency files
COPY NLP_requirements.txt .

# ============================================================
# GPU DEPENDENCIES (cuML)
# ============================================================
# Install cuML from NVIDIA's PyPI index (GPU-accelerated UMAP)
# If this fails (e.g., no GPU), the container will still have umap-learn as fallback
RUN pip install cuml-cu11 --extra-index-url https://pypi.nvidia.com || true

# ============================================================
# CPU FALLBACK DEPENDENCIES
# ============================================================
# Ensure umap-learn is installed as CPU fallback for UMAP
RUN pip install umap-learn

# ============================================================
# CORE DEPENDENCIES
# ============================================================
# Install PyTorch with CUDA 12.1 support (compatible with RAPIDS CUDA 12.2 base)
# If CUDA is not available, PyTorch will automatically fall back to CPU
RUN pip install torch torchvision torchaudio --index-url https://download.pytorch.org/whl/cu121

# Install the rest of the Python dependencies (BERTopic, LIWC, WordCloud, etc.)
RUN pip install -r NLP_requirements.txt

# ============================================================
# COPY SOURCE CODE
# ============================================================
# Copy all project source code into the container
COPY . .

# ============================================================
# DEFAULT COMMAND
# ============================================================
# Keep the container alive for interactive use
# The GUI will override this with specific commands (e.g., python core/Analytical_NLP_Engine.py)
CMD ["bash"]