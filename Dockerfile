FROM apache/airflow:3.1.6

USER root
RUN apt-get update && apt-get install -y \
    wget \
    git \
    poppler-utils \
    libgl1-mesa-glx \
    libglib2.0-0 \
    && rm -rf /var/lib/apt/lists/*

USER airflow
COPY requirements.txt /requirements.txt

RUN pip install --user --upgrade pip setuptools wheel

# Install PyTorch with CUDA 11.8
RUN pip install --no-cache-dir --user \
    torch==2.6.0 \
    torchvision==0.21.0 \
    torchaudio==2.6.0 \
    --index-url https://download.pytorch.org/whl/cu118

# Install ALL python deps from requirements (includes transformers==4.46.3, tokenizers==0.20.3, addict, matplotlib...)
RUN pip install --no-cache-dir --user -r /requirements.txt

# Optional extras (chỉ giữ nếu bạn thật sự dùng markdown/python-multipart)
RUN pip install --no-cache-dir --user \
    markdown \
    python-multipart

# Clone DeepSeek-OCR-2 (thực ra không bắt buộc, nhưng bạn muốn giữ thì OK)
WORKDIR /opt/airflow
RUN git clone --depth 1 https://github.com/deepseek-ai/DeepSeek-OCR-2.git

RUN mkdir -p /opt/airflow/ocr_input /opt/airflow/ocr_output

ENV PYTHONPATH="${PYTHONPATH:-}:/opt/airflow/DeepSeek-OCR-2"
ENV HF_HOME="/opt/airflow/.cache/huggingface"
ENV CUDA_VISIBLE_DEVICES="0"

RUN python -c "import torch; print(f'PyTorch: {torch.__version__}'); print(f'CUDA: {torch.cuda.is_available()}')"