FROM us-docker.pkg.dev/vertex-ai/training/pytorch-xla.2-4.py310:latest

WORKDIR /app

# Copy requirements and install additional packages
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY src/ ./src/

ENTRYPOINT ["python", "src/train.py"]