# Use Vertex AI Ray GPU container as base
FROM us-docker.pkg.dev/vertex-ai/training/ray-gpu.2-47.py311:latest

WORKDIR /app

# Install additional dependencies on top of Ray base
COPY requirements.txt /app/requirements.txt
RUN pip install -r /app/requirements.txt

# Copy production code
COPY src /app/src
COPY config.yml /app/config.yml

ENV PYTHONPATH=/app
ENV APP_MODE=api
ENV PORT=8080

# Multi-mode container supporting Ray distributed training
CMD ["/bin/sh", "-lc", "\
  if [ \"$APP_MODE\" = \"api\" ]; then \
    uvicorn src.weight_extractor.api:app --host 0.0.0.0 --port ${PORT}; \
  elif [ \"$APP_MODE\" = \"train\" ]; then \
    python -m src.weight_extractor.train --config /app/config.yml; \
  elif [ \"$APP_MODE\" = \"train_distributed\" ]; then \
    ray start --head --dashboard-host=0.0.0.0 --dashboard-port=8265 && \
    python -m src.weight_extractor.train_distributed --config /app/config.yml; \
  elif [ \"$APP_MODE\" = \"predict\" ]; then \
    python -m src.weight_extractor.predict \"$@\"; \
  else \
    echo \"Unknown APP_MODE=$APP_MODE\"; exit 1; \
  fi \
"]