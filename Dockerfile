FROM node:22-bookworm-slim

# git нужен npm для зависимостей Baileys; python — для ядра бота
RUN apt-get update && apt-get install -y --no-install-recommends git python3 python3-pip python3-venv ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY requirements.txt .
RUN python3 -m venv /venv && /venv/bin/pip install --no-cache-dir -r requirements.txt

COPY gateway/package.json gateway/package.json
RUN cd gateway && npm install --omit=dev --no-audit --no-fund

COPY . .
ENV PATH="/venv/bin:$PATH" \
    NODE_OPTIONS="--max-old-space-size=200" \
    PYTHONUNBUFFERED=1
CMD ["bash", "start.sh"]
