FROM python:3.11-slim-bookworm

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

RUN apt-get -o Acquire::Retries=3 -o Acquire::ForceIPv4=true update \
    && apt-get install -y --no-install-recommends ca-certificates nodejs npm \
    && npm install -g typescript@5.9.3 \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY . .

RUN if [ -f /app/certs/russian_trusted_root_ca_pem.crt ]; then \
        cp /app/certs/russian_trusted_root_ca_pem.crt /usr/local/share/ca-certificates/russian_trusted_root_ca.crt; \
        update-ca-certificates; \
    fi

EXPOSE 8000

CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8000"]
