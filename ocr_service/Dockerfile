FROM python:3.11-slim-bookworm

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

RUN apt-get -o Acquire::Retries=3 -o Acquire::ForceIPv4=true update \
    && apt-get install -y --no-install-recommends \
        ca-certificates \
        tar \
        libglib2.0-0 \
        libgl1 \
        libsm6 \
        libxext6 \
        libxrender1 \
        libgomp1 \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install -r requirements.txt

RUN mkdir -p /opt/ocr-models/det /opt/ocr-models/rec /opt/ocr-models/cls

COPY Multilingual_PP-OCRv3_det_infer.tar /tmp/det.tar
COPY cyrillic_PP-OCRv3_rec_infer.tar /tmp/rec.tar
COPY ch_ppocr_mobile_v2.0_cls_infer.tar /tmp/cls.tar

RUN tar -xf /tmp/det.tar -C /opt/ocr-models/det --strip-components=1 \
    && rm -f /tmp/det.tar

RUN tar -xf /tmp/rec.tar -C /opt/ocr-models/rec --strip-components=1 \
    && rm -f /tmp/rec.tar

RUN tar -xf /tmp/cls.tar -C /opt/ocr-models/cls --strip-components=1 \
    && rm -f /tmp/cls.tar

COPY . .

EXPOSE 8010

CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8010"]
