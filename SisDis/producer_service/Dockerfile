FROM python:3.10-slim

WORKDIR /app

COPY producer_service/requirements.txt .
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        build-essential \
        libfreetype6-dev \
        libpng-dev \
    && rm -rf /var/lib/apt/lists/*
RUN pip install --no-cache-dir -r requirements.txt

COPY core_lib /app/core_lib
COPY producer_service/yahoo_producer.py /app/yahoo_producer.py

ENV PYTHONPATH=/app:/app/core_lib

ENTRYPOINT ["python", "yahoo_producer.py"]
