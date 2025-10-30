FROM python:3.10-slim

WORKDIR /app

COPY text_ai_service/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY core_lib /app/core_lib
COPY text_ai_service/kafka_ai_consumer.py /app/kafka_ai_consumer.py
COPY text_ai_service/mock_ai.py /app/mock_ai.py

ENV PYTHONPATH=/app:/app/core_lib

ENTRYPOINT ["python", "kafka_ai_consumer.py"]
