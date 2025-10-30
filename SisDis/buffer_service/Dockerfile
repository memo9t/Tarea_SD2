FROM python:3.10-slim

WORKDIR /app

COPY buffer_service/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY core_lib /app/core_lib
COPY buffer_service/buffer_handler.py /app/buffer_handler.py

ENV PYTHONPATH=/app:/app/core_lib

ENTRYPOINT ["python", "buffer_handler.py"]
