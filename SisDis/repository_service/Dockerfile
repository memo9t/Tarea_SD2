FROM python:3.10-slim

WORKDIR /app

COPY repository_service/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY core_lib /app/core_lib
COPY repository_service/repository_api.py /app/repository_api.py

ENV PYTHONPATH=/app:/app/core_lib

ENTRYPOINT ["python", "repository_api.py"]
