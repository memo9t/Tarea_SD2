FROM python:3.10-slim

WORKDIR /app

COPY quality_service/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY quality_service/quality_checker.py /app/quality_checker.py

ENTRYPOINT ["python", "quality_checker.py"]
