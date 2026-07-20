FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY producer.py .
COPY data/ ./data/

ENV PYTHONUNBUFFERED=1

CMD ["python3", "producer.py"]