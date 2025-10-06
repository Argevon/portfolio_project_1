FROM python:3.11-slim

WORKDIR /app

COPY producer.py /app/

RUN pip install --no-cache-dir \
    requests==2.28.2 \
    azure-storage-blob==12.10.0 \
    kafka-python==2.0.2 \
    python-dotenv==0.21.0

CMD ["python", "producer.py"]
