FROM python:3.11-slim

WORKDIR /app

COPY consumer_postgres.py /app/

RUN pip install --no-cache-dir \
    kafka-python==2.0.2 \
    python-dotenv==0.21.0 \
    psycopg2-binary==2.9.9

CMD ["python", "consumer_postgres.py"]
