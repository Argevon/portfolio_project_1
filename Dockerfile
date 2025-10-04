FROM python:3.9-slim

WORKDIR /app

COPY requirements.txt
COPY src/ ./src/

RUN pip install -no-cache-dir -r requirements.txt

ENV AZURE_STORAGE_CONNECTION_STRING=""

CMD ["python", "src/main.py"]