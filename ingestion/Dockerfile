FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir requests schedule python-dotenv

COPY fetch.py .

CMD ["python", "fetch.py"]