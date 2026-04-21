FROM python:3.11-slim

# Install Java (required by Spark) and curl
RUN apt-get update && apt-get install -y \
    default-jdk-headless \
    curl \
    procps \
    && rm -rf /var/lib/apt/lists/*

# Install PySpark and dependencies
RUN pip install --no-cache-dir \
    pyspark==3.5.1 \
    psycopg2-binary \
    python-dotenv

# Download the PostgreSQL JDBC driver (needed for spark.write.jdbc)
RUN curl -L https://jdbc.postgresql.org/download/postgresql-42.7.1.jar \
    -o /opt/postgresql-42.7.1.jar

WORKDIR /app
COPY transform.py .
COPY data_quality.py .

CMD ["spark-submit", \
     "--jars", "/opt/postgresql-42.7.1.jar", \
     "--driver-class-path", "/opt/postgresql-42.7.1.jar", \
     "transform.py"]