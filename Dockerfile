# Dockerfile for PySpark 3.5 + Apache Hudi 0.15.0 E-commerce Recommendation System
FROM openjdk:11-jdk-slim

# Set environment variables
ENV SPARK_VERSION=3.5.0 \
    HADOOP_VERSION=3 \
    SPARK_HOME=/opt/spark \
    PYTHONUNBUFFERED=1 \
    DEBIAN_FRONTEND=noninteractive

# Install Python 3.10 and dependencies
RUN apt-get update && apt-get install -y \
    python3.10 \
    python3-pip \
    python3.10-dev \
    wget \
    curl \
    procps \
    && rm -rf /var/lib/apt/lists/*

# Create symbolic links for python
RUN ln -sf /usr/bin/python3.10 /usr/bin/python && \
    ln -sf /usr/bin/python3.10 /usr/bin/python3

# Download and install Apache Spark
RUN wget -q https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz && \
    tar -xzf spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz && \
    mv spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} ${SPARK_HOME} && \
    rm spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz

# Set PATH
ENV PATH="${SPARK_HOME}/bin:${SPARK_HOME}/sbin:${PATH}" \
    PYSPARK_PYTHON=python3 \
    PYSPARK_DRIVER_PYTHON=python3

# Set working directory
WORKDIR /app

# Copy requirements file
COPY requirements.txt .

# Install Python dependencies
RUN pip3 install --no-cache-dir -r requirements.txt

# Copy project files
COPY configs/ ./configs/
COPY src/ ./src/
COPY scripts/ ./scripts/
COPY data/raw/ ./data/raw/

# Create necessary directories
RUN mkdir -p /app/data/processed /app/data/quarantine /app/data/output

# Set permissions
RUN chmod +x /app/scripts/*.sh

# Default command
CMD ["/bin/bash"]
