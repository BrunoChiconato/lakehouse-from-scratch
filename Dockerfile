FROM python:3.13-bullseye

WORKDIR /app

RUN apt-get update && apt-get install -y \
    openjdk-11-jre-headless \
    procps \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME /usr/lib/jvm/java-11-openjdk-amd64

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt
