FROM python:3.11-slim

RUN apt-get update && apt-get install -y \
    nmap \
    bash \
    curl \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /opt/airflow

CMD ["bash"]