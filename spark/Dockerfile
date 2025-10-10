FROM python:3.11.9-slim

ENV PIP_NO_CACHE_DIR=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONPATH=/workspace/spark

RUN apt-get update \
 && apt-get install -y --no-install-recommends \
    bash \
    openjdk-17-jre-headless \
    liblzma5 \
    procps \
 && rm -rf /var/lib/apt/lists/*

COPY spark/requirements.txt /tmp/requirements.txt

RUN pip install --upgrade pip \
 && pip install -r /tmp/requirements.txt

WORKDIR /workspace

ENTRYPOINT ["bash", "/workspace/spark/run_all.sh"]
