FROM ghcr.io/astral-sh/uv:0.5-python3.10-bookworm AS builder

ENV UV_LINK_MODE=copy \
    UV_COMPILE_BYTECODE=1 \
    UV_CACHE_DIR=/root/.cache/uv

WORKDIR /workspace

COPY uv.lock pyproject.toml ./

RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-install-project --group pyspark --group sql

COPY README.md ./
COPY src/ ./src/
COPY tests/fixtures/ ./tests/fixtures/

RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --group pyspark --group sql

RUN uv cache prune --ci

FROM python:3.10-slim

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        openjdk-21-jre-headless \
        curl \
        procps \
    && rm -rf /var/lib/apt/lists/*

ENV SPARK_VERSION=3.5.3
ENV HADOOP_VERSION=3
ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin
ENV PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-0.10.9.7-src.zip

RUN curl -L "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz" \
    -o /tmp/spark.tgz && \
    tar -xzf /tmp/spark.tgz -C /opt && \
    mv "/opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}" "${SPARK_HOME}" && \
    rm /tmp/spark.tgz

RUN curl -L "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.7.1/iceberg-spark-runtime-3.5_2.12-1.7.1.jar" \
    -o "${SPARK_HOME}/jars/iceberg-spark-runtime-3.5_2.12-1.7.1.jar"

WORKDIR /workspace

COPY --from=builder /workspace/.venv /workspace/.venv
COPY --from=builder /workspace/src /workspace/src
COPY --from=builder /workspace/tests/fixtures /workspace/tests/fixtures

ENV VIRTUAL_ENV=/workspace/.venv
ENV PATH="/workspace/.venv/bin:$PATH"

CMD ["python3", "/workspace/ci/integration-tests/scripts/test-spark.py"]
