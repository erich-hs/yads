FROM mcr.microsoft.com/mssql/server:2022-latest

USER root

# Install Python, build dependencies for pyodbc, and ODBC runtime
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        python3 \
        python3-venv \
        curl \
        gnupg2 \
        unixodbc \
        unixodbc-dev \
        g++ \
    && rm -rf /var/lib/apt/lists/*

# Add Microsoft repository and install ODBC driver
RUN curl -fsSL https://packages.microsoft.com/keys/microsoft.asc | gpg --dearmor -o /usr/share/keyrings/microsoft-prod.gpg && \
    curl -fsSL https://packages.microsoft.com/config/ubuntu/22.04/prod.list > /etc/apt/sources.list.d/mssql-release.list && \
    apt-get update && \
    ACCEPT_EULA=Y apt-get install -y msodbcsql18 && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /workspace

# Copy uv binary
COPY --from=ghcr.io/astral-sh/uv:0.5 /uv /usr/local/bin/uv

# Copy project files
COPY uv.lock pyproject.toml README.md ./
COPY src/ ./src/
COPY tests/fixtures/ ./tests/fixtures/

# Create venv and install dependencies (including pyodbc)
RUN uv sync --frozen --group sql && \
    uv pip install pyodbc

# Copy integration test scripts
COPY ci/integration-tests/scripts/ /workspace/ci/integration-tests/scripts/

# Copy entrypoint script
COPY ci/integration-tests/docker/sqlserver/entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

ENV VIRTUAL_ENV=/workspace/.venv
ENV PATH="/workspace/.venv/bin:$PATH"

# SQL Server configuration
ENV ACCEPT_EULA=Y
ENV MSSQL_SA_PASSWORD=YadsTest123!
ENV MSSQL_PID=Developer

ENTRYPOINT ["/entrypoint.sh"]
CMD ["uv", "run", "python", "/workspace/ci/integration-tests/scripts/test-sqlserver.py"]
