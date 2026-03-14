# Use the official uv image to get the binary
FROM ghcr.io/astral-sh/uv:latest AS uv_bin

# Use Python 3.13 slim
FROM python:3.13-slim

# Copy uv binary from the build stage
COPY --from=uv_bin /uv /uvx /bin/

# Install system-level dependencies for GCP and data processing
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Set working directory inside the container
WORKDIR /app

# Copy dependency files to leverage Docker cache
COPY pyproject.toml uv.lock ./

# Install dependencies globally in the container
RUN uv pip install --system --no-cache -r pyproject.toml

# Copy the scripts into the container
COPY kestra/scripts/ /app/scripts/

# Default command
CMD ["python", "/app/scripts/ingest_web_to_gcs.py"]