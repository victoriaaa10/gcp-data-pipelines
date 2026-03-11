# Use the official uv image for the binary
FROM ghcr.io/astral-sh/uv:latest AS uv_bin

# Python 3.13 slim
FROM python:3.13-slim

# Copy uv binaries
COPY --from=uv_bin /uv /uvx /bin/

# Set working directory inside the container
WORKDIR /app

# Copy dependency files to leverage Docker cache
COPY pyproject.toml uv.lock ./

# Install dependencies using uv
RUN uv pip install --system --no-cache -e .

# Copy scripts to /app/scripts
COPY kestra/scripts/ /app/scripts/

# Default command
CMD ["python", "scripts/ingest_web_to_gcs.py"]