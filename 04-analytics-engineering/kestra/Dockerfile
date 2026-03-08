# Use the official uv image for the build stage
FROM ghcr.io/astral-sh/uv:latest AS uv_bin

# Use Python 3.13 slim as the base
FROM python:3.13-slim

# Copy the uv binary from the build stage
COPY --from=uv_bin /uv /uvx /bin/

# Set working directory
WORKDIR /app

# Copy dependency files 
COPY pyproject.toml uv.lock ./

# Install dependencies using uv
RUN uv pip install --system --no-cache .

# Copy the scripts to the container
COPY kestra/scripts/ /app/scripts/

# Default command (can be overridden by Kestra)
CMD ["python", "scripts/ingest_web_to_gcs.py"]