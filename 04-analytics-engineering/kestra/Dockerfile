# Use the official uv image for the build stage
FROM ghcr.io/astral-sh/uv:latest AS uv_bin

# Use Python 3.13 slim as the base
FROM python:3.13-slim

# Copy the uv binary from the build stage
COPY --from=uv_bin /uv /uvx /bin/

# Set working directory
WORKDIR /app

# Copy dependency files to the container
COPY pyproject.toml uv.lock ./

# Install dependencies into the system site-packages 
RUN uv pip install --system --no-cache .

# Copy the application code to the container 
COPY kestra/scripts/ /app/scripts/

# Set the default command to run the ingestion script
CMD ["python", "scripts/ingest_web_to_gcs.py"]