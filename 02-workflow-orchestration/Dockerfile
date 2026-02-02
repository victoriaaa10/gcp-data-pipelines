# Use a slim Python image for a smaller footprint
FROM python:3.13-slim

# Install uv directly into the image from the official binaries
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

# Set the working directory inside the container
WORKDIR /app

# 1. Copy dependency files first (best practice for layer caching)
COPY pyproject.toml uv.lock ./

# 2. Install dependencies (strictly using the lockfile)
# --frozen ensures uv doesn't try to update the lockfile during build
RUN uv sync --frozen

# 3. Copy the application logic
COPY src/ ./src/
COPY ingest_data.py .

# 4. Install the project in editable mode so 'src' is discoverable
RUN uv pip install -e .

# Define the entry point for the container
ENTRYPOINT ["uv", "run", "ingest_data.py"]