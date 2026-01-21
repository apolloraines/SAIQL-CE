# SAIQL-Charlie Production Docker Image
# =====================================
# Multi-stage build for optimized production deployment

# Build stage
FROM python:3.11-slim as builder

# Set build arguments
ARG BUILD_DATE
ARG VCS_REF
ARG VERSION=1.0.0

# Set environment variables
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

# Install build dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    gcc \
    g++ \
    && rm -rf /var/lib/apt/lists/*

# Create application directory
WORKDIR /app

# Copy requirements and install Python dependencies
COPY requirements.txt requirements-prod.txt ./
RUN pip install --user -r requirements-prod.txt

# Production stage
FROM python:3.11-slim as production

# Set labels for metadata
LABEL maintainer="Apollo & Claude <noreply@saiql.dev>" \
      description="SAIQL-Charlie: Semantic AI Query Language Database System" \
      version="${VERSION}" \
      build-date="${BUILD_DATE}" \
      vcs-ref="${VCS_REF}"

# Set environment variables
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONPATH=/app \
    SAIQL_ENV=production \
    SAIQL_HOST=0.0.0.0 \
    SAIQL_PORT=8000 \
    SAIQL_LOG_LEVEL=INFO

# Install runtime dependencies
RUN apt-get update && apt-get install -y \
    curl \
    postgresql-client \
    mysql-client \
    sqlite3 \
    && rm -rf /var/lib/apt/lists/* \
    && apt-get clean

# Create non-root user for security
RUN groupadd -r saiql && useradd -r -g saiql -s /bin/bash saiql

# Create application directories
RUN mkdir -p /app /data /logs /config \
    && chown -R saiql:saiql /app /data /logs /config

# Copy Python dependencies from builder
COPY --from=builder /root/.local /home/saiql/.local

# Set user environment
USER saiql
ENV PATH=/home/saiql/.local/bin:$PATH

# Copy application code
WORKDIR /app
COPY --chown=saiql:saiql . .

# Create default configuration
RUN mkdir -p /config/saiql && \
    cp config/server_config.json /config/saiql/ && \
    cp config/database_config.json /config/saiql/

# Expose ports
EXPOSE 8000 8001

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=30s --retries=3 \
    CMD curl -f http://localhost:8000/health || exit 1

# Volume mounts
VOLUME ["/data", "/logs", "/config"]

# Default command
CMD ["python", "-m", "interface.saiql_server", "--host", "0.0.0.0", "--port", "8000"]