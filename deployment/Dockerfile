# SAIQL Production Dockerfile
# Optimized for small-scale production deployments like Nova
FROM python:3.11-slim

LABEL maintainer="SAIQL Team"
LABEL version="5.0.0"
LABEL description="SAIQL Database Engine - Production Ready"

# Set environment variables
ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1
ENV SAIQL_ENV=production
ENV SAIQL_PORT=5432
ENV SAIQL_HOST=0.0.0.0

# Create application user (security best practice)
RUN groupadd -r saiql && useradd --no-log-init -r -g saiql saiql

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    libffi-dev \
    libssl-dev \
    freetds-dev \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Create directories
RUN mkdir -p /app/saiql /app/logs /app/data /app/config
WORKDIR /app/saiql

# Copy requirements first (for better Docker layer caching)
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt && \
    # Harness dependencies: ensure SQLAlchemy is present in the image.
    pip install --no-cache-dir SQLAlchemy

# Copy SAIQL source code
COPY . .

# Create production configuration
COPY deployment/config/production.json /app/config/
COPY deployment/scripts/entrypoint.sh /app/
COPY deployment/scripts/healthcheck.sh /app/

# Set permissions
RUN chown -R saiql:saiql /app && \
    chmod +x /app/entrypoint.sh /app/healthcheck.sh

# Switch to non-root user
USER saiql

# Expose SAIQL port
EXPOSE 5432

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=3 \
    CMD /app/healthcheck.sh

# Set entrypoint
ENTRYPOINT ["/app/entrypoint.sh"]
CMD ["saiql-server"]
