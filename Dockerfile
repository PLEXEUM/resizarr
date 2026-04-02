# Use Python 3.11 slim as our base image
FROM python:3.11-slim

# Set working directory inside the container
WORKDIR /app

# Copy requirements first (helps Docker cache dependencies)
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the app
COPY app/ ./app/

# Create directories for volumes
RUN mkdir -p /app/config /app/logs

# Expose port 7227
EXPOSE 7227

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:7227/health')"

# Start the app
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "7227"]