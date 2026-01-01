FROM python:3.11-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy consumer code
COPY consumer.py .

# Create log directory
RUN mkdir -p /var/log && chmod 777 /var/log

# Run as non-root user
RUN useradd -m -u 1000 consumer && \
    chown -R consumer:consumer /app /var/log
USER consumer

# Run consumer
CMD ["python", "-u", "consumer.py"]
