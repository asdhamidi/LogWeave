FROM python:3.11-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy producer code
COPY producer.py .

# Create log directory
RUN mkdir -p /var/log && chmod 777 /var/log

# Run as non-root user
RUN useradd -m -u 1000 producer && \
    chown -R producer:producer /app /var/log
USER producer

# The producer reads from stdin (omprog will pipe to this)
CMD ["python", "-u", "producer.py"]
