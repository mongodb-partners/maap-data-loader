FROM downloads.unstructured.io/unstructured-io/unstructured:latest

# Set up working directory
WORKDIR /code

# Copy requirements and environment files
COPY requirements.txt ./
COPY .env ./
COPY ./config.py ./config.py

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY ./local/database/ ./local/database/
COPY ./local/models/ ./local/models/
COPY ./local/services/ ./local/services/
COPY ./local/utils/ ./local/utils/
# COPY ./local/ ./local/
COPY ./enterprise/ ./enterprise/
COPY ./app.py ./app.py
COPY ./config.py ./config.py
COPY .env .env

# Set permissions and create upload directory
USER root
RUN chmod -R 755 /code && \
    mkdir -p uploaded_files && \
    chown -R notebook-user:notebook-user uploaded_files && \
    chmod 755 uploaded_files

USER notebook-user

# Expose the API port
EXPOSE 8184

# Run the FastAPI application
ENTRYPOINT ["python3", "app.py"]