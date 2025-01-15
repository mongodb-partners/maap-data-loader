# Use Python base image
FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Copy requirements file
COPY requirements.txt .

COPY .env .

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Expose port 8182
EXPOSE 8182

# Command to run the application
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8182"]