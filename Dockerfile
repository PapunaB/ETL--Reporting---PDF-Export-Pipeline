# Use Python 3.9 as the base image
FROM python:3.9-slim

# Set working directory
WORKDIR /app

# Copy requirements file
COPY requirements.txt .

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy the application code
COPY . .

# Create directories for outputs
RUN mkdir -p /app/reports

# Install the package in development mode
# This makes the etl_pipeline package importable
RUN pip install -e .

# Set environment variables
ENV DB_TYPE=postgresql
ENV PG_HOST=postgres
ENV PG_PORT=5432
ENV PG_DATABASE=sales
ENV PG_USER=postgres
ENV PG_PASSWORD=password
ENV REPORTS_DIR=/app/reports

# Command to run the ETL pipeline
CMD ["python", "src/main.py"]
