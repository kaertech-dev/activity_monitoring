# Use Python 3.10 slim as base
FROM python:3.10-slim

# Set working directory inside container
WORKDIR /app

# Copy requirements first (for caching)
COPY requirements.txt .

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy project files into container
COPY . .

# Expose port 5000
EXPOSE 5000

# Run the FastAPI app with uvicorn
CMD ["uvicorn", "monitoring_v1:app", "--host", "0.0.0.0", "--port", "5000"]
