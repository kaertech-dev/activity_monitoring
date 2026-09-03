# Use Python 3.10 slim as base
FROM python:3.10-slim

# Set working directory inside container
WORKDIR /app

# Copy requirements first (for caching)
COPY requirementsv1.txt .

# Install dependencies
RUN pip install --no-cache-dir -r requirementsv1.txt

# Copy project files into container
COPY . .

# Expose port 5000
EXPOSE 5000

# Run the Flask app factory
CMD ["python", "main.py"]