FROM python:3-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

COPY requirements.txt .
RUN python -m pip install --no-cache-dir -r requirements.txt

WORKDIR /app
COPY . .

RUN adduser -u 5678 --disabled-password --gecos "" appuser && chown -R appuser /app
USER appuser

ENV PORT=5000
EXPOSE 5000

CMD ["uvicorn", "monitoring_v2:app", "--host", "0.0.0.0", "--port", "5000", "--reload"]
