FROM python:3.11-slim

WORKDIR /app

ENV PYTHONPATH=/app

COPY api/requirements.txt /app/api/requirements.txt

RUN pip install --no-cache-dir -r /app/api/requirements.txt

COPY . /app

CMD ["gunicorn", "--bind", "0.0.0.0:5000", "--workers", "2", "api.app:app"]