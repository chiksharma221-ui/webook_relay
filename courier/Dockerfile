FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY courier ./courier
COPY chaos ./chaos

RUN useradd --create-home --uid 10001 courier && mkdir -p /app/chaos/results && chown -R courier /app
USER courier

EXPOSE 8000
# The same image runs the API, workers, the chaos receiver and the chaos test;
# docker-compose.yml picks the command for each.
CMD ["python", "-m", "courier", "api"]
