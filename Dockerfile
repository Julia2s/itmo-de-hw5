FROM python:3.12.8-slim-bookworm
ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY src/ ./src/
COPY scripts/ ./scripts/
RUN useradd --uid 10001 --create-home worker && mkdir /app/data && chown worker /app/data
USER 10001:10001
CMD ["python", "src/worker.py", "--help"]
