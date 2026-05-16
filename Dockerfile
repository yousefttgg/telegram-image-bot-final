FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y gcc && \
    pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir aiogram asyncpg aiohttp

COPY . .

EXPOSE 8080

CMD ["python", "main.py"]