FROM python:3.11-slim

WORKDIR /app

RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir \
    aiogram \
    asyncpg \
    aiohttp

COPY . .

EXPOSE 8080

CMD ["python", "main.py"]
