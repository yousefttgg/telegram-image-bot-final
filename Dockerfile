FROM python:3.11-slim

WORKDIR /app

RUN pip install --no-cache-dir \
    aiogram==3.15.0 \
    asyncpg==0.30.0 \
    aiohttp==3.11.11

COPY . .

EXPOSE 8080

CMD ["python", "main.py"]
