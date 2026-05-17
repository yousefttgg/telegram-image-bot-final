FROM python:3.11-slim

LABEL version="3.0.0-sqlite"

WORKDIR /app

RUN mkdir -p /data

# تثبيت المكتبات - SQLite فقط، لا postgres
RUN pip install --no-cache-dir \
    aiogram==3.13.1 \
    aiohttp==3.10.10

COPY main.py .

EXPOSE 8080

CMD ["python", "main.py"]
