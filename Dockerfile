FROM python:3.11-slim

WORKDIR /app

# تثبيت المكتبات
RUN pip install --no-cache-dir \
    aiogram==3.7.0 \
    aiosqlite \
    aiohttp

COPY main.py .

# لا تغيّر المنفذ هنا، سيُعرَّف في الكود
EXPOSE 8080

CMD ["python", "main.py"]