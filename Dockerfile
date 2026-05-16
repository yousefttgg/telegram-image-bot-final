FROM python:3.11-slim

WORKDIR /app

# تثبيت المتطلبات النظامية لـ psycopg2-binary
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# نسخ ملف المتطلبات أولاً للاستفادة من cache
COPY requirements.txt .

# تثبيت المكتبات
RUN pip install --no-cache-dir -r requirements.txt

# نسخ الكود
COPY main.py .

# المنفذ
EXPOSE 8080

# تشغيل البوت
CMD ["python", "main.py"]
