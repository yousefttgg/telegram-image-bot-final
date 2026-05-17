FROM python:3.11-slim

WORKDIR /app

# إنشاء مجلد البيانات الدائمة
RUN mkdir -p /data

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY main.py .

EXPOSE 8080

CMD ["python", "main.py"]
