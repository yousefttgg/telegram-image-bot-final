FROM python:3.9-slim

# ضبط بيئة العمل داخل السيرفر
WORKDIR /app

# نسخ جميع ملفاتك (main.py, requirements.txt) إلى السيرفر
COPY . .

# تثبيت المكتبات المطلوبة
RUN pip install --no-cache-dir -r requirements.txt

# الأمر البرمجي لتشغيل البوت (تأكد أن الاسم main.py مطابق لملفك)
CMD ["python", "main.py"]
