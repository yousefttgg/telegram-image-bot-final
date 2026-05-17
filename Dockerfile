‏FROM python:3.11-slim
‏
‏WORKDIR /app
‏
‏RUN apt-get update && apt-get install -y \
‏    gcc \
‏    && rm -rf /var/lib/apt/lists/*
‏
‏COPY requirements.txt .
‏RUN pip install --no-cache-dir -r requirements.txt
‏
‏COPY . .
‏
‏RUN mkdir -p /app/data
‏
‏ENV PYTHONUNBUFFERED=1
‏ENV DB_PATH=/app/data/bot_data.db
‏
‏EXPOSE 8080
‏
‏CMD ["python", "main.py"]
‏