from flask import Flask, request
import os
import subprocess
import urllib.parse
from datetime import datetime
import boto3
from botocore.exceptions import BotoCoreError, ClientError
import threading
import schedule
import time

app = Flask(__name__)

# --- Конфигурация ---
DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("❌ Не задана переменная DATABASE_URL")

R2_BUCKET_NAME = os.getenv("R2_BUCKET_NAME")
R2_ENDPOINT = os.getenv("R2_ENDPOINT")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")

OUTPUT_DIR = "./backups"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# --- Функция бэкапа ---
def perform_backup():
    print("🔄 Запуск автоматического бэкапа...")
    try:
        result = urllib.parse.urlparse(DATABASE_URL)
        if result.scheme not in ("postgres", "postgresql"):
            print("❌ Неверная схема DATABASE_URL")
            return False

        username = result.username
        password = result.password
        host = result.hostname
        port = result.port or 5432
        database = result.path.lstrip('/')

        timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        filename = f"{database}_backup_{timestamp}.sql"
        filepath = os.path.join(OUTPUT_DIR, filename)

        env = os.environ.copy()
        env["PGPASSWORD"] = password

        cmd = [
            "pg_dump",
            "-h", host,
            "-p", str(port),
            "-U", username,
            "-d", database,
            "-f", filepath,
            "--clean",
            "--if-exists",
            "--no-owner",
            "--format=plain"
        ]

        print(f"Запуск pg_dump в {filepath}...")
        subprocess.run(cmd, env=env, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        print(f"✅ Бэкап создан: {filepath}")

        # Загрузка в R2
        if upload_to_r2(filepath, filename):
            os.remove(filepath)
            print(f"🗑 Локальный файл удалён: {filepath}")
            return True
        else:
            print(f"❌ Загрузка в R2 не удалась, файл сохранён локально")
            return False

    except subprocess.CalledProcessError as e:
        error = e.stderr.decode() if e.stderr else str(e)
        print(f"❌ Ошибка pg_dump: {error}")
    except Exception as e:
        print(f"❌ Ошибка бэкапа: {str(e)}")
    return False

# --- Загрузка в R2 ---
def upload_to_r2(filepath, filename):
    try:
        s3_client = boto3.client(
            's3',
            endpoint_url=R2_ENDPOINT,
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
            region_name='auto'
        )
        s3_client.upload_file(
            filepath,
            R2_BUCKET_NAME,
            filename,
            ExtraArgs={'ContentType': 'text/plain'}
        )
        print(f"✅ Успешно загружено в R2: {filename}")
        return True
    except (BotoCoreError, ClientError) as e:
        print(f"❌ Ошибка R2: {e}")
        return False

# --- Фоновая задача ---
def run_scheduler():
    # Расписание: каждое воскресенье в 03:00
    schedule.every().sunday.at("03:00").do(perform_backup)

    # Для тестирования можно раскомментировать:
    # schedule.every(5).minutes.do(perform_backup)  # каждые 5 минут

    print("⏰ Планировщик запущен. Следующий бэкап по расписанию...")
    while True:
        schedule.run_pending()
        time.sleep(60)  # проверяем раз в минуту

# --- Flask маршруты ---
@app.route("/backup", methods=["POST"])
def trigger_backup():
    # Опционально: ручной запуск через API
    if request.headers.get("Authorization") != "Bearer " + os.getenv("BACKUP_API_KEY"):
        return {"error": "Unauthorized"}, 401

    success = perform_backup()
    if success:
        return {"status": "success", "message": "Backup completed and uploaded to R2"}, 200
    else:
        return {"status": "failed"}, 500

@app.route("/health", methods=["GET"])
def health():
    return {"status": "ok", "scheduler": "running"}, 200

# --- Запуск ---
if __name__ == "__main__":
    # Запускаем планировщик в отдельном потоке
    thread = threading.Thread(target=run_scheduler, daemon=True)
    thread.start()

    # Запуск Flask
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", 5000)))
