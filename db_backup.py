from flask import Flask, request
import os
import subprocess
import urllib.parse
from datetime import datetime
import boto3
from botocore.exceptions import BotoCoreError, ClientError

app = Flask(__name__)

# --- Конфигурация из переменных окружения ---
DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("❌ Не задана переменная окружения DATABASE_URL")

R2_BUCKET_NAME = os.getenv("R2_BUCKET_NAME")
R2_ENDPOINT = os.getenv("R2_ENDPOINT")  # например: https://<account>.r2.cloudflarestorage.com
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")

OUTPUT_DIR = "./backups"
os.makedirs(OUTPUT_DIR, exist_ok=True)

@app.route("/backup", methods=["GET", "POST"])
def trigger_backup():
    if request.method == "POST" and request.headers.get("Authorization") != "Bearer " + os.getenv("BACKUP_API_KEY"):
        return {"error": "Unauthorized"}, 401

    try:
        # Парсим DATABASE_URL
        result = urllib.parse.urlparse(DATABASE_URL)
        if result.scheme not in ("postgres", "postgresql"):
            return {"error": "Invalid DB scheme"}, 400

        username = result.username
        password = result.password
        host = result.hostname
        port = result.port or 5432
        database = result.path.lstrip('/')

        # Имя файла
        timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        filename = f"{database}_backup_{timestamp}.sql"
        filepath = os.path.join(OUTPUT_DIR, filename)

        # Устанавливаем пароль в окружение
        env = os.environ.copy()
        env["PGPASSWORD"] = password

        # Команда pg_dump
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

        print(f"Запуск резервного копирования в {filepath}...")
        subprocess.run(cmd, env=env, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        print(f"✅ Бэкап создан: {filepath}")

        # Загрузка в R2
        if upload_to_r2(filepath, filename):
            os.remove(filepath)  # Удаляем локальный файл
            return {"status": "success", "file": filename, "uploaded_to_r2": True}, 200
        else:
            return {"status": "failed", "error": "Upload to R2 failed"}, 500

    except subprocess.CalledProcessError as e:
        error = e.stderr.decode() if e.stderr else str(e)
        print(f"❌ Ошибка pg_dump: {error}")
        return {"error": "pg_dump failed", "details": error}, 500
    except Exception as e:
        print(f"❌ Неизвестная ошибка: {str(e)}")
        return {"error": str(e)}, 500


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
        print(f"✅ Загружено в R2: {filename}")
        return True
    except (BotoCoreError, ClientError) as e:
        print(f"❌ Ошибка загрузки в R2: {e}")
        return False


@app.route("/health", methods=["GET"])
def health():
    return {"status": "ok"}, 200


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", 5000)))
