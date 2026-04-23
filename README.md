# 📥 Розподіл заявок Bitrix24 (Streamlit + Flask Web UI + VPS-ready)

Застосунок для авто-розподілу заявок у Bitrix24 між менеджерами.

Тепер у репозиторії є **два інтерфейси**:
- `app.py` — поточний Streamlit-інтерфейс.
- `webapp.py` — веб-інтерфейс на Flask, який працює через URL (зручно для VPS + Nginx).

## Що зберігається на VPS

Усі службові файли для роботи доступні в GitHub, а секрети задаються на VPS:
- `.env` — секретні ключі/вебхуки (`BITRIX_WEBHOOK_URL`, `FLASK_SECRET_KEY`, тощо).
- `config/users.json`, `config/managers.json`, `config/directions.json` — робочі конфіги.
- `distribution_history.db` — локальна SQLite історія розподілу.

> У GitHub тримайте приклади: `.env.example` та `config/*.example.json`.

---

## Структура проєкту

```text
.
├── app.py
├── webapp.py
├── templates/
│   ├── login.html
│   └── index.html
├── static/
│   └── styles.css
├── requirements.txt
├── .env.example
├── config/
│   ├── users.example.json
│   ├── managers.example.json
│   └── directions.example.json
├── onboarding_media/
├── deployment/
│   └── systemd-distribution.service.example
└── README.md
```

---

## Локальний запуск (Flask Web UI)

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

cp .env.example .env
cp config/users.example.json config/users.json
cp config/managers.example.json config/managers.json
cp config/directions.example.json config/directions.json

python webapp.py
```

Після запуску інтерфейс доступний на:
- `http://127.0.0.1:8080`

---

## Деплой Flask-інтерфейсу на VPS (Ubuntu + systemd + Nginx)

### 1) Підготовка

```bash
sudo apt update
sudo apt install -y python3 python3-venv python3-pip nginx
```

### 2) Розміщення проєкту

```bash
sudo mkdir -p /opt/distribution_of_applications_site_v2
sudo chown -R $USER:$USER /opt/distribution_of_applications_site_v2
cd /opt/distribution_of_applications_site_v2
# git clone ...
```

### 3) Встановлення залежностей

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
```

### 4) Налаштування конфігів

```bash
cp .env.example .env
cp config/users.example.json config/users.json
cp config/managers.example.json config/managers.json
cp config/directions.example.json config/directions.json
```

Обовʼязково заповніть:
- `BITRIX_WEBHOOK_URL`
- `FLASK_SECRET_KEY`
- логіни/паролі у `config/users.json`

### 5) systemd сервіс (Flask)

Приклад `distribution.service`:

```ini
[Unit]
Description=Distribution Web App (Flask)
After=network.target

[Service]
User=ubuntu
WorkingDirectory=/opt/distribution_of_applications_site_v2
EnvironmentFile=/opt/distribution_of_applications_site_v2/.env
ExecStart=/opt/distribution_of_applications_site_v2/.venv/bin/python webapp.py
Restart=always
RestartSec=3

[Install]
WantedBy=multi-user.target
```

Активація:

```bash
sudo cp deployment/systemd-distribution.service.example /etc/systemd/system/distribution.service
# відредагуйте під Flask (ExecStart + порт)
sudo systemctl daemon-reload
sudo systemctl enable distribution.service
sudo systemctl start distribution.service
sudo systemctl status distribution.service
```

### 6) Nginx reverse proxy (URL доступ)

```nginx
server {
    listen 80;
    server_name your-domain.com;

    location / {
        proxy_pass http://127.0.0.1:8080;
        proxy_http_version 1.1;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
    }
}
```

Після цього UI доступний за URL домену.

---

## Streamlit-режим (за потреби)

```bash
streamlit run app.py
```

---

## Важливо

- `distribution_history.db` створюється автоматично.
- Рекомендовано робити щоденні backup БД.
