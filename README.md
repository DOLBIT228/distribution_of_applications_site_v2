# 📥 Розподіл заявок Bitrix24 (Streamlit + VPS-ready)

Застосунок для авто-розподілу заявок у Bitrix24 між менеджерами.

## Що оновлено для VPS

- Додано папку `onboarding_media/` для завантаження onboarding-відео.
- `gif-1.webm` переміщено у `onboarding_media/gif-1.webm`.
- Додано підтримку конфігурації **без `st.secrets`**:
  - через `.env` для ключів інтеграцій,
  - через `config/*.json` для списків користувачів/менеджерів/напрямків.

---

## Структура проєкту

```text
.
├── app.py
├── requirements.txt
├── .env.example
├── config/
│   ├── users.example.json
│   ├── managers.example.json
│   └── directions.example.json
├── onboarding_media/
│   └── gif-1.webm
├── deployment/
│   └── systemd-distribution.service.example
└── README.md
```

---

## Конфігурація: пріоритет джерел

1. Якщо є `st.secrets` (Streamlit Cloud) — застосунок читає дані звідти.
2. Якщо `st.secrets` не задані (VPS) — використовує:
   - `.env` для:
     - `BITRIX_WEBHOOK_URL` (обов’язково),
     - `CHATBOT_WEBHOOK_URL` (опційно),
     - `TELEGRAM_BOT_TOKEN` (опційно),
     - `TELEGRAM_CHAT_ID` (опційно).
   - `config/users.json`, `config/managers.json`, `config/directions.json`.

---

## Локальний запуск

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

cp .env.example .env
cp config/users.example.json config/users.json
cp config/managers.example.json config/managers.json
cp config/directions.example.json config/directions.json

streamlit run app.py
```

---

## Чітка інструкція деплою на VPS (Ubuntu)

### 1) Підготовка сервера

```bash
sudo apt update
sudo apt install -y python3 python3-venv python3-pip nginx
```

### 2) Розміщення проєкту

```bash
sudo mkdir -p /opt/distribution_of_applications_site_v2
sudo chown -R $USER:$USER /opt/distribution_of_applications_site_v2
cd /opt/distribution_of_applications_site_v2
# Далі скопіюйте/клонуйте проєкт
```

### 3) Встановлення залежностей

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
```

### 4) Налаштування конфігів (заміна Streamlit secrets)

```bash
cp .env.example .env
cp config/users.example.json config/users.json
cp config/managers.example.json config/managers.json
cp config/directions.example.json config/directions.json
```

Потім заповніть:
- `.env` → мінімум `BITRIX_WEBHOOK_URL`
- `config/users.json` → логіни/паролі/manager_id
- `config/managers.json` → менеджери для розподілу
- `config/directions.json` → funnel/status/logic

### 5) Додавання onboarding файлів

Складайте ваші файли в:

```text
onboarding_media/
```

Наприклад:
- `onboarding_media/gif-1.webm`
- `onboarding_media/gif-2.webm`
- ...

### 6) Запуск як systemd сервіс

```bash
sudo cp deployment/systemd-distribution.service.example /etc/systemd/system/distribution.service
sudo nano /etc/systemd/system/distribution.service
# за потреби змініть User, WorkingDirectory, ExecStart

sudo systemctl daemon-reload
sudo systemctl enable distribution.service
sudo systemctl start distribution.service
sudo systemctl status distribution.service
```

### 7) Reverse proxy через Nginx

```bash
sudo nano /etc/nginx/sites-available/distribution
```

Приклад конфігу:

```nginx
server {
    listen 80;
    server_name your-domain.com;

    location / {
        proxy_pass http://127.0.0.1:8501;
        proxy_http_version 1.1;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection "upgrade";
    }
}
```

Активувати:

```bash
sudo ln -s /etc/nginx/sites-available/distribution /etc/nginx/sites-enabled/distribution
sudo nginx -t
sudo systemctl reload nginx
```

(Опційно) TLS через Certbot.

---

## Що до чого підключати (коротко)

- **Bitrix24** → `.env: BITRIX_WEBHOOK_URL`
- **Користувачі входу в UI** → `config/users.json`
- **Менеджери для розподілу** → `config/managers.json`
- **Напрямки/статуси Bitrix** → `config/directions.json`
- **Чат-бот (опційно)** → `.env` (`CHATBOT_WEBHOOK_URL` або Telegram змінні)
- **Onboarding відео** → `onboarding_media/*`

---

## Важливо

- `distribution_history.db` створюється автоматично поруч із `app.py`.
- Для продакшну рекомендується щоденний backup БД.
