from __future__ import annotations

from collections import defaultdict
from contextlib import closing
from datetime import date, datetime, time, timedelta
from flask import Flask, flash, jsonify, redirect, render_template, request, session, url_for
from pathlib import Path
import hmac
import json
import os
import sqlite3
import threading
import time as time_module
from functools import wraps
from typing import Dict, List, Optional, Tuple

import requests
from dotenv import load_dotenv
from werkzeug.security import check_password_hash, generate_password_hash


load_dotenv()

DB_PATH = "distribution_history.db"
DEFAULT_BATCH_SIZE = 3
CONFIG_DIR = Path("config")
USERS_CONFIG_PATH = CONFIG_DIR / "users.json"
DIRECTIONS_CONFIG_PATH = CONFIG_DIR / "directions.json"
MANAGERS_CONFIG_PATH = CONFIG_DIR / "managers.json"
SITE_DEAL_TYPES = ["Сайт (Тест)"]
INSTAGRAM_DEAL_TYPES = ["Консультація", "Термін", "Повторне", "База"]
TEAM_LEAD_ROLE = "team_lead"
MANAGER_ROLE = "manager"
CONSULTATION_PREFIX = "КОНСУЛЬТАЦІЯ"
REPEAT_PREFIX = "ПОВТОРНЕ"
BASE_PREFIX = "БАЗА"
BLOCK_PREFIX = "БЛОК"
WORKING_HOURS_START = 10
WORKING_HOURS_END = 19
FRESH_LEAD_MINUTES = 15
TERM_FIELD_CODE = "UF_CRM_1749123119"
TERM_PRIORITY_ORDER = {
    "46945": 0,  # Ближчим часом
    "46947": 1,  # Завчасно
    "46949": 2,  # Без терміну
    "47027": 3,  # На майбутнє
}
TERM_PRIORITY_LABELS = {
    "БЛИЖЧИМ ЧАСОМ": 0,
    "ЗАВЧАСНО": 1,
    "БЕЗ ТЕРМІНУ": 2,
    "НА МАЙБУТНЄ": 3,
}

app = Flask(__name__)
# Ключем підписуються cookie сесій: з відомим ключем будь-хто підробить сесію Team Lead.
_secret_key = str(os.getenv("FLASK_SECRET_KEY", "") or "").strip()
if len(_secret_key) < 32 or _secret_key == "change_me_in_vps":
    raise RuntimeError(
        "Задайте FLASK_SECRET_KEY у .env: випадковий рядок від 32 символів "
        "(python -c \"import secrets; print(secrets.token_hex(32))\")."
    )
app.secret_key = _secret_key
app.config["PERMANENT_SESSION_LIFETIME"] = timedelta(days=30)
app.config["SESSION_REFRESH_EACH_REQUEST"] = True
# Lax: браузер не надсилає cookie з POST-запитами з чужих сайтів (захист від CSRF).
app.config["SESSION_COOKIE_SAMESITE"] = "Lax"
app.config["SESSION_COOKIE_HTTPONLY"] = True
# Увімкніть (SESSION_COOKIE_SECURE=true), щойно сайт працює через HTTPS.
app.config["SESSION_COOKIE_SECURE"] = str(os.getenv("SESSION_COOKIE_SECURE", "")).strip().lower() in {"1", "true", "yes"}

LOGIN_ATTEMPTS_LIMIT = 10
LOGIN_ATTEMPTS_WINDOW = timedelta(minutes=15)
_login_failures: Dict[str, List[datetime]] = {}
_login_lock = threading.Lock()


def _env_required(key: str) -> str:
    value = str(os.getenv(key, "") or "").strip()
    if not value:
        raise KeyError(f"Відсутня env-змінна: {key}")
    return value


def _load_json_list(path: Path, label: str) -> List[Dict]:
    resolved_path = path
    if not resolved_path.exists():
        example_path = path.with_name(f"{path.stem}.example{path.suffix}")
        if example_path.exists():
            resolved_path = example_path
        else:
            raise FileNotFoundError(f"Не знайдено {label}: {path.as_posix()}")
    content = json.loads(resolved_path.read_text(encoding="utf-8"))
    if not isinstance(content, list):
        raise ValueError(f"Очікується список об'єктів у {resolved_path.as_posix()}")
    return content


def _save_json_list(path: Path, data: List[Dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def normalize_role(role: str | None) -> str:
    role_value = str(role or "").strip().lower()
    if role_value in {TEAM_LEAD_ROLE, MANAGER_ROLE}:
        return role_value
    return MANAGER_ROLE


def get_config_users() -> List[Dict]:
    return _load_json_list(USERS_CONFIG_PATH, "конфігурацію користувачів")


def get_direction_config() -> Dict[str, Dict]:
    directions = _load_json_list(DIRECTIONS_CONFIG_PATH, "конфігурацію напрямків")
    return {item["name"]: item for item in directions}


def get_managers_config() -> Dict[str, int]:
    managers = _load_json_list(MANAGERS_CONFIG_PATH, "конфігурацію менеджерів")
    return {str(item["name"]): int(item["id"]) for item in managers}


def get_manager_direction_map() -> Dict[str, str]:
    return {
        str(user.get("name") or ""): str(user.get("direction") or "").strip()
        for user in get_config_users()
        if normalize_role(str(user.get("role") or MANAGER_ROLE)) == MANAGER_ROLE and str(user.get("name") or "").strip()
    }


def get_managers_for_direction(direction_name: str) -> Dict[str, int]:
    """All managers are available for every direction; own-direction managers go first."""
    manager_options = get_managers_config()
    manager_directions = get_manager_direction_map()
    own = {
        name: manager_id
        for name, manager_id in manager_options.items()
        if str(manager_directions.get(name) or "").strip() == direction_name
    }
    others = {name: manager_id for name, manager_id in manager_options.items() if name not in own}
    return {**own, **others}


def _is_password_hash(value: str) -> bool:
    return value.startswith(("scrypt:", "pbkdf2:"))


def _password_matches(stored: str, password: str) -> bool:
    if _is_password_hash(stored):
        return check_password_hash(stored, password)
    # Старий запис відкритим текстом: після успішного входу його замінить хеш.
    return bool(stored) and hmac.compare_digest(stored.encode("utf-8"), password.encode("utf-8"))


_DUMMY_PASSWORD_HASH = generate_password_hash("not-a-real-password")


def _session_user(user: Dict) -> Dict:
    return {
        "login": str(user["login"]),
        "name": str(user.get("name") or user["login"]),
        "manager_id": int(user["manager_id"]),
        "role": normalize_role(str(user.get("role") or MANAGER_ROLE)),
        "direction": str(user.get("direction") or "").strip(),
    }


def _store_session_user(user: Dict) -> None:
    session["user_login"] = user["login"]
    session["user_name"] = user["name"]
    session["user_role"] = user["role"]
    session["user_manager_id"] = user["manager_id"]
    session["user_direction"] = user.get("direction", "")


def find_config_user(login: str) -> Optional[Dict]:
    return next((user for user in get_config_users() if str(user["login"]) == login), None)


def get_auth_user(login: str, password: str) -> Optional[Dict]:
    users = get_config_users()
    for user in users:
        if str(user["login"]) != login:
            continue
        stored = str(user.get("password") or "")
        if not _password_matches(stored, password):
            return None
        if not _is_password_hash(stored):
            user["password"] = generate_password_hash(password)
            _save_json_list(USERS_CONFIG_PATH, users)
        return _session_user(user)
    # Той самий час відповіді, що й для існуючого логіна з неправильним паролем.
    check_password_hash(_DUMMY_PASSWORD_HASH, password)
    return None


def _client_ip() -> str:
    # За nginx (README) справжня адреса приходить у X-Real-IP; напряму довіряємо лише remote_addr.
    remote = str(request.remote_addr or "")
    if remote in {"127.0.0.1", "::1"}:
        return str(request.headers.get("X-Real-IP") or remote)
    return remote


def _too_many_login_failures(ip: str) -> bool:
    now = datetime.now()
    with _login_lock:
        recent = [moment for moment in _login_failures.get(ip, []) if now - moment < LOGIN_ATTEMPTS_WINDOW]
        if recent:
            _login_failures[ip] = recent
        else:
            _login_failures.pop(ip, None)
        return len(recent) >= LOGIN_ATTEMPTS_LIMIT


def _record_login_failure(ip: str) -> None:
    with _login_lock:
        _login_failures.setdefault(ip, []).append(datetime.now())


def add_manager_account(name: str, login: str, password: str, bitrix_id: int, direction: str) -> Tuple[bool, str]:
    users = get_config_users()
    managers = _load_json_list(MANAGERS_CONFIG_PATH, "конфігурацію менеджерів")
    directions = get_direction_config()

    name_clean = str(name).strip()
    login_clean = str(login).strip()
    password_clean = str(password).strip()
    direction_clean = str(direction).strip()

    if not name_clean or not login_clean or not password_clean:
        return False, "Заповніть усі поля: ім'я, логін, пароль та ID Bitrix24."
    if direction_clean not in directions:
        return False, "Оберіть валідний напрямок для менеджера."

    if any(str(item.get("login", "")).strip().lower() == login_clean.lower() for item in users):
        return False, "Користувач з таким логіном вже існує."

    if any(str(item.get("name", "")).strip().lower() == name_clean.lower() for item in managers):
        return False, "Менеджер з таким ім'ям вже існує."

    if any(int(item.get("id", 0)) == int(bitrix_id) for item in managers):
        return False, "Менеджер з таким Bitrix24 ID вже існує."

    users.append(
        {
            "login": login_clean,
            "password": generate_password_hash(password_clean),
            "name": name_clean,
            "manager_id": int(bitrix_id),
            "role": MANAGER_ROLE,
            "direction": direction_clean,
        }
    )
    managers.append({"name": name_clean, "id": int(bitrix_id), "direction": direction_clean})

    _save_json_list(USERS_CONFIG_PATH, users)
    _save_json_list(MANAGERS_CONFIG_PATH, managers)
    return True, f"Менеджера «{name_clean}» успішно додано."


def remove_manager_account(login_or_name: str, current_user_login: str) -> Tuple[bool, str]:
    users = get_config_users()
    managers = _load_json_list(MANAGERS_CONFIG_PATH, "конфігурацію менеджерів")

    target = str(login_or_name or "").strip().lower()
    if not target:
        return False, "Оберіть менеджера для видалення."

    target_user = next(
        (
            item
            for item in users
            if str(item.get("login", "")).strip().lower() == target
            or str(item.get("name", "")).strip().lower() == target
        ),
        None,
    )
    if not target_user:
        return False, "Не знайдено користувача для видалення."

    target_login = str(target_user.get("login", "")).strip()
    target_name = str(target_user.get("name", "")).strip()
    target_manager_id = int(target_user.get("manager_id", 0))
    target_role = normalize_role(str(target_user.get("role") or MANAGER_ROLE))

    if target_role != MANAGER_ROLE:
        return False, "Можна видаляти лише акаунти з роллю менеджера."

    if target_login.lower() == str(current_user_login).strip().lower():
        return False, "Неможливо видалити власний акаунт."

    users_updated = [item for item in users if str(item.get("login", "")).strip().lower() != target_login.lower()]
    managers_updated = [item for item in managers if int(item.get("id", 0)) != target_manager_id]

    if len(users_updated) == len(users):
        return False, "Не вдалося видалити менеджера з users.json."

    _save_json_list(USERS_CONFIG_PATH, users_updated)
    _save_json_list(MANAGERS_CONFIG_PATH, managers_updated)
    return True, f"Менеджера «{target_name or target_login}» успішно видалено."


def get_chatbot_config() -> Dict[str, str]:
    return {
        "webhook_url": str(os.getenv("CHATBOT_WEBHOOK_URL", "")).strip(),
        "telegram_bot_token": str(os.getenv("TELEGRAM_BOT_TOKEN", "")).strip(),
        "telegram_chat_id": str(os.getenv("TELEGRAM_CHAT_ID", "")).strip(),
    }


def bitrix_request(method: str, payload: Dict) -> Dict:
    base_url = _env_required("BITRIX_WEBHOOK_URL").rstrip("/")
    response = requests.post(f"{base_url}/{method}.json", json=payload, timeout=30)
    response.raise_for_status()
    data = response.json()
    if "error" in data:
        raise RuntimeError(f"Bitrix API error: {data.get('error_description') or data['error']}")
    return data


def fetch_deals(category_id: int, stage_id: str, limit: int | None = None) -> List[Dict]:
    deals: List[Dict] = []
    start = 0
    while True:
        payload = {
            "filter": {"CATEGORY_ID": category_id, "STAGE_ID": stage_id},
            "order": {"ID": "ASC"},
            "select": [
                "ID",
                "TITLE",
                "ASSIGNED_BY_ID",
                "SOURCE_ID",
                TERM_FIELD_CODE,
                "DATE_MODIFY",
                "DATE_CREATE",
            ],
            "start": start,
        }
        data = bitrix_request("crm.deal.list", payload)
        chunk = data.get("result", [])
        deals.extend(chunk)
        if limit is not None and len(deals) >= limit:
            return deals[:limit]
        next_start = data.get("next")
        if next_start is None or not chunk:
            break
        start = int(next_start)
    return deals


def fetch_deal_count_for_manager(category_id: int, stage_id: str, manager_id: int) -> int:
    payload = {"filter": {"CATEGORY_ID": category_id, "STAGE_ID": stage_id, "ASSIGNED_BY_ID": int(manager_id)}}
    data = bitrix_request("crm.deal.list", payload)
    total = data.get("total")
    return int(total) if total is not None else len(data.get("result", []))


def fetch_source_map() -> Dict[str, str]:
    payload = {"filter": {"ENTITY_ID": "SOURCE"}}
    data = bitrix_request("crm.status.list", payload)
    return {str(item.get("STATUS_ID", "")): str(item.get("NAME", "")) for item in data.get("result", [])}


def get_direction_logic(direction_name: str, direction: Dict) -> str:
    logic = str(direction.get("distribution_logic") or "").strip().lower()
    if logic in {"site", "instagram"}:
        return logic
    direction_upper = direction_name.strip().upper()
    return "instagram" if "INSTAGRAM" in direction_upper else "site"


def get_deal_types_for_logic(logic: str) -> List[str]:
    return SITE_DEAL_TYPES if logic == "site" else INSTAGRAM_DEAL_TYPES


def classify_deal_type(deal: Dict, source_map: Dict[str, str], logic: str) -> str:
    if logic == "site":
        return SITE_DEAL_TYPES[0]
    title = str(deal.get("TITLE") or "").strip().upper()
    if title.startswith(CONSULTATION_PREFIX):
        return "Консультація"
    if title.startswith(REPEAT_PREFIX):
        return "Повторне"
    if title.startswith(BASE_PREFIX):
        return "База"
    return "Термін"


def is_blocked_deal(deal: Dict) -> bool:
    title = str(deal.get("TITLE") or "").strip().upper()
    return BLOCK_PREFIX in title


def parse_term_priority(value: str | None) -> int:
    if value is None:
        return max(TERM_PRIORITY_ORDER.values()) + 1
    text_value = str(value).strip()
    if not text_value:
        return max(TERM_PRIORITY_ORDER.values()) + 1
    if text_value in TERM_PRIORITY_ORDER:
        return TERM_PRIORITY_ORDER[text_value]
    upper_value = text_value.upper()
    if upper_value in TERM_PRIORITY_LABELS:
        return TERM_PRIORITY_LABELS[upper_value]
    return max(TERM_PRIORITY_ORDER.values()) + 1


def parse_datetime_value(value: str | None) -> datetime:
    if not value:
        return datetime.max
    text_value = str(value).strip()
    if not text_value:
        return datetime.max
    text_value = text_value.replace("Z", "+00:00")
    try:
        return datetime.fromisoformat(text_value)
    except ValueError:
        return datetime.max


# --- Working hours priority logic ---
def is_working_hours_priority_enabled(direction: Dict) -> bool:
    return bool(direction.get("working_hours_priority_enabled", False))


def apply_working_hours_priority(deals: List[Dict]) -> List[Dict]:
    now = datetime.now()

    if not (WORKING_HOURS_START <= now.hour < WORKING_HOURS_END):
        return deals

    fresh_threshold = now - timedelta(minutes=FRESH_LEAD_MINUTES)

    fresh_deals: List[Dict] = []
    regular_deals: List[Dict] = []

    for deal in deals:
        created_at = parse_datetime_value(str(deal.get("DATE_CREATE") or ""))

        if created_at >= fresh_threshold:
            fresh_deals.append(deal)
        else:
            regular_deals.append(deal)

    return fresh_deals + regular_deals


def update_deal_assignment_and_stage(deal_id: int, manager_id: int, next_stage_id: str) -> None:
    payload = {"id": int(deal_id), "fields": {"ASSIGNED_BY_ID": int(manager_id), "STAGE_ID": str(next_stage_id)}}
    bitrix_request("crm.deal.update", payload)


def init_db() -> None:
    with closing(sqlite3.connect(DB_PATH)) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS distribution_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                distribution_date TEXT NOT NULL,
                direction_name TEXT NOT NULL,
                manager_name TEXT NOT NULL,
                deal_type TEXT NOT NULL,
                deal_id INTEGER NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS distribution_control_state (
                distribution_date TEXT NOT NULL,
                direction_name TEXT NOT NULL,
                is_running INTEGER NOT NULL DEFAULT 0,
                managers_json TEXT NOT NULL DEFAULT '[]',
                updated_by TEXT,
                updated_at TEXT NOT NULL,
                PRIMARY KEY (distribution_date, direction_name)
            )
            """
        )
        control_columns = {row[1] for row in conn.execute("PRAGMA table_info(distribution_control_state)")}
        if "batch_size" not in control_columns:
            # Розмір пачки, з яким запустили розподіл: його використовує серверний цикл.
            conn.execute("ALTER TABLE distribution_control_state ADD COLUMN batch_size INTEGER")
        conn.commit()


def get_control_state(direction_name: str) -> Dict:
    distribution_date = date.today().isoformat()
    with closing(sqlite3.connect(DB_PATH)) as conn:
        row = conn.execute(
            """
            SELECT is_running, managers_json, updated_by, updated_at, batch_size
            FROM distribution_control_state
            WHERE distribution_date = ? AND direction_name = ?
            """,
            (distribution_date, direction_name),
        ).fetchone()
    if not row:
        return {"running": False, "managers": []}
    try:
        managers = json.loads(str(row[1] or "[]"))
    except json.JSONDecodeError:
        managers = []
    return {
        "running": bool(int(row[0])),
        "managers": managers,
        "updated_by": row[2],
        "updated_at": row[3],
        "batch_size": int(row[4]) if row[4] else None,
    }


def set_control_state(
    direction_name: str,
    running: bool,
    managers: List[str],
    updated_by: str,
    batch_size: int | None = None,
) -> None:
    """batch_size=None залишає збережений раніше розмір пачки."""
    distribution_date = date.today().isoformat()
    with closing(sqlite3.connect(DB_PATH)) as conn:
        conn.execute(
            """
            INSERT INTO distribution_control_state (
                distribution_date, direction_name, is_running, managers_json, updated_by, updated_at, batch_size
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(distribution_date, direction_name) DO UPDATE SET
                is_running = excluded.is_running,
                managers_json = excluded.managers_json,
                updated_by = excluded.updated_by,
                updated_at = excluded.updated_at,
                batch_size = COALESCE(excluded.batch_size, distribution_control_state.batch_size)
            """,
            (
                distribution_date,
                direction_name,
                1 if running else 0,
                json.dumps(managers, ensure_ascii=False),
                updated_by,
                datetime.utcnow().isoformat(),
                int(batch_size) if batch_size else None,
            ),
        )
        conn.commit()


def store_distribution_rows(direction_name: str, rows: List[Dict]) -> None:
    if not rows:
        return
    distribution_date = date.today().isoformat()
    with closing(sqlite3.connect(DB_PATH)) as conn:
        conn.executemany(
            """
            INSERT INTO distribution_history (distribution_date, direction_name, manager_name, deal_type, deal_id)
            VALUES (?, ?, ?, ?, ?)
            """,
            [
                (
                    distribution_date,
                    direction_name,
                    row["manager"],
                    row["deal_type"],
                    int(row["deal_id"]),
                )
                for row in rows
            ],
        )
        conn.commit()


def get_daily_summary(direction_name: str) -> Dict[str, Dict[str, int]]:
    distribution_date = date.today().isoformat()
    with closing(sqlite3.connect(DB_PATH)) as conn:
        rows = conn.execute(
            """
            SELECT manager_name, deal_type, COUNT(*)
            FROM distribution_history
            WHERE distribution_date = ? AND direction_name = ?
            GROUP BY manager_name, deal_type
            """,
            (distribution_date, direction_name),
        ).fetchall()
    summary: Dict[str, Dict[str, int]] = defaultdict(dict)
    for manager_name, deal_type, count in rows:
        summary[str(manager_name)][str(deal_type)] = int(count)
    return summary


def get_daily_manager_state(direction_name: str, selected_managers: List[str], deal_types: List[str]) -> Dict[str, Dict]:
    distribution_date = date.today().isoformat()
    with closing(sqlite3.connect(DB_PATH)) as conn:
        state = {manager_name: {deal_type: 0 for deal_type in deal_types} for manager_name in selected_managers}
        for manager_name in selected_managers:
            state[manager_name].update({"total": 0, "last_type": None})

        rows = conn.execute(
            """
            SELECT manager_name, deal_type, COUNT(*) AS cnt, MAX(id) AS last_row_id
            FROM distribution_history
            WHERE distribution_date = ? AND direction_name = ?
            GROUP BY manager_name, deal_type
            """,
            (distribution_date, direction_name),
        ).fetchall()

        last_row_by_manager: Dict[str, int] = {}
        for manager_name, deal_type, count, last_row_id in rows:
            manager_name = str(manager_name)
            deal_type = str(deal_type)
            if manager_name not in state:
                continue
            state[manager_name][deal_type] = int(count)
            state[manager_name]["total"] += int(count)
            if last_row_id is not None:
                prev_last = last_row_by_manager.get(manager_name)
                if prev_last is None or int(last_row_id) > prev_last:
                    last_row_by_manager[manager_name] = int(last_row_id)

        for manager_name, last_row_id in last_row_by_manager.items():
            row = conn.execute("SELECT deal_type FROM distribution_history WHERE id = ?", (int(last_row_id),)).fetchone()
            if row:
                state[manager_name]["last_type"] = str(row[0])
        return state


def select_manager_for_deal(
    deal_type: str,
    selected_managers: List[str],
    manager_state: Dict[str, Dict],
    remaining_slots: Dict[str, int],
) -> str:
    under_limit = [m for m in selected_managers if int(remaining_slots[m]) > 0]
    if not under_limit:
        raise RuntimeError("Немає доступних менеджерів для добору до ліміту.")
    maximum_remaining = max(int(remaining_slots[m]) for m in under_limit)
    candidates = [m for m in under_limit if int(remaining_slots[m]) == maximum_remaining]
    preferred = [m for m in candidates if manager_state[m].get("last_type") != deal_type]
    tie_pool = preferred or candidates
    minimum_type_count = min(int(manager_state[m][deal_type]) for m in tie_pool)
    final_candidates = [m for m in tie_pool if int(manager_state[m][deal_type]) == minimum_type_count]
    return final_candidates[0]


def build_stop_report_message(direction_name: str, selected_managers: List[str], deal_types: List[str]) -> str:
    summary = get_daily_summary(direction_name)
    summary_managers = sorted(summary.keys())
    managers_to_show = list(dict.fromkeys(selected_managers + summary_managers))

    lines = [f"📊 Звіт по розподілу ({direction_name}) за {date.today().isoformat()}:"]
    if not summary:
        lines.append("За сьогодні ще немає розподілених заявок.")
        return "\n".join(lines)

    for manager in managers_to_show:
        manager_summary = summary.get(manager, {})
        total = sum(int(manager_summary.get(deal_type, 0)) for deal_type in deal_types)
        if total == 0:
            continue

        detail = ", ".join(
            f"{deal_type}: {int(manager_summary.get(deal_type, 0))}"
            for deal_type in deal_types
            if int(manager_summary.get(deal_type, 0)) > 0
        )
        lines.append(f"• {manager}: {total} шт. ({detail})")

    if len(lines) == 1:
        lines.append("За обраними менеджерами немає розподілених заявок.")

    return "\n".join(lines)


def send_chatbot_message(text: str) -> None:
    chatbot_cfg = get_chatbot_config()
    webhook_url = chatbot_cfg["webhook_url"]
    telegram_token = chatbot_cfg["telegram_bot_token"]
    telegram_chat_id = chatbot_cfg["telegram_chat_id"]

    try:
        if webhook_url:
            requests.post(webhook_url, json={"text": text}, timeout=15).raise_for_status()
            return

        if telegram_token and telegram_chat_id:
            requests.post(
                f"https://api.telegram.org/bot{telegram_token}/sendMessage",
                json={"chat_id": telegram_chat_id, "text": text},
                timeout=15,
            ).raise_for_status()
    except Exception:
        pass


def clear_daily_distribution(direction_name: str) -> int:
    distribution_date = date.today().isoformat()
    with closing(sqlite3.connect(DB_PATH)) as conn:
        cursor = conn.execute(
            """
            DELETE FROM distribution_history
            WHERE distribution_date = ? AND direction_name = ?
            """,
            (distribution_date, direction_name),
        )
        conn.commit()
        return int(cursor.rowcount or 0)


def run_distribution_once(direction_name: str, selected_managers: List[str], batch_size: int) -> Dict:
    directions = get_direction_config()

    if direction_name not in directions:
        return {"status": "warning", "message": "Вибраний напрямок не знайдено", "results": []}

    direction = directions[direction_name]
    logic = get_direction_logic(direction_name, direction)
    category_id = int(direction["funnel_id"])
    source_stage_id = str(direction["status_id"])
    next_stage_id = str(direction.get("next_status_id") or "").strip()
    in_progress_stage_id = str(direction.get("in_progress_status_id") or next_stage_id).strip()
    target_stage_id = in_progress_stage_id or next_stage_id

    if not target_stage_id:
        return {
            "status": "warning",
            "message": "Для напрямку не задано in_progress_status_id або next_status_id",
            "results": [],
        }

    if not selected_managers:
        return {"status": "warning", "message": "Оберіть хоча б одного менеджера.", "results": []}

    deals_all = fetch_deals(category_id, source_stage_id)
    source_map = fetch_source_map()
    deal_types = get_deal_types_for_logic(logic)
    manager_options = get_managers_for_direction(direction_name)

    manager_ids = {name: manager_options[name] for name in selected_managers if name in manager_options}
    if len(manager_ids) != len(selected_managers):
        return {"status": "warning", "message": "Деякі менеджери не знайдені в конфігурації", "results": []}

    in_progress_counts = {
        manager_name: fetch_deal_count_for_manager(category_id, in_progress_stage_id, manager_ids[manager_name])
        for manager_name in selected_managers
    }

    remaining_slots = {m: max(0, batch_size - in_progress_counts[m]) for m in selected_managers}
    available_managers = [m for m in selected_managers if remaining_slots[m] > 0]

    if not available_managers:
        return {
            "status": "warning",
            "message": (
                f"Немає вільних слотів: у всіх менеджерів вже є по {batch_size} "
                "активних угод у статусі 'Угода в роботі'."
            ),
            "results": [],
            "in_progress_counts": in_progress_counts,
        }

    if not deals_all:
        return {"status": "info", "message": "Немає заявок для розподілу", "results": []}

    max_for_batch = sum(remaining_slots[m] for m in available_managers)
    eligible_deals = [deal for deal in deals_all if not is_blocked_deal(deal)]
    if logic == "instagram":
        eligible_deals.sort(
            key=lambda deal: (
                parse_term_priority(str(deal.get(TERM_FIELD_CODE) or "")),
                parse_datetime_value(str(deal.get("DATE_MODIFY") or "")),
                int(deal.get("ID") or 0),
            )
        )

    if is_working_hours_priority_enabled(direction):
        eligible_deals = apply_working_hours_priority(eligible_deals)
    target_deals = eligible_deals[: min(len(eligible_deals), max_for_batch)]
    manager_state = get_daily_manager_state(direction_name, available_managers, deal_types)

    results = []
    for deal in target_deals:
        control_state = get_control_state(direction_name)
        if not control_state.get("running"):
            break
        deal_type = classify_deal_type(deal, source_map, logic)
        manager_name = select_manager_for_deal(deal_type, available_managers, manager_state, remaining_slots)
        manager_id = manager_ids[manager_name]

        manager_state[manager_name][deal_type] += 1
        manager_state[manager_name]["total"] += 1
        manager_state[manager_name]["last_type"] = deal_type
        remaining_slots[manager_name] -= 1

        update_deal_assignment_and_stage(int(deal["ID"]), manager_id, target_stage_id)
        results.append(
            {
                "deal_id": int(deal["ID"]),
                "deal_title": deal.get("TITLE", ""),
                "deal_type": deal_type,
                "manager": manager_name,
            }
        )

    store_distribution_rows(direction_name, results)
    return {
        "status": "success",
        "message": (
            f"Успішно розподілено {len(results)} заявок. "
            f"Менеджерів з доступними слотами: {len(available_managers)}. Ціль в роботі: {batch_size}."
        ),
        "results": results,
        "in_progress_counts": in_progress_counts,
    }


# --- Серверний авто-розподіл ---
# Раніше цикл крутився таймером у вкладці браузера. Телефон (Safari) присипляє вкладку,
# тож заявки приходили лише при відкритті сторінки — одразу всією пачкою.
# Тепер цикл працює на сервері й не залежить від того, чи відкрита сторінка.
WORKER_TICK_SECONDS = 5
_direction_locks: Dict[str, threading.Lock] = defaultdict(threading.Lock)
_direction_locks_guard = threading.Lock()
_last_run_by_direction: Dict[str, Dict] = {}
_last_run_at: Dict[str, float] = {}


def _lock_for_direction(direction_name: str) -> threading.Lock:
    with _direction_locks_guard:
        return _direction_locks[direction_name]


def resolve_batch_size(direction_name: str, control_state: Dict) -> int:
    direction = get_direction_config().get(direction_name, {})
    return int(control_state.get("batch_size") or direction.get("batch_size") or DEFAULT_BATCH_SIZE)


def run_distribution_for_direction(direction_name: str) -> Dict:
    """Один прохід розподілу з менеджерами й пачкою, збереженими при старті.

    Блокування не дає двом проходам (сервер + кнопка, кілька вкладок) розподілити
    одну заявку двічі.
    """
    lock = _lock_for_direction(direction_name)
    if not lock.acquire(blocking=False):
        return {"status": "info", "message": "Розподіл уже виконується", "results": []}
    try:
        control_state = get_control_state(direction_name)
        if not control_state.get("running"):
            return {"status": "info", "message": "Розподіл зупинено або поставлено на паузу", "results": []}
        managers = [str(m) for m in control_state.get("managers", [])]
        batch_size = resolve_batch_size(direction_name, control_state)
        try:
            result = run_distribution_once(direction_name, managers, batch_size)
        except Exception as exc:  # noqa: BLE001 — помилку показуємо в інтерфейсі, цикл не падає
            result = {"status": "error", "message": f"Помилка розподілу: {exc}", "results": []}
        result["finished_at"] = datetime.now().strftime("%H:%M:%S")
        _last_run_by_direction[direction_name] = {k: v for k, v in result.items() if k != "results"}
        return result
    finally:
        _last_run_at[direction_name] = time_module.monotonic()
        lock.release()


def get_last_run(direction_name: str) -> Dict:
    return dict(_last_run_by_direction.get(direction_name) or {})


def _auto_distribution_loop() -> None:
    while True:
        try:
            directions = get_direction_config()
            for direction_name, direction in directions.items():
                if not get_control_state(direction_name).get("running"):
                    continue
                interval = max(5, int(direction.get("auto_interval_seconds") or 30))
                last = _last_run_at.get(direction_name)
                if last is not None and time_module.monotonic() - last < interval:
                    continue
                run_distribution_for_direction(direction_name)
        except Exception as exc:  # noqa: BLE001
            print(f"[auto-distribution] {exc}", flush=True)
        time_module.sleep(WORKER_TICK_SECONDS)


def start_auto_distribution_worker() -> None:
    thread = threading.Thread(target=_auto_distribution_loop, name="auto-distribution", daemon=True)
    thread.start()


def login_required(fn):
    @wraps(fn)
    def wrapper(*args, **kwargs):
        if not session.get("user_login"):
            return redirect(url_for("login"))
        # Роль і напрямок щоразу з users.json: видалений чи понижений користувач
        # не зберігає доступ на всі 30 днів життя cookie.
        user = find_config_user(str(session["user_login"]))
        if not user:
            session.clear()
            return redirect(url_for("login"))
        _store_session_user(_session_user(user))
        # Лише справжній JSON: форма з чужого сайту не може надіслати application/json.
        if request.method == "POST" and request.path.startswith("/api/") and not request.is_json:
            return jsonify({"status": "error", "message": "Очікується JSON"}), 415
        return fn(*args, **kwargs)

    return wrapper


def get_accessible_directions_for_user() -> List[str]:
    directions = get_direction_config()
    if session.get("user_role") == TEAM_LEAD_ROLE:
        return list(directions.keys())
    assigned_direction = str(session.get("user_direction") or "").strip()
    if assigned_direction in directions:
        return [assigned_direction]
    return []


def team_lead_required(fn):
    @wraps(fn)
    def wrapper(*args, **kwargs):
        if session.get("user_role") != TEAM_LEAD_ROLE:
            return jsonify({"status": "error", "message": "Доступ дозволено лише Team Lead"}), 403
        return fn(*args, **kwargs)

    return wrapper


@app.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        ip = _client_ip()
        if _too_many_login_failures(ip):
            flash("Забагато невдалих спроб. Спробуйте за 15 хвилин.", "error")
            return render_template("login.html"), 429
        login_value = str(request.form.get("login", "")).strip()
        password = str(request.form.get("password", "")).strip()
        user = get_auth_user(login_value, password)
        if not user:
            _record_login_failure(ip)
            flash("Невірний логін або пароль", "error")
            return render_template("login.html")
        session.clear()
        _store_session_user(user)
        session.permanent = True
        return redirect(url_for("index"))
    return render_template("login.html")


@app.post("/logout")
def logout():
    session.clear()
    return redirect(url_for("login"))


@app.route("/")
@login_required
def index():
    directions = get_direction_config()
    accessible_directions = get_accessible_directions_for_user()
    if not accessible_directions:
        return "Для користувача не налаштовано доступний напрямок. Зверніться до Team Lead.", 403
    requested_direction = str(request.args.get("direction") or "").strip()
    selected_direction = requested_direction if requested_direction in accessible_directions else accessible_directions[0]
    managers = get_managers_for_direction(selected_direction)
    managers_by_direction = {direction_name: get_managers_for_direction(direction_name) for direction_name in directions.keys()}
    deal_types_by_direction = {
        direction_name: get_deal_types_for_logic(get_direction_logic(direction_name, direction_cfg))
        for direction_name, direction_cfg in directions.items()
    }
    summary = get_daily_summary(selected_direction)
    removable_users = [
        user_item
        for user_item in get_config_users()
        if normalize_role(str(user_item.get("role") or MANAGER_ROLE)) == MANAGER_ROLE
    ]
    return render_template(
        "index.html",
        directions=directions,
        accessible_directions=accessible_directions,
        managers=managers,
        managers_by_direction=managers_by_direction,
        manager_home_directions=get_manager_direction_map(),
        selected_direction=selected_direction,
        summary=summary,
        deal_types=deal_types_by_direction[selected_direction],
        deal_types_by_direction=deal_types_by_direction,
        user_name=session.get("user_name", "Користувач"),
        user_role=session.get("user_role", MANAGER_ROLE),
        removable_users=removable_users,
        default_batch_size=int(directions[selected_direction].get("batch_size") or DEFAULT_BATCH_SIZE),
        auto_interval_seconds=int(directions[selected_direction].get("auto_interval_seconds") or 30),
    )


@app.get("/api/summary")
@login_required
def summary_api():
    direction_name = str(request.args.get("direction", "")).strip()
    if not direction_name:
        return jsonify({"status": "warning", "message": "Не задано напрямок"}), 400
    if direction_name not in get_accessible_directions_for_user():
        return jsonify({"status": "warning", "message": "У вас немає доступу до цього напрямку"}), 403
    return jsonify({"status": "success", "summary": get_daily_summary(direction_name)})


@app.get("/api/control-state")
@login_required
def control_state_api():
    direction_name = str(request.args.get("direction", "")).strip()
    if not direction_name:
        return jsonify({"status": "warning", "message": "Не задано напрямок"}), 400
    if direction_name not in get_accessible_directions_for_user():
        return jsonify({"status": "warning", "message": "У вас немає доступу до цього напрямку"}), 403
    return jsonify({
        "status": "success",
        "control": get_control_state(direction_name),
        "last_run": get_last_run(direction_name),
        "summary": get_daily_summary(direction_name),
    })


@app.post("/api/distribute-once")
@login_required
def distribute_once_api():
    """Позачерговий прохід (той самий, що робить серверний цикл)."""
    payload = (request.get_json(silent=True) or {})
    direction_name = str(payload.get("direction", "")).strip()
    if direction_name not in get_direction_config():
        return jsonify({"status": "warning", "message": "Вибраний напрямок не знайдено"}), 400
    if direction_name not in get_accessible_directions_for_user():
        return jsonify({"status": "warning", "message": "У вас немає доступу до цього напрямку"}), 403

    result = run_distribution_for_direction(direction_name)
    result["summary"] = get_daily_summary(direction_name)
    return jsonify(result)


def _requested_batch_size(payload: Dict, direction_name: str) -> int:
    direction_batch = int(get_direction_config().get(direction_name, {}).get("batch_size") or DEFAULT_BATCH_SIZE)
    if session.get("user_role") != TEAM_LEAD_ROLE:
        return direction_batch
    try:
        requested = int(payload.get("batch_size") or 0)
    except (TypeError, ValueError):
        requested = 0
    return requested if requested > 0 else direction_batch


def _kick_distribution(direction_name: str) -> None:
    """Перший прохід одразу після старту, не чекаючи наступного тіку циклу."""
    threading.Thread(target=run_distribution_for_direction, args=(direction_name,), daemon=True).start()


@app.post("/api/control")
@login_required
def control_api():
    payload = (request.get_json(silent=True) or {})
    action = str(payload.get("action", "")).strip()
    direction_name = str(payload.get("direction", "")).strip()
    selected_managers = [str(m) for m in payload.get("managers", [])]
    reason = str(payload.get("reason", "")).strip()
    accessible_directions = get_accessible_directions_for_user()
    if direction_name and direction_name not in accessible_directions:
        return jsonify({"status": "warning", "message": "У вас немає доступу до цього напрямку"}), 403

    if action in {"pause", "stop", "reconfigure"} and not reason:
        return jsonify({"status": "warning", "message": "Причина обов'язкова"}), 400

    if direction_name not in get_direction_config():
        return jsonify({"status": "warning", "message": "Вибраний напрямок не знайдено"}), 400

    if action in {"start", "reconfigure"} and not selected_managers:
        return jsonify({"status": "warning", "message": "Оберіть хоча б одного менеджера."}), 400

    if action == "start":
        set_control_state(
            direction_name,
            True,
            selected_managers,
            str(session.get("user_name", "")),
            _requested_batch_size(payload, direction_name),
        )
        _kick_distribution(direction_name)
        managers_text = ", ".join(selected_managers) if selected_managers else "не обрано"
        send_chatbot_message(
            "\n".join(
                [
                    "▶️ Розподіл заявок розпочато.",
                    f"Напрямок: {direction_name}",
                    f"Користувач: {session.get('user_name', '-')}",
                    f"Менеджери: {managers_text}",
                ]
            )
        )
        return jsonify({"status": "success", "message": "Авто-розподіл запущено"})

    if action == "pause":
        state = get_control_state(direction_name)
        set_control_state(direction_name, False, [str(m) for m in state.get("managers", [])], str(session.get("user_name", "")))
        send_chatbot_message(
            "\n".join(
                [
                    "⏸️ Розподіл поставлено на паузу.",
                    f"Напрямок: {direction_name}",
                    f"Користувач: {session.get('user_name', '-')}",
                    f"Причина: {reason}",
                ]
            )
        )
        return jsonify({"status": "success", "message": "Авто-розподіл на паузі"})

    if action == "stop":
        set_control_state(direction_name, False, [], str(session.get("user_name", "")))
        directions = get_direction_config()
        direction_cfg = directions.get(direction_name, {})
        deal_types = get_deal_types_for_logic(get_direction_logic(direction_name, direction_cfg))
        stop_report = build_stop_report_message(direction_name, selected_managers, deal_types)
        send_chatbot_message(
            "\n\n".join(
                [
                    "\n".join(
                        [
                            "⏹️ Розподіл зупинено.",
                            f"Напрямок: {direction_name}",
                            f"Користувач: {session.get('user_name', '-')}",
                            f"Причина: {reason}",
                        ]
                    ),
                    stop_report,
                ]
            )
        )
        return jsonify({"status": "success", "message": "Авто-розподіл зупинено"})

    if action == "reconfigure":
        set_control_state(
            direction_name,
            True,
            selected_managers,
            str(session.get("user_name", "")),
            _requested_batch_size(payload, direction_name),
        )
        _kick_distribution(direction_name)
        previous_managers = [str(m) for m in payload.get("previous_managers", [])]
        previous_text = ", ".join(previous_managers) if previous_managers else "не обрано"
        new_text = ", ".join(selected_managers) if selected_managers else "не обрано"
        send_chatbot_message(
            "\n".join(
                [
                    "🔄 Змінено менеджерів у розподілі.",
                    f"Напрямок: {direction_name}",
                    f"Користувач: {session.get('user_name', '-')}",
                    f"Було: {previous_text}",
                    f"Стало: {new_text}",
                    f"Причина: {reason}",
                ]
            )
        )
        return jsonify({"status": "success", "message": "Склад менеджерів оновлено"})

    return jsonify({"status": "warning", "message": "Невідома дія"}), 400


@app.post("/api/managers/add")
@login_required
@team_lead_required
def add_manager_api():
    payload = (request.get_json(silent=True) or {})
    name = str(payload.get("name", "")).strip()
    login_value = str(payload.get("login", "")).strip()
    password = str(payload.get("password", "")).strip()
    bitrix_id = int(payload.get("bitrix_id", 0))
    direction = str(payload.get("direction", "")).strip()
    ok, message = add_manager_account(
        name=name,
        login=login_value,
        password=password,
        bitrix_id=bitrix_id,
        direction=direction,
    )
    status = "success" if ok else "error"
    code = 200 if ok else 400
    return jsonify({"status": status, "message": message}), code


@app.post("/api/managers/remove")
@login_required
@team_lead_required
def remove_manager_api():
    payload = (request.get_json(silent=True) or {})
    login_or_name = str(payload.get("login_or_name", "")).strip()
    ok, message = remove_manager_account(login_or_name=login_or_name, current_user_login=str(session.get("user_login")))
    status = "success" if ok else "error"
    code = 200 if ok else 400
    return jsonify({"status": status, "message": message}), code


@app.post("/api/clear-daily")
@login_required
@team_lead_required
def clear_daily_api():
    payload = (request.get_json(silent=True) or {})
    direction_name = str(payload.get("direction", "")).strip()
    if not direction_name:
        return jsonify({"status": "warning", "message": "Оберіть напрямок"}), 400
    deleted_rows = clear_daily_distribution(direction_name)
    return jsonify(
        {
            "status": "success",
            "message": f"Очищено записів: {deleted_rows}",
            "deleted_rows": deleted_rows,
            "summary": get_daily_summary(direction_name),
        }
    )


if __name__ == "__main__":
    init_db()
    start_auto_distribution_worker()
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "8080")), debug=False)
