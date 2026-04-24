from __future__ import annotations

from collections import defaultdict
from contextlib import closing
from datetime import date
from pathlib import Path
import json
import os
import sqlite3
from functools import wraps
from typing import Dict, List, Optional, Tuple

import requests
from dotenv import load_dotenv
from flask import Flask, flash, jsonify, redirect, render_template, request, session, url_for


load_dotenv()

DB_PATH = "distribution_history.db"
DEFAULT_BATCH_SIZE = 3
CONFIG_DIR = Path("config")
USERS_CONFIG_PATH = CONFIG_DIR / "users.json"
DIRECTIONS_CONFIG_PATH = CONFIG_DIR / "directions.json"
MANAGERS_CONFIG_PATH = CONFIG_DIR / "managers.json"
SITE_DEAL_TYPES = ["Сайт (Тест)"]
TEAM_LEAD_ROLE = "team_lead"
MANAGER_ROLE = "manager"

app = Flask(__name__)
app.secret_key = os.getenv("FLASK_SECRET_KEY", "change_me_in_vps")


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


def get_auth_user(login: str, password: str) -> Optional[Dict]:
    for user in get_config_users():
        if str(user["login"]) == login and str(user["password"]) == password:
            return {
                "login": str(user["login"]),
                "name": str(user.get("name") or user["login"]),
                "manager_id": int(user["manager_id"]),
                "role": normalize_role(str(user.get("role") or MANAGER_ROLE)),
            }
    return None


def add_manager_account(name: str, login: str, password: str, bitrix_id: int) -> Tuple[bool, str]:
    users = get_config_users()
    managers = _load_json_list(MANAGERS_CONFIG_PATH, "конфігурацію менеджерів")

    name_clean = str(name).strip()
    login_clean = str(login).strip()
    password_clean = str(password).strip()

    if not name_clean or not login_clean or not password_clean:
        return False, "Заповніть усі поля: ім'я, логін, пароль та ID Bitrix24."

    if any(str(item.get("login", "")).strip().lower() == login_clean.lower() for item in users):
        return False, "Користувач з таким логіном вже існує."

    if any(str(item.get("name", "")).strip().lower() == name_clean.lower() for item in managers):
        return False, "Менеджер з таким ім'ям вже існує."

    if any(int(item.get("id", 0)) == int(bitrix_id) for item in managers):
        return False, "Менеджер з таким Bitrix24 ID вже існує."

    users.append(
        {
            "login": login_clean,
            "password": password_clean,
            "name": name_clean,
            "manager_id": int(bitrix_id),
            "role": MANAGER_ROLE,
        }
    )
    managers.append({"name": name_clean, "id": int(bitrix_id)})

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
            "select": ["ID", "TITLE", "ASSIGNED_BY_ID", "SOURCE_ID"],
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


def classify_deal_type(deal: Dict, source_map: Dict[str, str]) -> str:
    return "Сайт (Тест)"


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
    managers_to_show = selected_managers or sorted(summary.keys())

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
    manager_options = get_managers_config()

    if direction_name not in directions:
        return {"status": "warning", "message": "Вибраний напрямок не знайдено", "results": []}

    direction = directions[direction_name]
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
    deal_types = SITE_DEAL_TYPES

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
    target_deals = deals_all[: min(len(deals_all), max_for_batch)]
    manager_state = get_daily_manager_state(direction_name, available_managers, deal_types)

    results = []
    for deal in target_deals:
        deal_type = classify_deal_type(deal, source_map)
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


def login_required(fn):
    @wraps(fn)
    def wrapper(*args, **kwargs):
        if not session.get("user_login"):
            return redirect(url_for("login"))
        return fn(*args, **kwargs)

    return wrapper


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
        login_value = str(request.form.get("login", "")).strip()
        password = str(request.form.get("password", "")).strip()
        user = get_auth_user(login_value, password)
        if not user:
            flash("Невірний логін або пароль", "error")
            return render_template("login.html")
        session["user_login"] = user["login"]
        session["user_name"] = user["name"]
        session["user_role"] = user["role"]
        session["user_manager_id"] = user["manager_id"]
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
    managers = get_managers_config()
    selected_direction = request.args.get("direction") or next(iter(directions.keys()))
    summary = get_daily_summary(selected_direction)
    removable_users = [
        user_item
        for user_item in get_config_users()
        if normalize_role(str(user_item.get("role") or MANAGER_ROLE)) == MANAGER_ROLE
    ]
    return render_template(
        "index.html",
        directions=directions,
        managers=managers,
        selected_direction=selected_direction,
        summary=summary,
        deal_types=SITE_DEAL_TYPES,
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
    return jsonify({"status": "success", "summary": get_daily_summary(direction_name)})


@app.post("/api/distribute-once")
@login_required
def distribute_once_api():
    payload = request.get_json(force=True)
    direction_name = str(payload.get("direction", "")).strip()
    selected_managers = [str(m) for m in payload.get("managers", [])]
    directions = get_direction_config()
    if direction_name not in directions:
        return jsonify({"status": "warning", "message": "Вибраний напрямок не знайдено"}), 400

    direction_batch = int(directions[direction_name].get("batch_size") or DEFAULT_BATCH_SIZE)
    is_team_lead = session.get("user_role") == TEAM_LEAD_ROLE
    batch_size = int(payload.get("batch_size", direction_batch)) if is_team_lead else direction_batch

    if not direction_name or not selected_managers:
        return jsonify({"status": "warning", "message": "Оберіть напрямок і менеджерів"}), 400

    try:
        result = run_distribution_once(direction_name, selected_managers, batch_size)
    except Exception as exc:
        return jsonify({"status": "error", "message": str(exc)}), 500

    result["summary"] = get_daily_summary(direction_name)
    return jsonify(result)


@app.post("/api/control")
@login_required
def control_api():
    payload = request.get_json(force=True)
    action = str(payload.get("action", "")).strip()
    direction_name = str(payload.get("direction", "")).strip()
    selected_managers = [str(m) for m in payload.get("managers", [])]
    reason = str(payload.get("reason", "")).strip()

    if action in {"pause", "stop", "reconfigure"} and not reason:
        return jsonify({"status": "warning", "message": "Причина обов'язкова"}), 400

    if action == "start":
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
        stop_report = build_stop_report_message(direction_name, selected_managers, SITE_DEAL_TYPES)
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
    payload = request.get_json(force=True)
    name = str(payload.get("name", "")).strip()
    login_value = str(payload.get("login", "")).strip()
    password = str(payload.get("password", "")).strip()
    bitrix_id = int(payload.get("bitrix_id", 0))
    ok, message = add_manager_account(name=name, login=login_value, password=password, bitrix_id=bitrix_id)
    status = "success" if ok else "error"
    code = 200 if ok else 400
    return jsonify({"status": status, "message": message}), code


@app.post("/api/managers/remove")
@login_required
@team_lead_required
def remove_manager_api():
    payload = request.get_json(force=True)
    login_or_name = str(payload.get("login_or_name", "")).strip()
    ok, message = remove_manager_account(login_or_name=login_or_name, current_user_login=str(session.get("user_login")))
    status = "success" if ok else "error"
    code = 200 if ok else 400
    return jsonify({"status": status, "message": message}), code


@app.post("/api/clear-daily")
@login_required
@team_lead_required
def clear_daily_api():
    payload = request.get_json(force=True)
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
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "8080")), debug=False)
