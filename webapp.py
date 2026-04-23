from __future__ import annotations

from collections import defaultdict
from contextlib import closing
from datetime import date
from pathlib import Path
import json
import os
import sqlite3
from functools import wraps
from typing import Dict, List, Optional

import requests
from dotenv import load_dotenv
from flask import Flask, flash, jsonify, redirect, render_template, request, session, url_for


load_dotenv()

DB_PATH = "distribution_history.db"
CONFIG_DIR = Path("config")
USERS_CONFIG_PATH = CONFIG_DIR / "users.json"
DIRECTIONS_CONFIG_PATH = CONFIG_DIR / "directions.json"
MANAGERS_CONFIG_PATH = CONFIG_DIR / "managers.json"
SITE_DEAL_TYPES = ["Сайт (Тест)"]


app = Flask(__name__)
app.secret_key = os.getenv("FLASK_SECRET_KEY", "change_me_in_vps")


def _env_required(key: str) -> str:
    value = str(os.getenv(key, "") or "").strip()
    if not value:
        raise KeyError(f"Відсутня env-змінна: {key}")
    return value


def _load_json_list(path: Path, label: str) -> List[Dict]:
    if not path.exists():
        raise FileNotFoundError(f"Не знайдено {label}: {path.as_posix()}")
    content = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(content, list):
        raise ValueError(f"Очікується список об'єктів у {path.as_posix()}")
    return content


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
            }
    return None


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
    maximum_remaining = max(int(remaining_slots[m]) for m in under_limit)
    candidates = [m for m in under_limit if int(remaining_slots[m]) == maximum_remaining]
    preferred = [m for m in candidates if manager_state[m].get("last_type") != deal_type]
    tie_pool = preferred or candidates
    minimum_type_count = min(int(manager_state[m][deal_type]) for m in tie_pool)
    final_candidates = [m for m in tie_pool if int(manager_state[m][deal_type]) == minimum_type_count]
    return final_candidates[0]


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


def run_distribution_once(direction_name: str, selected_managers: List[str], batch_size: int) -> Dict:
    directions = get_direction_config()
    manager_options = get_managers_config()
    direction = directions[direction_name]

    category_id = int(direction["funnel_id"])
    target_stage_id = str(direction["next_status_id"])
    source_stage_id = str(direction["status_id"])
    in_progress_stage_id = str(direction["in_progress_status_id"])

    deals_all = fetch_deals(category_id, source_stage_id)
    deal_types = SITE_DEAL_TYPES

    manager_ids = {name: manager_options[name] for name in selected_managers}
    in_progress_counts = {
        manager_name: fetch_deal_count_for_manager(category_id, in_progress_stage_id, manager_ids[manager_name])
        for manager_name in selected_managers
    }
    remaining_slots = {m: max(0, batch_size - in_progress_counts[m]) for m in selected_managers}
    available_managers = [m for m in selected_managers if remaining_slots[m] > 0]

    if not available_managers:
        return {"status": "warning", "message": "Немає вільних слотів", "results": []}
    if not deals_all:
        return {"status": "info", "message": "Немає заявок для розподілу", "results": []}

    max_for_batch = sum(remaining_slots[m] for m in available_managers)
    target_deals = deals_all[: min(len(deals_all), max_for_batch)]
    manager_state = get_daily_manager_state(direction_name, available_managers, deal_types)

    results = []
    for deal in target_deals:
        deal_type = "Сайт (Тест)"
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
    return {"status": "success", "message": f"Розподілено {len(results)} заявок", "results": results}


def login_required(fn):
    @wraps(fn)
    def wrapper(*args, **kwargs):
        if not session.get("user_login"):
            return redirect(url_for("login"))
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
    return render_template(
        "index.html",
        directions=directions,
        managers=managers,
        selected_direction=selected_direction,
        summary=summary,
        deal_types=SITE_DEAL_TYPES,
        user_name=session.get("user_name", "Користувач"),
    )


@app.post("/api/distribute-once")
@login_required
def distribute_once_api():
    payload = request.get_json(force=True)
    direction_name = str(payload.get("direction", "")).strip()
    selected_managers = [str(m) for m in payload.get("managers", [])]
    batch_size = int(payload.get("batch_size", 3))

    if not direction_name or not selected_managers:
        return jsonify({"status": "warning", "message": "Оберіть напрямок і менеджерів"}), 400

    try:
        result = run_distribution_once(direction_name, selected_managers, batch_size)
    except Exception as exc:
        return jsonify({"status": "error", "message": str(exc)}), 500

    result["summary"] = get_daily_summary(direction_name)
    return jsonify(result)


if __name__ == "__main__":
    init_db()
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "8080")), debug=False)
