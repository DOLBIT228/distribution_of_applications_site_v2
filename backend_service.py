from __future__ import annotations

from collections import defaultdict
from contextlib import closing
from datetime import date
from pathlib import Path
import json
import os
import sqlite3
from typing import Dict, List, Optional

import requests
from dotenv import load_dotenv
from flask import Flask, flash, redirect, render_template, request, session, url_for


load_dotenv()

DB_PATH = "distribution_history.db"
DEFAULT_BATCH_SIZE = 3
CONFIG_DIR = Path("config")
USERS_CONFIG_PATH = CONFIG_DIR / "users.json"
DIRECTIONS_CONFIG_PATH = CONFIG_DIR / "directions.json"
MANAGERS_CONFIG_PATH = CONFIG_DIR / "managers.json"


app = Flask(__name__)
app.secret_key = os.getenv("APP_SECRET_KEY", "change-me-in-production")


def _env_required(key: str) -> str:
    value = str(os.getenv(key, "") or "").strip()
    if not value:
        raise KeyError(f"Missing env variable: {key}")
    return value


def _load_json_list(path: Path, label: str) -> List[Dict]:
    if not path.exists():
        raise FileNotFoundError(f"{label} not found: {path.as_posix()}")
    try:
        content = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise ValueError(f"Invalid JSON in {path.as_posix()}: {exc}") from exc
    if not isinstance(content, list):
        raise ValueError(f"Expected list in {path.as_posix()}")
    return content


def get_config_users() -> List[Dict]:
    return _load_json_list(USERS_CONFIG_PATH, "Users config")


def get_direction_config() -> Dict[str, Dict]:
    directions = _load_json_list(DIRECTIONS_CONFIG_PATH, "Directions config")
    return {item["name"]: item for item in directions}


def get_managers_config() -> Dict[str, int]:
    managers = _load_json_list(MANAGERS_CONFIG_PATH, "Managers config")
    return {str(item["name"]): int(item["id"]) for item in managers}


def get_auth_user(login: str, password: str) -> Optional[Dict]:
    users = get_config_users()
    for user in users:
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
    payload = {
        "filter": {
            "CATEGORY_ID": category_id,
            "STAGE_ID": stage_id,
            "ASSIGNED_BY_ID": int(manager_id),
        },
    }
    data = bitrix_request("crm.deal.list", payload)
    total = data.get("total")
    if total is not None:
        return int(total)
    return len(data.get("result", []))


def update_deal_assignment_and_stage(deal_id: int, manager_id: int, next_stage_id: str) -> None:
    payload = {
        "id": int(deal_id),
        "fields": {
            "ASSIGNED_BY_ID": int(manager_id),
            "STAGE_ID": str(next_stage_id),
        },
    }
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
            INSERT INTO distribution_history (
                distribution_date, direction_name, manager_name, deal_type, deal_id
            ) VALUES (?, ?, ?, ?, ?)
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
        cursor = conn.execute(
            """
            SELECT manager_name, deal_type, COUNT(*)
            FROM distribution_history
            WHERE distribution_date = ? AND direction_name = ?
            GROUP BY manager_name, deal_type
            """,
            (distribution_date, direction_name),
        )
        summary: Dict[str, Dict[str, int]] = defaultdict(dict)
        for manager_name, deal_type, count in cursor.fetchall():
            summary[str(manager_name)][str(deal_type)] = int(count)
        return summary


def distribute_once(direction_name: str, selected_managers: List[str]) -> Dict:
    if not selected_managers:
        return {"status": "error", "message": "Select at least one manager"}

    directions = get_direction_config()
    direction = directions[direction_name]

    category_id = int(direction["funnel_id"])
    source_stage_id = str(direction["status_id"])
    target_stage_id = str(direction["next_status_id"])
    in_progress_stage_id = str(direction.get("in_progress_status_id") or target_stage_id)
    batch_size = int(direction.get("batch_size") or DEFAULT_BATCH_SIZE)

    manager_options = get_managers_config()
    manager_ids = {name: manager_options[name] for name in selected_managers}

    remaining_slots = {
        manager_name: max(0, batch_size - fetch_deal_count_for_manager(category_id, in_progress_stage_id, manager_id))
        for manager_name, manager_id in manager_ids.items()
    }
    available_managers = [m for m in selected_managers if remaining_slots[m] > 0]
    if not available_managers:
        return {"status": "warning", "message": "No available managers in this cycle"}

    max_for_batch = sum(remaining_slots[m] for m in available_managers)
    deals = fetch_deals(category_id, source_stage_id, limit=max_for_batch)
    if not deals:
        return {"status": "warning", "message": "No deals found in source stage"}

    summary = get_daily_summary(direction_name)
    manager_state = {
        manager_name: {
            "total": sum(summary.get(manager_name, {}).values()),
        }
        for manager_name in available_managers
    }

    results: List[Dict] = []
    for deal in deals:
        under_limit = [m for m in available_managers if remaining_slots[m] > 0]
        if not under_limit:
            break
        target_manager = min(under_limit, key=lambda m: (manager_state[m]["total"], m))

        update_deal_assignment_and_stage(int(deal["ID"]), manager_ids[target_manager], target_stage_id)
        manager_state[target_manager]["total"] += 1
        remaining_slots[target_manager] -= 1

        results.append(
            {
                "deal_id": int(deal["ID"]),
                "deal_title": deal.get("TITLE", ""),
                "deal_type": "Сайт (Тест)",
                "manager": target_manager,
            }
        )

    store_distribution_rows(direction_name, results)
    return {"status": "success", "message": f"Distributed {len(results)} deals", "results": results}


def require_auth():
    if not session.get("user"):
        return redirect(url_for("login"))
    return None


@app.route("/")
def index():
    if session.get("user"):
        return redirect(url_for("dashboard"))
    return redirect(url_for("login"))


@app.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        login_value = request.form.get("login", "").strip()
        password = request.form.get("password", "")
        user = get_auth_user(login_value, password)
        if user:
            session["user"] = user
            return redirect(url_for("dashboard"))
        flash("Невірний логін або пароль", "error")
    return render_template("login.html")


@app.route("/logout")
def logout():
    session.clear()
    return redirect(url_for("login"))


@app.route("/dashboard", methods=["GET", "POST"])
def dashboard():
    auth_redirect = require_auth()
    if auth_redirect:
        return auth_redirect

    directions = get_direction_config()
    managers = get_managers_config()
    selected_direction = request.form.get("direction") or (next(iter(directions.keys())) if directions else "")
    selected_managers = request.form.getlist("managers")

    distribution_result = None
    if request.method == "POST" and request.form.get("action") == "distribute":
        distribution_result = distribute_once(selected_direction, selected_managers)

    summary = get_daily_summary(selected_direction) if selected_direction else {}

    return render_template(
        "dashboard.html",
        user=session.get("user", {}),
        directions=list(directions.keys()),
        managers=list(managers.keys()),
        selected_direction=selected_direction,
        selected_managers=selected_managers,
        distribution_result=distribution_result,
        summary=summary,
    )


if __name__ == "__main__":
    init_db()
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "8000")), debug=False)
