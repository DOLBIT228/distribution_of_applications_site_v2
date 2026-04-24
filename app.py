from collections import defaultdict
from datetime import date, datetime
from zoneinfo import ZoneInfo
from pathlib import Path
import base64
import re
import sqlite3
import time
from typing import Dict, List, Optional

import requests
import streamlit as st


st.set_page_config(page_title="Розподіл заявок", page_icon="📥", layout="wide")

st.markdown(
    """
    <style>
        .stApp {
            background: linear-gradient(180deg, #f8fbff 0%, #eef5ff 100%);
        }
        .block-container {
            padding-top: 1.5rem;
            padding-bottom: 2rem;
        }
        div[data-testid="stMetric"] {
            background: #ffffff;
            border: 1px solid #dbe7ff;
            border-radius: 14px;
            padding: 12px 16px;
            box-shadow: 0 6px 18px rgba(27, 74, 149, 0.08);
        }
    </style>
    """,
    unsafe_allow_html=True,
)

DB_PATH = "distribution_history.db"
DASHBOARD_URL = "https://panel-for-manager-call.streamlit.app/"
DEFAULT_BATCH_SIZE = 3
ONBOARDING_MEDIA_DIR = Path("onboarding_media")

SITE_DEAL_TYPES = ["Сайт (Тест)"]
CONSULTATION_PREFIX = "КОНСУЛЬТАЦІЯ"
REPEAT_PREFIX = "ПОВТОРНЕ"
BASE_PREFIX = "БАЗА"
UNTIL_MONTH_PREFIX = "ДО МІСЯЦЯ"
USA_PREFIX = "США"
BLOCK_PREFIX = "БЛОК"
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
TERM_NEAR_LABEL = "Ближчим часом"
TERM_FUTURE_LABEL = "Майбутнє"


def _secret_required(path: str):
    cursor = st.secrets
    for key in path.split("."):
        if key not in cursor:
            raise KeyError(f"Відсутній секрет: {path}")
        cursor = cursor[key]
    return cursor


def _secret_optional(path: str, default=None):
    cursor = st.secrets
    for key in path.split("."):
        if key not in cursor:
            return default
        cursor = cursor[key]
    return cursor


def get_auth_user(login: str, password: str) -> Optional[Dict]:
    users = _secret_required("auth.users")
    for user in users:
        if str(user["login"]) == login and str(user["password"]) == password:
            return {
                "login": str(user["login"]),
                "name": str(user.get("name") or user["login"]),
                "manager_id": int(user["manager_id"]),
            }
    return None


def bitrix_request(method: str, payload: Dict) -> Dict:
    base_url = _secret_required("bitrix.webhook_url").rstrip("/")
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
            "filter": {
                "CATEGORY_ID": category_id,
                "STAGE_ID": stage_id,
            },
            "order": {"ID": "ASC"},
            "select": [
                "ID",
                "TITLE",
                "PHONE",
                "ASSIGNED_BY_ID",
                "SOURCE_ID",
                TERM_FIELD_CODE,
                "DATE_MODIFY",
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


def fetch_source_map() -> Dict[str, str]:
    payload = {"filter": {"ENTITY_ID": "SOURCE"}}
    data = bitrix_request("crm.status.list", payload)
    return {str(item.get("STATUS_ID", "")): str(item.get("NAME", "")) for item in data.get("result", [])}


def get_direction_logic(direction_name: str, direction: Dict) -> str:
    return "instagram"


def classify_deal_type(deal: Dict, source_map: Dict[str, str], logic: str) -> str:
    title = str(deal.get("TITLE") or "").strip().upper()
    if title.startswith(CONSULTATION_PREFIX):
        return "Консультація"
    return "Термін"


def get_deal_types_for_logic(logic: str) -> List[str]:
    return ["Консультація", "Термін", "Повторне", "База"]


def is_prefix_in_title(deal: Dict, prefix: str) -> bool:
    title = str(deal.get("TITLE") or "").strip().upper()
    return title.startswith(prefix)


def _collect_phone_like_values(value) -> List[str]:
    if value is None:
        return []
    if isinstance(value, str):
        return [value]
    if isinstance(value, dict):
        values: List[str] = []
        for nested_value in value.values():
            values.extend(_collect_phone_like_values(nested_value))
        return values
    if isinstance(value, (list, tuple, set)):
        values: List[str] = []
        for nested_value in value:
            values.extend(_collect_phone_like_values(nested_value))
        return values
    return [str(value)]


def deal_has_us_number(deal: Dict) -> bool:
    # Телефон з +1 може бути в TITLE або в полях PHONE/communications.
    values_to_check: List[str] = _collect_phone_like_values(deal.get("TITLE"))
    values_to_check.extend(_collect_phone_like_values(deal.get("PHONE")))

    # Деякі інсталяції зберігають контакти у custom полях, тож перевіряємо всі UF_CRM_* поля.
    for field_name, field_value in deal.items():
        if str(field_name).startswith("UF_CRM_"):
            values_to_check.extend(_collect_phone_like_values(field_value))

    return any(bool(re.search(r"\+1(?:[\s\-().]*\d){7,}", text)) for text in values_to_check)


def is_after_distribution_time() -> bool:
    # Обмеження для заявок з номером +1 діє до 17:00 за київським часом.
    timezone_name = str(_secret_optional("distribution.timezone", "Europe/Kyiv") or "Europe/Kyiv")
    try:
        now_local = datetime.now(ZoneInfo(timezone_name))
    except Exception:
        now_local = datetime.now(ZoneInfo("Europe/Kyiv"))
    return now_local.hour >= 17


def parse_term_priority(value: Optional[str]) -> int:
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


def parse_datetime_value(value: Optional[str]) -> datetime:
    if not value:
        return datetime.max
    text_value = str(value).strip()
    if not text_value:
        return datetime.max
    text_value = text_value.replace("Z", "+00:00")
    try:
        return datetime.fromisoformat(text_value)
    except ValueError:
        for fmt in ("%d.%m.%Y %H:%M:%S", "%d.%m.%Y %H:%M", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M"):
            try:
                return datetime.strptime(text_value, fmt)
            except ValueError:
                continue
    return datetime.max


def is_skipped_by_title_prefix(deal: Dict) -> bool:
    return is_prefix_in_title(deal, USA_PREFIX) or is_prefix_in_title(deal, BLOCK_PREFIX)


def is_until_month_deal(deal: Dict) -> bool:
    return is_prefix_in_title(deal, UNTIL_MONTH_PREFIX)


def get_term_group_label(value: Optional[str]) -> Optional[str]:
    term_priority = parse_term_priority(value)
    if term_priority == TERM_PRIORITY_ORDER["46945"]:
        return TERM_NEAR_LABEL
    if term_priority in {
        TERM_PRIORITY_ORDER["46947"],
        TERM_PRIORITY_ORDER["46949"],
        TERM_PRIORITY_ORDER["47027"],
    }:
        return TERM_FUTURE_LABEL
    return None


def update_deal_assignment_and_stage(deal_id: int, manager_id: int, next_stage_id: str) -> None:
    payload = {
        "id": int(deal_id),
        "fields": {
            "ASSIGNED_BY_ID": int(manager_id),
            "STAGE_ID": str(next_stage_id),
        },
    }
    bitrix_request("crm.deal.update", payload)


def get_direction_config() -> Dict[str, Dict]:
    directions = _secret_required("directions")
    return {item["name"]: item for item in directions}


def get_managers_config() -> Dict[str, int]:
    managers = _secret_required("managers")
    return {str(item["name"]): int(item["id"]) for item in managers}


def init_db() -> None:
    conn = sqlite3.connect(DB_PATH)
    try:
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
        existing_columns = {
            str(row[1])
            for row in conn.execute("PRAGMA table_info(distribution_history)").fetchall()
        }
        if "term_group" not in existing_columns:
            conn.execute("ALTER TABLE distribution_history ADD COLUMN term_group TEXT")
        conn.commit()
    finally:
        conn.close()


def render_onboarding_video(media_path: Path) -> None:
    encoded_video = base64.b64encode(media_path.read_bytes()).decode("utf-8")
    st.markdown(
        f"""
        <div class="onboarding-video-wrap">
            <video autoplay loop muted playsinline controls preload="metadata">
                <source src="data:video/webm;base64,{encoded_video}" type="video/webm">
                Ваш браузер не підтримує відтворення відео.
            </video>
        </div>
        """,
        unsafe_allow_html=True,
    )


def store_distribution_rows(direction_name: str, rows: List[Dict]) -> None:
    if not rows:
        return

    distribution_date = date.today().isoformat()
    conn = sqlite3.connect(DB_PATH)
    try:
        conn.executemany(
            """
            INSERT INTO distribution_history (
                distribution_date,
                direction_name,
                manager_name,
                deal_type,
                deal_id,
                term_group
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    distribution_date,
                    direction_name,
                    row["manager"],
                    row["deal_type"],
                    int(row["deal_id"]),
                    row.get("term_group"),
                )
                for row in rows
            ],
        )
        conn.commit()
    finally:
        conn.close()


def get_daily_summary(direction_name: str) -> Dict[str, Dict[str, int]]:
    distribution_date = date.today().isoformat()
    conn = sqlite3.connect(DB_PATH)
    try:
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
    finally:
        conn.close()


def build_stop_report_message(
    direction_name: str,
    selected_managers: List[str],
    deal_types: List[str],
) -> str:
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
    webhook_url = str(_secret_optional("chatbot.webhook_url", "") or "").strip()
    telegram_token = str(_secret_optional("chatbot.telegram_bot_token", "") or "").strip()
    telegram_chat_id = str(_secret_optional("chatbot.telegram_chat_id", "") or "").strip()

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
        # Бот не має блокувати основну бізнес-логіку розподілу.
        pass


def get_daily_manager_state(
    direction_name: str,
    selected_managers: List[str],
    deal_types: List[str],
) -> Dict[str, Dict[str, Optional[str] | int]]:
    distribution_date = date.today().isoformat()
    conn = sqlite3.connect(DB_PATH)
    try:
        state: Dict[str, Dict[str, Optional[str] | int]] = {
            manager_name: {deal_type: 0 for deal_type in deal_types} for manager_name in selected_managers
        }

        for manager_name in selected_managers:
            state[manager_name].update({"total": 0, "last_type": None})

        cursor = conn.execute(
            """
            SELECT manager_name,
                   deal_type,
                   COUNT(*) AS cnt,
                   MAX(id) AS last_row_id
            FROM distribution_history
            WHERE distribution_date = ? AND direction_name = ?
            GROUP BY manager_name, deal_type
            """,
            (distribution_date, direction_name),
        )
        rows = cursor.fetchall()

        last_row_by_manager: Dict[str, int] = {}
        for manager_name, deal_type, count, last_row_id in rows:
            manager_name = str(manager_name)
            deal_type = str(deal_type)
            if manager_name not in state:
                continue

            if deal_type not in state[manager_name]:
                state[manager_name][deal_type] = 0
            state[manager_name][deal_type] = int(count)
            state[manager_name]["total"] = int(state[manager_name]["total"]) + int(count)

            if last_row_id is not None:
                prev_last = last_row_by_manager.get(manager_name)
                if prev_last is None or int(last_row_id) > prev_last:
                    last_row_by_manager[manager_name] = int(last_row_id)

        for manager_name, last_row_id in last_row_by_manager.items():
            deal_type_cursor = conn.execute(
                "SELECT deal_type FROM distribution_history WHERE id = ?",
                (int(last_row_id),),
            )
            deal_type_row = deal_type_cursor.fetchone()
            if deal_type_row:
                state[manager_name]["last_type"] = str(deal_type_row[0])

        return state
    finally:
        conn.close()


def get_last_manager_for_type_today(direction_name: str, deal_type: str) -> Optional[str]:
    distribution_date = date.today().isoformat()
    conn = sqlite3.connect(DB_PATH)
    try:
        row = conn.execute(
            """
            SELECT manager_name
            FROM distribution_history
            WHERE distribution_date = ? AND direction_name = ? AND deal_type = ?
            ORDER BY id DESC
            LIMIT 1
            """,
            (distribution_date, direction_name, deal_type),
        ).fetchone()
        if not row:
            return None
        return str(row[0])
    finally:
        conn.close()


def select_manager_round_robin(
    selected_managers: List[str],
    remaining_slots: Dict[str, int],
    last_manager: Optional[str],
) -> str:
    under_limit = [manager for manager in selected_managers if int(remaining_slots[manager]) > 0]
    if not under_limit:
        raise RuntimeError("Немає доступних менеджерів для добору до ліміту.")

    if not last_manager or last_manager not in selected_managers:
        return under_limit[0]

    start_idx = (selected_managers.index(last_manager) + 1) % len(selected_managers)
    for offset in range(len(selected_managers)):
        manager = selected_managers[(start_idx + offset) % len(selected_managers)]
        if manager in under_limit:
            return manager

    raise RuntimeError("Немає доступних менеджерів для round-robin.")


def select_manager_for_deal(
    deal_type: str,
    selected_managers: List[str],
    manager_state: Dict[str, Dict[str, Optional[str] | int]],
    logic: str,
    remaining_slots: Dict[str, int],
    batch_size: int,
) -> str:
    under_limit = [manager for manager in selected_managers if int(remaining_slots[manager]) > 0]
    if not under_limit:
        raise RuntimeError("Немає доступних менеджерів для добору до ліміту.")

    maximum_remaining = max(int(remaining_slots[manager]) for manager in under_limit)
    candidates = [manager for manager in under_limit if int(remaining_slots[manager]) == maximum_remaining]

    preferred_candidates = [
        manager for manager in candidates if manager_state[manager].get("last_type") != deal_type
    ]
    tie_pool = preferred_candidates or candidates

    minimum_type_count = min(int(manager_state[manager][deal_type]) for manager in tie_pool)
    final_candidates = [manager for manager in tie_pool if int(manager_state[manager][deal_type]) == minimum_type_count]
    return final_candidates[0]


def clear_daily_distribution(direction_name: str) -> int:
    distribution_date = date.today().isoformat()
    conn = sqlite3.connect(DB_PATH)
    try:
        cursor = conn.execute(
            """
            DELETE FROM distribution_history
            WHERE distribution_date = ? AND direction_name = ?
            """,
            (distribution_date, direction_name),
        )
        conn.commit()
        return int(cursor.rowcount or 0)
    finally:
        conn.close()


def build_summary_table(direction_name: str, selected_managers: List[str], deal_types: List[str]) -> List[Dict]:
    summary = get_daily_summary(direction_name)
    distribution_date = date.today().isoformat()
    conn = sqlite3.connect(DB_PATH)
    term_group_summary: Dict[str, Dict[str, int]] = defaultdict(lambda: {TERM_NEAR_LABEL: 0, TERM_FUTURE_LABEL: 0})
    try:
        cursor = conn.execute(
            """
            SELECT manager_name, term_group, COUNT(*)
            FROM distribution_history
            WHERE distribution_date = ? AND direction_name = ? AND term_group IS NOT NULL
            GROUP BY manager_name, term_group
            """,
            (distribution_date, direction_name),
        )
        for manager_name, term_group, count in cursor.fetchall():
            manager_key = str(manager_name)
            term_group_key = str(term_group)
            if term_group_key in {TERM_NEAR_LABEL, TERM_FUTURE_LABEL}:
                term_group_summary[manager_key][term_group_key] = int(count)
    finally:
        conn.close()

    table: List[Dict] = []

    managers_to_show = selected_managers or sorted(summary.keys())
    for manager in managers_to_show:
        row = {"Менеджер": manager}
        for deal_type in deal_types:
            row[deal_type] = summary.get(manager, {}).get(deal_type, 0)
        row["Термін: ближчий час"] = term_group_summary.get(manager, {}).get(TERM_NEAR_LABEL, 0)
        row["Термін: майбутнє"] = term_group_summary.get(manager, {}).get(TERM_FUTURE_LABEL, 0)
        table.append(row)

    return table


def run_distribution_once(
    *,
    category_id: int,
    direction_name: str,
    target_stage_id: str,
    in_progress_stage_id: str,
    distribution_logic: str,
    deal_types: List[str],
    batch_size: int,
    selected_managers: List[str],
    manager_options: Dict[str, int],
    deals_all: List[Dict],
    source_map: Dict[str, str],
    repeat_stage_id: str,
) -> Dict:
    if not selected_managers:
        return {"status": "warning", "message": "Оберіть хоча б одного менеджера."}

    if not deals_all:
        return {"status": "info", "message": "Немає заявок для розподілу у вибраному статусі."}

    manager_ids = {name: manager_options[name] for name in selected_managers}
    in_progress_counts = {
        manager_name: fetch_deal_count_for_manager(category_id, in_progress_stage_id, manager_ids[manager_name])
        for manager_name in selected_managers
    }

    remaining_slots = {
        manager_name: max(0, batch_size - in_progress_counts[manager_name])
        for manager_name in selected_managers
    }
    available_managers = [manager_name for manager_name in selected_managers if remaining_slots[manager_name] > 0]

    if not available_managers:
        return {
            "status": "warning",
            "message": (
                f"Немає вільних слотів: у всіх менеджерів вже є по {batch_size} "
                "активних угод у статусі 'Угода в роботі'."
            ),
            "in_progress_counts": in_progress_counts,
            "results": [],
        }

    max_for_batch = sum(remaining_slots[manager_name] for manager_name in available_managers)

    term_deals: List[Dict] = []
    consultation_deals: List[Dict] = []
    for deal in deals_all:
        if is_skipped_by_title_prefix(deal):
            continue
        if deal_has_us_number(deal) and not is_after_distribution_time():
            continue
        deal_type = classify_deal_type(deal, source_map, distribution_logic)
        if deal_type == "Консультація":
            consultation_deals.append(deal)
        else:
            term_deals.append(deal)
    term_deals.sort(
        key=lambda item: (
            0 if is_until_month_deal(item) else 1,
            parse_term_priority(item.get(TERM_FIELD_CODE)),
            int(item["ID"]),
        )
    )

    repeat_deals: List[Dict] = []
    if repeat_stage_id:
        repeat_all = fetch_deals(category_id, repeat_stage_id, limit=None)
        repeat_deals = [
            deal
            for deal in repeat_all
            if is_prefix_in_title(deal, REPEAT_PREFIX) or is_prefix_in_title(deal, BASE_PREFIX)
        ]
        repeat_deals.sort(key=lambda item: (parse_datetime_value(item.get("DATE_MODIFY")), int(item["ID"])))

    manager_state = get_daily_manager_state(direction_name, available_managers, deal_types)
    results = []
    last_manager_by_type = {
        "Консультація": get_last_manager_for_type_today(direction_name, "Консультація"),
        "Термін": get_last_manager_for_type_today(direction_name, "Термін"),
    }

    def register_result(deal: Dict, manager_name: str, deal_type: str, term_group: Optional[str] = None):
        manager_id = manager_ids[manager_name]
        if deal_type not in manager_state[manager_name]:
            manager_state[manager_name][deal_type] = 0
        manager_state[manager_name][deal_type] = int(manager_state[manager_name][deal_type]) + 1
        manager_state[manager_name]["total"] = int(manager_state[manager_name]["total"]) + 1
        manager_state[manager_name]["last_type"] = deal_type
        remaining_slots[manager_name] = int(remaining_slots[manager_name]) - 1
        update_deal_assignment_and_stage(int(deal["ID"]), manager_id, target_stage_id)
        results.append(
            {
                "deal_id": int(deal["ID"]),
                "deal_title": deal.get("TITLE", ""),
                "deal_type": deal_type,
                "manager": manager_name,
                "next_stage": target_stage_id,
                "term_group": term_group,
            }
        )

    for deal in repeat_deals:
        if len(results) >= max_for_batch:
            break
        responsible_id = int(deal.get("ASSIGNED_BY_ID") or 0)
        responsible_name = next((name for name in available_managers if manager_ids[name] == responsible_id), None)
        is_repeat = is_prefix_in_title(deal, REPEAT_PREFIX)
        is_base = is_prefix_in_title(deal, BASE_PREFIX)

        if responsible_name and int(remaining_slots[responsible_name]) > 0:
            register_result(deal, responsible_name, "Повторне" if is_repeat else "База")
            continue

        if is_base:
            fallback_manager = select_manager_for_deal(
                "База",
                available_managers,
                manager_state,
                distribution_logic,
                remaining_slots,
                batch_size,
            )
            register_result(deal, fallback_manager, "База")

    for deal in consultation_deals:
        if len(results) >= max_for_batch:
            break
        manager_name = select_manager_round_robin(
            available_managers,
            remaining_slots,
            last_manager_by_type["Консультація"],
        )
        register_result(deal, manager_name, "Консультація")
        last_manager_by_type["Консультація"] = manager_name

    for deal in term_deals:
        if len(results) >= max_for_batch:
            break
        manager_name = select_manager_round_robin(
            available_managers,
            remaining_slots,
            last_manager_by_type["Термін"],
        )
        register_result(deal, manager_name, "Термін", get_term_group_label(deal.get(TERM_FIELD_CODE)))
        last_manager_by_type["Термін"] = manager_name

    store_distribution_rows(direction_name, results)

    return {
        "status": "success",
        "message": (
            f"Успішно розподілено {len(results)} заявок. "
            f"Менеджерів з доступними слотами: {len(available_managers)}. Ціль в роботі: {batch_size}."
        ),
        "in_progress_counts": in_progress_counts,
        "results": results,
    }


@st.fragment
def render_onboarding_modal() -> None:
    if "onboarding_step" not in st.session_state:
        st.session_state["onboarding_step"] = 0

    steps = [
        {
            "title": "Вітаємо в авто-розподілі заявок",
            "description": "Цей інструмент створений для спрощення роботи менеджерів із дзвінків. Він повністю бере на себе логіку розподілу заявок між менеджерами в напрямку «Сайт».",
            "media_file": "gif-1.webm"
        },
        {
            "title": "Оберіть менеджерів",
            "description": "Для початку розподілу заявок необхідно обрати менеджерів, які будуть працювати на зміні.",
            "media_file": "gif-2.webm"
        },
        {
            "title": "Запуск та контроль розподілу",
            "description": "Наступним кроком потрібно натиснути кнопку «Почати авто-розподіл». Після цього програма почне розподіляти заявки між обраними менеджерами та буде продовжувати це автоматично до зупинки.",
            "media_file": "gif-3.webm"
        },
        {
            "title": "Пауза",
            "description": "Якщо необхідно поставити паузу, натисніть на відповідну кнопку та вкажіть причину. Наприклад: «Обід». *Пауза — це довготривала зупинка автоматичного розподілу.",
            "media_file": "gif-4.webm"
        },
        {
            "title": "Пауза для зміни менеджера",
            "description": "Якщо потрібно змінити менеджера в процесі розподілу, натисніть на відповідну кнопку «Пауза для зміни менеджера» та вкажіть причину. Наприклад: «Калібрування». *Пауза для зміни менеджера — це короткотривала зупинка автоматичного розподілу, лише для того, щоб додати або прибрати менеджера з авто-розподілу.",
            "media_file": "gif-5.webm"
        },
        {
            "title": "Зупинити авто-розподіл",
            "description": "Якщо потрібно завершити робочий день або зміну, натисніть кнопку «Зупинити авто-розподіл». У цьому випадку також потрібно буде вказати причину, і авто-розподіл буде повністю зупинений.",
            "media_file": "gif-6.webm"
        },
        {
            "title": "Фіксація",
            "description": "Для кращого розуміння того, який менеджер отримав які заявки, нижче наведена таблиця, що фіксує кількість заявок по кожному менеджеру та джерелу.",
            "media_file": "gif-7.webm"
        }
    ]

    step = int(st.session_state.get("onboarding_step", 0))
    step = max(0, min(step, len(steps) - 1))

    st.markdown(
        """
        <style>
            .onboarding-backdrop {
                position: fixed;
                inset: 0;
                background: rgba(8, 16, 34, 0.72);
                z-index: 9998;
            }
            .st-key-onboarding_panel {
                position: fixed;
                top: 50%;
                left: 50%;
                transform: translate(-50%, -50%);
                width: min(980px, 96vw);
                z-index: 10000;
                background: #ffffff;
                border-radius: 20px;
                padding: 28px;
                border: 1px solid #d8e3ff;
                box-shadow: 0 24px 54px rgba(0, 0, 0, 0.35);
            }
            .st-key-onboarding_panel .onboarding-text {
                min-height: 72px;
                font-size: 1.04rem;
            }
            .st-key-onboarding_panel .onboarding-gif-slot {
                border: 2px dashed #bdd0ff;
                border-radius: 14px;
                height: min(430px, 52vh);
                background: linear-gradient(180deg, #f7faff 0%, #ecf3ff 100%);
                display: flex;
                align-items: center;
                justify-content: center;
                text-align: center;
                padding: 20px;
                color: #3b4b6b;
                font-size: 1.02rem;
            }
            .st-key-onboarding_panel .onboarding-video-wrap {
                display: flex;
                justify-content: center;
                margin: 0 auto 0.5rem auto;
                width: min(760px, 100%);
            }
            .st-key-onboarding_panel .onboarding-video-wrap video {
                width: 100%;
                max-height: min(360px, 45vh);
                border-radius: 14px;
                border: 1px solid #d4e2ff;
                background: #000;
                object-fit: contain;
            }
            .st-key-onboarding_panel .stButton > button {
                width: 100%;
            }
        </style>
        <div class="onboarding-backdrop"></div>
        """,
        unsafe_allow_html=True,
    )

    with st.container(key="onboarding_panel"):
        st.markdown(f"### {steps[step]['title']}")
        st.markdown(
            f"<div class='onboarding-text'>{steps[step]['description']}</div>",
            unsafe_allow_html=True,
        )
        media_filename = str(steps[step]["media_file"])
        media_path = ONBOARDING_MEDIA_DIR / media_filename
        if media_path.exists():
            render_onboarding_video(media_path)
        else:
            st.markdown(
                f"""
                <div class="onboarding-gif-slot">
                    <div>
                        <strong>Файл не знайдено</strong><br/>
                        Додайте медіа: <code>{media_path.as_posix()}</code>
                    </div>
                </div>
                """,
                unsafe_allow_html=True,
            )
        st.caption(f"Крок {step + 1} з {len(steps)}")

        col_prev, col_next, col_close = st.columns(3)
        with col_prev:
            if st.button("Назад", disabled=step == 0, key="onboarding_prev"):
                st.session_state["onboarding_step"] = step - 1
        with col_next:
            next_label = "Завершити" if step == len(steps) - 1 else "Далі"
            if st.button(next_label, type="primary", key="onboarding_next"):
                if step >= len(steps) - 1:
                    st.session_state["onboarding_step"] = 0
                    st.session_state["show_onboarding"] = False
                else:
                    st.session_state["onboarding_step"] = step + 1
        with col_close:
            if st.button("Закрити", key="onboarding_close"):
                st.session_state["onboarding_step"] = 0
                st.session_state["show_onboarding"] = False
                st.rerun()


def login_screen() -> None:
    st.title("Вхід в систему розподілу заявок")
    with st.form("login"):
        username = st.text_input("Логін")
        password = st.text_input("Пароль", type="password")
        submitted = st.form_submit_button("Увійти")

    if submitted:
        user = get_auth_user(username.strip(), password)
        if user:
            st.session_state["authenticated"] = True
            st.session_state["user"] = user
            st.rerun()
        else:
            st.error("Невірний логін або пароль.")


def distribution_screen() -> None:
    user = st.session_state.get("user", {})

    st.title("Розподіл заявок між менеджерами")
    st.caption(
        f"Користувач: {user.get('name', '-')} | ID менеджера акаунта: {user.get('manager_id', '-')}")

    top_actions_col1, top_actions_col2, top_actions_col3 = st.columns([1, 1, 6])
    with top_actions_col1:
        if st.button("Вийти"):
            st.session_state.clear()
            st.rerun()
    with top_actions_col2:
        if st.button("Як це працює", help="Відкрити онбординг повторно"):
            st.session_state["onboarding_step"] = 0
            st.session_state["show_onboarding"] = True
            st.rerun()

    direction_options = get_direction_config()
    manager_options = get_managers_config()

    if "auto_distribution_state" not in st.session_state:
        st.session_state["auto_distribution_state"] = "stopped"
    if "auto_distribution_last_run" not in st.session_state:
        st.session_state["auto_distribution_last_run"] = None
    if "last_in_progress_counts" not in st.session_state:
        st.session_state["last_in_progress_counts"] = {}
    if "pending_control_action" not in st.session_state:
        st.session_state["pending_control_action"] = None
    if "manager_selection" not in st.session_state:
        st.session_state["manager_selection"] = []
    if "active_managers" not in st.session_state:
        st.session_state["active_managers"] = []
    if "reconfig_previous_managers" not in st.session_state:
        st.session_state["reconfig_previous_managers"] = []
    if "show_onboarding" not in st.session_state:
        st.session_state["show_onboarding"] = False

    if st.session_state.get("show_onboarding"):
        render_onboarding_modal()
        st.info("Онбординг активний. Основний екран не оновлюється, доки не закриєте онбординг.")
        return

    with st.container(key="onboarding_managers_block"):
        col1, col2 = st.columns(2)
        with col1:
            direction_name = st.selectbox("Напрямок", list(direction_options.keys()))
        with col2:
            st.multiselect(
                "Менеджери для розподілу",
                options=list(manager_options.keys()),
                key="manager_selection",
                help="ID менеджерів не показуються в інтерфейсі.",
            )
    selected_managers = list(st.session_state.get("manager_selection", []))

    direction = direction_options[direction_name]
    category_id = int(direction["funnel_id"])
    stage_id = str(direction["status_id"])
    next_stage_id = str(direction.get("next_status_id") or "").strip()
    in_progress_stage_id = str(direction.get("in_progress_status_id") or next_stage_id).strip()
    target_stage_id = in_progress_stage_id or next_stage_id
    distribution_logic = get_direction_logic(direction_name, direction)
    deal_types = get_deal_types_for_logic(distribution_logic)
    batch_size = int(direction.get("batch_size") or DEFAULT_BATCH_SIZE)
    auto_interval_seconds = int(direction.get("auto_interval_seconds") or 30)
    repeat_stage_id = str(direction.get("repeat_status_id") or "").strip()

    if not target_stage_id:
        st.warning(
            "Для цього напрямку не задано `in_progress_status_id` (або запасний `next_status_id`) у secrets.toml. "
            "Розподіл заблоковано."
        )

    with st.spinner("Отримуємо заявки та джерела..."):
        deals_all = fetch_deals(category_id, stage_id, limit=None)
        source_map = fetch_source_map()

    if st.button("Оновити статус"):
        st.rerun()

    available_count = len(deals_all)
    st.info(f"Знайдено заявок у статусі: **{available_count}**")

    st.caption("Логіка напрямку: Instagram")

    with st.container(key="onboarding_actions_block"):
        action_col1, action_col2, action_col3, action_col4 = st.columns(4)
        with action_col1:
            if st.button(
                "Почати авто-розподіл",
                type="primary",
                disabled=(
                    not target_stage_id
                    or not selected_managers
                    or st.session_state["auto_distribution_state"] == "running"
                ),
            ):
                st.session_state["active_managers"] = selected_managers.copy()
                st.session_state["auto_distribution_state"] = "running"
                st.session_state["pending_control_action"] = None
                managers_text = ", ".join(st.session_state["active_managers"]) if st.session_state["active_managers"] else "не обрано"
                send_chatbot_message(
                    "\n".join(
                        [
                            "▶️ Розподіл заявок розпочато.",
                            f"Напрямок: {direction_name}",
                            f"Користувач: {user.get('name', '-')}",
                            f"Менеджери: {managers_text}",
                        ]
                    )
                )
                st.rerun()

        with action_col2:
            if st.button(
                "Пауза",
                disabled=st.session_state["auto_distribution_state"] != "running",
            ):
                st.session_state["pending_control_action"] = "pause"

        with action_col3:
            if st.button("Зупинити авто-розподіл", disabled=st.session_state["auto_distribution_state"] == "stopped"):
                st.session_state["pending_control_action"] = "stop"

        with action_col4:
            if st.button(
                "Пауза для зміни менеджерів",
                disabled=st.session_state["auto_distribution_state"] != "running",
                help="Коротка пауза: змініть список менеджерів і продовжіть без повної зупинки.",
            ):
                st.session_state["reconfig_previous_managers"] = list(st.session_state.get("active_managers", []))
                st.session_state["auto_distribution_state"] = "reconfiguring"
                st.session_state["pending_control_action"] = None
                st.rerun()

    pending_action = st.session_state.get("pending_control_action")
    if pending_action in {"pause", "stop"}:
        action_label = "паузу" if pending_action == "pause" else "зупинку"
        with st.form(f"{pending_action}_reason_form", clear_on_submit=True):
            reason = st.text_input(f"Вкажіть причину, чому ставите на {action_label}")
            confirm = st.form_submit_button(f"Підтвердити {action_label}")
            cancel = st.form_submit_button("Скасувати")

            if cancel:
                st.session_state["pending_control_action"] = None
                st.rerun()

            if confirm:
                if not reason.strip():
                    st.warning("Причина обов'язкова.")
                else:
                    if pending_action == "pause":
                        st.session_state["auto_distribution_state"] = "paused"
                        send_chatbot_message(
                            "\n".join(
                                [
                                    "⏸️ Розподіл поставлено на паузу.",
                                    f"Напрямок: {direction_name}",
                                    f"Користувач: {user.get('name', '-')}",
                                    f"Причина: {reason.strip()}",
                                ]
                            )
                        )
                    else:
                        st.session_state["auto_distribution_state"] = "stopped"
                        st.session_state["auto_distribution_last_run"] = None
                        stop_report = build_stop_report_message(direction_name, selected_managers, deal_types)
                        send_chatbot_message(
                            "\n\n".join(
                                [
                                    "\n".join(
                                        [
                                            "⏹️ Розподіл зупинено.",
                                            f"Напрямок: {direction_name}",
                                            f"Користувач: {user.get('name', '-')}",
                                            f"Причина: {reason.strip()}",
                                        ]
                                    ),
                                    stop_report,
                                ]
                            )
                        )
                    st.session_state["pending_control_action"] = None
                    st.rerun()

    auto_state = st.session_state["auto_distribution_state"]
    active_managers = list(st.session_state.get("active_managers", []))
    should_autorefresh = False
    if auto_state == "running":
        if not active_managers:
            st.warning("Авто-режим зупинено: оберіть хоча б одного менеджера для розподілу.")
            st.session_state["auto_distribution_state"] = "stopped"
            st.rerun()

        st.success(
            f"Авто-режим увімкнено. Перевірка та розподіл виконуються кожні {auto_interval_seconds} сек."
        )
        with st.spinner("Авто-режим: запускаємо розподіл..."):
            run_result = run_distribution_once(
                category_id=category_id,
                direction_name=direction_name,
                target_stage_id=target_stage_id,
                in_progress_stage_id=in_progress_stage_id,
                distribution_logic=distribution_logic,
                deal_types=deal_types,
                batch_size=batch_size,
                selected_managers=active_managers,
                manager_options=manager_options,
                deals_all=deals_all,
                source_map=source_map,
                repeat_stage_id=repeat_stage_id,
            )
        st.session_state["auto_distribution_last_run"] = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")

        status = run_result["status"]
        if status == "success":
            st.success(run_result["message"])
        elif status == "warning":
            st.warning(run_result["message"])
        else:
            st.info(run_result["message"])

        if run_result.get("in_progress_counts"):
            st.session_state["last_in_progress_counts"] = run_result["in_progress_counts"]

        st.caption(
            f"Останній авто-запуск: {st.session_state.get('auto_distribution_last_run', '-')}. "
            "Сторінка перезапуститься автоматично після оновлення таблиць."
        )
        if active_managers:
            st.caption(f"Активні менеджери в поточному розподілі: {', '.join(active_managers)}")
        should_autorefresh = True
    elif auto_state == "reconfiguring":
        st.warning(
            "Коротка пауза для зміни ділення. Оновіть список менеджерів і натисніть "
            "«Продовжити з новим діленням»."
        )
        previous_managers = list(st.session_state.get("reconfig_previous_managers", []))
        with st.form("manager_change_reason_form", clear_on_submit=True):
            reason = st.text_input("Вкажіть причину зміни менеджерів")
            apply_change = st.form_submit_button(
                "Продовжити з новим діленням",
                type="primary",
                disabled=not selected_managers,
            )
            cancel_change = st.form_submit_button("Скасувати зміну менеджерів")

            if cancel_change:
                st.session_state["reconfig_previous_managers"] = []
                st.session_state["auto_distribution_state"] = "running"
                st.rerun()

            if apply_change:
                if not reason.strip():
                    st.warning("Причина зміни менеджерів обов'язкова.")
                else:
                    st.session_state["active_managers"] = selected_managers.copy()
                    st.session_state["auto_distribution_state"] = "running"
                    st.session_state["reconfig_previous_managers"] = []

                    previous_text = ", ".join(previous_managers) if previous_managers else "не обрано"
                    new_text = ", ".join(selected_managers) if selected_managers else "не обрано"
                    send_chatbot_message(
                        "\n".join(
                            [
                                "🔄 Змінено менеджерів у розподілі.",
                                f"Напрямок: {direction_name}",
                                f"Користувач: {user.get('name', '-')}",
                                f"Було: {previous_text}",
                                f"Стало: {new_text}",
                                f"Причина: {reason.strip()}",
                            ]
                        )
                    )
                    st.rerun()
    elif auto_state == "paused":
        st.warning("Авто-розподіл на паузі. Для продовження натисніть «Почати авто-розподіл».")
    else:
        st.info("Авто-розподіл зупинено.")

    st.subheader("Таблиця розподілу за сьогодні")
    summary_managers = sorted(get_daily_summary(direction_name).keys())
    if auto_state in {"running", "reconfiguring"} and active_managers:
        managers_for_table = list(dict.fromkeys(active_managers + selected_managers + summary_managers))
    else:
        managers_for_table = list(dict.fromkeys(selected_managers + summary_managers))
    st.dataframe(build_summary_table(direction_name, managers_for_table, deal_types), use_container_width=True)

    with st.container(key="onboarding_workload_block"):
        st.subheader("Кількість в роботі у менеджера")
        counts = st.session_state.get("last_in_progress_counts", {})
        if counts and managers_for_table:
            st.dataframe(
                [
                    {"Менеджер": name, "Активних в роботі": counts.get(name, 0)}
                    for name in managers_for_table
                ],
                use_container_width=True,
            )
        elif managers_for_table:
            st.info("Дані з'являться після першого запуску авто-розподілу.")
        else:
            st.info("Оберіть менеджерів, щоб побачити таблицю навантаження.")

    if st.button("Очистити значення", type="secondary"):
        deleted_rows = clear_daily_distribution(direction_name)
        if deleted_rows:
            st.success(f"Очищено записів: {deleted_rows}. Історію розподілу за сьогодні скинуто.")
        else:
            st.info("Немає значень для очищення за сьогодні у цьому напрямку.")
        st.rerun()

    if should_autorefresh:
        time.sleep(auto_interval_seconds)
        st.rerun()

    if st.session_state.get("show_onboarding"):
        render_onboarding_modal()


init_db()
st.link_button("⬅ Назад до панелі менеджера", DASHBOARD_URL)
st.divider()

try:
    if st.session_state.get("authenticated"):
        distribution_screen()
    else:
        login_screen()
except Exception as exc:
    st.error(f"Критична помилка: {exc}")
    st.stop()
