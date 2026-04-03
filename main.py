from fastapi import FastAPI, Request, BackgroundTasks
from fastapi.responses import HTMLResponse
import uvicorn
import requests
import os
import re
import sqlite3
import json
from datetime import datetime
import uuid
from typing import Optional, List, Dict, Any
import asyncio

app = FastAPI()

CHANNEL_ACCESS_TOKEN = os.getenv("CHANNEL_ACCESS_TOKEN", "")

# ===== 群組 ID =====
A_GROUP_ID = os.getenv("A_GROUP_ID", "Cb3579e75c94437ed22aafc7b1f6aecdd")   # A群（進度群）

# ★ 修正圖2：業務群改成動態從 DB 讀取，不再寫死 B/C
#   只需在 DB groups 表設定 group_type='SALES_GROUP' 即可自動生效
#   初始種子仍保留 B/C 方便測試
B_GROUP_ID = "Cd14f3ee775f1d9f5cfdafb223173cbef"
C_GROUP_ID = "C1a647fcb29a74842eceeb18e7a53823d"

DB_PATH = os.getenv("DB_PATH", "/var/data/loan_system.db")

COMPANY_LIST = [
    "和裕商品", "和裕機車", "亞太商品", "亞太機車",
    "手機分期", "貸救補", "第一", "分貝", "麻吉",
    "亞太", "和裕", "21", "喬美", "鄉", "銀", "C", "商", "代"
]

STATUS_WORDS = [
    "婉拒", "核准", "補件", "補資料", "等保書", "退件", "不承作", "照會",
    "保密", "NA", "待撥款", "可送", "缺資料", "無可知情", "聯絡人皆可知情",
    "補行照", "補照會", "補照片", "補時段", "補案件資料", "補聯徵", "補保人"
]

ACTION_KEYWORDS = [
    "結案", "刪掉", "不追了", "全部不送", "已撥款結案",
    "補件", "補資料", "缺資料", "婉拒", "核准", "照會", "退件", "等保書",
    "不承作", "待撥款", "補行照", "補照會", "補照片", "補時段", "補案件資料",
    "補聯徵", "補保人", "保密", "無可知情", "聯絡人皆可知情"
]

DELETE_KEYWORDS = ["結案", "刪掉", "不追了", "全部不送", "已撥款結案"]
BLOCK_KEYWORDS = ["鼎信", "禾基"]

IGNORE_NAME_WORDS = {
    "信用不良", "不需要了", "不用了", "不要了", "結案", "補件", "核准", "婉拒",
    "照會", "等保書", "待撥款", "缺資料", "補資料", "資料補", "補來", "退件",
    "助理", "AI助理", "先生", "小姐", "無可知情", "聯絡人皆可知情"
}

CHINESE_NAME_RE = re.compile(r"[\u4e00-\u9fff]{2,4}")
ID_RE = re.compile(r"[A-Z][12]\d{8}")
DATE_PREFIX_RE = re.compile(r"^\s*(\d{2,4}/\d{1,2}/\d{1,2})\s*[-－]?\s*")
DATE_NAME_ID_INLINE_RE = re.compile(
    r"^\s*(\d{2,4}/\d{1,2}/\d{1,2})\s*[-－]?\s*([\u4e00-\u9fff]{2,4})\s*([A-Z][12]\d{8})",
    re.IGNORECASE,
)
DATE_NAME_ONLY_RE = re.compile(
    r"^\s*(\d{2,4}/\d{1,2}/\d{1,2})\s*[-－]?\s*([\u4e00-\u9fff]{2,4})(?:\s*$|\s*[-－/])",
    re.IGNORECASE,
)
PLAN_ROUTE_RE = re.compile(
    r"^\s*(\d{2,4}/\d{1,2}/\d{1,2})\s*[-－]?\s*([\u4e00-\u9fff]{2,4})\s*[-－/]\s*([^\n]+)$",
    re.IGNORECASE,
)


# =========================
# 基本工具
# =========================
def now_iso() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def short_id() -> str:
    return str(uuid.uuid4())[:8]


def get_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def extract_first_line(text: str) -> str:
    lines = [line.strip() for line in text.splitlines() if line.strip()]
    return lines[0] if lines else ""


def normalize_spaces(text: str) -> str:
    return re.sub(r"\s+", " ", text or "").strip()


def strip_ai_trigger(text: str) -> str:
    return re.sub(r"(@ai助理|#ai助理|@ai|#ai)", "", text or "", flags=re.IGNORECASE).strip()


def has_ai_trigger(text: str) -> bool:
    t = (text or "").lower()
    return ("@ai" in t) or ("#ai" in t)


def contains_bu_word(text: str) -> bool:
    return "補" in (text or "")


# ★ 修正圖4：should_push_to_a_group 拆掉，改由呼叫端自行控制時機
#   不再在「判讀階段」直接推送，避免結案後補資料立即推 A 群
def should_push_to_a_group(text: str) -> bool:
    return has_ai_trigger(text) and contains_bu_word(text)


def normalize_first_line(text: str) -> str:
    return DATE_PREFIX_RE.sub("", text).strip()


def extract_possible_names(text: str) -> List[str]:
    return CHINESE_NAME_RE.findall(text or "")


def extract_id_no(text: str) -> str:
    m = ID_RE.search((text or "").upper())
    return m.group(0) if m else ""


def extract_company(text: str) -> str:
    for c in COMPANY_LIST:
        if c in (text or ""):
            return c
    return ""


def contains_status_word(text: str) -> bool:
    return any(w in (text or "") for w in STATUS_WORDS)


def has_business_action_word(text: str) -> bool:
    return any(w in (text or "") for w in ACTION_KEYWORDS)


def is_blocked(text: str) -> bool:
    return any(w in (text or "") for w in BLOCK_KEYWORDS)


def is_closed_text(text: str) -> bool:
    return any(w in (text or "") for w in DELETE_KEYWORDS)


def parse_header_fields(text: str) -> Dict[str, str]:
    lines = [x.strip() for x in (text or "").splitlines() if x.strip()]
    first = lines[0] if lines else ""
    compact = re.sub(r"\s+", "", text or "")

    result = {"date": "", "name": "", "id_no": ""}

    m = DATE_NAME_ID_INLINE_RE.search(first)
    if m:
        result["date"], result["name"], result["id_no"] = m.group(1), m.group(2), m.group(3).upper()
        return result

    m = DATE_NAME_ID_INLINE_RE.search(compact)
    if m:
        result["date"], result["name"], result["id_no"] = m.group(1), m.group(2), m.group(3).upper()
        return result

    m = DATE_NAME_ONLY_RE.search(first)
    if m:
        result["date"], result["name"] = m.group(1), m.group(2)
        if len(lines) >= 2:
            id_m = ID_RE.search(lines[1].upper())
            if id_m:
                result["id_no"] = id_m.group(0)
        return result

    # ★ 修正圖3：「月/日-姓名」（無年份），第二行為身分證
    SHORT_DATE_NAME_RE2 = re.compile(
        r"^\s*(\d{1,2}/\d{1,2})\s*[-－]\s*([\u4e00-\u9fff]{2,4})\s*$"
    )
    m = SHORT_DATE_NAME_RE2.search(first)
    if m:
        result["date"] = m.group(1)
        result["name"] = m.group(2)
        if len(lines) >= 2:
            id_m = ID_RE.search(lines[1].upper())
            if id_m:
                result["id_no"] = id_m.group(0)
        return result

    result["id_no"] = extract_id_no(text)

    first_line = extract_first_line(text)
    first_line = re.split(r"[:：]|->", first_line, maxsplit=1)[0].strip()
    first_line = re.split(
        r"結案|補件|補資料|婉拒|核准|照會|退件|等保書|缺資料|待撥款|保密|無可知情|聯絡人皆可知情",
        first_line,
        maxsplit=1,
    )[0].strip()

    if "｜" in first_line:
        first_line = first_line.split("｜", 1)[0].strip()

    m_name = CHINESE_NAME_RE.search(first_line)
    if m_name:
        result["name"] = m_name.group(0)

    return result


def extract_name(text: str) -> str:
    name = parse_header_fields(text).get("name", "")
    if name and name not in IGNORE_NAME_WORDS:
        return name
    return ""


def looks_like_new_case_block(block: str) -> bool:
    fields = parse_header_fields(block)
    return bool(fields["date"] and fields["name"] and fields["id_no"])


def extract_route_plan(block: str) -> str:
    first = extract_first_line(block)
    if not first:
        return ""

    compact = normalize_spaces(first)
    m = PLAN_ROUTE_RE.search(compact)
    if not m:
        no_space = re.sub(r"\s+", "", first)
        m = PLAN_ROUTE_RE.search(no_space)
    if not m:
        return ""

    tail = m.group(3).strip()
    if ID_RE.search(tail.upper()):
        return ""
    if "/" not in tail:
        return ""
    return tail.strip("-/ ")


def looks_like_route_plan_block(block: str) -> bool:
    fields = parse_header_fields(block)
    if not fields["date"] or not fields["name"] or fields["id_no"]:
        return False
    return bool(extract_route_plan(block))


def is_format_trigger(block: str) -> bool:
    first = extract_first_line(block)
    if "｜" in first and len([p for p in first.split("｜") if p.strip()]) >= 2:
        return True
    if "->" in first:
        return True
    return False


def get_group_name(group_id: str) -> str:
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT group_name FROM groups WHERE group_id = ?", (group_id,))
    row = cur.fetchone()
    conn.close()
    if row:
        return row["group_name"]
    if group_id == A_GROUP_ID:
        return "A群"
    return "未知群組"


# ★ 修正圖2：動態取得所有業務群 ID
def get_sales_group_ids() -> List[str]:
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT group_id FROM groups WHERE group_type = 'SALES_GROUP' AND is_active = 1")
    rows = cur.fetchall()
    conn.close()
    return [r["group_id"] for r in rows]


# ★ 修正圖2：動態取得所有行政群 ID
def get_admin_group_ids() -> List[str]:
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT group_id FROM groups WHERE group_type = 'ADMIN_GROUP' AND is_active = 1")
    rows = cur.fetchall()
    conn.close()
    return [r["group_id"] for r in rows]


def get_block_display_text(block_text: str) -> str:
    name = extract_name(block_text)
    first_line = extract_first_line(block_text)
    if name and name not in first_line:
        return f"{name}｜{first_line}"
    if name:
        return name
    return first_line


# =========================
# LINE API
# =========================
def push_text(to_group_id: str, text: str):
    if not CHANNEL_ACCESS_TOKEN:
        return False, "未設定 CHANNEL_ACCESS_TOKEN"

    url = "https://api.line.me/v2/bot/message/push"
    headers = {
        "Authorization": f"Bearer {CHANNEL_ACCESS_TOKEN}",
        "Content-Type": "application/json",
    }
    data = {
        "to": to_group_id,
        "messages": [{"type": "text", "text": text[:4900]}],
    }

    try:
        resp = requests.post(url, headers=headers, json=data, timeout=10)
        if 200 <= resp.status_code < 300:
            return True, ""
        return False, f"LINE push失敗({resp.status_code}) {resp.text[:200]}"
    except Exception as e:
        return False, f"LINE push例外: {str(e)}"


def reply_text(reply_token: str, text: str):
    if not CHANNEL_ACCESS_TOKEN or reply_token == "TEST":
        return

    url = "https://api.line.me/v2/bot/message/reply"
    headers = {
        "Authorization": f"Bearer {CHANNEL_ACCESS_TOKEN}",
        "Content-Type": "application/json",
    }
    data = {
        "replyToken": reply_token,
        "messages": [{"type": "text", "text": text[:4900]}],
    }
    try:
        requests.post(url, headers=headers, json=data, timeout=10)
    except Exception:
        pass


def make_quick_reply_item(label: str, text: str):
    return {
        "type": "action",
        "action": {
            "type": "message",
            "label": label[:20],
            "text": text,
        },
    }


def reply_quick_reply(reply_token: str, text: str, items):
    if not CHANNEL_ACCESS_TOKEN or reply_token == "TEST":
        return

    url = "https://api.line.me/v2/bot/message/reply"
    headers = {
        "Authorization": f"Bearer {CHANNEL_ACCESS_TOKEN}",
        "Content-Type": "application/json",
    }
    data = {
        "replyToken": reply_token,
        "messages": [{
            "type": "text",
            "text": text[:4900],
            "quickReply": {"items": items[:13]},
        }],
    }
    try:
        requests.post(url, headers=headers, json=data, timeout=10)
    except Exception:
        pass


# =========================
# 資料庫初始化
# =========================
def ensure_column(cur, table: str, column: str, definition: str):
    cur.execute(f"PRAGMA table_info({table})")
    cols = [row[1] for row in cur.fetchall()]
    if column not in cols:
        cur.execute(f"ALTER TABLE {table} ADD COLUMN {column} {definition}")


def init_db():
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
    CREATE TABLE IF NOT EXISTS groups (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        group_id TEXT UNIQUE NOT NULL,
        group_name TEXT,
        group_type TEXT NOT NULL,
        is_active INTEGER DEFAULT 1,
        created_at TEXT NOT NULL
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS customers (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        case_id TEXT UNIQUE NOT NULL,
        customer_name TEXT NOT NULL,
        id_no TEXT,
        source_group_id TEXT NOT NULL,
        company TEXT,
        route_plan TEXT,
        last_update TEXT,
        status TEXT NOT NULL DEFAULT 'ACTIVE',
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS case_logs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        case_id TEXT NOT NULL,
        customer_name TEXT NOT NULL,
        id_no TEXT,
        company TEXT,
        message_text TEXT NOT NULL,
        from_group_id TEXT NOT NULL,
        created_at TEXT NOT NULL
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS pending_actions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        action_id TEXT UNIQUE NOT NULL,
        action_type TEXT NOT NULL,
        payload TEXT NOT NULL,
        created_at TEXT NOT NULL
    )
    """)

    ensure_column(cur, "customers", "route_plan", "TEXT")

    conn.commit()
    conn.close()


def seed_groups():
    conn = get_conn()
    cur = conn.cursor()
    now = now_iso()

    rows = [
        (A_GROUP_ID, "A群", "A_GROUP", 1, now),
        (B_GROUP_ID, "B群", "SALES_GROUP", 1, now),
        (C_GROUP_ID, "C群", "SALES_GROUP", 1, now),
    ]

    for row in rows:
        cur.execute("""
        INSERT OR IGNORE INTO groups (group_id, group_name, group_type, is_active, created_at)
        VALUES (?, ?, ?, ?, ?)
        """, row)

    conn.commit()
    conn.close()


# =========================
# DB CRUD
# =========================
def create_customer_record(name: str, id_no: str, company: str, source_group_id: str, text: str, route_plan: str = ""):
    conn = get_conn()
    cur = conn.cursor()
    now = now_iso()
    case_id = short_id()

    cur.execute("""
    INSERT INTO customers (
        case_id, customer_name, id_no, source_group_id, company,
        route_plan, last_update, status, created_at, updated_at
    )
    VALUES (?, ?, ?, ?, ?, ?, ?, 'ACTIVE', ?, ?)
    """, (case_id, name, id_no, source_group_id, company, route_plan, text, now, now))

    cur.execute("""
    INSERT INTO case_logs (
        case_id, customer_name, id_no, company, message_text, from_group_id, created_at
    )
    VALUES (?, ?, ?, ?, ?, ?, ?)
    """, (case_id, name, id_no, company, text, source_group_id, now))

    conn.commit()
    conn.close()
    return case_id


def update_customer(
    case_id: str,
    company: Optional[str],
    text: Optional[str],
    from_group_id: str,
    status: Optional[str] = None,
    name: Optional[str] = None,
    source_group_id: Optional[str] = None,
    route_plan: Optional[str] = None,
):
    conn = get_conn()
    cur = conn.cursor()
    now = now_iso()

    fields = []
    values: List[Any] = []

    if company is not None:
        fields.append("company = ?")
        values.append(company)
    if text is not None:
        fields.append("last_update = ?")
        values.append(text)
    if status is not None:
        fields.append("status = ?")
        values.append(status)
    if name is not None:
        fields.append("customer_name = ?")
        values.append(name)
    if source_group_id is not None:
        fields.append("source_group_id = ?")
        values.append(source_group_id)
    if route_plan is not None:
        fields.append("route_plan = ?")
        values.append(route_plan)

    fields.append("updated_at = ?")
    values.append(now)
    values.append(case_id)

    sql = f"UPDATE customers SET {', '.join(fields)} WHERE case_id = ?"
    cur.execute(sql, values)

    cur.execute("SELECT * FROM customers WHERE case_id = ?", (case_id,))
    row = cur.fetchone()

    if row and text is not None:
        cur.execute("""
        INSERT INTO case_logs (
            case_id, customer_name, id_no, company, message_text, from_group_id, created_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (
            row["case_id"], row["customer_name"], row["id_no"], row["company"],
            text, from_group_id, now,
        ))

    conn.commit()
    conn.close()


def find_active_by_id_no(id_no: str):
    if not id_no:
        return None
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
    SELECT * FROM customers
    WHERE id_no = ? AND status = 'ACTIVE'
    ORDER BY updated_at DESC
    LIMIT 1
    """, (id_no,))
    row = cur.fetchone()
    conn.close()
    return row


def find_any_by_id_no(id_no: str):
    if not id_no:
        return []
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
    SELECT * FROM customers
    WHERE id_no = ?
    ORDER BY updated_at DESC
    """, (id_no,))
    rows = cur.fetchall()
    conn.close()
    return rows


def find_active_by_name(name: str):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
    SELECT * FROM customers
    WHERE customer_name = ? AND status = 'ACTIVE'
    ORDER BY updated_at DESC
    """, (name,))
    rows = cur.fetchall()
    conn.close()
    return rows


def find_any_by_name(name: str):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
    SELECT * FROM customers
    WHERE customer_name = ?
    ORDER BY updated_at DESC
    """, (name,))
    rows = cur.fetchall()
    conn.close()
    return rows


def save_pending_action(action_id: str, action_type: str, payload: Dict[str, Any]):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
    INSERT OR REPLACE INTO pending_actions (action_id, action_type, payload, created_at)
    VALUES (?, ?, ?, ?)
    """, (action_id, action_type, json.dumps(payload, ensure_ascii=False), now_iso()))
    conn.commit()
    conn.close()


def get_pending_action(action_id: str):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT * FROM pending_actions WHERE action_id = ?", (action_id,))
    row = cur.fetchone()
    conn.close()
    if not row:
        return None
    data = dict(row)
    try:
        data["payload"] = json.loads(data["payload"])
    except Exception:
        data["payload"] = {}
    return data


def delete_pending_action(action_id: str):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("DELETE FROM pending_actions WHERE action_id = ?", (action_id,))
    conn.commit()
    conn.close()


# =========================
# Quick Reply 按鈕建立
# =========================
def send_reopen_case_buttons(reply_token: str, block_text: str, closed_rows, source_group_id: str,
                              push_to_a_after_reopen: bool = False):
    """
    ★ 修正圖4：新增 push_to_a_after_reopen 參數
       True  = 重啟後才推 A 群（補資料情境）
       False = 重啟後不推 A 群
    """
    action_id = short_id()
    closed_rows = sorted(closed_rows, key=lambda x: x["updated_at"], reverse=True)[:1]
    payload = {
        "block_text": block_text,
        "case_ids": [r["case_id"] for r in closed_rows],
        "source_group_id": source_group_id,
        "push_to_a_after_reopen": push_to_a_after_reopen,  # ★ 新增
    }
    save_pending_action(action_id, "reopen_or_new_case", payload)

    items = []
    for c in closed_rows:
        label = f'重啟-{c["customer_name"]}-{c["company"] or "未填"}'
        text = f"REOPEN_CASE|{action_id}|{c['case_id']}"
        items.append(make_quick_reply_item(label, text))
    items.append(make_quick_reply_item("建立新案件", f"CREATE_NEW_CASE|{action_id}"))
    items.append(make_quick_reply_item("取消", f"CANCEL_REOPEN|{action_id}"))

    reply_quick_reply(reply_token, "⚠️ 此客戶案件已結案，請選擇要重啟原案件或建立新案件", items)


def send_ambiguous_case_buttons(reply_token: str, block_text: str, matches):
    action_id = short_id()
    payload = {
        "block_text": block_text,
        "case_ids": [m["case_id"] for m in matches],
    }
    save_pending_action(action_id, "route_a_case", payload)

    items = []
    for c in matches[:10]:
        label = f"{c['customer_name']}-{c['company'] or '未填'}-{get_group_name(c['source_group_id'])}"
        text = f"SELECT_CASE|{action_id}|{c['case_id']}"
        items.append(make_quick_reply_item(label, text))

    reply_quick_reply(reply_token, "⚠️ 多筆同名客戶，請選擇要回貼的案件", items)


def send_transfer_case_buttons(reply_token: str, customer, source_group_id: str, block_text: str, allow_new: bool = True):
    action_id = short_id()
    payload = {
        "case_id": customer["case_id"],
        "target_group_id": source_group_id,
        "block_text": block_text,
        "name": extract_name(block_text) or customer["customer_name"],
    }
    save_pending_action(action_id, "transfer_customer", payload)

    old_group = get_group_name(customer["source_group_id"])
    new_group = get_group_name(source_group_id)
    items = [
        make_quick_reply_item(f"沿用{old_group}", f"KEEP_OLD_CASE|{action_id}"),
        make_quick_reply_item(f"改到{new_group}", f"CONFIRM_TRANSFER|{action_id}"),
    ]
    if allow_new:
        items.append(make_quick_reply_item("建立新案件", f"CREATE_NEW_FROM_TRANSFER|{action_id}"))
    items.append(make_quick_reply_item("取消", f"CANCEL_TRANSFER|{action_id}"))

    reply_quick_reply(
        reply_token,
        f"⚠️ 別的群組已有同名客戶，請選擇要沿用原案件、改到{new_group}，或建立新案件",
        items,
    )


# ★ 修正圖1：新增「確認建立新客戶」按鈕（同身分證但跨群時詢問）
def send_confirm_new_case_buttons(reply_token: str, block_text: str,
                                   existing_customer, source_group_id: str):
    """
    當業務群輸入新客戶，但其他群已有相同身分證時，
    詢問是否確定要在本群建立新案件。
    """
    action_id = short_id()
    payload = {
        "block_text": block_text,
        "source_group_id": source_group_id,
        "existing_case_id": existing_customer["case_id"],
    }
    save_pending_action(action_id, "confirm_new_case_with_existing_id", payload)

    old_group = get_group_name(existing_customer["source_group_id"])
    new_group = get_group_name(source_group_id)
    name = extract_name(block_text) or existing_customer["customer_name"]

    items = [
        make_quick_reply_item(f"沿用{old_group}案件", f"USE_EXISTING_CASE|{action_id}"),
        make_quick_reply_item(f"在{new_group}建立新案件", f"FORCE_CREATE_NEW|{action_id}"),
        make_quick_reply_item("取消", f"CANCEL_NEW_CASE|{action_id}"),
    ]
    reply_quick_reply(
        reply_token,
        f"⚠️ {name} 的身分證已存在於{old_group}，確定要在{new_group}建立新案件嗎？",
        items,
    )


# =========================
# 主要業務邏輯
# =========================
def build_update_reply(name: str, pushed_to_a: bool, base_msg: str = "已更新客戶") -> str:
    lines = [f"{base_msg}：{name}"]
    if pushed_to_a:
        lines.append(f"✅ 已回貼A群：{name}")
    return "\n".join(lines)


def handle_new_case_block(block_text: str, source_group_id: str, reply_token: str):
    """
    ★ 修正圖1：同身分證在其他群時改為跳確認按鈕，不再直接回警告文字
    """
    fields = parse_header_fields(block_text)
    name = fields["name"]
    id_no = fields["id_no"]
    company = extract_company(block_text)

    if not name or not id_no:
        return None

    existing = find_active_by_id_no(id_no)
    if existing:
        if existing["source_group_id"] == source_group_id:
            # 同群同身分證 → 更新
            update_customer(
                existing["case_id"],
                company or existing["company"] or "",
                block_text,
                source_group_id,
                name=name,
            )
            return f"🔄 已更新客戶：{name}"
        else:
            # ★ 修正圖1：跨群同身分證 → 跳確認按鈕
            send_confirm_new_case_buttons(reply_token, block_text, existing, source_group_id)
            return "QUICK_REPLY_SENT"

    create_customer_record(name, id_no, company, source_group_id, block_text)
    return f"🆕 已建立客戶：{name}"


def handle_route_plan_block(block_text: str, source_group_id: str, reply_token: str):
    fields = parse_header_fields(block_text)
    name = fields["name"]
    route_plan = extract_route_plan(block_text)
    if not name or not route_plan:
        return None

    active_rows = find_active_by_name(name)
    same_group = [r for r in active_rows if r["source_group_id"] == source_group_id]
    other_group = [r for r in active_rows if r["source_group_id"] != source_group_id]

    if same_group:
        row = same_group[0]
        update_customer(
            row["case_id"],
            row["company"],
            block_text,
            source_group_id,
            route_plan=route_plan,
        )
        return f"📝 已更新排序：{name}"

    if other_group:
        send_transfer_case_buttons(reply_token, other_group[0], source_group_id, block_text, allow_new=True)
        return "QUICK_REPLY_SENT"

    create_customer_record(name, "", "", source_group_id, block_text, route_plan=route_plan)
    return f"🆕 已建立客戶：{name}"


def handle_bc_case_block(block_text: str, source_group_id: str, reply_token: str, source_text: str = ""):
    if is_blocked(block_text):
        return "❌ 含禁止關鍵字，已略過"

    if looks_like_new_case_block(block_text):
        # ★ 修正圖1：傳入 reply_token
        return handle_new_case_block(block_text, source_group_id, reply_token)

    if looks_like_route_plan_block(block_text):
        return handle_route_plan_block(block_text, source_group_id, reply_token)

    name = extract_name(block_text)
    id_no = extract_id_no(block_text)
    company = extract_company(block_text)

    if not name or name in IGNORE_NAME_WORDS:
        return None

    # ★ 修正圖4：push_to_a 只在「案件未結案且有效更新」後執行
    #   不在結案後補資料觸發時直接推送
    push_source_text = source_text or block_text
    want_push_a = should_push_to_a_group(push_source_text)
    has_action_word = has_business_action_word(block_text)

    if id_no:
        existing = find_active_by_id_no(id_no)
        if existing:
            if existing["source_group_id"] == source_group_id:
                new_status = "CLOSED" if is_closed_text(block_text) else existing["status"]
                update_customer(
                    existing["case_id"],
                    company or existing["company"] or "",
                    block_text,
                    source_group_id,
                    status=new_status,
                    name=name,
                )
                # ★ 修正圖4：結案後不推 A 群
                pushed_to_a = False
                if want_push_a and new_status != "CLOSED":
                    ok, _ = push_text(A_GROUP_ID, block_text)
                    pushed_to_a = ok
                return build_update_reply(name, pushed_to_a)

            if is_closed_text(block_text):
                return f"⚠️ 同身分證案件存在於{get_group_name(existing['source_group_id'])}：{name}"

            send_transfer_case_buttons(reply_token, existing, source_group_id, block_text, allow_new=True)
            return "QUICK_REPLY_SENT"

        create_customer_record(name, id_no, company, source_group_id, block_text)
        pushed_to_a = False
        if want_push_a:
            ok, _ = push_text(A_GROUP_ID, block_text)
            pushed_to_a = ok
        if pushed_to_a:
            return f"🆕 已建立客戶：{name}\n✅ 已回貼A群：{name}"
        return f"🆕 已建立客戶：{name}"

    any_rows = find_any_by_name(name)
    same_group_rows = [r for r in any_rows if r["source_group_id"] == source_group_id]
    same_group_active = [r for r in same_group_rows if r["status"] == "ACTIVE"]
    same_group_closed = [r for r in same_group_rows if r["status"] != "ACTIVE"]
    other_group_active = [r for r in any_rows if r["source_group_id"] != source_group_id and r["status"] == "ACTIVE"]
    other_group_closed = [r for r in any_rows if r["source_group_id"] != source_group_id and r["status"] != "ACTIVE"]

    # ★ 修正圖4：結案後補資料 → 跳重啟按鈕，且帶 push_to_a_after_reopen=True
    if has_action_word and same_group_closed and not same_group_active and not is_closed_text(block_text):
        send_reopen_case_buttons(
            reply_token, block_text, same_group_closed, source_group_id,
            push_to_a_after_reopen=want_push_a  # ★ 等重啟後才推
        )
        return "QUICK_REPLY_SENT"

    if has_action_word and same_group_active:
        customer = same_group_active[0]
        new_status = "CLOSED" if is_closed_text(block_text) else customer["status"]
        update_customer(
            customer["case_id"],
            company or customer["company"] or "",
            block_text,
            source_group_id,
            status=new_status,
        )
        pushed_to_a = False
        # ★ 修正圖4：結案時不推 A 群
        if want_push_a and new_status != "CLOSED":
            ok, _ = push_text(A_GROUP_ID, block_text)
            pushed_to_a = ok
        return build_update_reply(name, pushed_to_a)

    if not has_action_word and not is_format_trigger(block_text):
        return None

    if same_group_active:
        row = same_group_active[0]
        update_customer(
            row["case_id"],
            company or row["company"] or "",
            block_text,
            source_group_id,
        )
        pushed_to_a = False
        if want_push_a:
            ok, _ = push_text(A_GROUP_ID, block_text)
            pushed_to_a = ok
        return build_update_reply(name, pushed_to_a)

    if other_group_active:
        send_transfer_case_buttons(reply_token, other_group_active[0], source_group_id, block_text, allow_new=True)
        return "QUICK_REPLY_SENT"

    if same_group_closed and is_closed_text(block_text):
        row = same_group_closed[0]
        update_customer(
            row["case_id"],
            company or row["company"] or "",
            block_text,
            source_group_id,
            status="CLOSED",
        )
        return f"已更新客戶：{name}"

    if same_group_closed and not is_closed_text(block_text):
        send_reopen_case_buttons(
            reply_token, block_text, same_group_closed, source_group_id,
            push_to_a_after_reopen=want_push_a
        )
        return "QUICK_REPLY_SENT"

    if other_group_closed and not is_closed_text(block_text):
        send_transfer_case_buttons(reply_token, other_group_closed[0], source_group_id, block_text, allow_new=True)
        return "QUICK_REPLY_SENT"

    create_customer_record(name, "", company, source_group_id, block_text)
    pushed_to_a = False
    if want_push_a:
        ok, _ = push_text(A_GROUP_ID, block_text)
        pushed_to_a = ok
    if pushed_to_a:
        return f"🆕 已建立客戶：{name}\n✅ 已回貼A群：{name}"
    return f"🆕 已建立客戶：{name}"


def find_customer_for_a_block(block_text: str, reply_token: str):
    id_no = extract_id_no(block_text)
    name = extract_name(block_text)

    if id_no:
        row = find_active_by_id_no(id_no)
        if row:
            return row

    if name:
        matches = find_active_by_name(name)
        if len(matches) == 1:
            return matches[0]
        if len(matches) > 1:
            send_ambiguous_case_buttons(reply_token, block_text, matches)
            return "MULTIPLE"

    return None


def handle_a_case_block(block_text: str, reply_token: str):
    if is_blocked(block_text):
        return "❌ 含禁止關鍵字，已略過"

    customer = find_customer_for_a_block(block_text, reply_token)
    if customer == "MULTIPLE":
        return "QUICK_REPLY_SENT"

    if not customer:
        return f"⚠️ 找不到對應客戶：{get_block_display_text(block_text)}"

    company = extract_company(block_text) or customer["company"] or ""
    new_status = "CLOSED" if is_closed_text(block_text) else None
    route_plan = extract_route_plan(block_text) or None

    update_customer(
        customer["case_id"],
        company,
        block_text,
        A_GROUP_ID,
        status=new_status,
        route_plan=route_plan,
    )

    ok, err = push_text(customer["source_group_id"], block_text)
    if not ok:
        return f"❌ 找到客戶：{customer['customer_name']}，但回貼{get_group_name(customer['source_group_id'])}失敗：{err}"

    if new_status == "CLOSED":
        return f"✅ 已結案並回貼到{get_group_name(customer['source_group_id'])}：{customer['customer_name']}"
    return f"✅ 已回貼到{get_group_name(customer['source_group_id'])}：{customer['customer_name']}"


# =========================
# 指令按鈕處理
# =========================
def handle_command_text(text: str, reply_token: str):

    # ★ 修正圖1：新增確認建立新案件的按鈕處理
    if text.startswith("FORCE_CREATE_NEW|"):
        _, action_id = text.split("|", 1)
        action = get_pending_action(action_id)
        if not action or action["action_type"] != "confirm_new_case_with_existing_id":
            reply_text(reply_token, "⚠️ 找不到待確認資料")
            return True
        payload = action["payload"]
        block_text = payload.get("block_text", "")
        source_group_id = payload.get("source_group_id", "")
        name = extract_name(block_text)
        id_no = extract_id_no(block_text)
        company = extract_company(block_text)
        route_plan = extract_route_plan(block_text)
        create_customer_record(name, id_no, company, source_group_id, block_text, route_plan=route_plan)
        reply_text(reply_token, f"🆕 已在{get_group_name(source_group_id)}建立新案件：{name}")
        delete_pending_action(action_id)
        return True

    if text.startswith("USE_EXISTING_CASE|"):
        _, action_id = text.split("|", 1)
        action = get_pending_action(action_id)
        if not action or action["action_type"] != "confirm_new_case_with_existing_id":
            reply_text(reply_token, "⚠️ 找不到待確認資料")
            return True
        payload = action["payload"]
        block_text = payload.get("block_text", "")
        existing_case_id = payload.get("existing_case_id", "")
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("SELECT * FROM customers WHERE case_id = ?", (existing_case_id,))
        customer = cur.fetchone()
        conn.close()
        if not customer:
            reply_text(reply_token, "⚠️ 原案件不存在")
            delete_pending_action(action_id)
            return True
        reply_text(reply_token, f"✅ 已沿用{get_group_name(customer['source_group_id'])}案件：{customer['customer_name']}")
        delete_pending_action(action_id)
        return True

    if text.startswith("CANCEL_NEW_CASE|"):
        _, action_id = text.split("|", 1)
        delete_pending_action(action_id)
        reply_text(reply_token, "✅ 已取消")
        return True

    if text.startswith("CONFIRM_TRANSFER|"):
        _, action_id = text.split("|", 1)
        action = get_pending_action(action_id)
        if not action or action["action_type"] != "transfer_customer":
            reply_text(reply_token, "⚠️ 找不到待確認資料")
            return True

        payload = action["payload"]
        case_id = payload.get("case_id", "")
        target_group_id = payload.get("target_group_id", "")
        block_text = payload.get("block_text", "")
        new_name = payload.get("name", extract_name(block_text))

        conn = get_conn()
        cur = conn.cursor()
        cur.execute("SELECT * FROM customers WHERE case_id = ?", (case_id,))
        customer = cur.fetchone()
        conn.close()

        if not customer:
            reply_text(reply_token, "⚠️ 原案件不存在")
            delete_pending_action(action_id)
            return True

        update_customer(
            case_id,
            extract_company(block_text) or customer["company"] or "",
            block_text,
            target_group_id,
            name=new_name,
            source_group_id=target_group_id,
            route_plan=extract_route_plan(block_text) or None,
        )
        reply_text(reply_token, f"✅ 已改到{get_group_name(target_group_id)}")
        delete_pending_action(action_id)
        return True

    if text.startswith("KEEP_OLD_CASE|"):
        _, action_id = text.split("|", 1)
        action = get_pending_action(action_id)
        if not action or action["action_type"] != "transfer_customer":
            reply_text(reply_token, "⚠️ 找不到待確認資料")
            return True

        payload = action["payload"]
        case_id = payload.get("case_id", "")
        block_text = payload.get("block_text", "")

        conn = get_conn()
        cur = conn.cursor()
        cur.execute("SELECT * FROM customers WHERE case_id = ?", (case_id,))
        customer = cur.fetchone()
        conn.close()

        if not customer:
            reply_text(reply_token, "⚠️ 原案件不存在")
            delete_pending_action(action_id)
            return True

        update_customer(
            case_id,
            extract_company(block_text) or customer["company"] or "",
            block_text,
            customer["source_group_id"],
            route_plan=extract_route_plan(block_text) or None,
        )
        reply_text(reply_token, f"✅ 已沿用{get_group_name(customer['source_group_id'])}原案件")
        delete_pending_action(action_id)
        return True

    if text.startswith("CREATE_NEW_FROM_TRANSFER|"):
        _, action_id = text.split("|", 1)
        action = get_pending_action(action_id)
        if not action or action["action_type"] != "transfer_customer":
            reply_text(reply_token, "⚠️ 找不到建立新案件資料")
            return True

        payload = action["payload"]
        block_text = payload.get("block_text", "")
        target_group_id = payload.get("target_group_id", "")
        name = extract_name(block_text)
        id_no = extract_id_no(block_text)
        company = extract_company(block_text)
        route_plan = extract_route_plan(block_text)
        create_customer_record(name, id_no, company, target_group_id, block_text, route_plan=route_plan)
        reply_text(reply_token, f"🆕 已建立新案件：{name}")
        delete_pending_action(action_id)
        return True

    if text.startswith("CANCEL_TRANSFER|"):
        _, action_id = text.split("|", 1)
        delete_pending_action(action_id)
        reply_text(reply_token, "✅ 已取消")
        return True

    if text.startswith("SELECT_CASE|"):
        _, action_id, case_id = text.split("|", 2)
        action = get_pending_action(action_id)
        if not action or action["action_type"] != "route_a_case":
            reply_text(reply_token, "⚠️ 找不到待確認案件")
            return True

        block_text = action["payload"].get("block_text", "")

        conn = get_conn()
        cur = conn.cursor()
        cur.execute("SELECT * FROM customers WHERE case_id = ?", (case_id,))
        customer = cur.fetchone()
        conn.close()

        if not customer:
            reply_text(reply_token, "⚠️ 案件不存在")
            delete_pending_action(action_id)
            return True

        company = extract_company(block_text) or customer["company"] or ""
        new_status = "CLOSED" if is_closed_text(block_text) else None
        route_plan = extract_route_plan(block_text) or None

        update_customer(case_id, company, block_text, A_GROUP_ID, status=new_status, route_plan=route_plan)
        ok, err = push_text(customer["source_group_id"], block_text)
        if not ok:
            reply_text(reply_token, f"❌ 找到案件但回貼失敗：{err}")
            delete_pending_action(action_id)
            return True

        if new_status == "CLOSED":
            reply_text(reply_token, f"✅ 已結案並回貼到{get_group_name(customer['source_group_id'])}：{customer['customer_name']}")
        else:
            reply_text(reply_token, f"✅ 已回貼到{get_group_name(customer['source_group_id'])}：{customer['customer_name']}")
        delete_pending_action(action_id)
        return True

    if text.startswith("REOPEN_CASE|"):
        _, action_id, case_id = text.split("|", 2)
        action = get_pending_action(action_id)
        if not action or action["action_type"] != "reopen_or_new_case":
            reply_text(reply_token, "⚠️ 找不到重啟案件資料")
            return True

        payload = action["payload"]
        block_text = payload.get("block_text", "")
        push_to_a_after_reopen = payload.get("push_to_a_after_reopen", False)  # ★ 修正圖4

        conn = get_conn()
        cur = conn.cursor()
        cur.execute("SELECT * FROM customers WHERE case_id = ?", (case_id,))
        customer = cur.fetchone()
        conn.close()

        if not customer:
            reply_text(reply_token, "⚠️ 原案件不存在")
            delete_pending_action(action_id)
            return True

        update_customer(
            case_id,
            extract_company(block_text) or customer["company"] or "",
            block_text,
            customer["source_group_id"],
            status="ACTIVE",
            route_plan=extract_route_plan(block_text) or None,
        )

        # ★ 修正圖4：重啟後才推 A 群
        pushed_to_a = False
        if push_to_a_after_reopen:
            ok, _ = push_text(A_GROUP_ID, block_text)
            pushed_to_a = ok

        msg = f"✅ 已重啟案件：{customer['customer_name']}"
        if pushed_to_a:
            msg += f"\n✅ 已回貼A群：{customer['customer_name']}"
        reply_text(reply_token, msg)
        delete_pending_action(action_id)
        return True

    if text.startswith("CREATE_NEW_CASE|"):
        _, action_id = text.split("|", 1)
        action = get_pending_action(action_id)
        if not action or action["action_type"] != "reopen_or_new_case":
            reply_text(reply_token, "⚠️ 找不到建立新案件資料")
            return True

        block_text = action["payload"].get("block_text", "")
        source_group_id = action["payload"].get("source_group_id", B_GROUP_ID)
        push_to_a_after_reopen = action["payload"].get("push_to_a_after_reopen", False)
        name = extract_name(block_text)
        id_no = extract_id_no(block_text)
        company = extract_company(block_text)
        route_plan = extract_route_plan(block_text)
        create_customer_record(name, id_no, company, source_group_id, block_text, route_plan=route_plan)

        pushed_to_a = False
        if push_to_a_after_reopen:
            ok, _ = push_text(A_GROUP_ID, block_text)
            pushed_to_a = ok

        msg = f"🆕 已建立新案件：{name}"
        if pushed_to_a:
            msg += f"\n✅ 已回貼A群：{name}"
        reply_text(reply_token, msg)
        delete_pending_action(action_id)
        return True

    if text.startswith("CANCEL_REOPEN|"):
        _, action_id = text.split("|", 1)
        delete_pending_action(action_id)
        reply_text(reply_token, "✅ 已取消")
        return True

    return False


# =========================
# Webhook
# =========================
# Webhook
# =========================
def process_event(event: dict):
    """
    背景處理單一 LINE 事件。
    Webhook 收到後立即回 200，這裡在背景執行，不佔用 LINE 的 5 秒限制。
    """
    if event.get("type") != "message":
        return
    message = event.get("message", {})
    if message.get("type") != "text":
        return

    text = message.get("text", "").strip()
    reply_token = event.get("replyToken", "")
    group_id = event.get("source", {}).get("groupId")

    if not text:
        return

    if handle_command_text(text, reply_token):
        return

    sales_group_ids = get_sales_group_ids()
    admin_group_ids = get_admin_group_ids()

    # 業務群 / 行政群
    if group_id in sales_group_ids or group_id in admin_group_ids:
        raw_text = text
        clean_text = strip_ai_trigger(raw_text)

        is_creation_like = looks_like_new_case_block(raw_text) or any(
            looks_like_new_case_block(b) for b in split_multi_cases(raw_text)
        )
        is_route_like = looks_like_route_plan_block(raw_text) or any(
            looks_like_route_plan_block(b) for b in split_multi_cases(raw_text)
        )

        should_process = False
        if is_creation_like or is_route_like:
            should_process = True
        elif has_ai_trigger(raw_text):
            should_process = True

        if not should_process:
            return

        process_text = raw_text if (is_creation_like or is_route_like) else clean_text
        if process_text in {"", "助理", "AI助理"}:
            return

        blocks = split_multi_cases(process_text)
        if not blocks:
            blocks = [process_text]

        results = []
        quick_reply_sent = False

        for idx, block in enumerate(blocks, start=1):
            result = handle_bc_case_block(block, group_id, reply_token, source_text=raw_text)
            if result == "QUICK_REPLY_SENT":
                quick_reply_sent = True
                break
            if result:
                if len(blocks) > 1:
                    results.append(f"第{idx}筆：{result}")
                else:
                    results.append(result)

        if not quick_reply_sent and results:
            reply_text(reply_token, "\n".join(results))
        return

    # A 群
    if group_id == A_GROUP_ID:
        if not has_ai_trigger(text):
            return

        clean_text = strip_ai_trigger(text)
        if clean_text in {"", "助理", "AI助理"}:
            return

        blocks = split_multi_cases(clean_text)
        if not blocks:
            blocks = [clean_text]

        results = []
        for idx, block in enumerate(blocks, start=1):
            result = handle_a_case_block(block, reply_token)
            if result == "QUICK_REPLY_SENT":
                continue
            if result:
                if len(blocks) > 1:
                    results.append(f"第{idx}筆：{result}")
                else:
                    results.append(result)

        if results:
            reply_text(reply_token, "\n".join(results))


@app.post("/callback")
async def callback(request: Request, background_tasks: BackgroundTasks):
    """立即回 200 給 LINE，所有事件丟到背景處理，避免 5 秒 timeout。"""
    body = await request.json()
    for event in body.get("events", []):
        background_tasks.add_task(process_event, event)
    return {"status": "ok"}


# =========================
# 管理 API：新增群組
# =========================
@app.post("/admin/add_group")
async def add_group(request: Request):
    """
    新增業務群或行政群，不需改程式碼，直接打 API 即可。
    Body: {"group_id": "Cxxx", "group_name": "D群", "group_type": "SALES_GROUP"}
    group_type: SALES_GROUP / ADMIN_GROUP / A_GROUP
    """
    body = await request.json()
    group_id = body.get("group_id", "").strip()
    group_name = body.get("group_name", "").strip()
    group_type = body.get("group_type", "SALES_GROUP").strip()

    if not group_id or not group_name:
        return {"status": "error", "message": "group_id 和 group_name 必填"}

    conn = get_conn()
    cur = conn.cursor()
    try:
        cur.execute("""
        INSERT OR REPLACE INTO groups (group_id, group_name, group_type, is_active, created_at)
        VALUES (?, ?, ?, 1, ?)
        """, (group_id, group_name, group_type, now_iso()))
        conn.commit()
        return {"status": "ok", "message": f"已新增/更新群組：{group_name}({group_type})"}
    except Exception as e:
        return {"status": "error", "message": str(e)}
    finally:
        conn.close()


@app.get("/admin/groups", response_class=HTMLResponse)
def list_groups():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT * FROM groups ORDER BY group_type, group_name")
    rows = cur.fetchall()
    conn.close()

    html = "<h2>群組列表</h2><table border='1' cellpadding='6'>"
    html += "<tr><th>名稱</th><th>類型</th><th>Group ID</th><th>啟用</th></tr>"
    for r in rows:
        html += f"<tr><td>{r['group_name']}</td><td>{r['group_type']}</td><td>{r['group_id']}</td><td>{'✅' if r['is_active'] else '❌'}</td></tr>"
    html += "</table><br><a href='/'>回首頁</a>"
    return html


# =========================
# 報表
# =========================
@app.get("/", response_class=HTMLResponse)
def home():
    return """
    <h2>貸款系統</h2>
    <p>系統正常運作中</p>
    <a href="/report">看日報</a> |
    <a href="/admin/groups">群組管理</a>
    """


@app.get("/report", response_class=HTMLResponse)
def report():
    conn = get_conn()
    cur = conn.cursor()

    html = "<h2>📊 日報</h2>"

    for company in COMPANY_LIST:
        html += f"<b>{company}</b><br>"
        has_data = False

        cur.execute(
            """
            SELECT * FROM customers
            WHERE status = 'ACTIVE' AND (company = ? OR route_plan LIKE ?)
            ORDER BY updated_at DESC
            """,
            (company, f"%{company}%"),
        )
        rows = cur.fetchall()

        for row in rows:
            has_data = True
            date_text = row["updated_at"][5:10].replace("-", "/") if row["updated_at"] else ""
            html += (
                f"{date_text}"
                f"-{row['customer_name']}"
                f"-{row['company'] or ''}"
                f"-{row['route_plan'] or ''}"
                f"-{row['last_update'] or ''}"
                f"-{get_group_name(row['source_group_id'])}"
                f"<br>"
            )

        if not has_data:
            html += "（無資料）<br>"

        html += "——————————<br>"

    conn.close()
    return html


# =========================
# 啟動
# =========================
@app.on_event("startup")
def startup():
    init_db()
    seed_groups()


if __name__ == "__main__":
    init_db()
    seed_groups()
    uvicorn.run(app, host="0.0.0.0", port=10000)


# =========================
# ★ 修正圖3：修正 split_multi_cases（補在原函式旁邊）
# =========================
def looks_like_case_start(line: str) -> bool:
    line = line.strip()
    if not line:
        return False
    if re.match(r"^[\[【(（]\d+[\]】)）]", line):
        return False
    if line.startswith("@") or line.startswith("#"):
        return False
    if line in {"助理", "AI助理"}:
        return False

    compact = re.sub(r"\s+", "", line)
    if DATE_NAME_ID_INLINE_RE.search(compact):
        return True
    if DATE_NAME_ONLY_RE.search(line):
        return True

    # ★ 修正圖3：支援「月/日-姓名」格式（無年份，如 3/31-許永松）
    SHORT_DATE_NAME_RE = re.compile(
        r"^\s*\d{1,2}/\d{1,2}\s*[-－]\s*([\u4e00-\u9fff]{2,4})\s*$"
    )
    if SHORT_DATE_NAME_RE.search(line):
        return True

    if looks_like_route_plan_block(line):
        return True
    if is_format_trigger(line):
        return True

    name = extract_name(line)
    company = extract_company(line)
    has_strong = contains_status_word(line)
    if name and company:
        return True
    if name and has_strong:
        return True
    return False


def split_multi_cases(text: str) -> List[str]:
    text = (text or "").strip()
    if not text:
        return []

    text = re.sub(r"\n\s*/\s*\n", "\n<<<SPLIT>>>\n", text)
    text = re.sub(r"\n\s*\n+", "\n<<<SPLIT>>>\n", text)
    raw_parts = [p.strip() for p in text.split("<<<SPLIT>>>") if p.strip()]

    final_blocks: List[str] = []
    for part in raw_parts:
        lines = [line.rstrip() for line in part.splitlines() if line.strip()]
        if not lines:
            continue

        current: List[str] = []
        for line in lines:
            if looks_like_case_start(line) and current:
                final_blocks.append("\n".join(current).strip())
                current = []
            current.append(line.strip())
        if current:
            final_blocks.append("\n".join(current).strip())

    return [b for b in final_blocks if b.strip()]
