from fastapi import FastAPI, Request, Header, HTTPException
from fastapi.responses import HTMLResponse, JSONResponse
import uvicorn
import requests
import os
import re
import sqlite3
import json
import hmac
import base64
import hashlib
from datetime import datetime
import uuid
from typing import Optional, List, Tuple

app = FastAPI()

CHANNEL_ACCESS_TOKEN = os.getenv("CHANNEL_ACCESS_TOKEN", "")
CHANNEL_SECRET = os.getenv("CHANNEL_SECRET", "")

# ===== 群組 ID =====
A_GROUP_ID = "Cb3579e75c94437ed22aafc7b1f6aecdd"   # A群
B_GROUP_ID = "Cd14f3ee775f1d9f5cfdafb223173cbef"   # B群
C_GROUP_ID = "C1a647fcb29a74842eceeb18e7a53823d"   # C群

SALES_GROUP_IDS = {B_GROUP_ID, C_GROUP_ID}

# ===== Render Disk 永久保存 =====
DB_PATH = os.getenv("DB_PATH", "/var/data/loan_system.db")

COMPANY_LIST = [
    "和裕商品", "和裕機車", "亞太商品", "亞太機車",
    "亞太", "和裕", "21", "貸救補", "第一", "分貝", "麻吉",
    "手機分期", "中租", "裕融", "創鉅", "合信", "興達", "鄉民", "喬美", "和潤"
]
STATUS_WORDS = [
    "婉拒", "核准", "補件", "等保書", "退件", "不承作", "照會",
    "保密", "NA", "待撥款", "可送", "缺資料", "補資料", "撥款",
    "轉音", "陌生電話", "黑名單", "融資黑名單", "聯徵", "照會中"
]
DELETE_KEYWORDS = ["結案", "刪掉", "不追了", "全部不送", "已撥款結案"]
BLOCK_KEYWORDS = ["鼎信", "禾基"]
TRIGGER_TAGS = ["@AI", "#AI", "@助理", "#案件"]

CHINESE_NAME_RE = re.compile(r"[\u4e00-\u9fff]{2,4}")
ID_RE = re.compile(r"[A-Z][12]\d{8}")
DATE_PREFIX_RE = re.compile(r"^\d{2,4}/\d{1,2}/\d{1,2}[-－]\s*")
SEPARATOR_RE = re.compile(r"\n\s*/\s*\n|\n\s*\n+")


# =========================
# 基本工具
# =========================
def now_iso() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def today_str() -> str:
    return datetime.now().strftime("%Y-%m-%d")


def short_id() -> str:
    return str(uuid.uuid4())[:8]


def ensure_db_dir():
    db_dir = os.path.dirname(DB_PATH)
    if db_dir:
        os.makedirs(db_dir, exist_ok=True)


def get_conn():
    ensure_db_dir()
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def extract_first_line(text: str) -> str:
    lines = [line.strip() for line in text.splitlines() if line.strip()]
    return lines[0] if lines else ""


def normalize_first_line(text: str) -> str:
    return DATE_PREFIX_RE.sub("", text).strip()


def normalize_text(text: str) -> str:
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    text = text.replace("　", " ")
    text = re.sub(r"[ \t]+", " ", text)
    return text.strip()


def strip_trigger_tags(text: str) -> str:
    result = text
    for tag in TRIGGER_TAGS:
        result = result.replace(tag, "")
    return result.strip()


def extract_possible_names(text: str):
    return CHINESE_NAME_RE.findall(text)


def extract_name(text: str) -> str:
    first_line = normalize_first_line(extract_first_line(text))
    if not first_line:
        return ""

    if "｜" in first_line:
        left = first_line.split("｜", 1)[0].strip()
        m = CHINESE_NAME_RE.search(left)
        return m.group(0) if m else ""

    if "->" in first_line:
        left = first_line.split("->", 1)[0].strip()
        m = CHINESE_NAME_RE.search(left)
        return m.group(0) if m else ""

    if "→" in first_line:
        left = first_line.split("→", 1)[0].strip()
        m = CHINESE_NAME_RE.search(left)
        return m.group(0) if m else ""

    m = CHINESE_NAME_RE.search(first_line)
    return m.group(0) if m else ""


def extract_id_no(text: str) -> str:
    m = ID_RE.search(text.upper())
    return m.group(0) if m else ""


def extract_company(text: str) -> str:
    for c in COMPANY_LIST:
        if c in text:
            return c
    return ""


def extract_status(text: str) -> str:
    for w in STATUS_WORDS:
        if w in text:
            return w
    return ""


def contains_status_word(text: str) -> bool:
    return any(w in text for w in STATUS_WORDS)


def is_blocked(text: str) -> bool:
    return any(w in text for w in BLOCK_KEYWORDS)


def is_closed_text(text: str) -> bool:
    return any(w in text for w in DELETE_KEYWORDS)


def is_format_trigger(block: str) -> bool:
    first = normalize_first_line(extract_first_line(block))
    if "｜" in first and len([p for p in first.split("｜") if p.strip()]) >= 2:
        return True
    if "->" in first or "→" in first:
        return True
    return False


def is_fallback_trigger(block: str) -> bool:
    first = normalize_first_line(extract_first_line(block))
    name = extract_name(first)
    company = extract_company(first)
    has_strong = contains_status_word(first)
    return bool(name and (company or has_strong))


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
    if group_id == B_GROUP_ID:
        return "B群"
    if group_id == C_GROUP_ID:
        return "C群"
    return "未知群組"


def get_block_display_text(block_text: str) -> str:
    name = extract_name(block_text)
    first_line = extract_first_line(block_text)

    if name and name not in first_line:
        return f"{name}｜{first_line}"
    if name:
        return name
    return first_line


def verify_line_signature(body: bytes, signature: Optional[str]) -> bool:
    if not CHANNEL_SECRET:
        return True
    if not signature:
        return False
    digest = hmac.new(CHANNEL_SECRET.encode("utf-8"), body, hashlib.sha256).digest()
    computed = base64.b64encode(digest).decode("utf-8")
    return hmac.compare_digest(computed, signature)


def log_line_api_error(api_name: str, status_code: Optional[int], response_text: str, payload: dict, error_message: str = ""):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO api_logs (api_name, status_code, response_text, payload, error_message, created_at)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (api_name, status_code, response_text[:5000], json.dumps(payload, ensure_ascii=False), error_message[:1000], now_iso())
    )
    conn.commit()
    conn.close()


# =========================
# LINE API
# =========================
def line_post(api_name: str, url: str, data: dict) -> bool:
    if not CHANNEL_ACCESS_TOKEN:
        log_line_api_error(api_name, None, "", data, "CHANNEL_ACCESS_TOKEN 未設定")
        return False

    headers = {
        "Authorization": f"Bearer {CHANNEL_ACCESS_TOKEN}",
        "Content-Type": "application/json"
    }

    try:
        resp = requests.post(url, headers=headers, json=data, timeout=10)
        if resp.status_code >= 400:
            log_line_api_error(api_name, resp.status_code, resp.text, data, "LINE API 回傳錯誤")
            return False
        return True
    except requests.RequestException as e:
        log_line_api_error(api_name, None, "", data, str(e))
        return False



def push_text(to_group_id: str, text: str):
    data = {
        "to": to_group_id,
        "messages": [{"type": "text", "text": text[:5000]}]
    }
    return line_post("push", "https://api.line.me/v2/bot/message/push", data)



def reply_text(reply_token: str, text: str):
    if reply_token == "TEST":
        return True
    data = {
        "replyToken": reply_token,
        "messages": [{"type": "text", "text": text[:5000]}]
    }
    return line_post("reply", "https://api.line.me/v2/bot/message/reply", data)



def make_quick_reply_item(label: str, text: str):
    return {
        "type": "action",
        "action": {
            "type": "message",
            "label": label[:20],
            "text": text[:300]
        }
    }



def reply_quick_reply(reply_token: str, text: str, items):
    if reply_token == "TEST":
        return True
    data = {
        "replyToken": reply_token,
        "messages": [{
            "type": "text",
            "text": text[:5000],
            "quickReply": {"items": items[:13]}
        }]
    }
    return line_post("reply_quick_reply", "https://api.line.me/v2/bot/message/reply", data)


# =========================
# 資料庫初始化
# =========================
def init_db():
    conn = get_conn()
    cur = conn.cursor()

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS groups (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            group_id TEXT UNIQUE NOT NULL,
            group_name TEXT,
            group_type TEXT NOT NULL,
            is_active INTEGER DEFAULT 1,
            created_at TEXT NOT NULL
        )
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS customers (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            case_id TEXT UNIQUE NOT NULL,
            customer_name TEXT NOT NULL,
            id_no TEXT,
            source_group_id TEXT NOT NULL,
            company TEXT,
            last_update TEXT,
            latest_status TEXT,
            status TEXT NOT NULL DEFAULT 'ACTIVE',
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )
        """
    )

    cur.execute(
        """
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
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS pending_actions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            action_id TEXT UNIQUE NOT NULL,
            action_type TEXT NOT NULL,
            payload TEXT NOT NULL,
            created_at TEXT NOT NULL
        )
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS api_logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            api_name TEXT NOT NULL,
            status_code INTEGER,
            response_text TEXT,
            payload TEXT,
            error_message TEXT,
            created_at TEXT NOT NULL
        )
        """
    )

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
        cur.execute(
            """
            INSERT OR IGNORE INTO groups (group_id, group_name, group_type, is_active, created_at)
            VALUES (?, ?, ?, ?, ?)
            """,
            row,
        )

    conn.commit()
    conn.close()


# =========================
# DB CRUD
# =========================
def create_customer_record(name: str, id_no: str, company: str, source_group_id: str, text: str, latest_status: str = ""):
    conn = get_conn()
    cur = conn.cursor()
    now = now_iso()
    case_id = short_id()

    cur.execute(
        """
        INSERT INTO customers (
            case_id, customer_name, id_no, source_group_id, company,
            last_update, latest_status, status, created_at, updated_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, 'ACTIVE', ?, ?)
        """,
        (case_id, name, id_no, source_group_id, company, text, latest_status, now, now),
    )

    cur.execute(
        """
        INSERT INTO case_logs (
            case_id, customer_name, id_no, company, message_text, from_group_id, created_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (case_id, name, id_no, company, text, source_group_id, now),
    )

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
    latest_status: Optional[str] = None,
):
    conn = get_conn()
    cur = conn.cursor()
    now = now_iso()

    fields = []
    values = []

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
    if latest_status is not None:
        fields.append("latest_status = ?")
        values.append(latest_status)

    fields.append("updated_at = ?")
    values.append(now)
    values.append(case_id)

    sql = f"UPDATE customers SET {', '.join(fields)} WHERE case_id = ?"
    cur.execute(sql, values)

    cur.execute("SELECT * FROM customers WHERE case_id = ?", (case_id,))
    row = cur.fetchone()

    if row and text is not None:
        cur.execute(
            """
            INSERT INTO case_logs (
                case_id, customer_name, id_no, company, message_text, from_group_id, created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                row["case_id"], row["customer_name"], row["id_no"], row["company"],
                text, from_group_id, now
            ),
        )

    conn.commit()
    conn.close()



def find_active_by_id_no(id_no: str):
    if not id_no:
        return None

    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT * FROM customers
        WHERE id_no = ? AND status = 'ACTIVE'
        ORDER BY updated_at DESC
        LIMIT 1
        """,
        (id_no,),
    )
    row = cur.fetchone()
    conn.close()
    return row



def find_active_by_name(name: str):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT * FROM customers
        WHERE customer_name = ? AND status = 'ACTIVE'
        ORDER BY updated_at DESC
        """,
        (name,),
    )
    rows = cur.fetchall()
    conn.close()
    return rows



def save_pending_action(action_id: str, action_type: str, payload: str):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT OR REPLACE INTO pending_actions (action_id, action_type, payload, created_at)
        VALUES (?, ?, ?, ?)
        """,
        (action_id, action_type, payload, now_iso()),
    )
    conn.commit()
    conn.close()



def get_pending_action(action_id: str):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT * FROM pending_actions WHERE action_id = ?", (action_id,))
    row = cur.fetchone()
    conn.close()
    return row



def delete_pending_action(action_id: str):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("DELETE FROM pending_actions WHERE action_id = ?", (action_id,))
    conn.commit()
    conn.close()


# =========================
# 分段 / 判斷
# =========================
def looks_like_case_start(line: str) -> bool:
    line = normalize_text(line)
    if not line:
        return False

    if ID_RE.search(line):
        return True

    first = normalize_first_line(line)
    name = extract_name(first)
    company = extract_company(first)
    has_strong = contains_status_word(first)

    if is_format_trigger(first):
        return True
    if name and company:
        return True
    if name and has_strong:
        return True

    return False



def split_multi_cases(text: str) -> List[str]:
    """
    多筆分段：
    - 空白行分隔
    - 單獨一行 / 分隔
    - 新案件主句開新段
    - 黑名單/查詢次數/過多 這類補充不會被拆成獨立案件
    - 沒明確分隔時，若下一行像新案件，也會自動開新段
    """
    text = normalize_text(strip_trigger_tags(text))
    if not text:
        return []

    text = re.sub(r"\n\s*/\s*\n", "\n<<<SPLIT>>>\n", text)
    text = re.sub(r"\n\s*\n+", "\n<<<SPLIT>>>\n", text)
    raw_parts = [p.strip() for p in text.split("<<<SPLIT>>>") if p.strip()]

    final_blocks = []

    for part in raw_parts:
        lines = [line.strip() for line in part.splitlines() if line.strip()]
        if not lines:
            continue

        blocks = []
        current = []

        for line in lines:
            if looks_like_case_start(line):
                if current:
                    blocks.append("\n".join(current))
                    current = []
            current.append(line)

        if current:
            blocks.append("\n".join(current))

        for block in blocks:
            block_lines = [line.strip() for line in block.splitlines() if line.strip()]
            if not block_lines:
                continue

            first_line = normalize_first_line(block_lines[0])
            id_match = ID_RE.search(first_line)

            possible_names = extract_possible_names(first_line)
            excluded = {
                "等保書", "婉拒", "核准", "補件", "退件", "亞太", "和裕",
                "無可知情", "黑名單", "查詢次數", "過多", "不承作", "貸救補",
                "陌生電話", "電話直接", "融資黑名單"
            }
            possible_names = [n for n in possible_names if n not in excluded]

            if not id_match and len(possible_names) >= 2 and not is_format_trigger(first_line):
                remain = first_line
                for name in possible_names:
                    remain = remain.replace(name, "", 1)
                remain = remain.strip()

                for name in possible_names:
                    new_lines = [f"{name} {remain}".strip()]
                    if len(block_lines) > 1:
                        new_lines.extend(block_lines[1:])
                    final_blocks.append("\n".join(new_lines).strip())
            else:
                final_blocks.append(block.strip())

    return [b for b in final_blocks if b.strip()]



def detect_a_blocks(text: str) -> List[str]:
    raw_text = normalize_text(text)
    if not raw_text:
        return []

    clean_text = strip_trigger_tags(raw_text)
    split_blocks = split_multi_cases(clean_text)

    if len(split_blocks) > 1:
        return split_blocks

    # 單段但明顯可觸發
    if is_format_trigger(clean_text) or is_fallback_trigger(clean_text):
        return [clean_text]

    # 無分隔時，看每行是否像新案件，若有兩行以上就拆
    lines = [line.strip() for line in clean_text.splitlines() if line.strip()]
    if sum(1 for line in lines if looks_like_case_start(line)) >= 2:
        return split_multi_cases(clean_text)

    return []


# =========================
# 訊息格式
# =========================
def format_case_push(customer, block_text: str) -> str:
    company = extract_company(block_text) or customer["company"] or "未填"
    status = extract_status(block_text) or customer["latest_status"] or "最新進度"
    return (
        "【案件進度通知】\n"
        f"姓名：{customer['customer_name']}\n"
        f"公司：{company}\n"
        f"狀態：{status}\n"
        f"內容：{block_text}"
    )


# =========================
# 主要業務邏輯
# =========================
def handle_bc_case_block(block_text: str, source_group_id: str, reply_token: str):
    name = extract_name(block_text)
    id_no = extract_id_no(block_text)
    company = extract_company(block_text)
    latest_status = extract_status(block_text)

    if not name:
        return None

    if is_blocked(block_text):
        return "❌ 含禁止關鍵字，已略過"

    if id_no:
        existing = find_active_by_id_no(id_no)

        if existing:
            if existing["source_group_id"] == source_group_id:
                update_customer(
                    existing["case_id"],
                    company or existing["company"] or "",
                    block_text,
                    source_group_id,
                    name=name,
                    latest_status=latest_status or existing["latest_status"] or "",
                )
                return f"🔄 已更新客戶：{name}"

            if "轉件" in block_text or "轉" in block_text:
                update_customer(
                    existing["case_id"],
                    company or existing["company"] or "",
                    block_text,
                    source_group_id,
                    name=name,
                    source_group_id=source_group_id,
                    latest_status=latest_status or existing["latest_status"] or "",
                )
                return f"➡️ 已轉移客戶到{get_group_name(source_group_id)}：{name}"

            action_id = short_id()
            payload = json.dumps({
                "case_id": existing["case_id"],
                "target_group_id": source_group_id,
                "block_text": block_text,
                "new_name": name,
                "latest_status": latest_status,
            }, ensure_ascii=False)
            save_pending_action(action_id, "transfer_customer", payload)

            old_group = get_group_name(existing["source_group_id"])
            new_group = get_group_name(source_group_id)
            items = [
                make_quick_reply_item(f"轉到{new_group}", f"CONFIRM_TRANSFER|{action_id}"),
                make_quick_reply_item("維持原群", f"CANCEL_TRANSFER|{action_id}")
            ]
            reply_quick_reply(reply_token, f"⚠️ 此客戶已存在於{old_group}，要改到{new_group}嗎？", items)
            return "QUICK_REPLY_SENT"

    if not id_no:
        rows = find_active_by_name(name)
        same_group_rows = [
            r for r in rows
            if r["source_group_id"] == source_group_id and (not r["id_no"])
        ]
        if len(same_group_rows) == 1:
            row = same_group_rows[0]
            update_customer(
                row["case_id"],
                company or row["company"] or "",
                block_text,
                source_group_id,
                latest_status=latest_status or row["latest_status"] or "",
            )
            return f"🔄 已更新客戶：{name}"

    create_customer_record(name, id_no, company, source_group_id, block_text, latest_status=latest_status)
    return f"🆕 已建立客戶：{name}"



def send_ambiguous_case_buttons(reply_token: str, block_text: str, matches):
    action_id = short_id()
    payload = json.dumps({
        "block_text": block_text,
        "case_ids": [m["case_id"] for m in matches]
    }, ensure_ascii=False)
    save_pending_action(action_id, "route_a_case", payload)

    items = []
    for c in matches[:10]:
        label = f"{c['customer_name']}-{c['company'] or '未填'}-{get_group_name(c['source_group_id'])}"
        text = f"SELECT_CASE|{action_id}|{c['case_id']}"
        items.append(make_quick_reply_item(label, text))

    tip = "⚠️ 多筆同名客戶，請選擇要回貼的案件"
    if len(matches) > 10:
        tip += "（僅顯示前10筆）"
    reply_quick_reply(reply_token, tip, items)



def find_customer_for_a_block(block_text: str, reply_token: str):
    name = extract_name(block_text)
    id_no = extract_id_no(block_text)

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
    latest_status = extract_status(block_text) or customer["latest_status"] or ""
    new_status = "CLOSED" if is_closed_text(block_text) else None

    update_customer(
        customer["case_id"],
        company,
        block_text,
        A_GROUP_ID,
        status=new_status,
        latest_status=latest_status,
    )

    push_body = format_case_push(customer, block_text)
    push_text(customer["source_group_id"], push_body)

    if new_status == "CLOSED":
        return f"✅ 已結案並回貼到{get_group_name(customer['source_group_id'])}：{customer['customer_name']}"

    return f"✅ 已回貼到{get_group_name(customer['source_group_id'])}：{customer['customer_name']}"



def handle_command_text(text: str, reply_token: str):
    if text.startswith("CONFIRM_TRANSFER|"):
        _, action_id = text.split("|", 1)
        action = get_pending_action(action_id)

        if not action or action["action_type"] != "transfer_customer":
            reply_text(reply_token, "⚠️ 找不到待確認資料")
            return True

        payload = json.loads(action["payload"])
        case_id = payload["case_id"]
        target_group_id = payload["target_group_id"]
        block_text = payload["block_text"]
        new_name = payload["new_name"]
        latest_status = payload.get("latest_status", "")

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
            latest_status=latest_status or customer["latest_status"] or "",
        )

        reply_text(reply_token, f"✅ 已改到{get_group_name(target_group_id)}")
        delete_pending_action(action_id)
        return True

    if text.startswith("CANCEL_TRANSFER|"):
        _, action_id = text.split("|", 1)
        delete_pending_action(action_id)
        reply_text(reply_token, "✅ 已維持原群")
        return True

    if text.startswith("SELECT_CASE|"):
        _, action_id, case_id = text.split("|", 2)
        action = get_pending_action(action_id)

        if not action or action["action_type"] != "route_a_case":
            reply_text(reply_token, "⚠️ 找不到待確認案件")
            return True

        payload = json.loads(action["payload"])
        block_text = payload["block_text"]

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
        latest_status = extract_status(block_text) or customer["latest_status"] or ""
        new_status = "CLOSED" if is_closed_text(block_text) else None

        update_customer(
            case_id,
            company,
            block_text,
            A_GROUP_ID,
            status=new_status,
            latest_status=latest_status,
        )
        push_text(customer["source_group_id"], format_case_push(customer, block_text))

        if new_status == "CLOSED":
            reply_text(reply_token, f"✅ 已結案並回貼到{get_group_name(customer['source_group_id'])}：{customer['customer_name']}")
        else:
            reply_text(reply_token, f"✅ 已回貼到{get_group_name(customer['source_group_id'])}：{customer['customer_name']}")

        delete_pending_action(action_id)
        return True

    return False


# =========================
# API
# =========================
@app.post("/callback")
async def callback(request: Request, x_line_signature: Optional[str] = Header(default=None)):
    body_bytes = await request.body()

    if not verify_line_signature(body_bytes, x_line_signature):
        raise HTTPException(status_code=400, detail="Invalid LINE signature")

    try:
        body = json.loads(body_bytes.decode("utf-8"))
    except json.JSONDecodeError:
        raise HTTPException(status_code=400, detail="Invalid JSON")

    for event in body.get("events", []):
        if event.get("type") != "message":
            continue

        message = event.get("message", {})
        if message.get("type") != "text":
            continue

        text = normalize_text(message.get("text", ""))
        reply_token = event.get("replyToken", "TEST")
        group_id = event.get("source", {}).get("groupId")

        if not text or not group_id:
            continue

        # 按鈕命令
        if handle_command_text(text, reply_token):
            continue

        # ===== B / C 群 =====
        if group_id in SALES_GROUP_IDS:
            blocks = split_multi_cases(text)

            results = []
            quick_reply_sent = False

            for idx, block in enumerate(blocks, start=1):
                result = handle_bc_case_block(block, group_id, reply_token)

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
            continue

        # ===== A 群 =====
        if group_id == A_GROUP_ID:
            blocks = detect_a_blocks(text)
            if not blocks:
                return JSONResponse({"status": "ignored"})

            results = []
            quick_reply_sent = False

            for idx, block in enumerate(blocks, start=1):
                result = handle_a_case_block(block, reply_token)
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
            continue

    return {"status": "ok"}


@app.get("/", response_class=HTMLResponse)
def home():
    return """
    <h2>貸款系統</h2>
    <p>系統正常運作中</p>
    <a href="/report">看日報</a><br>
    <a href="/debug/customers">看案件</a><br>
    <a href="/debug/api-logs">看LINE錯誤紀錄</a>
    """


@app.get("/report", response_class=HTMLResponse)
def report():
    conn = get_conn()
    cur = conn.cursor()

    html = """
    <html>
    <head>
      <meta charset='utf-8'>
      <meta name='viewport' content='width=device-width, initial-scale=1'>
      <title>18:00 日報</title>
      <style>
        body { font-family: Arial, sans-serif; background:#f6f7fb; padding:24px; color:#222; }
        .wrap { max-width:980px; margin:0 auto; }
        .card { background:#fff; border-radius:18px; box-shadow:0 8px 24px rgba(0,0,0,.06); padding:18px; margin-bottom:16px; }
        .item { border-top:1px solid #eee; padding:10px 0; }
        .item:first-child { border-top:none; }
        .status { display:inline-block; background:#eef3ff; color:#2b57d9; padding:4px 10px; border-radius:999px; font-size:14px; margin:6px 0; }
        .muted { color:#666; }
      </style>
    </head>
    <body><div class='wrap'><h2>📊 18:00 日報</h2>
    """

    for company in COMPANY_LIST:
        html += f"<div class='card'><h3>{company}</h3>"
        has_data = False

        cur.execute(
            """
            SELECT * FROM customers
            WHERE company = ? AND updated_at LIKE ?
            ORDER BY updated_at DESC
            """,
            (company, f"{today_str()}%"),
        )
        rows = cur.fetchall()

        for row in rows:
            has_data = True
            date_text = row["updated_at"][5:10].replace("-", "/") if row["updated_at"] else ""
            html += (
                f"<div class='item'>"
                f"<div><b>{date_text}-{row['customer_name']}</b></div>"
                f"<div class='muted'>來源：{get_group_name(row['source_group_id'])}</div>"
                f"<div class='status'>{row['latest_status'] or '待追蹤'}</div>"
                f"<div>{row['last_update'] or ''}</div>"
                f"</div>"
            )

        if not has_data:
            html += "<div class='item muted'>（今天無資料）</div>"

        html += "</div>"

    conn.close()
    html += "</div></body></html>"
    return html


@app.get("/debug/customers")
def debug_customers():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT * FROM customers ORDER BY updated_at DESC LIMIT 200")
    rows = [dict(r) for r in cur.fetchall()]
    conn.close()
    return {"count": len(rows), "items": rows}


@app.get("/debug/api-logs")
def debug_api_logs():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT * FROM api_logs ORDER BY id DESC LIMIT 100")
    rows = [dict(r) for r in cur.fetchall()]
    conn.close()
    return {"count": len(rows), "items": rows}


@app.get("/debug/parse")
def debug_parse(text: str = ""):
    clean = normalize_text(text)
    return {
        "input": clean,
        "blocks": split_multi_cases(clean),
        "a_blocks": detect_a_blocks(clean),
    }


@app.on_event("startup")
def startup():
    init_db()
    seed_groups()


if __name__ == "__main__":
    init_db()
    seed_groups()
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", "10000")))
