from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
import uvicorn
import requests
import os
import re
import sqlite3
from datetime import datetime
import uuid

app = FastAPI()

CHANNEL_ACCESS_TOKEN = os.getenv("CHANNEL_ACCESS_TOKEN", "")

# ===== 群組 ID =====
A_GROUP_ID = "Cb3579e75c94437ed22aafc7b1f6aecdd"   # A群
B_GROUP_ID = "Cd14f3ee775f1d9f5cfdafb223173cbef"   # B群
C_GROUP_ID = "C1a647fcb29a74842eceeb18e7a53823d"   # C群

# ===== Render Disk 永久保存 =====
DB_PATH = "/var/data/loan_system.db"

COMPANY_LIST = [
    "亞太", "和裕", "21", "貸救補", "第一", "分貝", "麻吉",
    "手機分期", "和裕商品", "和裕機車", "亞太商品", "亞太機車"
]
STATUS_WORDS = [
    "婉拒", "核准", "補件", "等保書", "退件", "不承作", "照會",
    "保密", "NA", "待撥款", "可送", "缺資料", "補資料"
]
DELETE_KEYWORDS = ["結案", "刪掉", "不追了", "全部不送", "已撥款結案"]
BLOCK_KEYWORDS = ["鼎信", "禾基"]

# ===== 本次最小修正：避免把備註/結案詞當姓名 =====
IGNORE_NAME_WORDS = {
    "信用不良", "不需要了", "不用了", "不要了", "結案", "補件", "核准", "婉拒",
    "照會", "等保書", "待撥款", "缺資料", "補資料", "資料補", "補來", "退件"
}

CHINESE_NAME_RE = re.compile(r"[\u4e00-\u9fff]{2,4}")
ID_RE = re.compile(r"[A-Z][12]\d{8}")
DATE_PREFIX_RE = re.compile(r"^\d{2,4}/\d{1,2}/\d{1,2}[-－]\s*")


# =========================
# 基本工具
# =========================
def now_iso() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def today_str() -> str:
    return datetime.now().strftime("%m/%d")


def short_id() -> str:
    return str(uuid.uuid4())[:8]


def get_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def extract_first_line(text: str) -> str:
    lines = [line.strip() for line in text.splitlines() if line.strip()]
    return lines[0] if lines else ""


def normalize_first_line(text: str) -> str:
    return DATE_PREFIX_RE.sub("", text).strip()


def extract_possible_names(text: str):
    return CHINESE_NAME_RE.findall(text)


def extract_name(text: str) -> str:
    first_line = normalize_first_line(extract_first_line(text))
    if not first_line:
        return ""

    first_line = re.split(r"[:：]|->", first_line, maxsplit=1)[0].strip()
    first_line = ID_RE.sub("", first_line.upper()).strip()
    first_line = re.split(r"結案|補件|婉拒|核准|照會|等保書|退件|缺資料|補資料|資料補|補來", first_line, maxsplit=1)[0].strip()

    if not first_line:
        return ""

    if "｜" in first_line:
        first_line = first_line.split("｜", 1)[0].strip()
    if "|" in first_line:
        first_line = first_line.split("|", 1)[0].strip()

    m = re.match(r"^[一-鿿]{2,4}", first_line)
    return m.group(0) if m else ""

def extract_id_no(text: str) -> str:
    m = ID_RE.search(text.upper())
    return m.group(0) if m else ""


def extract_company(text: str) -> str:
    for c in COMPANY_LIST:
        if c in text:
            return c
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
    if "->" in first:
        return True
    return False


def is_fallback_trigger(block: str) -> bool:
    first = normalize_first_line(extract_first_line(block))
    name = extract_name(first)
    company = extract_company(first)
    strong_words = ["婉拒", "核准", "補件", "等保書", "不承作", "照會", "保密", "NA"]
    has_strong = any(w in first for w in strong_words)
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


# =========================
# LINE API
# =========================
def push_text(to_group_id: str, text: str):
    if not CHANNEL_ACCESS_TOKEN:
        return

    url = "https://api.line.me/v2/bot/message/push"
    headers = {
        "Authorization": f"Bearer {CHANNEL_ACCESS_TOKEN}",
        "Content-Type": "application/json"
    }
    data = {
        "to": to_group_id,
        "messages": [{"type": "text", "text": text}]
    }
    requests.post(url, headers=headers, json=data, timeout=10)


def reply_text(reply_token: str, text: str):
    if not CHANNEL_ACCESS_TOKEN or reply_token == "TEST":
        return

    url = "https://api.line.me/v2/bot/message/reply"
    headers = {
        "Authorization": f"Bearer {CHANNEL_ACCESS_TOKEN}",
        "Content-Type": "application/json"
    }
    data = {
        "replyToken": reply_token,
        "messages": [{"type": "text", "text": text}]
    }
    requests.post(url, headers=headers, json=data, timeout=10)


def make_quick_reply_item(label: str, text: str):
    return {
        "type": "action",
        "action": {
            "type": "message",
            "label": label[:20],
            "text": text
        }
    }


def reply_quick_reply(reply_token: str, text: str, items):
    if not CHANNEL_ACCESS_TOKEN or reply_token == "TEST":
        return

    url = "https://api.line.me/v2/bot/message/reply"
    headers = {
        "Authorization": f"Bearer {CHANNEL_ACCESS_TOKEN}",
        "Content-Type": "application/json"
    }
    data = {
        "replyToken": reply_token,
        "messages": [{
            "type": "text",
            "text": text,
            "quickReply": {"items": items}
        }]
    }
    requests.post(url, headers=headers, json=data, timeout=10)


# =========================
# 資料庫初始化
# =========================
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
def create_customer_record(name: str, id_no: str, company: str, source_group_id: str, text: str):
    conn = get_conn()
    cur = conn.cursor()
    now = now_iso()
    case_id = short_id()

    cur.execute("""
    INSERT INTO customers (
        case_id, customer_name, id_no, source_group_id, company,
        last_update, status, created_at, updated_at
    )
    VALUES (?, ?, ?, ?, ?, ?, 'ACTIVE', ?, ?)
    """, (case_id, name, id_no, source_group_id, company, text, now, now))

    cur.execute("""
    INSERT INTO case_logs (
        case_id, customer_name, id_no, company, message_text, from_group_id, created_at
    )
    VALUES (?, ?, ?, ?, ?, ?, ?)
    """, (case_id, name, id_no, company, text, source_group_id, now))

    conn.commit()
    conn.close()
    return case_id


def update_customer(case_id: str, company: str, text: str, from_group_id: str, status: str | None = None, name: str | None = None, source_group_id: str | None = None):
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
            text, from_group_id, now
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


def save_pending_action(action_id: str, action_type: str, payload: str):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
    INSERT OR REPLACE INTO pending_actions (action_id, action_type, payload, created_at)
    VALUES (?, ?, ?, ?)
    """, (action_id, action_type, payload, now_iso()))
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
    line = line.strip()
    if not line:
        return False

    if ID_RE.search(line.upper()):
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


def split_multi_cases(text: str):
    """
    多筆分段：
    - 空白行分隔
    - 單獨一行 / 分隔
    - 新案件主句開新段
    - 黑名單/查詢次數/過多 這類補充不會被拆成獨立案件
    """
    text = text.strip()
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

            # 避免把「:備註」或「->備註」後面的中文誤當第二個姓名
            header_line = re.split(r"[:：]|->", first_line, maxsplit=1)[0].strip()
            id_match = ID_RE.search(header_line.upper())

            possible_names = extract_possible_names(header_line)
            excluded = {
                "等保書", "婉拒", "核准", "補件", "退件", "亞太", "和裕",
                "無可知情", "黑名單", "查詢次數", "過多", "不承作", "貸救補",
                "信用不良", "不需要了", "不用了", "不要了", "結案", "缺資料",
                "補資料", "資料補", "補來"
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
                final_blocks.append(block)

    return final_blocks


# =========================
# 主要業務邏輯
# =========================
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


def send_reopen_case_buttons(reply_token: str, block_text: str, closed_rows):
    action_id = short_id()
    case_ids = ",".join([r["case_id"] for r in closed_rows[:10]])
    payload = f"{block_text}||{case_ids}"
    save_pending_action(action_id, "reopen_or_new_case", payload)

    items = []
    for c in closed_rows[:5]:
        label = f'重啟-{c["customer_name"]}-{c["company"] or "未填"}'
        text = f"REOPEN_CASE|{action_id}|{c['case_id']}"
        items.append(make_quick_reply_item(label, text))
    items.append(make_quick_reply_item("建立新案件", f"CREATE_NEW_CASE|{action_id}"))
    items.append(make_quick_reply_item("取消", f"CANCEL_REOPEN|{action_id}"))

    reply_quick_reply(reply_token, "⚠️ 此客戶案件已結案，請選擇要重啟原案件或建立新案件", items)


def handle_bc_case_block(block_text: str, source_group_id: str, reply_token: str):
  # ===== 先判斷是不是動作（補件 / 結案 / 婉拒 / 核准...）=====
action_words = ["結案","補件","婉拒","核准","照會","退件","等保書","缺資料","補資料"]

if any(w in block_text for w in action_words):
    name = extract_name(block_text)

    if not name:
        return None

    # 先找 ACTIVE
    active_rows = find_active_by_name(name)
    if active_rows:
        customer = active_rows[0]

        update_customer(
            customer["case_id"],
            extract_company(block_text) or customer["company"] or "",
            block_text,
            source_group_id
        )

        # 🔥 業務群同步回貼到 A 群
        push_text(A_GROUP_ID, block_text)

        return f"已更新客戶：{name}"

    # 沒 ACTIVE，再找是否有已結案案件
    any_rows = find_any_by_name(name)
    closed_rows = [r for r in any_rows if r["status"] != "ACTIVE"]

    if closed_rows:
        send_reopen_case_buttons(reply_token, block_text, closed_rows)
        return "QUICK_REPLY_SENT"

    return f"⚠️ 找不到案件：{name}"

    name = extract_name(block_text)
    id_no = extract_id_no(block_text)
    company = extract_company(block_text)

    # 避免把「信用不良 / 不需要了 / 結案」這類字當姓名建案
    if not name or name in IGNORE_NAME_WORDS:
        return None

    if is_blocked(block_text):
        return "❌ 含禁止關鍵字，已略過"

    # 如果沒有公司、沒有身分證、也不是明確格式/狀態，業務群不自動建案
    if not id_no and not company and not is_format_trigger(block_text) and not contains_status_word(block_text) and not is_closed_text(block_text):
        return None

    # 若目前沒有 ACTIVE 案件，但同名有已結案案件，則詢問重啟 / 新建
    active_rows = find_active_by_name(name)
    if not active_rows:
        any_rows = find_any_by_name(name)
        closed_rows = [r for r in any_rows if r["status"] != "ACTIVE"]
        if closed_rows and (contains_status_word(block_text) or is_closed_text(block_text)):
            send_reopen_case_buttons(reply_token, block_text, closed_rows)
            return "QUICK_REPLY_SENT"

    if id_no:
        existing = find_active_by_id_no(id_no)

        if existing:
            if existing["source_group_id"] == source_group_id:
                update_customer(
                    existing["case_id"],
                    company or existing["company"] or "",
                    block_text,
                    source_group_id,
                    name=name
                )
                return f"🔄 已更新客戶：{name}"

            if "轉件" in block_text or "轉" in block_text:
                update_customer(
                    existing["case_id"],
                    company or existing["company"] or "",
                    block_text,
                    source_group_id,
                    name=name,
                    source_group_id=source_group_id
                )
                return f"➡️ 已轉移客戶到{get_group_name(source_group_id)}：{name}"

            action_id = short_id()
            payload = f"{existing['case_id']}||{source_group_id}||{block_text}||{name}"
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
            if r["source_group_id"] == source_group_id
        ]
        if len(same_group_rows) == 1:
            row = same_group_rows[0]
            update_customer(
                row["case_id"],
                company or row["company"] or "",
                block_text,
                source_group_id
            )
            return f"🔄 已更新客戶：{name}"

    create_customer_record(name, id_no, company, source_group_id, block_text)
    return f"🆕 已建立客戶：{name}"


def send_ambiguous_case_buttons(reply_token: str, block_text: str, matches):
    action_id = short_id()
    case_ids = ",".join([m["case_id"] for m in matches])
    payload = f"{block_text}||{case_ids}"
    save_pending_action(action_id, "route_a_case", payload)

    items = []
    for c in matches[:10]:
        label = f"{c['customer_name']}-{c['company'] or '未填'}-{get_group_name(c['source_group_id'])}"
        text = f"SELECT_CASE|{action_id}|{c['case_id']}"
        items.append(make_quick_reply_item(label, text))

    reply_quick_reply(reply_token, "⚠️ 多筆同名客戶，請選擇要回貼的案件", items)


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
    new_status = "CLOSED" if is_closed_text(block_text) else None

    update_customer(
        customer["case_id"],
        company,
        block_text,
        A_GROUP_ID,
        status=new_status
    )

    push_text(customer["source_group_id"], block_text)

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

        case_id, target_group_id, block_text, new_name = action["payload"].split("||", 3)

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
            source_group_id=target_group_id
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

        block_text, _ = action["payload"].split("||", 1)

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

        update_customer(case_id, company, block_text, A_GROUP_ID, status=new_status)
        push_text(customer["source_group_id"], block_text)

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

        block_text, _ = action["payload"].split("||", 1)

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
            status="ACTIVE"
        )
        reply_text(reply_token, f"✅ 已重啟案件：{customer['customer_name']}")
        delete_pending_action(action_id)
        return True

    if text.startswith("CREATE_NEW_CASE|"):
        _, action_id = text.split("|", 1)
        action = get_pending_action(action_id)

        if not action or action["action_type"] != "reopen_or_new_case":
            reply_text(reply_token, "⚠️ 找不到建立新案件資料")
            return True

        block_text, _ = action["payload"].split("||", 1)
        name = extract_name(block_text)
        id_no = extract_id_no(block_text)
        company = extract_company(block_text)
        create_customer_record(name, id_no, company, A_GROUP_ID, block_text)
        reply_text(reply_token, f"🆕 已建立新案件：{name}")
        delete_pending_action(action_id)
        return True

    if text.startswith("CANCEL_REOPEN|"):
        _, action_id = text.split("|", 1)
        delete_pending_action(action_id)
        reply_text(reply_token, "✅ 已取消")
        return True

    return False


# =========================
# API
# =========================
@app.post("/callback")
async def callback(request: Request):
    body = await request.json()

    for event in body.get("events", []):
        if event.get("type") != "message":
            continue

        message = event.get("message", {})
        if message.get("type") != "text":
            continue

        text = message["text"].strip()
        reply_token = event["replyToken"]
        group_id = event.get("source", {}).get("groupId")

        if not text:
            continue

        # 按鈕命令
        if handle_command_text(text, reply_token):
            continue

        # ===== B / C 群 =====
        if group_id in [B_GROUP_ID, C_GROUP_ID]:
            raw_text = text
            clean_text = raw_text.replace("@AI", "").replace("@ai", "").replace("#AI", "").replace("#ai", "").strip()

            blocks = split_multi_cases(clean_text)
            if not blocks:
                blocks = [clean_text]

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
            raw_text = text
            has_trigger = ("@ai" in raw_text.lower()) or ("#ai" in raw_text.lower())

            if has_trigger:
                clean_text = raw_text.replace("@AI", "").replace("@ai", "").replace("#AI", "").replace("#ai", "").strip()
                blocks = split_multi_cases(clean_text)
            else:
                if is_format_trigger(raw_text) or is_fallback_trigger(raw_text):
                    blocks = [raw_text]
                else:
                    return {"status": "ignored"}

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
    <a href="/report">看日報</a>
    """


@app.get("/report", response_class=HTMLResponse)
def report():
    conn = get_conn()
    cur = conn.cursor()

    html = "<h2>📊 日報</h2>"

    for company in COMPANY_LIST:
        html += f"<b>{company}</b><br>"
        has_data = False

        cur.execute("""
        SELECT * FROM customers
        WHERE status = 'ACTIVE' AND company = ?
        ORDER BY updated_at DESC
        """, (company,))
        rows = cur.fetchall()

        for row in rows:
            has_data = True
            date_text = row["updated_at"][5:10].replace("-", "/") if row["updated_at"] else ""
            html += (
                f"{date_text}"
                f"-{row['customer_name']}"
                f"-{row['company'] or ''}"
                f"-{row['last_update'] or ''}"
                f"-{get_group_name(row['source_group_id'])}"
                f"<br>"
            )

        if not has_data:
            html += "（無資料）<br>"

        html += "——————————<br>"

    conn.close()
    return html


@app.on_event("startup")
def startup():
    init_db()
    seed_groups()


if __name__ == "__main__":
    init_db()
    seed_groups()
    uvicorn.run(app, host="0.0.0.0", port=10000)
