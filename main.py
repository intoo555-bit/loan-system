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

COMPANY_LIST = ["亞太", "和裕", "21"]
DELETE_KEYWORDS = ["結案", "刪掉", "不追了", "全部不送", "已撥款結案"]
BLOCK_KEYWORDS = ["鼎信", "禾基"]

# Render Disk 永久保存路徑
DB_PATH = "/var/data/loan_system.db"

CHINESE_NAME_RE = re.compile(r"[\u4e00-\u9fff]{2,4}")
ID_RE = re.compile(r"[A-Z][12]\d{8}")


# =========================
# 資料庫
# =========================
def get_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


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

    data = [
        (A_GROUP_ID, "A群", "A_GROUP", 1, now),
        (B_GROUP_ID, "B群", "SALES_GROUP", 1, now),
        (C_GROUP_ID, "C群", "SALES_GROUP", 1, now),
    ]

    for row in data:
        cur.execute("""
        INSERT OR IGNORE INTO groups (group_id, group_name, group_type, is_active, created_at)
        VALUES (?, ?, ?, ?, ?)
        """, row)

    conn.commit()
    conn.close()


# =========================
# 工具
# =========================
def now_iso():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def today_str():
    return datetime.now().strftime("%m/%d")


def short_id():
    return str(uuid.uuid4())[:8]


def extract_first_line(text):
    lines = [line.strip() for line in text.splitlines() if line.strip()]
    return lines[0] if lines else ""


def extract_possible_names(text):
    return CHINESE_NAME_RE.findall(text)


def extract_name(text):
    text = text.strip()
    if not text:
        return ""

    first_line = extract_first_line(text)
    first_line = re.sub(r"^\d{2,4}/\d{1,2}/\d{1,2}[-－]\s*", "", first_line)

    match = CHINESE_NAME_RE.search(first_line)
    if match:
        return match.group(0)

    return first_line.split("（")[0].split(" ")[0].strip()


def get_block_display_text(block_text):
    name = extract_name(block_text)
    first_line = extract_first_line(block_text)

    if name and name not in first_line:
        return f"{name}｜{first_line}"

    if name:
        return name

    return first_line


def extract_id_no(text):
    match = ID_RE.search(text.upper())
    return match.group(0) if match else ""


def extract_company(text):
    for c in COMPANY_LIST:
        if c in text:
            return c
    return ""


def get_group_name(group_id):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT group_name FROM groups WHERE group_id = ?", (group_id,))
    row = cur.fetchone()
    conn.close()
    if row:
        return row["group_name"]
    return "未知群組"


def is_blocked(text):
    return any(w in text for w in BLOCK_KEYWORDS)


def is_closed_text(text):
    return any(w in text for w in DELETE_KEYWORDS)


def looks_like_case_line(line):
    line = line.strip()
    if not line:
        return False

    if ID_RE.search(line):
        return True

    has_name = CHINESE_NAME_RE.search(line) is not None
    has_company = any(c in line for c in COMPANY_LIST)
    has_status_word = any(w in line for w in ["婉拒", "核准", "補件", "等保書", "退件", "無可知情"])

    return has_name and (has_company or has_status_word)


def is_valid_case_block(block):
    """
    B/C群比較嚴格：
    1. 有身分證
    2. 或有日期前綴 + 姓名
    """
    has_id = bool(extract_id_no(block))
    has_name = bool(extract_name(block))
    has_date_prefix = bool(re.search(r"\d{2,4}/\d{1,2}/\d{1,2}[-－]", block))

    if has_id:
        return True
    if has_date_prefix and has_name:
        return True

    return False


def is_valid_case_block_for_a(block):
    """
    A群放寬規則：
    1. 有身分證
    2. 或有日期前綴 + 姓名
    3. 或有姓名 + 公司
    4. 或有姓名 + 常見進度詞
    """
    has_id = bool(extract_id_no(block))
    has_name = bool(extract_name(block))
    has_date_prefix = bool(re.search(r"\d{2,4}/\d{1,2}/\d{1,2}[-－]", block))
    has_company = bool(extract_company(block))
    has_status_word = any(w in block for w in [
        "補件", "婉拒", "核准", "退件", "等保書", "照會",
        "待撥款", "缺", "不足", "補資料", "可送", "轉件", "NA"
    ])

    if has_id:
        return True
    if has_date_prefix and has_name:
        return True
    if has_name and has_company:
        return True
    if has_name and has_status_word:
        return True

    return False


def split_multi_cases(text):
    """
    支援：
    1. 一客戶一行
    2. 一客戶 + 多行補充
    3. 用 / 分隔
    4. 空白行分隔
    5. 同一行多名字拆分
    """
    text = text.strip()
    if not text:
        return []

    text = re.sub(r"\n\s*/\s*\n", "\n<<<SPLIT>>>\n", text)
    text = re.sub(r"\n\s*\n+", "\n<<<SPLIT>>>\n", text)

    raw_parts = [p.strip() for p in text.split("<<<SPLIT>>>") if p.strip()]
    final_parts = []

    for part in raw_parts:
        lines = [line.strip() for line in part.splitlines() if line.strip()]
        if not lines:
            continue

        blocks = []
        current_block = []

        for line in lines:
            if looks_like_case_line(line):
                if current_block:
                    blocks.append("\n".join(current_block))
                    current_block = []
            current_block.append(line)

        if current_block:
            blocks.append("\n".join(current_block))

        for block in blocks:
            block_lines = [line.strip() for line in block.splitlines() if line.strip()]
            if not block_lines:
                continue

            first_line = block_lines[0]
            rest_lines = block_lines[1:]
            id_match = ID_RE.search(first_line)

            possible_names = extract_possible_names(first_line)
            possible_names = [
                n for n in possible_names
                if n not in ["等保書", "婉拒", "核准", "補件", "退件", "亞太", "和裕", "無可知情"]
            ]

            if not id_match and len(possible_names) >= 2:
                remain = first_line
                for name in possible_names:
                    remain = remain.replace(name, "", 1)
                remain = remain.strip()

                for name in possible_names:
                    new_block_lines = [f"{name} {remain}".strip()]
                    new_block_lines.extend(rest_lines)
                    final_parts.append("\n".join(new_block_lines).strip())
            else:
                final_parts.append(block)

    return final_parts


# =========================
# LINE
# =========================
def push_text(to_group_id, text):
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


def reply_text(reply_token, text):
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


def make_quick_reply_item(label, text):
    return {
        "type": "action",
        "action": {
            "type": "message",
            "label": label[:20],
            "text": text
        }
    }


def reply_quick_reply(reply_token, text, items):
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
# DB CRUD
# =========================
def create_customer_record(name, id_no, company, source_group_id, text):
    conn = get_conn()
    cur = conn.cursor()

    case_id = short_id()
    now = now_iso()

    cur.execute("""
    INSERT INTO customers (
        case_id, customer_name, id_no, source_group_id, company,
        last_update, status, created_at, updated_at
    )
    VALUES (?, ?, ?, ?, ?, ?, 'ACTIVE', ?, ?)
    """, (
        case_id, name, id_no, source_group_id, company,
        text, now, now
    ))

    cur.execute("""
    INSERT INTO case_logs (
        case_id, customer_name, id_no, company, message_text, from_group_id, created_at
    )
    VALUES (?, ?, ?, ?, ?, ?, ?)
    """, (
        case_id, name, id_no, company, text, source_group_id, now
    ))

    conn.commit()
    conn.close()
    return case_id


def find_active_by_id_no(id_no):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
    SELECT * FROM customers
    WHERE status = 'ACTIVE' AND id_no = ?
    LIMIT 1
    """, (id_no,))
    row = cur.fetchone()
    conn.close()
    return row


def find_active_by_name(name):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
    SELECT * FROM customers
    WHERE status = 'ACTIVE' AND customer_name = ?
    ORDER BY updated_at DESC
    """, (name,))
    rows = cur.fetchall()
    conn.close()
    return rows


def update_customer(case_id, **kwargs):
    if not kwargs:
        return

    conn = get_conn()
    cur = conn.cursor()

    fields = []
    values = []

    for k, v in kwargs.items():
        fields.append(f"{k} = ?")
        values.append(v)

    fields.append("updated_at = ?")
    values.append(now_iso())

    values.append(case_id)

    sql = f"UPDATE customers SET {', '.join(fields)} WHERE case_id = ?"
    cur.execute(sql, values)
    conn.commit()
    conn.close()


def insert_case_log(case_id, customer_name, id_no, company, text, from_group_id):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
    INSERT INTO case_logs (
        case_id, customer_name, id_no, company, message_text, from_group_id, created_at
    )
    VALUES (?, ?, ?, ?, ?, ?, ?)
    """, (
        case_id, customer_name, id_no, company, text, from_group_id, now_iso()
    ))
    conn.commit()
    conn.close()


def save_pending_action(action_id, action_type, payload):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
    INSERT OR REPLACE INTO pending_actions (action_id, action_type, payload, created_at)
    VALUES (?, ?, ?, ?)
    """, (action_id, action_type, payload, now_iso()))
    conn.commit()
    conn.close()


def get_pending_action(action_id):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT * FROM pending_actions WHERE action_id = ?", (action_id,))
    row = cur.fetchone()
    conn.close()
    return row


def delete_pending_action(action_id):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("DELETE FROM pending_actions WHERE action_id = ?", (action_id,))
    conn.commit()
    conn.close()


# =========================
# 業務邏輯
# =========================
def handle_bc_case_block(block_text, source_group_id, reply_token):
    name = extract_name(block_text)
    id_no = extract_id_no(block_text)
    company = extract_company(block_text)

    if not name:
        return None

    if is_blocked(block_text):
        return "❌ 含禁止轉發關鍵字，已攔截"

    if id_no:
        existing = find_active_by_id_no(id_no)

        if existing:
            if existing["source_group_id"] == source_group_id:
                update_customer(
                    existing["case_id"],
                    customer_name=name,
                    company=company or existing["company"],
                    last_update=block_text
                )
                insert_case_log(existing["case_id"], name, id_no, company or existing["company"], block_text, source_group_id)
                return f"🔄 已更新客戶：{name}"

            if "轉件" in block_text or "轉" in block_text:
                update_customer(
                    existing["case_id"],
                    source_group_id=source_group_id,
                    customer_name=name,
                    company=company or existing["company"],
                    last_update=block_text
                )
                insert_case_log(existing["case_id"], name, id_no, company or existing["company"], block_text, source_group_id)
                return f"➡️ 已轉移客戶到{get_group_name(source_group_id)}：{name}"

            action_id = short_id()
            payload = f"{existing['case_id']}||{source_group_id}||{block_text}"
            save_pending_action(action_id, "transfer_customer", payload)

            old_group = get_group_name(existing["source_group_id"])
            new_group = get_group_name(source_group_id)

            items = [
                make_quick_reply_item(f"轉到{new_group}", f"CONFIRM_TRANSFER|{action_id}"),
                make_quick_reply_item("維持原群", f"CANCEL_TRANSFER|{action_id}")
            ]

            reply_quick_reply(
                reply_token,
                f"⚠️ 此客戶已存在於{old_group}，要改到{new_group}嗎？",
                items
            )
            return "QUICK_REPLY_SENT"

    if not id_no:
        rows = find_active_by_name(name)
        same_group_rows = [r for r in rows if r["source_group_id"] == source_group_id and not r["id_no"]]
        if len(same_group_rows) == 1:
            row = same_group_rows[0]
            update_customer(
                row["case_id"],
                company=company or row["company"],
                last_update=block_text
            )
            insert_case_log(row["case_id"], name, "", company or row["company"], block_text, source_group_id)
            return f"🔄 已更新客戶：{name}"

    create_customer_record(name, id_no, company, source_group_id, block_text)
    return f"🆕 已建立客戶：{name}"


def send_ambiguous_case_buttons(reply_token, block_text, matches):
    action_id = short_id()
    case_ids = ",".join([m["case_id"] for m in matches])
    payload = f"{block_text}||{case_ids}"
    save_pending_action(action_id, "route_a_case", payload)

    items = []
    for c in matches[:10]:
        label = f"{c['customer_name']}-{c['company'] or '未填'}-{get_group_name(c['source_group_id'])}"
        text = f"SELECT_CASE|{action_id}|{c['case_id']}"
        items.append(make_quick_reply_item(label, text))

    reply_quick_reply(
        reply_token,
        "⚠️ 多筆同名客戶，請選擇要回貼的案件",
        items
    )


def find_customer_for_a_block(block_text, reply_token):
    name = extract_name(block_text)
    id_no = extract_id_no(block_text)

    if id_no:
        row = find_active_by_id_no(id_no)
        if row:
            return row

    matches = find_active_by_name(name)

    if len(matches) == 1:
        return matches[0]

    if len(matches) > 1:
        send_ambiguous_case_buttons(reply_token, block_text, matches)
        return "MULTIPLE"

    return None


def handle_a_case_block(block_text, reply_token):
    if is_blocked(block_text):
        return "❌ 含禁止轉發關鍵字，已攔截"

    customer = find_customer_for_a_block(block_text, reply_token)

    if customer == "MULTIPLE":
        return "QUICK_REPLY_SENT"

    if not customer:
        display_text = get_block_display_text(block_text)
        return f"⚠️ 找不到對應客戶：{display_text}"

    company = extract_company(block_text) or customer["company"]

    if is_closed_text(block_text):
        update_customer(
            customer["case_id"],
            company=company,
            last_update=block_text,
            status="CLOSED"
        )
        insert_case_log(customer["case_id"], customer["customer_name"], customer["id_no"], company, block_text, A_GROUP_ID)
        push_text(customer["source_group_id"], f"{block_text}\n（此客戶已結案）")
        return f"✅ 已結案並回貼到{get_group_name(customer['source_group_id'])}：{customer['customer_name']}"

    update_customer(
        customer["case_id"],
        company=company,
        last_update=block_text
    )
    insert_case_log(customer["case_id"], customer["customer_name"], customer["id_no"], company, block_text, A_GROUP_ID)
    push_text(customer["source_group_id"], block_text)
    return f"✅ 已回貼到{get_group_name(customer['source_group_id'])}：{customer['customer_name']}"


def handle_command_text(text, reply_token):
    if text.startswith("CONFIRM_TRANSFER|"):
        _, action_id = text.split("|", 1)
        action = get_pending_action(action_id)

        if not action or action["action_type"] != "transfer_customer":
            reply_text(reply_token, "⚠️ 找不到待確認資料")
            return True

        case_id, target_group_id, block_text = action["payload"].split("||", 2)

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
            source_group_id=target_group_id,
            last_update=block_text
        )
        insert_case_log(case_id, customer["customer_name"], customer["id_no"], customer["company"], block_text, target_group_id)

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

        company = extract_company(block_text) or customer["company"]

        if is_closed_text(block_text):
            update_customer(
                case_id,
                company=company,
                last_update=block_text,
                status="CLOSED"
            )
            insert_case_log(case_id, customer["customer_name"], customer["id_no"], company, block_text, A_GROUP_ID)
            push_text(customer["source_group_id"], f"{block_text}\n（此客戶已結案）")
            reply_text(reply_token, f"✅ 已結案並回貼到{get_group_name(customer['source_group_id'])}：{customer['customer_name']}")
        else:
            update_customer(
                case_id,
                company=company,
                last_update=block_text
            )
            insert_case_log(case_id, customer["customer_name"], customer["id_no"], company, block_text, A_GROUP_ID)
            push_text(customer["source_group_id"], block_text)
            reply_text(reply_token, f"✅ 已回貼到{get_group_name(customer['source_group_id'])}：{customer['customer_name']}")

        delete_pending_action(action_id)
        return True

    return False


# =========================
# API
# =========================
@app.post("/callback")
async def callback(request: Request):
    body = await request.json()

    for event in body.get("events", []):
        source = event.get("source", {})
        group_id = source.get("groupId")
        print("GROUP ID:", group_id)

        if event.get("type") != "message":
            continue

        message = event.get("message", {})
        if message.get("type") != "text":
            continue

        text = message["text"]
        reply_token = event["replyToken"]

        if handle_command_text(text, reply_token):
            continue

        blocks = split_multi_cases(text)

        if group_id in [B_GROUP_ID, C_GROUP_ID]:
            valid_blocks = [block for block in blocks if is_valid_case_block(block)]
        elif group_id == A_GROUP_ID:
            valid_blocks = [block for block in blocks if is_valid_case_block_for_a(block)]
        else:
            valid_blocks = []

        if not valid_blocks:
            return {"status": "ignored"}

        if group_id in [B_GROUP_ID, C_GROUP_ID]:
            results = []
            quick_reply_sent = False

            for idx, block in enumerate(valid_blocks, start=1):
                result = handle_bc_case_block(block, group_id, reply_token)
                if result == "QUICK_REPLY_SENT":
                    quick_reply_sent = True
                    break
                if result:
                    if len(valid_blocks) > 1:
                        results.append(f"第{idx}筆：{result}")
                    else:
                        results.append(result)

            if not quick_reply_sent and results:
                reply_text(reply_token, "\n".join(results))
            continue

        if group_id == A_GROUP_ID:
            results = []
            quick_reply_sent = False

            for idx, block in enumerate(valid_blocks, start=1):
                result = handle_a_case_block(block, reply_token)
                if result == "QUICK_REPLY_SENT":
                    quick_reply_sent = True
                    break
                if result:
                    if len(valid_blocks) > 1:
                        results.append(f"第{idx}筆：{result}")
                    else:
                        results.append(result)

            if not quick_reply_sent and results:
                reply_text(reply_token, "\n".join(results))
            continue

        reply_text(reply_token, "⚠️ 此群組未設定")

    return {"status": "ok"}


@app.get("/", response_class=HTMLResponse)
def home():
    return """
    <h2>貸款系統 SQLite版</h2>
    <form action="/send">
        <input name="msg" style="width:300px"/>
        <button>送出</button>
    </form>
    <br>
    <a href="/report">看日報</a>
    """


@app.get("/send")
def send(msg: str):
    blocks = split_multi_cases(msg)
    valid_blocks = [block for block in blocks if is_valid_case_block(block)]

    results = []
    for idx, block in enumerate(valid_blocks, start=1):
        result = handle_bc_case_block(block, B_GROUP_ID, "TEST")
        if result != "QUICK_REPLY_SENT" and result:
            if len(valid_blocks) > 1:
                results.append(f"第{idx}筆：{result}")
            else:
                results.append(result)

    return "\n".join(results)


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

        for customer in rows:
            has_data = True
            update_date = customer["updated_at"][5:10].replace("-", "/") if customer["updated_at"] else ""
            html += (
                f"{update_date}"
                f"-{customer['customer_name']}"
                f"-{customer['company']}"
                f"-{customer['last_update']}"
                f"-{get_group_name(customer['source_group_id'])}"
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
    uvicorn.run(app, host="0.0.0.0", port=8000)
