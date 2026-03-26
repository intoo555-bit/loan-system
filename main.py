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
  
