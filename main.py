from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
import uvicorn
import requests
import os
import re
import sqlite3
from datetime import datetime

app = FastAPI()

CHANNEL_ACCESS_TOKEN = os.getenv("CHANNEL_ACCESS_TOKEN", "")

# ===== 群組 ID =====
A_GROUP_ID = "Cb3579e75c94437ed22aafc7b1f6aecdd"   # A群
B_GROUP_ID = "Cd14f3ee775f1d9f5cfdafb223173cbef"   # B群
C_GROUP_ID = "C1a647fcb29a74842eceeb18e7a53823d"   # C群

# ===== 永久磁碟資料庫 =====
DB_PATH = "/var/data/loan_system.db"

COMPANY_LIST = ["亞太", "和裕", "21", "貸救補", "第一", "分貝", "麻吉", "手機分期"]
STATUS_WORDS = ["婉拒", "核准", "補件", "等保書", "退件", "不承作", "照會", "保密", "NA"]
DELETE_KEYWORDS = ["結案", "刪掉", "不追了", "全部不送", "已撥款結案"]
BLOCK_KEYWORDS = ["鼎信", "禾基"]

CHINESE_NAME_RE = re.compile(r"[\u4e00-\u9fff]{2,4}")
ID_RE = re.compile(r"[A-Z][12]\d{8}")
DATE_PREFIX_RE = re.compile(r"^\d{2,4}/\d{1,2}/\d{1,2}[-－]\s*")


# =========================
# 基本工具
# =========================
def now_iso() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def get_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def extract_first_line(text: str) -> str:
    lines = [line.strip() for line in text.splitlines() if line.strip()]
    return lines[0] if lines else ""


def normalize_first_line(text: str) -> str:
    return DATE_PREFIX_RE.sub("", text).strip()


def extract_name(text: str) -> str:
    first_line = normalize_first_line(extract_first_line(text))
    if not first_line:
        return ""

    # 格式：姓名｜方案｜狀態
    if "｜" in first_line:
        left = first_line.split("｜", 1)[0].strip()
        m = CHINESE_NAME_RE.search(left)
        return m.group(0) if m else ""

    # 格式：姓名 -> 補充
    if "->" in first_line:
        left = first_line.split("->", 1)[0].strip()
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


def contains_status_word(text: str) -> bool:
    return any(w in text for w in STATUS_WORDS)


def is_blocked(text: str) -> bool:
    return any(w in text for w in BLOCK_KEYWORDS)


def is_closed_text(text: str) -> bool:
    return any(w in text for w in DELETE_KEYWORDS)


def get_block_display_text(block_text: str) -> str:
    name = extract_name(block_text)
    first_line = extract_first_line(block_text)

    if name and name not in first_line:
        return f"{name}｜{first_line}"
    if name:
        return name
    return first_line


def get_group_name(group_id: str) -> str:
    if group_id == A_GROUP_ID:
        return "A群"
    if group_id == B_GROUP_ID:
        return "B群"
    if group_id == C_GROUP_ID:
        return "C群"
    return "未知群組"


# =========================
# LINE 回覆
# =========================
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


def push_text(group_id: str, text: str):
    if not CHANNEL_ACCESS_TOKEN:
        return

    url = "https://api.line.me/v2/bot/message/push"
    headers = {
        "Authorization": f"Bearer {CHANNEL_ACCESS_TOKEN}",
        "Content-Type": "application/json"
    }
    data = {
        "to": group_id,
        "messages": [{"type": "text", "text": text}]
    }
    requests.post(url, headers=headers, json=data, timeout=10)


# =========================
# DB
# =========================
def init_db():
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
    CREATE TABLE IF NOT EXISTS customers (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL,
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
        customer_id INTEGER NOT NULL,
        message_text TEXT NOT NULL,
        from_group_id TEXT NOT NULL,
        created_at TEXT NOT NULL
    )
    """)

    conn.commit()
    conn.close()


def create_customer(name: str, id_no: str, source_group_id: str, company: str, text: str):
    conn = get_conn()
    cur = conn.cursor()
    now = now_iso()

    cur.execute("""
    INSERT INTO customers (
        name, id_no, source_group_id, company, last_update, status, created_at, updated_at
    )
    VALUES (?, ?, ?, ?, ?, 'ACTIVE', ?, ?)
    """, (name, id_no, source_group_id, company, text, now, now))

    customer_id = cur.lastrowid

    cur.execute("""
    INSERT INTO case_logs (customer_id, message_text, from_group_id, created_at)
    VALUES (?, ?, ?, ?)
    """, (customer_id, text, source_group_id, now))

    conn.commit()
    conn.close()
    return customer_id


def update_customer(customer_id: int, company: str, text: str, status: str | None = None):
    conn = get_conn()
    cur = conn.cursor()
    now = now_iso()

    if status:
        cur.execute("""
        UPDATE customers
        SET company = ?, last_update = ?, status = ?, updated_at = ?
        WHERE id = ?
        """, (company, text, status, now, customer_id))
    else:
        cur.execute("""
        UPDATE customers
        SET company = ?, last_update = ?, updated_at = ?
        WHERE id = ?
        """, (company, text, now, customer_id))

    cur.execute("""
    INSERT INTO case_logs (customer_id, message_text, from_group_id, created_at)
    VALUES (?, ?, ?, ?)
    """, (customer_id, text, A_GROUP_ID, now))

    conn.commit()
    conn.close()


def find_customer_by_id_no(id_no: str):
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


def find_customers_by_name(name: str):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
    SELECT * FROM customers
    WHERE name = ? AND status = 'ACTIVE'
    ORDER BY updated_at DESC
    """, (name,))
    rows = cur.fetchall()
    conn.close()
    return rows


# =========================
# 觸發 / 分段
# =========================
def is_format_trigger(block: str) -> bool:
    first = normalize_first_line(extract_first_line(block))
    if "｜" in first and len([p for p in first.split("｜") if p.strip()]) >= 2:
        return True
    if "->" in first:
        return True
    return False


def is_fallback_trigger(block: str) -> bool:
    """
    容錯版：
    沒標記、沒格式時，只要第一行像案件主句就觸發
    """
    first = normalize_first_line(extract_first_line(block))
    name = extract_name(first)
    company = extract_company(first)
    strong_words = ["婉拒", "核准", "補件", "等保書", "不承作", "照會", "保密", "NA"]

    has_strong = any(w in first for w in strong_words)

    if name and (company or has_strong):
        return True

    return False


def looks_like_case_start(line: str) -> bool:
    line = line.strip()
    if not line:
        return False

    if ID_RE.search(line):
        return True

    first = normalize_first_line(line)
    name = extract_name(first)
    company = extract_company(first)
    strong_words = ["婉拒", "核准", "補件", "等保書", "退件", "不承作", "照會", "NA"]
    has_strong = any(w in first for w in strong_words)

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
    - 黑名單/查詢次數這種補充不會自己拆出去
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

        final_blocks.extend(blocks)

    return final_blocks


# =========================
# 主邏輯
# =========================
def handle_bc_text(text: str):
    name = extract_name(text)
    id_no = extract_id_no(text)
    company = extract_company(text)

    if not name:
        return None

    if is_blocked(text):
        return "❌ 含禁止關鍵字，已略過"

    customer = find_customer_by_id_no(id_no) if id_no else None

    if not customer and name:
        rows = find_customers_by_name(name)
        if len(rows) == 1 and not id_no:
            customer = rows[0]

    if customer:
        update_customer(customer["id"], company or customer["company"] or "", text)
        return f"🔄 已更新客戶：{name}"

    create_customer(name, id_no, B_GROUP_ID, company, text)
    return f"🆕 已建立客戶：{name}"


def handle_a_block(block_text: str):
    if is_blocked(block_text):
        return "❌ 含禁止關鍵字，已略過"

    name = extract_name(block_text)
    id_no = extract_id_no(block_text)
    company = extract_company(block_text)

    customer = None

    if id_no:
        customer = find_customer_by_id_no(id_no)

    if not customer and name:
        rows = find_customers_by_name(name)
        if len(rows) == 1:
            customer = rows[0]
        else:
            customer = None

    if not customer:
        return f"⚠️ 找不到對應客戶：{get_block_display_text(block_text)}"

    new_status = "CLOSED" if is_closed_text(block_text) else None

    update_customer(
        customer["id"],
        company or customer["company"] or "",
        block_text,
        status=new_status
    )

    push_text(customer["source_group_id"], block_text)

    if new_status == "CLOSED":
        return f"✅ 已結案並回貼到{get_group_name(customer['source_group_id'])}：{customer['name']}"

    return f"✅ 已回貼到{get_group_name(customer['source_group_id'])}：{customer['name']}"


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

        # ===== B / C群 =====
        if group_id in [B_GROUP_ID, C_GROUP_ID]:
            result = handle_bc_text(text)
            if result:
                reply_text(reply_token, result)
            continue

        # ===== A群 =====
        if group_id == A_GROUP_ID:
            raw_text = text
            has_trigger = ("@AI" in raw_text) or ("#AI" in raw_text)

            if has_trigger:
                clean_text = raw_text.replace("@AI", "").replace("#AI", "").strip()

                # 有標記時：
                # 沒有明確分隔就整段一筆
                has_explicit_separator = (
                    re.search(r"\n\s*/\s*\n", clean_text) is not None or
                    re.search(r"\n\s*\n+", clean_text) is not None
                )

                if has_explicit_separator:
                    blocks = split_multi_cases(clean_text)
                else:
                    blocks = [clean_text]
            else:
                # 沒標記：只接受格式觸發或容錯版
                if is_format_trigger(raw_text) or is_fallback_trigger(raw_text):
                    blocks = [raw_text]
                else:
                    return {"status": "ignored"}

            results = []
            for idx, block in enumerate(blocks, start=1):
                result = handle_a_block(block)
                if result:
                    if len(blocks) > 1:
                        results.append(f"第{idx}筆：{result}")
                    else:
                        results.append(result)

            if results:
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
                f"-{row['name']}"
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


if __name__ == "__main__":
    init_db()
    uvicorn.run(app, host="0.0.0.0", port=10000)
