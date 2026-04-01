from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
import uvicorn
import requests
import os
import re
import sqlite3
import json
from datetime import datetime
import uuid
from typing import Optional, List, Tuple

app = FastAPI()

CHANNEL_ACCESS_TOKEN = os.getenv("CHANNEL_ACCESS_TOKEN", "")

A_GROUP_ID = "Cb3579e75c94437ed22aafc7b1f6aecdd"
B_GROUP_ID = "Cd14f3ee775f1d9f5cfdafb223173cbef"
C_GROUP_ID = "C1a647fcb29a74842eceeb18e7a53823d"

DB_PATH = os.getenv("DB_PATH", "/var/data/loan_system.db")

COMPANY_LIST = [
    "喬美", "亞太", "和裕", "21", "貸10", "鄉", "銀", "C", "商", "代",
    "和裕商品", "和裕機車", "亞太商品", "亞太機車",
    "手機分期", "貸救補", "第一", "分貝", "麻吉"
]

STATUS_WORDS = [
    "婉拒", "核准", "補件", "補資料", "缺資料", "等保書", "退件", "不承作", "照會",
    "待撥款", "可送", "保密", "NA", "無可知情", "聯絡人皆可知情",
    "補行照", "行照", "補照會", "照會時段", "補時段", "補照片", "補聯徵", "補保人", "補保證人",
]

DELETE_KEYWORDS = ["結案", "刪掉", "不追了", "全部不送", "已撥款結案"]
BLOCK_KEYWORDS = ["鼎信", "禾基"]

IGNORE_NAME_WORDS = {
    "信用不良", "不需要了", "不用了", "不要了", "結案", "補件", "核准", "婉拒",
    "照會", "等保書", "待撥款", "缺資料", "補資料", "資料補", "補來", "退件",
    "助理", "AI助理", "先生", "小姐", "無可知情", "聯絡人皆可知情"
}

ACTION_KEYWORDS = [
    "結案", "刪掉", "不追了", "全部不送", "已撥款結案",
    "補件", "補資料", "缺資料", "婉拒", "核准", "照會",
    "退件", "等保書", "不承作", "待撥款",
    "補行照", "行照", "補照會", "照會時段", "補時段",
    "補保人", "保證人", "補保證人", "補聯徵", "補照片",
    "無可知情", "聯絡人皆可知情", "保密", "NA"
]

CHINESE_NAME_RE = re.compile(r"[\u4e00-\u9fff]{2,4}")
ID_RE = re.compile(r"[A-Z][12]\d{8}")
DATE_PREFIX_RE = re.compile(r"^\s*\d{2,4}/\d{1,2}/\d{1,2}\s*[-－]?\s*")
DATE_NAME_ID_INLINE_RE = re.compile(
    r"^\s*(\d{2,4}/\d{1,2}/\d{1,2})\s*[-－]?\s*([\u4e00-\u9fff]{2,4})\s*([A-Z][12]\d{8})",
    re.IGNORECASE,
)
DATE_NAME_ONLY_RE = re.compile(
    r"^\s*(\d{2,4}/\d{1,2}/\d{1,2})\s*[-－]?\s*([\u4e00-\u9fff]{2,4})\s*$"
)
CASE_HEADER_RE = re.compile(
    r"^\s*(\d{2,4}/\d{1,2}/\d{1,2})\s*[-－]?\s*([\u4e00-\u9fff]{2,4})",
    re.IGNORECASE,
)

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

def has_ai_trigger(text: str) -> bool:
    t = (text or "").lower()
    return ("@ai" in t) or ("#ai" in t)

def strip_ai_trigger(text: str) -> str:
    return re.sub(r"(@ai助理|#ai助理|@ai|#ai)", "", text, flags=re.IGNORECASE).strip()

def extract_name(text: str) -> str:
    lines = [x.strip() for x in (text or "").splitlines() if x.strip()]
    if not lines:
        return ""
    for line in lines[:2]:
        m = CASE_HEADER_RE.search(line)
        if m:
            return m.group(2)
    full = lines[0]
    full = re.split(r"[:：]|->", full, maxsplit=1)[0].strip()
    if "｜" in full:
        full = full.split("｜", 1)[0].strip()
    full = re.split(
        r"[:：]|->|結案|補件|婉拒|核准|照會|退件|等保書|缺資料|補資料|補行照|補照會|補時段|無可知情|聯絡人皆可知情",
        full,
        maxsplit=1
    )[0].strip()
    m = CHINESE_NAME_RE.search(full)
    return m.group(0) if m else ""

def extract_id_no(text: str) -> str:
    m = ID_RE.search((text or "").upper())
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

def has_business_action_word(text: str) -> bool:
    return any(w in text for w in ACTION_KEYWORDS) or ("補" in text)

def should_push_to_a_group(text: str) -> bool:
    t = text or ""
    return has_ai_trigger(t) and ("補" in t)

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

def push_text(to_group_id: str, text: str):
    if not CHANNEL_ACCESS_TOKEN:
        return False, "未設定 CHANNEL_ACCESS_TOKEN"
    url = "https://api.line.me/v2/bot/message/push"
    headers = {
        "Authorization": f"Bearer {CHANNEL_ACCESS_TOKEN}",
        "Content-Type": "application/json"
    }
    data = {"to": to_group_id, "messages": [{"type": "text", "text": text[:4900]}]}
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
        "Content-Type": "application/json"
    }
    data = {"replyToken": reply_token, "messages": [{"type": "text", "text": text[:4900]}]}
    try:
        requests.post(url, headers=headers, json=data, timeout=10)
    except Exception:
        pass

def make_quick_reply_item(label: str, text: str):
    return {"type": "action", "action": {"type": "message", "label": label[:20], "text": text}}

def reply_quick_reply(reply_token: str, text: str, items):
    if not CHANNEL_ACCESS_TOKEN or reply_token == "TEST":
        return
    url = "https://api.line.me/v2/bot/message/reply"
    headers = {
        "Authorization": f"Bearer {CHANNEL_ACCESS_TOKEN}",
        "Content-Type": "application/json"
    }
    data = {"replyToken": reply_token, "messages": [{
        "type": "text", "text": text[:4900], "quickReply": {"items": items[:13]}
    }]}
    try:
        requests.post(url, headers=headers, json=data, timeout=10)
    except Exception:
        pass

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
    cols = [r[1] for r in cur.execute("PRAGMA table_info(customers)").fetchall()]
    if "route_plan" not in cols:
        cur.execute("ALTER TABLE customers ADD COLUMN route_plan TEXT")
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
        (B_GROUP_ID, "B群", "B_GROUP", 1, now),
        (C_GROUP_ID, "C群", "C_GROUP", 1, now),
    ]
    for row in rows:
        cur.execute("""
        INSERT OR IGNORE INTO groups (group_id, group_name, group_type, is_active, created_at)
        VALUES (?, ?, ?, ?, ?)
        """, row)
    conn.commit()
    conn.close()

def create_customer_record(name: str, id_no: str, company: str, source_group_id: str, text: str, route_plan: str = ""):
    conn = get_conn()
    cur = conn.cursor()
    now = now_iso()
    case_id = short_id()
    cur.execute("""
    INSERT INTO customers (
        case_id, customer_name, id_no, source_group_id, company, route_plan,
        last_update, status, created_at, updated_at
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

def update_customer(case_id: str, company: Optional[str], text: Optional[str], from_group_id: str,
                    status: Optional[str] = None, name: Optional[str] = None,
                    source_group_id: Optional[str] = None, route_plan: Optional[str] = None):
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
        """, (row["case_id"], row["customer_name"], row["id_no"], row["company"], text, from_group_id, now))
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

def save_pending_action(action_id: str, action_type: str, payload: dict):
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
    d = dict(row)
    try:
        d["payload"] = json.loads(d["payload"])
    except Exception:
        d["payload"] = {}
    return d

def delete_pending_action(action_id: str):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("DELETE FROM pending_actions WHERE action_id = ?", (action_id,))
    conn.commit()
    conn.close()

def looks_like_case_start(line: str) -> bool:
    line = line.strip()
    if not line:
        return False
    if line.startswith("@") or line.startswith("#"):
        return False
    if line in {"助理", "AI助理"}:
        return False
    return bool(CASE_HEADER_RE.search(line))

def split_multi_cases(text: str):
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
        current = []
        for line in lines:
            if looks_like_case_start(line) and current:
                final_blocks.append("\n".join(current).strip())
                current = []
            current.append(line)
        if current:
            final_blocks.append("\n".join(current).strip())
    return final_blocks

def looks_like_new_case_block(block: str) -> bool:
    lines = [x.strip() for x in block.splitlines() if x.strip()]
    if not lines:
        return False
    first = lines[0]
    if DATE_NAME_ID_INLINE_RE.search(first.upper()):
        return True
    if DATE_NAME_ONLY_RE.search(first):
        for line in lines[1:3]:
            if ID_RE.search(line.upper()):
                return True
    compact = block.replace(" ", "").replace("\n", "")
    if DATE_NAME_ID_INLINE_RE.search(compact.upper()):
        return True
    return False

def extract_route_plan(block: str) -> str:
    lines = [x.strip() for x in block.splitlines() if x.strip()]
    if not lines:
        return ""
    first = lines[0]
    if CASE_HEADER_RE.search(first):
        after = CASE_HEADER_RE.sub("", first, count=1).strip()
        after = after.lstrip("-－")
        if after and "/" in after:
            return after
    compact = block.replace(" ", "").replace("\n", "")
    m = CASE_HEADER_RE.search(compact)
    if m:
        after = compact[m.end():].lstrip("-－")
        if "/" in after and not ID_RE.search(after.upper()):
            return after
    return ""

def looks_like_route_plan_block(block: str) -> bool:
    lines = [x.strip() for x in block.splitlines() if x.strip()]
    if not lines:
        return False
    first = lines[0]
    if not CASE_HEADER_RE.search(first):
        return False
    if extract_id_no(block):
        return False
    if "/" not in block:
        return False
    return bool(extract_route_plan(block))

def send_reopen_case_buttons(reply_token: str, block_text: str, closed_rows):
    action_id = short_id()
    closed_rows = sorted(closed_rows, key=lambda x: x["updated_at"], reverse=True)[:1]
    payload = {"block_text": block_text, "case_ids": [r["case_id"] for r in closed_rows]}
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
    payload = {"block_text": block_text, "case_ids": [m["case_id"] for m in matches]}
    save_pending_action(action_id, "route_a_case", payload)
    items = []
    for c in matches[:10]:
        label = f"{c['customer_name']}-{c['company'] or '未填'}-{get_group_name(c['source_group_id'])}"
        text = f"SELECT_CASE|{action_id}|{c['case_id']}"
        items.append(make_quick_reply_item(label, text))
    reply_quick_reply(reply_token, "⚠️ 多筆同名客戶，請選擇要回貼的案件", items)

def send_transfer_case_buttons(reply_token: str, block_text: str, existing_row, source_group_id: str):
    action_id = short_id()
    payload = {
        "case_id": existing_row["case_id"],
        "target_group_id": source_group_id,
        "block_text": block_text,
        "name": extract_name(block_text),
    }
    save_pending_action(action_id, "transfer_customer", payload)
    old_group = get_group_name(existing_row["source_group_id"])
    new_group = get_group_name(source_group_id)
    items = [
        make_quick_reply_item(f"沿用{old_group}", f"KEEP_CASE|{action_id}"),
        make_quick_reply_item(f"改到{new_group}", f"CONFIRM_TRANSFER|{action_id}"),
        make_quick_reply_item("建立新案件", f"CREATE_NEW_CROSS|{action_id}"),
        make_quick_reply_item("取消", f"CANCEL_TRANSFER|{action_id}"),
    ]
    reply_quick_reply(reply_token, "⚠️ 別的群組已有同名客戶，請選擇要沿用原案件、改到目前群組，或建立新案件", items)

def handle_new_case_block(block_text: str, source_group_id: str):
    name = extract_name(block_text)
    id_no = extract_id_no(block_text)
    company = extract_company(block_text)
    if not name or not id_no or name in IGNORE_NAME_WORDS:
        return None
    existing_by_id = find_active_by_id_no(id_no)
    if existing_by_id:
        if existing_by_id["source_group_id"] == source_group_id:
            update_customer(existing_by_id["case_id"], company or existing_by_id["company"] or "", block_text, source_group_id, name=name)
            return f"已更新客戶：{name}"
        return ("SAME_ID_OTHER_GROUP", existing_by_id)
    create_customer_record(name, id_no, company, source_group_id, block_text)
    return f"🆕 已建立客戶：{name}"

def handle_route_plan_block(block_text: str, source_group_id: str):
    name = extract_name(block_text)
    route_plan = extract_route_plan(block_text)
    if not name or not route_plan:
        return None
    rows = find_active_by_name(name)
    same_group_rows = [r for r in rows if r["source_group_id"] == source_group_id]
    target = same_group_rows[0] if same_group_rows else (rows[0] if rows else None)
    if not target:
        return f"⚠️ 找不到可更新排序的客戶：{name}"
    update_customer(target["case_id"], target["company"] or "", block_text, source_group_id, route_plan=route_plan)
    return f"已更新排序：{name}"

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

def handle_bc_case_block(block_text: str, source_group_id: str, reply_token: str):
    if is_blocked(block_text):
        return "❌ 含禁止關鍵字，已略過"
    if looks_like_new_case_block(block_text):
        result = handle_new_case_block(block_text, source_group_id)
        if isinstance(result, tuple) and result[0] == "SAME_ID_OTHER_GROUP":
            send_transfer_case_buttons(reply_token, block_text, result[1], source_group_id)
            return "QUICK_REPLY_SENT"
        return result
    if looks_like_route_plan_block(block_text):
        return handle_route_plan_block(block_text, source_group_id)
    if not has_ai_trigger(block_text):
        return None
    clean_block = strip_ai_trigger(block_text)
    name = extract_name(clean_block)
    id_no = extract_id_no(clean_block)
    company = extract_company(clean_block)
    if not name or name in IGNORE_NAME_WORDS:
        return None
    pushed_to_a = False
    pushed_error = ""
    if should_push_to_a_group(block_text):
        ok, err = push_text(A_GROUP_ID, clean_block)
        if ok:
            pushed_to_a = True
        else:
            pushed_error = err
    if id_no:
        existing_by_id = find_active_by_id_no(id_no)
        if existing_by_id:
            if existing_by_id["source_group_id"] == source_group_id:
                new_status = "CLOSED" if is_closed_text(clean_block) else existing_by_id["status"]
                update_customer(existing_by_id["case_id"], company or existing_by_id["company"] or "", clean_block, source_group_id, status=new_status, name=name)
                msgs = [f"已更新客戶：{name}"]
                if pushed_to_a:
                    msgs.append(f"✅ 已回貼A群：{name}")
                elif pushed_error:
                    msgs.append(f"❌ A群回貼失敗：{pushed_error}")
                return "\n".join(msgs)
            send_transfer_case_buttons(reply_token, clean_block, existing_by_id, source_group_id)
            return "QUICK_REPLY_SENT"
        create_customer_record(name, id_no, company, source_group_id, clean_block)
        msgs = [f"🆕 已建立客戶：{name}"]
        if pushed_to_a:
            msgs.append(f"✅ 已回貼A群：{name}")
        elif pushed_error:
            msgs.append(f"❌ A群回貼失敗：{pushed_error}")
        return "\n".join(msgs)
    rows = find_active_by_name(name)
    same_group_rows = [r for r in rows if r["source_group_id"] == source_group_id]
    other_group_rows = [r for r in rows if r["source_group_id"] != source_group_id]
    has_action_word = has_business_action_word(clean_block)
    if same_group_rows:
        target = same_group_rows[0]
        if target["status"] != "ACTIVE" and has_action_word and not is_closed_text(clean_block):
            send_reopen_case_buttons(reply_token, clean_block, [target])
            return "QUICK_REPLY_SENT"
        new_status = "CLOSED" if is_closed_text(clean_block) else target["status"]
        update_customer(target["case_id"], company or target["company"] or "", clean_block, source_group_id, status=new_status)
        msgs = [f"已更新客戶：{name}"]
        if pushed_to_a:
            msgs.append(f"✅ 已回貼A群：{name}")
        elif pushed_error:
            msgs.append(f"❌ A群回貼失敗：{pushed_error}")
        return "\n".join(msgs)
    if other_group_rows:
        send_transfer_case_buttons(reply_token, clean_block, other_group_rows[0], source_group_id)
        return "QUICK_REPLY_SENT"
    create_customer_record(name, "", company, source_group_id, clean_block)
    msgs = [f"🆕 已建立客戶：{name}"]
    if pushed_to_a:
        msgs.append(f"✅ 已回貼A群：{name}")
    elif pushed_error:
        msgs.append(f"❌ A群回貼失敗：{pushed_error}")
    return "\n".join(msgs)

def handle_a_case_block(block_text: str, reply_token: str):
    if is_blocked(block_text):
        return "❌ 含禁止關鍵字，已略過"
    clean_block = strip_ai_trigger(block_text)
    customer = find_customer_for_a_block(clean_block, reply_token)
    if customer == "MULTIPLE":
        return "QUICK_REPLY_SENT"
    if not customer:
        return f"⚠️ 找不到對應客戶：{get_block_display_text(clean_block)}"
    company = extract_company(clean_block) or customer["company"] or ""
    new_status = "CLOSED" if is_closed_text(clean_block) else None
    update_customer(customer["case_id"], company, clean_block, A_GROUP_ID, status=new_status)
    ok, err = push_text(customer["source_group_id"], clean_block)
    if not ok:
        return f"❌ 找到客戶：{customer['customer_name']}，但回貼{get_group_name(customer['source_group_id'])}失敗：{err}"
    if new_status == "CLOSED":
        return f"✅ 已結案並回貼到{get_group_name(customer['source_group_id'])}：{customer['customer_name']}"
    return f"✅ 已回貼到{get_group_name(customer['source_group_id'])}：{customer['customer_name']}"

def handle_command_text(text: str, reply_token: str):
    if text.startswith("KEEP_CASE|"):
        _, action_id = text.split("|", 1)
        action = get_pending_action(action_id)
        if not action:
            reply_text(reply_token, "⚠️ 找不到待確認資料")
            return True
        payload = action["payload"]
        reply_text(reply_token, f"✅ 已沿用原案件：{payload.get('name','')}")
        delete_pending_action(action_id)
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
        new_name = payload.get("name", "")
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("SELECT * FROM customers WHERE case_id = ?", (case_id,))
        customer = cur.fetchone()
        conn.close()
        if not customer:
            reply_text(reply_token, "⚠️ 原案件不存在")
            delete_pending_action(action_id)
            return True
        update_customer(case_id, extract_company(block_text) or customer["company"] or "", block_text, target_group_id, name=new_name, source_group_id=target_group_id)
        reply_text(reply_token, f"✅ 已改到{get_group_name(target_group_id)}")
        delete_pending_action(action_id)
        return True
    if text.startswith("CREATE_NEW_CROSS|"):
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
        create_customer_record(name, id_no, company, target_group_id, block_text)
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
        payload = action["payload"]
        block_text = payload.get("block_text", "")
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
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("SELECT * FROM customers WHERE case_id = ?", (case_id,))
        customer = cur.fetchone()
        conn.close()
        if not customer:
            reply_text(reply_token, "⚠️ 原案件不存在")
            delete_pending_action(action_id)
            return True
        update_customer(case_id, extract_company(block_text) or customer["company"] or "", block_text, customer["source_group_id"], status="ACTIVE")
        reply_text(reply_token, f"✅ 已重啟案件：{customer['customer_name']}")
        delete_pending_action(action_id)
        return True
    if text.startswith("CREATE_NEW_CASE|"):
        _, action_id = text.split("|", 1)
        action = get_pending_action(action_id)
        if not action or action["action_type"] != "reopen_or_new_case":
            reply_text(reply_token, "⚠️ 找不到建立新案件資料")
            return True
        payload = action["payload"]
        block_text = payload.get("block_text", "")
        name = extract_name(block_text)
        id_no = extract_id_no(block_text)
        company = extract_company(block_text)
        create_customer_record(name, id_no, company, B_GROUP_ID, block_text)
        reply_text(reply_token, f"🆕 已建立新案件：{name}")
        delete_pending_action(action_id)
        return True
    if text.startswith("CANCEL_REOPEN|"):
        _, action_id = text.split("|", 1)
        delete_pending_action(action_id)
        reply_text(reply_token, "✅ 已取消")
        return True
    return False

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
        if handle_command_text(text, reply_token):
            continue
        if group_id in [B_GROUP_ID, C_GROUP_ID]:
            blocks = split_multi_cases(text)
            if not blocks:
                blocks = [text]
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
        if group_id == A_GROUP_ID:
            if not has_ai_trigger(text):
                continue
            clean_text = strip_ai_trigger(text)
            if clean_text in {"", "助理", "AI助理"}:
                continue
            blocks = split_multi_cases(clean_text)
            if not blocks:
                blocks = [clean_text]
            results = []
            quick_reply_sent = False
            for idx, block in enumerate(blocks, start=1):
                result = handle_a_case_block(block, reply_token)
                if result == "QUICK_REPLY_SENT":
                    quick_reply_sent = True
                    continue
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
        WHERE status = 'ACTIVE' AND (company = ? OR route_plan LIKE ?)
        ORDER BY updated_at DESC
        """, (company, f"%{company}%"))
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
