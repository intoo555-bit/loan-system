from fastapi import FastAPI, Request, BackgroundTasks
from fastapi.responses import HTMLResponse
import uvicorn
import requests as req_lib
import os
import re
import sqlite3
import json
from datetime import datetime
import uuid
from typing import Optional, List, Dict, Any

app = FastAPI()

CHANNEL_ACCESS_TOKEN = os.getenv("CHANNEL_ACCESS_TOKEN", "")
A_GROUP_ID = os.getenv("A_GROUP_ID", "Cb3579e75c94437ed22aafc7b1f6aecdd")
B_GROUP_ID = "Cd14f3ee775f1d9f5cfdafb223173cbef"
C_GROUP_ID = "C1a647fcb29a74842eceeb18e7a53823d"
DB_PATH = os.getenv("DB_PATH", "/var/data/loan_system.db")

# 日報三段結構
REPORT_SECTION_1 = [
    "麻吉", "和潤", "中租", "裕融", "21汽車", "亞太", "創鉅", "21",
    "第一", "合信", "興達", "和裕", "鄉民", "喬美",
    "分貝汽車", "分貝機車", "貸救補", "預付手機分期", "融易", "手機分期",
    "送件", "待撥款",
]
REPORT_SECTION_2 = [
    "銀行", "零卡", "商品貸", "代書", "當舖專案", "核准",
]
REPORT_SECTION_3 = [
    "房地", "核准(房地)",
]
# 合併（用於其他地方參考）
REPORT_SECTIONS = REPORT_SECTION_1 + REPORT_SECTION_2 + REPORT_SECTION_3

# 公司辨識（解析訊息用）
COMPANY_LIST = [
    "和裕商品", "和裕機車", "亞太商品", "亞太機車", "手機分期", "貸救補",
    "分貝汽車", "分貝機車", "21汽車", "鄉民", "喬美", "麻吉", "亞太",
    "和裕", "第一", "合信", "興達", "中租", "裕融", "創鉅", "和潤",
    "銀行", "零卡", "商品貸", "代書", "當舖", "融易", "21",
    "分貝", "鄉", "銀", "C", "商", "代",
]

STATUS_WORDS = [
    "婉拒", "核准", "補件", "補資料", "等保書", "退件", "不承作", "照會",
    "保密", "NA", "待撥款", "可送", "缺資料", "無可知情", "聯絡人皆可知情",
    "補行照", "補照會", "補照片", "補時段", "補案件資料", "補聯徵", "補保人",
    "已補", "轉", "申覆",
]

ACTION_KEYWORDS = [
    "結案", "刪掉", "不追了", "全部不送", "已撥款結案",
    "補件", "補資料", "缺資料", "婉拒", "核准", "照會", "退件", "等保書",
    "不承作", "待撥款", "補行照", "補照會", "補照片", "補時段", "補案件資料",
    "補聯徵", "補保人", "保密", "無可知情", "聯絡人皆可知情", "已補",
]

DELETE_KEYWORDS = ["結案", "刪掉", "不追了", "全部不送", "已撥款結案", "違約金結案", "已支付違約金", "以收到違約金", "收到違約金", "違約金已收", "以收到違約金", "違約金"]
BLOCK_KEYWORDS = ["鼎信", "禾基"]

# 專案別名對照
COMPANY_ALIAS = {
    "熊速貸": "亞太商品",
    "工會機車動擔": "亞太工會",
    "機車動擔設定": "亞太機車",
    "維力商品貸": "和裕商品",
    "維力機車專": "和裕機車",
    "維力機車貸": "和裕機車",
}

APPROVAL_EXCLUDE_KEYWORDS = ["初估", "待補", "照會", "金主初估", "需補", "補資料才"]

IGNORE_NAME_SET = {
    "信用不良", "不需要了", "不用了", "不要了", "結案", "補件", "核准", "婉拒",
    "申覆", "申請", "待審", "初估",
    "照會", "等保書", "待撥款", "缺資料", "補資料", "資料補", "補來", "退件",
    "助理", "AI助理", "先生", "小姐", "無可知情", "聯絡人皆可知情", "下一家",
    "可知情", "聯絡人", "皆可知情", "不可知情", "無空間", "信用卡",
    "機車貸", "汽車貸", "商品貸", "無貸款", "合照後", "後補", "來補",
    "空間", "貸款", "申請", "提供", "保證", "聯徵", "繳息",
}

CHINESE_NAME_RE = re.compile(r"[\u4e00-\u9fff]{2,4}")
ID_RE = re.compile(r"[A-Z][12]\d{8}")

# 支援有無 - 的日期格式，如 115/3/2廖俊宏 或 115/3/2-廖俊宏
DATE_NAME_ID_INLINE_RE = re.compile(
    r"^\s*(\d{2,4}/\d{1,2}/\d{1,2})\s*[-－]?\s*([\u4e00-\u9fff]{2,4})\s*([A-Z][12]\d{8})",
    re.IGNORECASE,
)
DATE_NAME_ONLY_RE = re.compile(
    r"^\s*(\d{2,4}/\d{1,2}/\d{1,2})\s*[-－]?\s*([\u4e00-\u9fff]{2,4})(?:\s*$|\s*[-－/\u4e00-\u9fff])",
    re.IGNORECASE,
)
# 短日期：3/2廖俊宏 或 3/2-廖俊宏（有無-都支援）
SHORT_DATE_NAME_RE = re.compile(
    r"^\s*(\d{1,2}/\d{1,2})\s*[-－]?\s*([\u4e00-\u9fff]{2,4})"
)
# 送件順序格式：4/1-高郡惠-喬美/亞太/和裕
ROUTE_ORDER_RE = re.compile(
    r"^\s*(\d{1,4}/\d{1,2}(?:/\d{1,2})?)\s*[-－]\s*([\u4e00-\u9fff]{2,4})\s*[-－]\s*((?:[^\s/\n]+/)+[^\s/\n@]+)(?:\s*@AI)?\s*$",
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
    lines = [l.strip() for l in text.splitlines() if l.strip()]
    return lines[0] if lines else ""





def normalize_ai_text(text: str) -> str:
    """統一化文字：全形轉半形、去空白"""
    text = (text or "")
    # 全形＠→@、全形空格→半形
    text = text.replace("＠", "@").replace("　", " ")
    # 全形英文 Ａ-Ｚ ａ-ｚ → 半形
    result = ""
    for c in text:
        code = ord(c)
        if 0xFF01 <= code <= 0xFF5E:
            result += chr(code - 0xFEE0)
        else:
            result += c
    return result


def has_ai_trigger(text: str) -> bool:
    """支援全形半形大小寫，如 @AI / ＠AI / @ai / @ A I / ＠ＡＩ"""
    normalized = re.sub(r"\s+", "", normalize_ai_text(text))
    return bool(re.search(r"@ai|#ai", normalized, re.IGNORECASE))


def strip_ai_trigger(text: str) -> str:
    """去掉所有 @AI 觸發詞"""
    text = normalize_ai_text(text)
    return re.sub(r"(@ai助理|#ai助理|@ai|#ai)", "", text, flags=re.IGNORECASE).strip()


def is_blocked(text: str) -> bool:
    return any(w in (text or "") for w in BLOCK_KEYWORDS)


def is_closed_text(text: str) -> bool:
    return any(w in (text or "") for w in DELETE_KEYWORDS)


def contains_status_word(text: str) -> bool:
    return any(w in (text or "") for w in STATUS_WORDS)


def has_business_action_word(text: str) -> bool:
    return any(w in (text or "") for w in ACTION_KEYWORDS)


def extract_id_no(text: str) -> str:
    m = ID_RE.search((text or "").upper())
    return m.group(0) if m else ""


def extract_company(text: str) -> str:
    for alias, real in COMPANY_ALIAS.items():
        if alias in (text or ""):
            return real
    for c in COMPANY_LIST:
        if c in (text or ""):
            return c
    return ""


def get_group_name(group_id: str) -> str:
    if not group_id:
        return "未知群組"
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT group_name FROM groups WHERE group_id=?", (group_id,))
    row = cur.fetchone()
    conn.close()
    return row["group_name"] if row else "未知群組"


def get_sales_group_ids() -> List[str]:
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT group_id FROM groups WHERE group_type='SALES_GROUP' AND is_active=1")
    rows = cur.fetchall()
    conn.close()
    return [r["group_id"] for r in rows]


def get_admin_group_ids() -> List[str]:
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT group_id FROM groups WHERE group_type='ADMIN_GROUP' AND is_active=1")
    rows = cur.fetchall()
    conn.close()
    return [r["group_id"] for r in rows]


# =========================
# 送件順序工具
# =========================
def parse_route_order_line(line: str) -> Dict:
    """解析「4/1-高郡惠-喬美/亞太/和裕」，回傳 dict 或 {}"""
    m = ROUTE_ORDER_RE.match(line.strip())
    if not m:
        return {}
    companies_str = m.group(3).strip()
    companies = [c.strip() for c in companies_str.split("/") if c.strip()]
    if len(companies) < 2:
        return {}
    return {"date": m.group(1), "name": m.group(2), "companies": companies}


def is_route_order_line(line: str) -> bool:
    return bool(parse_route_order_line(line))


def make_route_json(companies: List[str], current_index: int = 0, history: List = None) -> str:
    return json.dumps({"order": companies, "current_index": current_index, "history": history or []}, ensure_ascii=False)


def parse_route_json(route_plan: str) -> Dict:
    if not route_plan:
        return {"order": [], "current_index": 0, "history": []}
    try:
        data = json.loads(route_plan)
        if isinstance(data, dict) and "order" in data:
            return data
    except Exception:
        pass
    return {"order": [], "current_index": 0, "history": []}


def get_current_company(route_plan: str) -> str:
    data = parse_route_json(route_plan)
    order, idx = data.get("order", []), data.get("current_index", 0)
    return order[idx] if order and 0 <= idx < len(order) else ""


def get_next_company(route_plan: str) -> str:
    data = parse_route_json(route_plan)
    order, idx = data.get("order", []), data.get("current_index", 0)
    next_idx = idx + 1
    return order[next_idx] if next_idx < len(order) else ""


def advance_route(route_plan: str, status: str) -> str:
    data = parse_route_json(route_plan)
    order, idx, history = data.get("order", []), data.get("current_index", 0), data.get("history", [])
    current = order[idx] if 0 <= idx < len(order) else ""
    if current:
        history.append({"company": current, "status": status, "date": now_iso()[:10]})
    data["current_index"] = idx + 1
    data["history"] = history
    return json.dumps(data, ensure_ascii=False)


def advance_route_to(route_plan: str, target: str, status: str):
    """轉到指定公司，回傳 (新json, ok, err)"""
    data = parse_route_json(route_plan)
    order, idx, history = data.get("order", []), data.get("current_index", 0), data.get("history", [])
    target_idx = next((i for i, c in enumerate(order) if target in c or c in target), None)
    if target_idx is None:
        return route_plan, False, f"找不到 {target} 在送件順序中"
    for i in range(idx, target_idx):
        history.append({"company": order[i], "status": status, "date": now_iso()[:10]})
    data["current_index"] = target_idx
    data["history"] = history
    return json.dumps(data, ensure_ascii=False), True, ""


# =========================
# 名稱解析
# =========================
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
    m = SHORT_DATE_NAME_RE.search(first)
    if m:
        result["date"], result["name"] = m.group(1), m.group(2)
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
        first_line, maxsplit=1,
    )[0].strip()
    if "｜" in first_line:
        first_line = first_line.split("｜", 1)[0].strip()
    m_name = CHINESE_NAME_RE.search(first_line)
    if m_name:
        result["name"] = m_name.group(0)
    return result


def extract_name(text: str) -> str:
    name = parse_header_fields(text).get("name", "")
    return name if name and name not in IGNORE_NAME_SET else ""


def looks_like_new_case_block(block: str) -> bool:
    f = parse_header_fields(block)
    return bool(f["date"] and f["name"] and f["id_no"])


def is_format_trigger(block: str) -> bool:
    first = extract_first_line(block)
    if "｜" in first and len([p for p in first.split("｜") if p.strip()]) >= 2:
        return True
    return "->" in first


def looks_like_case_start(line: str) -> bool:
    line = line.strip()
    if not line or re.match(r"^[\[【(（]\d+[\]】)）]", line):
        return False
    if line.startswith("@") or line.startswith("#") or line in {"助理", "AI助理"}:
        return False
    compact = re.sub(r"\s+", "", line)
    if DATE_NAME_ID_INLINE_RE.search(compact) or DATE_NAME_ONLY_RE.search(line):
        return True
    if SHORT_DATE_NAME_RE.search(line) or is_route_order_line(line) or is_format_trigger(line):
        return True
    name = extract_name(line)
    return bool(name and (extract_company(line) or contains_status_word(line)))


def split_multi_cases(text: str) -> List[str]:
    text = (text or "").strip()
    if not text:
        return []
    text = re.sub(r"\n\s*/\s*\n", "\n<<<SPLIT>>>\n", text)
    text = re.sub(r"\n\s*\n+", "\n<<<SPLIT>>>\n", text)
    raw_parts = [p.strip() for p in text.split("<<<SPLIT>>>") if p.strip()]
    final_blocks: List[str] = []
    for part in raw_parts:
        lines = [l.rstrip() for l in part.splitlines() if l.strip()]
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


# =========================
# LINE API
# =========================
def push_text(to_group_id: str, text: str):
    if not CHANNEL_ACCESS_TOKEN:
        return False, "未設定 CHANNEL_ACCESS_TOKEN"
    try:
        resp = req_lib.post(
            "https://api.line.me/v2/bot/message/push",
            headers={"Authorization": f"Bearer {CHANNEL_ACCESS_TOKEN}", "Content-Type": "application/json"},
            json={"to": to_group_id, "messages": [{"type": "text", "text": text[:4900]}]},
            timeout=10,
        )
        return (200 <= resp.status_code < 300), "" if 200 <= resp.status_code < 300 else f"LINE push失敗({resp.status_code})"
    except Exception as e:
        return False, str(e)


def reply_text(reply_token: str, text: str):
    if not CHANNEL_ACCESS_TOKEN or reply_token == "TEST":
        return
    try:
        req_lib.post(
            "https://api.line.me/v2/bot/message/reply",
            headers={"Authorization": f"Bearer {CHANNEL_ACCESS_TOKEN}", "Content-Type": "application/json"},
            json={"replyToken": reply_token, "messages": [{"type": "text", "text": text[:4900]}]},
            timeout=10,
        )
    except Exception:
        pass


def make_quick_reply_item(label: str, text: str):
    return {"type": "action", "action": {"type": "message", "label": label[:20], "text": text}}


def reply_quick_reply(reply_token: str, text: str, items):
    if not CHANNEL_ACCESS_TOKEN or reply_token == "TEST":
        return
    try:
        req_lib.post(
            "https://api.line.me/v2/bot/message/reply",
            headers={"Authorization": f"Bearer {CHANNEL_ACCESS_TOKEN}", "Content-Type": "application/json"},
            json={"replyToken": reply_token, "messages": [{"type": "text", "text": text[:4900], "quickReply": {"items": items[:13]}}]},
            timeout=10,
        )
    except Exception:
        pass


# =========================
# 資料庫
# =========================
def ensure_column(cur, table: str, column: str, definition: str):
    cur.execute(f"PRAGMA table_info({table})")
    cols = [row[1] for row in cur.fetchall()]
    if column not in cols:
        cur.execute(f"ALTER TABLE {table} ADD COLUMN {column} {definition}")


def init_db():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""CREATE TABLE IF NOT EXISTS groups (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        group_id TEXT UNIQUE NOT NULL, group_name TEXT,
        group_type TEXT NOT NULL, is_active INTEGER DEFAULT 1, created_at TEXT NOT NULL
    )""")
    cur.execute("""CREATE TABLE IF NOT EXISTS customers (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        case_id TEXT UNIQUE NOT NULL, customer_name TEXT NOT NULL,
        id_no TEXT, source_group_id TEXT NOT NULL,
        company TEXT, route_plan TEXT, current_company TEXT, report_section TEXT,
        last_update TEXT, status TEXT NOT NULL DEFAULT 'ACTIVE',
        created_at TEXT NOT NULL, updated_at TEXT NOT NULL
    )""")
    cur.execute("""CREATE TABLE IF NOT EXISTS case_logs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        case_id TEXT NOT NULL, customer_name TEXT NOT NULL,
        id_no TEXT, company TEXT, message_text TEXT NOT NULL,
        from_group_id TEXT NOT NULL, created_at TEXT NOT NULL
    )""")
    cur.execute("""CREATE TABLE IF NOT EXISTS pending_actions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        action_id TEXT UNIQUE NOT NULL, action_type TEXT NOT NULL,
        payload TEXT NOT NULL, created_at TEXT NOT NULL
    )""")
    # 自動補欄位，不破壞舊資料
    for col, defn in [
        ("route_plan", "TEXT"),
        ("current_company", "TEXT"),
        ("report_section", "TEXT"),
        ("approved_amount", "TEXT"),       # 核准金額
        ("disbursement_date", "TEXT"),     # 撥款日期
    ]:
        ensure_column(cur, "customers", col, defn)
    # groups 表新增業務群對應欄位
    ensure_column(cur, "groups", "linked_sales_group_id", "TEXT")
    conn.commit()
    conn.close()


def seed_groups():
    conn = get_conn()
    cur = conn.cursor()
    now = now_iso()
    for row in [
        (A_GROUP_ID, "A群", "A_GROUP", 1, now),
        (B_GROUP_ID, "B群", "SALES_GROUP", 1, now),
        (C_GROUP_ID, "C群", "SALES_GROUP", 1, now),
    ]:
        cur.execute("INSERT OR IGNORE INTO groups (group_id,group_name,group_type,is_active,created_at) VALUES (?,?,?,?,?)", row)
    conn.commit()
    conn.close()


# =========================
# DB CRUD
# =========================
def create_customer_record(name, id_no, company, source_group_id, text,
                            route_plan="", current_company="", report_section="") -> str:
    conn = get_conn()
    cur = conn.cursor()
    now, case_id = now_iso(), short_id()
    cur.execute("""INSERT INTO customers
        (case_id,customer_name,id_no,source_group_id,company,route_plan,current_company,report_section,last_update,status,created_at,updated_at)
        VALUES (?,?,?,?,?,?,?,?,?,'ACTIVE',?,?)""",
        (case_id, name, id_no, source_group_id, company, route_plan, current_company, report_section, text, now, now))
    cur.execute("INSERT INTO case_logs (case_id,customer_name,id_no,company,message_text,from_group_id,created_at) VALUES (?,?,?,?,?,?,?)",
        (case_id, name, id_no, company, text, source_group_id, now))
    conn.commit(); conn.close()
    return case_id


def update_customer(case_id, company=None, text=None, from_group_id="", status=None,
                    name=None, source_group_id=None, route_plan=None,
                    current_company=None, report_section=None,
                    approved_amount=None, disbursement_date=None):
    conn = get_conn()
    cur = conn.cursor()
    now = now_iso()
    fields, values = [], []
    for col, val in [("company", company), ("last_update", text), ("status", status),
                     ("customer_name", name), ("source_group_id", source_group_id),
                     ("route_plan", route_plan), ("current_company", current_company),
                     ("report_section", report_section),
                     ("approved_amount", approved_amount),
                     ("disbursement_date", disbursement_date)]:
        if val is not None:
            fields.append(f"{col} = ?"); values.append(val)
    fields.append("updated_at = ?"); values.append(now); values.append(case_id)
    cur.execute(f"UPDATE customers SET {', '.join(fields)} WHERE case_id=?", values)
    cur.execute("SELECT * FROM customers WHERE case_id=?", (case_id,))
    row = cur.fetchone()
    if row and text is not None:
        cur.execute("INSERT INTO case_logs (case_id,customer_name,id_no,company,message_text,from_group_id,created_at) VALUES (?,?,?,?,?,?,?)",
            (row["case_id"], row["customer_name"], row["id_no"], row["company"], text, from_group_id, now))
    conn.commit(); conn.close()


def find_active_by_id_no(id_no):
    if not id_no: return None
    conn = get_conn(); cur = conn.cursor()
    cur.execute("SELECT * FROM customers WHERE id_no=? AND status='ACTIVE' ORDER BY updated_at DESC LIMIT 1", (id_no,))
    row = cur.fetchone(); conn.close(); return row


def find_active_by_name(name):
    conn = get_conn(); cur = conn.cursor()
    cur.execute("SELECT * FROM customers WHERE customer_name=? AND status='ACTIVE' ORDER BY updated_at DESC", (name,))
    rows = cur.fetchall(); conn.close(); return rows


def find_any_by_name(name):
    conn = get_conn(); cur = conn.cursor()
    cur.execute("SELECT * FROM customers WHERE customer_name=? ORDER BY updated_at DESC", (name,))
    rows = cur.fetchall(); conn.close(); return rows


def find_any_by_id_no(id_no):
    if not id_no: return []
    conn = get_conn(); cur = conn.cursor()
    cur.execute("SELECT * FROM customers WHERE id_no=? ORDER BY updated_at DESC", (id_no,))
    rows = cur.fetchall(); conn.close(); return rows


def save_pending_action(action_id, action_type, payload):
    conn = get_conn(); cur = conn.cursor()
    cur.execute("INSERT OR REPLACE INTO pending_actions (action_id,action_type,payload,created_at) VALUES (?,?,?,?)",
        (action_id, action_type, json.dumps(payload, ensure_ascii=False), now_iso()))
    conn.commit(); conn.close()


def get_pending_action(action_id):
    conn = get_conn(); cur = conn.cursor()
    cur.execute("SELECT * FROM pending_actions WHERE action_id=?", (action_id,))
    row = cur.fetchone(); conn.close()
    if not row: return None
    data = dict(row)
    try: data["payload"] = json.loads(data["payload"])
    except: data["payload"] = {}
    return data


def delete_pending_action(action_id):
    conn = get_conn(); cur = conn.cursor()
    cur.execute("DELETE FROM pending_actions WHERE action_id=?", (action_id,))
    conn.commit(); conn.close()


# =========================
# 核准金額 / 撥款名單工具
# =========================
def extract_approved_amount(text: str) -> str:
    """
    從訊息抓取核准金額（改良版）。
    支援格式：
    - 20萬 / 6W / 6.5萬 / (5).(8)萬
    - 降撥4萬 / 本利攤50萬
    - 120,000 / 40000（大數字自動換算）
    - 核准金額20萬 / 最高核貸金額20萬
    注意：「核准30期」的30是期數不是金額，會自動跳過
    """
    # 1. 先找有萬/W字的（最可靠）
    wan_patterns = [
        r"\((\d+)\)\s*\.\s*\((\d+)\)\s*萬",   # (5).(8)萬
        r"降撥\s*(\d+(?:\.\d+)?)\s*萬",             # 降撥4萬
        r"本利攤\s*(\d+(?:\.\d+)?)\s*萬",           # 本利攤50萬
        r"金額[:：]?\s*(\d+(?:\.\d+)?)\s*萬",       # 金額20萬
        r"核[准貸]\s*金額?\s*[:：]?\s*(\d+(?:\.\d+)?)\s*萬",  # 核准金額20萬
        r"(\d+(?:\.\d+)?)\s*[Ww萬]",                 # 20萬 / 6W
    ]
    for pat in wan_patterns:
        m = re.search(pat, text)
        if m:
            if m.lastindex == 2:  # (5).(8)萬格式
                return f"{m.group(1)}.{m.group(2)}萬"
            raw = m.group(1)
            try:
                num = float(raw)
                if num > 0:
                    return f"{int(num)}萬" if num == int(num) else f"{num}萬"
            except:
                pass

    # 2. 找大數字（超過10000視為元，換算成萬）
    for m in re.finditer(r"(\d{1,3}(?:,\d{3})+|\d{5,})", text):
        num_str = m.group(1).replace(",", "")
        try:
            num = int(num_str)
            if num >= 10000:
                wan = num // 10000
                return f"{wan}萬"
        except:
            pass

    # 3. 最後才用「核准+數字」，但排除期數
    m = re.search(r"核准\s*(\d+)", text)
    if m:
        num = int(m.group(1))
        after = text[m.end():][:5]
        if num <= 60 and any(c in after for c in ["期", "，", ",", "/"]):
            pass  # 這是期數，跳過
        elif num > 0:
            return f"{num}萬"

    return ""



async def extract_approved_amount_with_ai(text: str) -> str:
    api_key = os.getenv("ANTHROPIC_API_KEY", "")
    if not api_key:
        return ""
    try:
        resp = req_lib.post(
            "https://api.anthropic.com/v1/messages",
            headers={"x-api-key": api_key, "anthropic-version": "2023-06-01", "content-type": "application/json"},
            json={"model": "claude-haiku-4-5-20251001", "max_tokens": 100, "messages": [{"role": "user", "content": f"""貸款核准訊息如下，請判斷是否為真正核准並抓出金額。
訊息：{text}
只回傳JSON，不要其他文字：{{"is_approved": true或false, "amount": "金額字串或空字串"}}
金額格式如：20萬、6萬、50萬。注意：期數不是金額，月付不是金額，初估/待補/需補/照會=false。"""}]},
            timeout=15,
        )
        if resp.status_code == 200:
            import json as jl
            result = resp.json()["content"][0]["text"].strip().replace("```json","").replace("```","").strip()
            data = jl.loads(result)
            if data.get("is_approved") and data.get("amount"):
                return data["amount"]
    except Exception as e:
        print(f"AI解析失敗: {e}")
    return ""


def parse_disbursement_list(text: str) -> Dict:
    """
    解析撥款名單格式：
    04/01 21 撥款名單
    吳翰杰
    蔡文杰
    ...
    04/01 亞太排撥
    洪莉萍

    回傳 {"04/01": {"21": ["吳翰杰","蔡文杰"], "亞太": ["洪莉萍"]}}
    """
    result = {}
    current_date = ""
    current_company = ""

    # 撥款名單標頭正則
    DISB_HEADER_RE = re.compile(
        r"(\d{1,2}/\d{1,2})\s*([\w一-鿿]+?)\s*(撥款名單|排撥|商品撥款|機車撥款|汽車撥款|撥款)",
        re.IGNORECASE
    )

    lines = text.splitlines()
    for line in lines:
        line = line.strip()
        if not line:
            continue

        m = DISB_HEADER_RE.search(line)
        if m:
            current_date = m.group(1)
            current_company = m.group(2).strip()
            if current_date not in result:
                result[current_date] = {}
            if current_company not in result[current_date]:
                result[current_date][current_company] = []
            continue

        # 名字行（2-4個中文字，或含英文的姓名）
        if current_date and current_company:
            name_m = re.match(r"^([一-鿿]{2,4})\s*$", line)
            if name_m:
                result[current_date][current_company].append(name_m.group(1))

    return result


def is_disbursement_list(text: str) -> bool:
    """判斷是否為撥款名單"""
    return bool(re.search(r"撥款名單|排撥|商品撥款|機車撥款|汽車撥款|撥款", text))


def get_admin_group_for_sales(sales_group_id: str) -> Optional[str]:
    """
    找對應的行政群ID。
    優先找有設定 linked_sales_group_id 對應的，
    找不到就找任何啟用的行政群（所有業務群共用一個行政群的情況）。
    """
    conn = get_conn(); cur = conn.cursor()
    # 先找有明確對應的
    cur.execute("""
        SELECT group_id FROM groups
        WHERE group_type='ADMIN_GROUP' AND linked_sales_group_id=? AND is_active=1
        LIMIT 1
    """, (sales_group_id,))
    row = cur.fetchone()
    if row:
        conn.close()
        return row["group_id"]
    # 找不到就用任何啟用的行政群
    cur.execute("""
        SELECT group_id FROM groups
        WHERE group_type='ADMIN_GROUP' AND is_active=1
        LIMIT 1
    """)
    row = cur.fetchone(); conn.close()
    return row["group_id"] if row else None


def get_all_admin_groups() -> List[str]:
    """取得所有啟用的行政群ID（用於廣播撥款名單）"""
    conn = get_conn(); cur = conn.cursor()
    cur.execute("SELECT group_id FROM groups WHERE group_type='ADMIN_GROUP' AND is_active=1")
    rows = cur.fetchall(); conn.close()
    return [r["group_id"] for r in rows]


# =========================
# 狀態摘要提取
# =========================
def extract_status_summary(first_line: str, customer_name: str) -> str:
    """
    從訊息第一行提取狀態關鍵字，用於日報顯示。
    例：「林美伶 等保書 21」→「等保書」
        「劉依庭21 婉拒 【1】...」→「婉拒」
        「王鴻銘 21 待補」→「待補」
    """
    if not first_line:
        return ""

    # 去掉客戶姓名
    text = first_line.replace(customer_name, "").strip()

    # 去掉日期
    text = re.sub(r"^\d{1,4}/\d{1,2}(/\d{1,2})?[-－]?\s*", "", text).strip()

    # 去掉公司名稱（COMPANY_LIST）
    for c in sorted(COMPANY_LIST, key=len, reverse=True):
        text = text.replace(c, "").strip()

    # 去掉【N】之後的內容
    text = re.sub(r"【\d+】.*", "", text).strip()

    # 去掉多餘標點
    text = re.sub(r"^[-－/\s]+|[-－/\s]+$", "", text).strip()

    # 只取前20字
    return text[:20] if text else ""


# =========================
# 日報產生
# =========================
def build_section_map(all_rows) -> Dict[str, List[str]]:
    """把客戶列表轉成 section_map"""
    section_map: Dict[str, List[str]] = {}
    for row in all_rows:
        section = row["report_section"] or row["current_company"] or row["company"] or "送件"
        updated = row["updated_at"] or ""
        date_str = updated[5:10].replace("-", "/") if updated else ""
        company_str = row["current_company"] or row["company"] or ""

        last_update = row["last_update"] or ""
        first_line = last_update.splitlines()[0].strip() if last_update.strip() else ""
        status_short = extract_status_summary(first_line, row["customer_name"])

        if section == "待撥款":
            created = row["created_at"] or ""
            created_date = created[5:10].replace("-", "/") if created else date_str
            amount = row["approved_amount"] or ""
            disb_date = row["disbursement_date"] or ""
            amount_str = f"-核准{amount}" if amount else ""
            disb_str = f"(撥款{disb_date})" if disb_date else "(待撥款)"
            line = f"{created_date}-{row['customer_name']}-{company_str}{amount_str}{disb_str}"
        else:
            line = f"{date_str}-{row['customer_name']}-{company_str}"
            if status_short:
                line += f"-{status_short}"
        section_map.setdefault(section, []).append(line)
    return section_map


def build_segment(sections: List[str], section_map: Dict, shown: set) -> str:
    """把指定 sections 的內容組成一段文字"""
    lines = []
    for sec in sections:
        if sec in section_map:
            lines.append(sec)
            lines.extend(section_map[sec])
            lines.append("——————————————")
            shown.add(sec)
    return "\n".join(lines)


def generate_report_lines(group_id: str) -> List[str]:
    """
    產生日報，分三段對話框：
    第1段：貸款方案（麻吉~手機分期）+ 送件 + 待撥款
    第2段：民間方案（銀行/零卡/商品貸/代書/當舖/核准）
    第3段：房地（房地/核准(房地)）
    每段都只在有客戶時才顯示，每段超過4500字再自動切割。
    """
    group_name = get_group_name(group_id)
    today = datetime.now().strftime("%m/%d")
    conn = get_conn(); cur = conn.cursor()
    cur.execute("SELECT * FROM customers WHERE source_group_id=? AND status='ACTIVE' ORDER BY updated_at DESC", (group_id,))
    all_rows = cur.fetchall(); conn.close()

    if not all_rows:
        return [f"📊 {group_name} 日報 {today}\n（目前無有效案件）"]

    section_map = build_section_map(all_rows)
    shown = set()
    segments = []

    # 第1段：貸款方案 + 送件 + 待撥款
    seg1 = build_segment(REPORT_SECTION_1, section_map, shown)
    if seg1:
        segments.append(f"📊 {group_name} 日報 {today}\n{seg1}")

    # 第2段：民間方案
    seg2 = build_segment(REPORT_SECTION_2, section_map, shown)
    if seg2:
        segments.append(seg2)

    # 第3段：房地
    seg3 = build_segment(REPORT_SECTION_3, section_map, shown)
    if seg3:
        segments.append(seg3)

    # 未歸類的（不在任何section的）補到第1段後面
    extra = []
    for sec, lines in section_map.items():
        if sec not in shown:
            extra.append(sec)
            extra.extend(lines)
            extra.append("——————————————")
    if extra:
        if segments:
            segments[0] += "\n" + "\n".join(extra)
        else:
            segments.append(f"📊 {group_name} 日報 {today}\n" + "\n".join(extra))

    if not segments:
        return [f"📊 {group_name} 日報 {today}\n（目前無有效案件）"]

    # 每段超過4500字再切割
    final_segments = []
    for seg in segments:
        if len(seg) <= 4500:
            final_segments.append(seg)
        else:
            lines = seg.splitlines()
            current = ""
            for line in lines:
                if len(current) + len(line) + 1 > 4500:
                    final_segments.append(current.strip())
                    current = line + "\n"
                else:
                    current += line + "\n"
            if current.strip():
                final_segments.append(current.strip())

    return final_segments


def search_customer_info(name: str, group_id: str) -> str:
    rows = find_any_by_name(name)
    active = [r for r in rows if r["status"] == "ACTIVE" and r["source_group_id"] == group_id]
    if not active:
        active = [r for r in rows if r["status"] == "ACTIVE"]
    if not active:
        closed = [r for r in rows if r["status"] != "ACTIVE"]
        if closed:
            return f"❌ {name} 已結案（{closed[0]['updated_at'][:10]}）"
        return f"❌ 找不到客戶：{name}"
    r = active[0]
    route_data = parse_route_json(r["route_plan"] or "")
    order, idx = route_data.get("order", []), route_data.get("current_index", 0)
    history = route_data.get("history", [])
    lines = [f"👤 {name}"]
    if r["id_no"]:
        lines.append(f"身分證：{r['id_no']}")
    lines.append(f"所屬群組：{get_group_name(r['source_group_id'])}")
    if order:
        current = order[idx] if idx < len(order) else "（已完成）"
        lines.append(f"目前送件：{current}（第{idx+1}/{len(order)}家）")
        if idx + 1 < len(order):
            lines.append(f"下一家：{order[idx+1]}")
        if history:
            lines.append("送件歷程：")
            for h in history[-3:]:
                lines.append(f"  {h.get('date','')} {h.get('company','')} → {h.get('status','')}")
    last_update = (r["last_update"] or "").strip()
    if last_update:
        last_lines = [l.strip() for l in last_update.splitlines() if l.strip()]
        if last_lines:
            lines.append(f"最新進度：{last_lines[0][:80]}")
            for extra in last_lines[1:4]:
                lines.append(f"  {extra[:80]}")
    return "\n".join(lines)


# =========================
# Quick Reply 按鈕
# =========================
def send_reopen_case_buttons(reply_token, block_text, closed_rows, source_group_id, push_to_a_after_reopen=False):
    action_id = short_id()
    closed_rows = sorted(closed_rows, key=lambda x: x["updated_at"], reverse=True)[:1]
    save_pending_action(action_id, "reopen_or_new_case", {
        "block_text": block_text, "case_ids": [r["case_id"] for r in closed_rows],
        "source_group_id": source_group_id, "push_to_a_after_reopen": push_to_a_after_reopen,
    })
    items = [make_quick_reply_item(f'重啟-{c["customer_name"]}-{c["company"] or "未填"}',
                                    f"REOPEN_CASE|{action_id}|{c['case_id']}") for c in closed_rows]
    items += [make_quick_reply_item("建立新案件", f"CREATE_NEW_CASE|{action_id}"),
              make_quick_reply_item("取消", f"CANCEL_REOPEN|{action_id}")]
    reply_quick_reply(reply_token, "⚠️ 此客戶案件已結案，請選擇要重啟原案件或建立新案件", items)


def send_ambiguous_case_buttons(reply_token, block_text, matches):
    action_id = short_id()
    save_pending_action(action_id, "route_a_case", {"block_text": block_text, "case_ids": [m["case_id"] for m in matches]})
    items = [make_quick_reply_item(
        f"{c['customer_name']}-{c['company'] or '未填'}-{get_group_name(c['source_group_id'])}",
        f"SELECT_CASE|{action_id}|{c['case_id']}") for c in matches[:10]]
    reply_quick_reply(reply_token, "⚠️ 多筆同名客戶，請選擇要回貼的案件", items)


def send_transfer_case_buttons(reply_token, customer, source_group_id, block_text, allow_new=True):
    action_id = short_id()
    save_pending_action(action_id, "transfer_customer", {
        "case_id": customer["case_id"], "target_group_id": source_group_id,
        "block_text": block_text, "name": extract_name(block_text) or customer["customer_name"],
    })
    old_g, new_g = get_group_name(customer["source_group_id"]), get_group_name(source_group_id)
    items = [make_quick_reply_item(f"沿用{old_g}", f"KEEP_OLD_CASE|{action_id}"),
             make_quick_reply_item(f"改到{new_g}", f"CONFIRM_TRANSFER|{action_id}")]
    if allow_new:
        items.append(make_quick_reply_item("建立新案件", f"CREATE_NEW_FROM_TRANSFER|{action_id}"))
    items.append(make_quick_reply_item("取消", f"CANCEL_TRANSFER|{action_id}"))
    reply_quick_reply(reply_token, f"⚠️ 別的群組已有同名客戶，請選擇操作", items)


def send_confirm_new_case_buttons(reply_token, block_text, existing_customer, source_group_id):
    action_id = short_id()
    save_pending_action(action_id, "confirm_new_case_with_existing_id", {
        "block_text": block_text, "source_group_id": source_group_id,
        "existing_case_id": existing_customer["case_id"],
    })
    old_g, new_g = get_group_name(existing_customer["source_group_id"]), get_group_name(source_group_id)
    name = extract_name(block_text) or existing_customer["customer_name"]
    items = [make_quick_reply_item(f"沿用{old_g}案件", f"USE_EXISTING_CASE|{action_id}"),
             make_quick_reply_item(f"在{new_g}建立新案件", f"FORCE_CREATE_NEW|{action_id}"),
             make_quick_reply_item("取消", f"CANCEL_NEW_CASE|{action_id}")]
    reply_quick_reply(reply_token, f"⚠️ {name} 身分證已存在於{old_g}，確定要在{new_g}建立新案件嗎？", items)


# =========================
# 特殊指令
# =========================
def parse_special_command(text: str, group_id: str) -> Optional[Dict]:
    clean = strip_ai_trigger(text).strip()

    # 群組ID查詢
    if re.match(r"^群組ID$", clean):
        return {"type": "group_id"}

    # 日報
    if re.match(r"^日報$", clean):
        return {"type": "report"}

    # 查詢：支援所有格式
    # @AI 查 彭駿為 / 彭駿為 查@AI / 彭駿為@AI 查
    m = re.match(r"^查\s*([\u4e00-\u9fff]{2,4})$", clean)
    if m:
        return {"type": "search", "name": m.group(1)}
    m = re.match(r"^([\u4e00-\u9fff]{2,4})\s*查$", clean)
    if m:
        return {"type": "search", "name": m.group(1)}

    # 轉下一家
    m = re.match(r"^([\u4e00-\u9fff]{2,4})\s*轉下一家$", clean)
    if m:
        return {"type": "advance", "name": m.group(1), "target": None}

    # 轉指定公司
    m = re.match(r"^([\u4e00-\u9fff]{2,4})\s*轉(.+)$", clean)
    if m and m.group(2).strip() != "下一家":
        return {"type": "advance", "name": m.group(1), "target": m.group(2).strip()}

    # 結案
    m = re.match(r"^([\u4e00-\u9fff]{2,4})\s*(已結案|結案)$", clean)
    if m:
        return {"type": "close", "name": m.group(1)}

    # 婉拒
    m = re.match(r"^([\u4e00-\u9fff]{2,4})\s*婉拒$", clean)
    if m:
        return {"type": "reject", "name": m.group(1)}

    return None


def handle_special_command(cmd: Dict, reply_token: str, group_id: str):
    t = cmd["type"]

    if t == "group_id":
        gname = get_group_name(group_id)
        reply_text(reply_token, f"📋 此群組資訊\n名稱：{gname}\nID：{group_id}")
        return

    if t == "report":
        segs = generate_report_lines(group_id)
        reply_text(reply_token, segs[0])
        for seg in segs[1:]:
            push_text(group_id, seg)
        return

    if t == "search":
        reply_text(reply_token, search_customer_info(cmd["name"], group_id))
        return

    if t == "close":
        name = cmd["name"]
        rows = find_active_by_name(name)
        same = [r for r in rows if r["source_group_id"] == group_id]
        target = same[0] if same else (rows[0] if rows else None)
        if not target:
            reply_text(reply_token, f"❌ 找不到客戶：{name}"); return
        update_customer(target["case_id"], status="CLOSED", text=f"{name} 已結案", from_group_id=group_id)
        reply_text(reply_token, f"✅ {name} 已結案，從日報移除")
        return

    if t == "reject":
        name = cmd["name"]
        rows = find_active_by_name(name)
        same = [r for r in rows if r["source_group_id"] == group_id]
        target = same[0] if same else (rows[0] if rows else None)
        if not target:
            reply_text(reply_token, f"❌ 找不到客戶：{name}"); return
        route = target["route_plan"] or ""
        current, next_co = get_current_company(route), get_next_company(route)
        new_route = advance_route(route, "婉拒")
        update_customer(target["case_id"], route_plan=new_route,
                        current_company=next_co or current,
                        text=f"{name} {current} 婉拒", from_group_id=group_id)
        if next_co:
            reply_text(reply_token, f"✅ {name} {current} 婉拒\n➡️ 下一家：{next_co}")
        else:
            reply_text(reply_token, f"✅ {name} {current} 婉拒\n⚠️ 已無下一家送件方案")
        return

    if t == "set_amount":
        name, amount = cmd["name"], cmd["amount"]
        rows = find_active_by_name(name)
        same = [r for r in rows if r["source_group_id"] == group_id]
        target = same[0] if same else (rows[0] if rows else None)
        if not target:
            all_rows = find_active_by_name(name)
            target = all_rows[0] if all_rows else None
        if not target:
            reply_text(reply_token, f"❌ 找不到客戶：{name}"); return
        update_customer(target["case_id"], approved_amount=amount, from_group_id=group_id)
        reply_text(reply_token, f"✅ {name} 核准金額已更新：{amount}")
        return

    if t == "advance":
        name, target_co = cmd["name"], cmd.get("target")
        rows = find_active_by_name(name)
        same = [r for r in rows if r["source_group_id"] == group_id]
        target = same[0] if same else (rows[0] if rows else None)
        if not target:
            reply_text(reply_token, f"❌ 找不到客戶：{name}"); return
        route = target["route_plan"] or ""
        current = get_current_company(route)
        if target_co:
            new_route, ok, err = advance_route_to(route, target_co, "轉送")
            if not ok:
                reply_text(reply_token, f"❌ {err}"); return
            update_customer(target["case_id"], route_plan=new_route, current_company=target_co,
                            text=f"{name} 轉{target_co}", from_group_id=group_id)
            reply_text(reply_token, f"✅ {name} 已轉送：{current} → {target_co}")
        else:
            next_co = get_next_company(route)
            if not next_co:
                reply_text(reply_token, f"⚠️ {name} 已無下一家送件方案"); return
            new_route = advance_route(route, "轉送")
            update_customer(target["case_id"], route_plan=new_route, current_company=next_co,
                            text=f"{name} 轉下一家→{next_co}", from_group_id=group_id)
            reply_text(reply_token, f"✅ {name} 已轉送：{current} → {next_co}")


# =========================
# 主要業務邏輯
# =========================
def handle_new_case_block(block_text, source_group_id, reply_token) -> Optional[str]:
    f = parse_header_fields(block_text)
    name, id_no, company = f["name"], f["id_no"], extract_company(block_text)
    if not name or not id_no:
        return None
    existing = find_active_by_id_no(id_no)
    if existing:
        if existing["source_group_id"] == source_group_id:
            update_customer(existing["case_id"], company=company or existing["company"] or "",
                            text=block_text, from_group_id=source_group_id, name=name)
            return f"🔄 已更新客戶：{name}"
        send_confirm_new_case_buttons(reply_token, block_text, existing, source_group_id)
        return "QUICK_REPLY_SENT"
    create_customer_record(name, id_no, company, source_group_id, block_text)
    return f"🆕 已建立客戶：{name}"


def handle_route_order_block(block_text, source_group_id, reply_token) -> Optional[str]:
    parsed = parse_route_order_line(extract_first_line(block_text))
    if not parsed:
        return None
    name, companies = parsed["name"], parsed["companies"]
    route_json = make_route_json(companies)
    current_co = companies[0]
    rows = find_active_by_name(name)
    same = [r for r in rows if r["source_group_id"] == source_group_id]
    if same:
        update_customer(same[0]["case_id"], route_plan=route_json, current_company=current_co,
                        text=block_text, from_group_id=source_group_id)
        return f"📋 已更新 {name} 送件順序：{'/'.join(companies)}"
    other = [r for r in rows if r["source_group_id"] != source_group_id]
    if other:
        send_transfer_case_buttons(reply_token, other[0], source_group_id, block_text, allow_new=True)
        return "QUICK_REPLY_SENT"
    create_customer_record(name, "", current_co, source_group_id, block_text,
                           route_plan=route_json, current_company=current_co)
    return f"🆕 已建立客戶 {name}，送件順序：{'/'.join(companies)}"


def handle_a_case_block(block_text, reply_token) -> Optional[str]:
    if is_blocked(block_text):
        return "❌ 含禁止關鍵字，已略過"
    id_no = extract_id_no(block_text)
    name = extract_name(block_text)
    customer = None
    if id_no:
        customer = find_active_by_id_no(id_no)
    if not customer and name:
        matches = find_active_by_name(name)
        if len(matches) == 1:
            customer = matches[0]
        elif len(matches) > 1:
            send_ambiguous_case_buttons(reply_token, block_text, matches)
            return "QUICK_REPLY_SENT"
    if not customer:
        return f"⚠️ 找不到對應客戶：{name or extract_first_line(block_text)}"

    company = extract_company(block_text) or customer["company"] or ""
    new_status = "CLOSED" if is_closed_text(block_text) else None
    is_reject = "婉拒" in block_text
    is_approved = "核准" in block_text and new_status != "CLOSED"
    route = customer["route_plan"] or ""
    new_route, next_co = route, ""
    if is_reject and route:
        next_co = get_next_company(route)
        new_route = advance_route(route, "婉拒")

    # 核准時抓金額，移到待撥款區
    approved_amount = None
    new_report_section = None
    ai_amount_needed = False
    if is_approved:
        quick_amount = extract_approved_amount(block_text)
        if quick_amount:
            approved_amount = quick_amount
        else:
            ai_amount_needed = True
        new_report_section = "待撥款"

    update_customer(customer["case_id"], company=company, text=block_text,
                    from_group_id=A_GROUP_ID, status=new_status,
                    route_plan=new_route if new_route != route else None,
                    current_company=next_co if next_co else None,
                    approved_amount=approved_amount,
                    report_section=new_report_section)

    ok, err = push_text(customer["source_group_id"], block_text)
    if not ok:
        return f"❌ 找到客戶：{customer['customer_name']}，但回貼{get_group_name(customer['source_group_id'])}失敗：{err}"

    gname = get_group_name(customer["source_group_id"])
    msg = f"✅ 已結案並回貼到{gname}：{customer['customer_name']}" if new_status == "CLOSED" else f"✅ 已回貼到{gname}：{customer['customer_name']}"
    if is_reject:
        msg += f"\n➡️ 下一家：{next_co}" if next_co else f"\n⚠️ {customer['customer_name']} 已無下一家送件方案"

    if ai_amount_needed:
        import threading
        def ai_parse_and_update():
            import asyncio
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            try:
                ai_amount = loop.run_until_complete(extract_approved_amount_with_ai(block_text))
                if ai_amount:
                    update_customer(customer["case_id"], approved_amount=ai_amount, from_group_id=A_GROUP_ID)
                    push_text(A_GROUP_ID, f"✅ {customer['customer_name']} 核准金額已辨識：{ai_amount}")
                else:
                    push_text(A_GROUP_ID, f"⚠️ {customer['customer_name']} 核准金額無法辨識\n請手動補：{customer['customer_name']} 核准金額XX萬@AI")
            finally:
                loop.close()
        threading.Thread(target=ai_parse_and_update, daemon=True).start()

    return msg


# =========================
# 按鈕指令處理
# =========================
def handle_command_text(text: str, reply_token: str) -> bool:

    def get_action(action_id, expected_type):
        a = get_pending_action(action_id)
        if not a or a["action_type"] != expected_type:
            reply_text(reply_token, "⚠️ 找不到待確認資料")
            return None
        return a

    if text.startswith("FORCE_CREATE_NEW|"):
        _, action_id = text.split("|", 1)
        a = get_action(action_id, "confirm_new_case_with_existing_id")
        if not a: return True
        p = a["payload"]; block_text = p.get("block_text", ""); sg = p.get("source_group_id", "")
        name = extract_name(block_text)
        create_customer_record(name, extract_id_no(block_text), extract_company(block_text), sg, block_text)
        reply_text(reply_token, f"🆕 已在{get_group_name(sg)}建立新案件：{name}")
        delete_pending_action(action_id); return True

    if text.startswith("USE_EXISTING_CASE|"):
        _, action_id = text.split("|", 1)
        a = get_action(action_id, "confirm_new_case_with_existing_id")
        if not a: return True
        conn = get_conn(); cur = conn.cursor()
        cur.execute("SELECT * FROM customers WHERE case_id=?", (a["payload"].get("existing_case_id",""),))
        c = cur.fetchone(); conn.close()
        if not c: reply_text(reply_token, "⚠️ 原案件不存在"); delete_pending_action(action_id); return True
        reply_text(reply_token, f"✅ 已沿用{get_group_name(c['source_group_id'])}案件：{c['customer_name']}")
        delete_pending_action(action_id); return True

    if text.startswith("CANCEL_NEW_CASE|"):
        _, action_id = text.split("|", 1)
        delete_pending_action(action_id); reply_text(reply_token, "✅ 已取消"); return True

    if text.startswith("CONFIRM_TRANSFER|"):
        _, action_id = text.split("|", 1)
        a = get_action(action_id, "transfer_customer")
        if not a: return True
        p = a["payload"]; block_text = p.get("block_text", ""); tg = p.get("target_group_id", "")
        conn = get_conn(); cur = conn.cursor()
        cur.execute("SELECT * FROM customers WHERE case_id=?", (p.get("case_id",""),))
        c = cur.fetchone(); conn.close()
        if not c: reply_text(reply_token, "⚠️ 原案件不存在"); delete_pending_action(action_id); return True
        update_customer(c["case_id"], company=extract_company(block_text) or c["company"] or "",
                        text=block_text, from_group_id=tg, name=p.get("name", c["customer_name"]), source_group_id=tg)
        reply_text(reply_token, f"✅ 已改到{get_group_name(tg)}")
        delete_pending_action(action_id); return True

    if text.startswith("KEEP_OLD_CASE|"):
        _, action_id = text.split("|", 1)
        a = get_action(action_id, "transfer_customer")
        if not a: return True
        p = a["payload"]; block_text = p.get("block_text", "")
        conn = get_conn(); cur = conn.cursor()
        cur.execute("SELECT * FROM customers WHERE case_id=?", (p.get("case_id",""),))
        c = cur.fetchone(); conn.close()
        if not c: reply_text(reply_token, "⚠️ 原案件不存在"); delete_pending_action(action_id); return True
        update_customer(c["case_id"], company=extract_company(block_text) or c["company"] or "",
                        text=block_text, from_group_id=c["source_group_id"])
        reply_text(reply_token, f"✅ 已沿用{get_group_name(c['source_group_id'])}原案件")
        delete_pending_action(action_id); return True

    if text.startswith("CREATE_NEW_FROM_TRANSFER|"):
        _, action_id = text.split("|", 1)
        a = get_action(action_id, "transfer_customer")
        if not a: return True
        p = a["payload"]; block_text = p.get("block_text", ""); tg = p.get("target_group_id", "")
        name = extract_name(block_text)
        create_customer_record(name, extract_id_no(block_text), extract_company(block_text), tg, block_text)
        reply_text(reply_token, f"🆕 已建立新案件：{name}")
        delete_pending_action(action_id); return True

    if text.startswith("CANCEL_TRANSFER|"):
        _, action_id = text.split("|", 1)
        delete_pending_action(action_id); reply_text(reply_token, "✅ 已取消"); return True

    if text.startswith("SELECT_CASE|"):
        parts = text.split("|", 2)
        if len(parts) < 3: return False
        _, action_id, case_id = parts
        a = get_action(action_id, "route_a_case")
        if not a: return True
        block_text = a["payload"].get("block_text", "")
        conn = get_conn(); cur = conn.cursor()
        cur.execute("SELECT * FROM customers WHERE case_id=?", (case_id,))
        c = cur.fetchone(); conn.close()
        if not c: reply_text(reply_token, "⚠️ 案件不存在"); delete_pending_action(action_id); return True
        new_status = "CLOSED" if is_closed_text(block_text) else None
        update_customer(case_id, company=extract_company(block_text) or c["company"] or "",
                        text=block_text, from_group_id=A_GROUP_ID, status=new_status)
        ok, err = push_text(c["source_group_id"], block_text)
        if not ok:
            reply_text(reply_token, f"❌ 找到案件但回貼失敗：{err}"); delete_pending_action(action_id); return True
        gname = get_group_name(c["source_group_id"])
        msg = f"✅ 已結案並回貼到{gname}：{c['customer_name']}" if new_status == "CLOSED" else f"✅ 已回貼到{gname}：{c['customer_name']}"
        reply_text(reply_token, msg); delete_pending_action(action_id); return True

    if text.startswith("REOPEN_CASE|"):
        parts = text.split("|", 2)
        if len(parts) < 3: return False
        _, action_id, case_id = parts
        a = get_action(action_id, "reopen_or_new_case")
        if not a: return True
        p = a["payload"]; block_text = p.get("block_text", ""); push_to_a = p.get("push_to_a_after_reopen", False)
        conn = get_conn(); cur = conn.cursor()
        cur.execute("SELECT * FROM customers WHERE case_id=?", (case_id,))
        c = cur.fetchone(); conn.close()
        if not c: reply_text(reply_token, "⚠️ 原案件不存在"); delete_pending_action(action_id); return True
        update_customer(case_id, company=extract_company(block_text) or c["company"] or "",
                        text=block_text, from_group_id=c["source_group_id"], status="ACTIVE")
        pushed = False
        if push_to_a:
            ok, _ = push_text(A_GROUP_ID, block_text); pushed = ok
        msg = f"✅ 已重啟案件：{c['customer_name']}"
        if pushed: msg += f"\n✅ 已回貼A群：{c['customer_name']}"
        reply_text(reply_token, msg); delete_pending_action(action_id); return True

    if text.startswith("CREATE_NEW_CASE|"):
        _, action_id = text.split("|", 1)
        a = get_action(action_id, "reopen_or_new_case")
        if not a: return True
        p = a["payload"]; block_text = p.get("block_text", ""); sg = p.get("source_group_id", B_GROUP_ID)
        push_to_a = p.get("push_to_a_after_reopen", False); name = extract_name(block_text)
        create_customer_record(name, extract_id_no(block_text), extract_company(block_text), sg, block_text)
        pushed = False
        if push_to_a:
            ok, _ = push_text(A_GROUP_ID, block_text); pushed = ok
        msg = f"🆕 已建立新案件：{name}"
        if pushed: msg += f"\n✅ 已回貼A群：{name}"
        reply_text(reply_token, msg); delete_pending_action(action_id); return True

    if text.startswith("CANCEL_REOPEN|"):
        _, action_id = text.split("|", 1)
        delete_pending_action(action_id); reply_text(reply_token, "✅ 已取消"); return True

    return False


# =========================
# 撥款名單處理
# =========================
def handle_disbursement_list(text: str, reply_token: str):
    """
    解析A群撥款名單：
    1. 去DB查每個客戶的核准金額和所屬業務群
    2. 更新撥款日期
    3. 組合排撥名單推給所有行政群（格式：日期 排撥名單\n姓名\t公司+金額\t業務群）
    4. A群回報結果（成功幾筆/找不到幾筆）
    """
    parsed = parse_disbursement_list(text)
    if not parsed:
        reply_text(reply_token, "⚠️ 無法解析撥款名單格式")
        return

    all_push_lines = []
    disb_date_str = ""
    result_lines = []  # (idx, status, name, msg)
    global_idx = 0

    for disb_date, companies in parsed.items():
        disb_date_str = disb_date
        for company, names in companies.items():
            for name in names:
                global_idx += 1

                # 找客戶（進行中或待撥款）
                rows = find_active_by_name(name)
                if not rows:
                    conn = get_conn(); cur = conn.cursor()
                    cur.execute(
                        "SELECT * FROM customers WHERE customer_name=? ORDER BY updated_at DESC LIMIT 1",
                        (name,))
                    row = cur.fetchone(); conn.close()
                    rows = [row] if row else []

                if not rows:
                    result_lines.append((global_idx, False, name, f"⚠️ 找不到對應客戶：{name}"))
                    continue

                target = rows[0]
                sales_group_name = get_group_name(target["source_group_id"])
                approved_amount = target["approved_amount"] or ""

                # 更新撥款日期
                update_customer(
                    target["case_id"],
                    disbursement_date=disb_date,
                    report_section="待撥款",
                    text=name + " " + company + " 撥款" + disb_date,
                    from_group_id=A_GROUP_ID,
                )
                result_lines.append((global_idx, True, name, f"✅ {name}"))

                # 組合排撥名單行：姓名\t公司+金額\t業務群名稱
                company_amount = company + (" " + approved_amount if approved_amount else "")
                push_line = name + "\t" + company_amount + "\t" + sales_group_name
                all_push_lines.append(push_line)

    # 推送排撥名單給所有行政群
    all_admin_gids = get_all_admin_groups()
    pushed_ok = False
    if all_admin_gids and all_push_lines:
        push_msg = disb_date_str + " 排撥名單" + chr(10) + chr(10).join(all_push_lines)
        for admin_gid in all_admin_gids:
            ok, _ = push_text(admin_gid, push_msg)
            if ok:
                pushed_ok = True

    # 組合A群回覆
    ok_count = sum(1 for _, s, _, _ in result_lines if s)
    fail_count = len(result_lines) - ok_count

    msg_lines = []
    if pushed_ok:
        msg_lines.append(f"✅ 撥款名單已推送行政群，共{ok_count}筆")
    else:
        msg_lines.append(f"✅ 撥款名單處理完成，共{ok_count}筆")

    for idx, success, name, detail in result_lines:
        if success:
            msg_lines.append(f"第{idx}筆：✅ {name}")
        else:
            msg_lines.append(f"第{idx}筆：{detail}")

    reply_text(reply_token, chr(10).join(msg_lines))


from fastapi import FastAPI, Request, BackgroundTasks
from fastapi.responses import HTMLResponse
import uvicorn
import requests as req_lib
import os
import re
import sqlite3
import json
from datetime import datetime
import uuid
from typing import Optional, List, Dict, Any

app = FastAPI()

CHANNEL_ACCESS_TOKEN = os.getenv("CHANNEL_ACCESS_TOKEN", "")
A_GROUP_ID = os.getenv("A_GROUP_ID", "Cb3579e75c94437ed22aafc7b1f6aecdd")
B_GROUP_ID = "Cd14f3ee775f1d9f5cfdafb223173cbef"
C_GROUP_ID = "C1a647fcb29a74842eceeb18e7a53823d"
DB_PATH = os.getenv("DB_PATH", "/var/data/loan_system.db")

# 日報三段結構
REPORT_SECTION_1 = [
    "麻吉", "和潤", "中租", "裕融", "21汽車", "亞太", "創鉅", "21",
    "第一", "合信", "興達", "和裕", "鄉民", "喬美",
    "分貝汽車", "分貝機車", "貸救補", "預付手機分期", "融易", "手機分期",
    "送件", "待撥款",
]
REPORT_SECTION_2 = [
    "銀行", "零卡", "商品貸", "代書", "當舖專案", "核准",
]
REPORT_SECTION_3 = [
    "房地", "核准(房地)",
]
# 合併（用於其他地方參考）
REPORT_SECTIONS = REPORT_SECTION_1 + REPORT_SECTION_2 + REPORT_SECTION_3

# 公司辨識（解析訊息用）
COMPANY_LIST = [
    "和裕商品", "和裕機車", "亞太商品", "亞太機車", "手機分期", "貸救補",
    "分貝汽車", "分貝機車", "21汽車", "鄉民", "喬美", "麻吉", "亞太",
    "和裕", "第一", "合信", "興達", "中租", "裕融", "創鉅", "和潤",
    "銀行", "零卡", "商品貸", "代書", "當舖", "融易", "21",
    "分貝", "鄉", "銀", "C", "商", "代",
]

STATUS_WORDS = [
    "婉拒", "核准", "補件", "補資料", "等保書", "退件", "不承作", "照會",
    "保密", "NA", "待撥款", "可送", "缺資料", "無可知情", "聯絡人皆可知情",
    "補行照", "補照會", "補照片", "補時段", "補案件資料", "補聯徵", "補保人",
    "已補", "轉", "申覆",
]

ACTION_KEYWORDS = [
    "結案", "刪掉", "不追了", "全部不送", "已撥款結案",
    "補件", "補資料", "缺資料", "婉拒", "核准", "照會", "退件", "等保書",
    "不承作", "待撥款", "補行照", "補照會", "補照片", "補時段", "補案件資料",
    "補聯徵", "補保人", "保密", "無可知情", "聯絡人皆可知情", "已補",
]

DELETE_KEYWORDS = ["結案", "刪掉", "不追了", "全部不送", "已撥款結案", "違約金結案", "已支付違約金", "以收到違約金", "收到違約金", "違約金已收", "以收到違約金", "違約金"]
BLOCK_KEYWORDS = ["鼎信", "禾基"]

IGNORE_NAME_SET = {
    "信用不良", "不需要了", "不用了", "不要了", "結案", "補件", "核准", "婉拒",
    "申覆", "申請", "待審", "初估",
    "照會", "等保書", "待撥款", "缺資料", "補資料", "資料補", "補來", "退件",
    "助理", "AI助理", "先生", "小姐", "無可知情", "聯絡人皆可知情", "下一家",
    "可知情", "聯絡人", "皆可知情", "不可知情", "無空間", "信用卡",
    "機車貸", "汽車貸", "商品貸", "無貸款", "合照後", "後補", "來補",
    "空間", "貸款", "申請", "提供", "保證", "聯徵", "繳息",
}

CHINESE_NAME_RE = re.compile(r"[\u4e00-\u9fff]{2,4}")
ID_RE = re.compile(r"[A-Z][12]\d{8}")

# 支援有無 - 的日期格式，如 115/3/2廖俊宏 或 115/3/2-廖俊宏
DATE_NAME_ID_INLINE_RE = re.compile(
    r"^\s*(\d{2,4}/\d{1,2}/\d{1,2})\s*[-－]?\s*([\u4e00-\u9fff]{2,4})\s*([A-Z][12]\d{8})",
    re.IGNORECASE,
)
DATE_NAME_ONLY_RE = re.compile(
    r"^\s*(\d{2,4}/\d{1,2}/\d{1,2})\s*[-－]?\s*([\u4e00-\u9fff]{2,4})(?:\s*$|\s*[-－/\u4e00-\u9fff])",
    re.IGNORECASE,
)
# 短日期：3/2廖俊宏 或 3/2-廖俊宏（有無-都支援）
SHORT_DATE_NAME_RE = re.compile(
    r"^\s*(\d{1,2}/\d{1,2})\s*[-－]?\s*([\u4e00-\u9fff]{2,4})"
)
# 送件順序格式：4/1-高郡惠-喬美/亞太/和裕
ROUTE_ORDER_RE = re.compile(
    r"^\s*(\d{1,4}/\d{1,2}(?:/\d{1,2})?)\s*[-－]\s*([\u4e00-\u9fff]{2,4})\s*[-－]\s*((?:[^\s/\n]+/)+[^\s/\n@]+)(?:\s*@AI)?\s*$",
    re.IGNORECASE,
)


# =========================
# 基本工具
# =========================
# =========================
# 送件順序工具
# =========================
# =========================
# 名稱解析
# =========================
# =========================
# LINE API
# =========================
# =========================
# 資料庫
# =========================
# =========================
# DB CRUD
# =========================
# =========================
# 核准金額 / 撥款名單工具
# =========================
# =========================
# 狀態摘要提取
# =========================
# =========================
# 日報產生
# =========================
# =========================
# Quick Reply 按鈕
# =========================
# =========================
# 特殊指令
# =========================
# =========================
# 主要業務邏輯
# =========================
def handle_bc_case_block(block_text, source_group_id, reply_token, source_text="") -> Optional[str]:
    if is_blocked(block_text):
        return "❌ 含禁止關鍵字，已略過"
    if is_route_order_line(extract_first_line(block_text)):
        return handle_route_order_block(block_text, source_group_id, reply_token)
    if looks_like_new_case_block(block_text):
        return handle_new_case_block(block_text, source_group_id, reply_token)

    name = extract_name(block_text)
    id_no = extract_id_no(block_text)
    company = extract_company(block_text)
    if not name or name in IGNORE_NAME_SET:
        return None

    # want_push_a：原始訊息有@AI觸發 且 訊息含補件相關動作
    # 用 source_text（未去掉@AI的原始文字）來判斷@AI觸發
    # 這樣「彭駿為 補案件@AI」也能正確判斷
    raw_for_trigger = source_text or block_text
    has_bu_keyword = any(w in block_text for w in [
        "補", "照會", "缺資料", "補件", "補資料", "補照片",
        "補時段", "補聯徵", "補保人", "補行照", "補照會",
    ])
    want_push_a = has_ai_trigger(raw_for_trigger) and has_bu_keyword
    has_action = has_business_action_word(block_text)

    if id_no:
        existing = find_active_by_id_no(id_no)
        if existing:
            if existing["source_group_id"] == source_group_id:
                new_status = "CLOSED" if is_closed_text(block_text) else None
                update_customer(existing["case_id"], company=company or existing["company"] or "",
                                text=block_text, from_group_id=source_group_id, status=new_status, name=name)
                pushed = False
                if want_push_a and new_status != "CLOSED":
                    ok, _ = push_text(A_GROUP_ID, block_text); pushed = ok
                msg = f"已更新客戶：{name}"
                if pushed: msg += f"\n✅ 已回貼A群：{name}"
                return msg
            if is_closed_text(block_text):
                return f"⚠️ 同身分證案件存在於{get_group_name(existing['source_group_id'])}：{name}"
            send_transfer_case_buttons(reply_token, existing, source_group_id, block_text, allow_new=True)
            return "QUICK_REPLY_SENT"
        create_customer_record(name, id_no, company, source_group_id, block_text)
        pushed = False
        if want_push_a:
            ok, _ = push_text(A_GROUP_ID, block_text); pushed = ok
        msg = f"🆕 已建立客戶：{name}"
        if pushed: msg += f"\n✅ 已回貼A群：{name}"
        return msg

    any_rows = find_any_by_name(name)
    same_rows = [r for r in any_rows if r["source_group_id"] == source_group_id]
    same_active = [r for r in same_rows if r["status"] == "ACTIVE"]
    same_closed = [r for r in same_rows if r["status"] != "ACTIVE"]
    other_active = [r for r in any_rows if r["source_group_id"] != source_group_id and r["status"] == "ACTIVE"]
    other_closed = [r for r in any_rows if r["source_group_id"] != source_group_id and r["status"] != "ACTIVE"]

    if has_action and same_closed and not same_active and not is_closed_text(block_text):
        send_reopen_case_buttons(reply_token, block_text, same_closed, source_group_id, push_to_a_after_reopen=want_push_a)
        return "QUICK_REPLY_SENT"
    if has_action and same_active:
        c = same_active[0]
        new_status = "CLOSED" if is_closed_text(block_text) else None
        update_customer(c["case_id"], company=company or c["company"] or "",
                        text=block_text, from_group_id=source_group_id, status=new_status)
        pushed = False
        if want_push_a and new_status != "CLOSED":
            ok, _ = push_text(A_GROUP_ID, block_text); pushed = ok
        msg = f"已更新客戶：{name}"
        if pushed: msg += f"\n✅ 已回貼A群：{name}"
        return msg
    if not has_action and not is_format_trigger(block_text):
        return None
    if same_active:
        r = same_active[0]
        update_customer(r["case_id"], company=company or r["company"] or "",
                        text=block_text, from_group_id=source_group_id)
        pushed = False
        if want_push_a:
            ok, _ = push_text(A_GROUP_ID, block_text); pushed = ok
        msg = f"已更新客戶：{name}"
        if pushed: msg += f"\n✅ 已回貼A群：{name}"
        return msg
    if other_active:
        send_transfer_case_buttons(reply_token, other_active[0], source_group_id, block_text, allow_new=True)
        return "QUICK_REPLY_SENT"
    if same_closed and is_closed_text(block_text):
        r = same_closed[0]
        update_customer(r["case_id"], company=company or r["company"] or "",
                        text=block_text, from_group_id=source_group_id, status="CLOSED")
        return f"已更新客戶：{name}"
    if same_closed:
        send_reopen_case_buttons(reply_token, block_text, same_closed, source_group_id, push_to_a_after_reopen=want_push_a)
        return "QUICK_REPLY_SENT"
    if other_closed:
        send_transfer_case_buttons(reply_token, other_closed[0], source_group_id, block_text, allow_new=True)
        return "QUICK_REPLY_SENT"
    create_customer_record(name, "", company, source_group_id, block_text)
    pushed = False
    if want_push_a:
        ok, _ = push_text(A_GROUP_ID, block_text); pushed = ok
    msg = f"🆕 已建立客戶：{name}"
    if pushed: msg += f"\n✅ 已回貼A群：{name}"
    return msg


# =========================
# 按鈕指令處理
# =========================
# =========================
# 撥款名單處理
# =========================
# =========================
# Webhook
# =========================
def process_event(event: dict):
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

    # 任何群組都可以查群組ID（方便設定用）
    if has_ai_trigger(text):
        clean = strip_ai_trigger(text).strip()
        if clean == "群組ID":
            gname = get_group_name(group_id)
            reply_text(reply_token, f"📋 此群組資訊\n名稱：{gname}\nID：{group_id}")
            return

    sales_ids = get_sales_group_ids()
    admin_ids = get_admin_group_ids()

    # 業務群 / 行政群
    if group_id in sales_ids or group_id in admin_ids:
        raw = text
        # 特殊 @AI 指令
        if has_ai_trigger(raw):
            cmd = parse_special_command(raw, group_id)
            if cmd:
                handle_special_command(cmd, reply_token, group_id)
                return

        is_creation = looks_like_new_case_block(raw) or any(looks_like_new_case_block(b) for b in split_multi_cases(raw))
        is_route = is_route_order_line(extract_first_line(raw)) or any(is_route_order_line(extract_first_line(b)) for b in split_multi_cases(raw))
        should_process = is_creation or is_route or has_ai_trigger(raw)
        if not should_process:
            return

        process_text = raw if (is_creation or is_route) else strip_ai_trigger(raw)
        if process_text in {"", "助理", "AI助理"}:
            return

        blocks = split_multi_cases(process_text) or [process_text]
        results, conflicts, qr_sent = [], [], False

        for idx, block in enumerate(blocks, 1):
            result = handle_bc_case_block(block, group_id, reply_token, source_text=raw)
            if result == "QUICK_REPLY_SENT":
                if len(blocks) > 1:
                    name = extract_name(block) or f"第{idx}筆"
                    conflicts.append(f"⚠️ {name} 有衝突，請手動確認")
                    continue
                else:
                    qr_sent = True; break
            if result:
                results.append(f"第{idx}筆：{result}" if len(blocks) > 1 else result)

        all_msgs = results + conflicts
        if not qr_sent and all_msgs:
            reply_text(reply_token, "\n".join(all_msgs))
        return

    # A 群
    if group_id == A_GROUP_ID:
        # 撥款名單（不需要@AI觸發）
        if is_disbursement_list(text):
            handle_disbursement_list(text, reply_token)
            return

        if not has_ai_trigger(text):
            return
        cmd = parse_special_command(text, group_id)
        if cmd:
            handle_special_command(cmd, reply_token, group_id)
            return
        clean = strip_ai_trigger(text)
        if clean in {"", "助理", "AI助理"}:
            return
        blocks = split_multi_cases(clean) or [clean]
        results = []
        for idx, block in enumerate(blocks, 1):
            result = handle_a_case_block(block, reply_token)
            if result and result != "QUICK_REPLY_SENT":
                results.append(f"第{idx}筆：{result}" if len(blocks) > 1 else result)
        if results:
            reply_text(reply_token, "\n".join(results))


@app.post("/callback")
async def callback(request: Request, background_tasks: BackgroundTasks):
    body = await request.json()
    for event in body.get("events", []):
        background_tasks.add_task(process_event, event)
    return {"status": "ok"}


# =========================
# 管理 API
# =========================
@app.post("/admin/reset_data")
async def reset_data(request: Request):
    """
    清除所有測試資料（customers + case_logs + pending_actions）
    群組設定保留不動。
    需要帶 {"confirm": "yes"} 才會執行，避免誤觸。
    """
    body = await request.json()
    if body.get("confirm") != "yes":
        return {"status": "error", "message": '請帶 {"confirm": "yes"} 才會執行清除'}
    conn = get_conn()
    cur = conn.cursor()
    try:
        cur.execute("DELETE FROM customers")
        cur.execute("DELETE FROM case_logs")
        cur.execute("DELETE FROM pending_actions")
        # 重置自動遞增 ID
        cur.execute("DELETE FROM sqlite_sequence WHERE name IN ('customers','case_logs','pending_actions')")
        conn.commit()
        return {"status": "ok", "message": "✅ 已清除所有案件資料，群組設定保留"}
    except Exception as e:
        return {"status": "error", "message": str(e)}
    finally:
        conn.close()


@app.get("/admin/reset_data", response_class=HTMLResponse)
def reset_data_page():
    """清除測試資料的網頁介面"""
    return """
    <h2>⚠️ 清除測試資料</h2>
    <p>此操作將清除所有客戶案件紀錄，群組設定不受影響。</p>
    <p><b>清除後無法復原（除非從 Render Snapshot 還原）</b></p>
    <button onclick="doReset()" style="background:red;color:white;padding:10px 20px;font-size:16px;border:none;border-radius:6px;cursor:pointer">
        確認清除所有測試資料
    </button>
    <div id="result" style="margin-top:20px;font-size:18px;"></div>
    <br><a href="/">回首頁</a>
    <script>
    async function doReset() {
        if (!confirm('確定要清除所有案件資料嗎？')) return;
        const r = await fetch('/admin/reset_data', {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({confirm: 'yes'})
        });
        const data = await r.json();
        document.getElementById('result').innerText = data.message;
    }
    </script>
    """


@app.post("/admin/add_group")
async def add_group(request: Request):
    """新增群組 API。Body: {group_id, group_name, group_type: SALES_GROUP/ADMIN_GROUP/A_GROUP}"""
    body = await request.json()
    gid = body.get("group_id", "").strip()
    gname = body.get("group_name", "").strip()
    gtype = body.get("group_type", "SALES_GROUP").strip()
    if not gid or not gname:
        return {"status": "error", "message": "group_id 和 group_name 必填"}
    linked = body.get("linked_sales_group_id", "").strip()
    conn = get_conn(); cur = conn.cursor()
    try:
        cur.execute("""INSERT OR REPLACE INTO groups
            (group_id,group_name,group_type,is_active,linked_sales_group_id,created_at)
            VALUES (?,?,?,1,?,?)""",
                    (gid, gname, gtype, linked or None, now_iso()))
        conn.commit()
        msg = f"已新增/更新群組：{gname}({gtype})"
        if linked:
            msg += f"，對應業務群：{get_group_name(linked)}"
        return {"status": "ok", "message": msg}
    except Exception as e:
        return {"status": "error", "message": str(e)}
    finally:
        conn.close()


# =========================
# 網頁
# =========================
REPORT_PASSWORD = os.getenv("REPORT_PASSWORD", "admin123")
ADMIN_PASSWORD = os.getenv("ADMIN_PASSWORD", "admin_secret")
SESSION_NORMAL = "loan_normal_2026"
SESSION_ADMIN = "loan_admin_2026"

PAGE_CSS = """
<meta name="viewport" content="width=device-width,initial-scale=1">
<style>
*{box-sizing:border-box;margin:0;padding:0}
body{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;background:#f4f6f9;color:#1a1a2e;font-size:14px}
a{color:#3b82f6;text-decoration:none}
.btn{display:inline-flex;align-items:center;gap:6px;padding:7px 16px;border-radius:7px;border:1px solid #d1d5db;background:#fff;cursor:pointer;font-size:13px;color:#374151;transition:background .15s}
.btn:hover{background:#f3f4f6}
.btn-primary{background:#1a1a2e;color:#fff;border-color:#1a1a2e}
.btn-primary:hover{background:#2d2d4e}
.btn-danger{color:#dc2626;border-color:#fca5a5}
.btn-danger:hover{background:#fef2f2}
.input{width:100%;padding:8px 12px;border:1px solid #d1d5db;border-radius:7px;font-size:13px;background:#fff}
.input:focus{outline:none;border-color:#6366f1;box-shadow:0 0 0 3px rgba(99,102,241,.1)}
.badge{display:inline-flex;align-items:center;font-size:11px;padding:2px 7px;border-radius:4px;font-weight:500}
.b-ok{background:#dcfce7;color:#15803d}
.b-wait{background:#f3e8ff;color:#7c3aed}
.b-doc{background:#fff7ed;color:#c2410c}
.b-act{background:#eff6ff;color:#1d4ed8}
.b-rej{background:#fef2f2;color:#b91c1c}
.b-close{background:#f1f5f9;color:#64748b}
.topnav{background:#1a1a2e;color:#fff;padding:0 20px;display:flex;align-items:center;justify-content:space-between;height:52px;position:sticky;top:0;z-index:100}
.topnav-title{font-size:15px;font-weight:600}
.topnav-links{display:flex;gap:4px}
.nl{padding:6px 12px;border-radius:6px;color:rgba(255,255,255,.65);font-size:13px;transition:all .15s}
.nl:hover{background:rgba(255,255,255,.1);color:#fff}
.nl.active{background:rgba(255,255,255,.15);color:#fff;font-weight:500}
.page{max-width:1100px;margin:0 auto;padding:20px 16px}
.stats-grid{display:grid;grid-template-columns:repeat(4,1fr);gap:10px;margin-bottom:20px}
.stat-card{background:#fff;border:1px solid #e5e7eb;border-radius:9px;padding:14px 16px}
.stat-num{font-size:24px;font-weight:600}
.stat-lbl{font-size:11px;color:#9ca3af;margin-top:3px}
.search-bar{display:flex;gap:10px;margin-bottom:16px}
.group-card{background:#fff;border:1px solid #e5e7eb;border-radius:10px;margin-bottom:12px;overflow:hidden}
.group-hd{padding:11px 16px;display:flex;align-items:center;justify-content:space-between;cursor:pointer;user-select:none;transition:background .15s}
.group-hd:hover{background:rgba(0,0,0,.02)}
.group-hd-left{display:flex;align-items:center;gap:10px}
.group-dot{width:10px;height:10px;border-radius:50%;flex-shrink:0}
.group-title{font-size:14px;font-weight:600}
.group-meta{display:flex;gap:6px;flex-wrap:wrap}
.gm-pill{background:#f3f4f6;color:#6b7280;padding:2px 8px;border-radius:4px;font-size:11px}
.chevron{color:#9ca3af;font-size:11px;transition:transform .2s}
.group-body{display:none;border-top:1px solid #f3f4f6}
.group-body.open{display:block}
.seg-tabs{display:flex;padding:0 16px;gap:0;border-bottom:1px solid #f3f4f6}
.seg-tab{padding:9px 14px;font-size:12px;font-weight:500;color:#9ca3af;cursor:pointer;border-bottom:2px solid transparent;margin-bottom:-1px;transition:all .15s}
.seg-tab:hover{color:#6366f1}
.seg-tab.active{color:#1a1a2e;border-bottom-color:#6366f1}
.seg-panel{display:none}
.seg-panel.active{display:block}
.sec-hd{padding:8px 16px 4px;font-size:11px;font-weight:600;color:#9ca3af;text-transform:uppercase;letter-spacing:.5px;background:#fafafa;border-bottom:1px solid #f3f4f6}
.cust-row{padding:8px 20px;display:flex;align-items:center;justify-content:space-between;border-bottom:1px solid #f9fafb;cursor:pointer;transition:background .1s}
.cust-row:last-child{border-bottom:none}
.cust-row:hover{background:#f8faff}
.cust-name{font-size:13px;font-weight:500;display:flex;align-items:center;gap:6px}
.cust-detail{font-size:11px;color:#9ca3af;margin-top:2px}
.cust-date{font-size:11px;color:#d1d5db;flex-shrink:0;margin-left:10px}
.empty-sec{padding:14px 20px;font-size:12px;color:#d1d5db;font-style:italic}
.modal-bg{display:none;position:fixed;inset:0;background:rgba(0,0,0,.45);z-index:200;align-items:center;justify-content:center}
.modal-bg.show{display:flex}
.modal{background:#fff;border-radius:12px;padding:24px;width:90%;max-width:480px;max-height:80vh;overflow-y:auto}
.modal-title{font-size:16px;font-weight:600;margin-bottom:16px}
.modal-close{float:right;cursor:pointer;color:#9ca3af;font-size:18px;line-height:1}
.form-row{margin-bottom:14px}
.form-row label{display:block;font-size:12px;color:#6b7280;margin-bottom:5px;font-weight:500}
.result-ok{background:#dcfce7;color:#15803d;padding:8px 12px;border-radius:6px;font-size:12px;margin-top:10px}
.result-err{background:#fef2f2;color:#dc2626;padding:8px 12px;border-radius:6px;font-size:12px;margin-top:10px}
.history-row{padding:10px 16px;border-bottom:1px solid #f3f4f6;display:flex;align-items:center;justify-content:space-between}
.history-row:last-child{border-bottom:none}
.search-result{background:#fff;border:1px solid #e5e7eb;border-radius:10px;padding:16px;margin-bottom:12px}
.route-line{padding:4px 0;font-size:12px;color:#6b7280;display:flex;align-items:center;gap:6px}
@media(max-width:640px){
  .stats-grid{grid-template-columns:repeat(2,1fr)}
  .topnav-links .nl{padding:5px 8px;font-size:12px}
  .page{padding:12px 10px}
}
</style>
"""

GROUP_COLORS = ["#1a1a2e","#2d5016","#7c2d12","#1e3a5f","#4a1d6e","#1e4d4d",
                "#78350f","#164e63","#4c1d95","#052e16"]

def check_auth(request: Request) -> str:
    """回傳 'admin'/'normal'/'' """
    t = request.cookies.get("auth_token","")
    if t == SESSION_ADMIN: return "admin"
    if t == SESSION_NORMAL: return "normal"
    return ""

def make_topnav(role: str, active: str) -> str:
    links = [("📊 日報","/report","report"),("🔍 查詢","/search","search"),
             ("📁 歷史","/history","history")]
    if role == "admin":
        links += [("⚙️ 群組管理","/admin/groups","admin"),
                  ("🗑️ 清除資料","/admin/reset_data","reset")]
    nav = "".join(f'<a class="nl {"active" if a==active else ""}" href="{u}">{n}</a>'
                  for n,u,a in links)
    nav += '<a class="nl" href="/logout">登出</a>'
    return f'<nav class="topnav"><div class="topnav-title">貸款案件管理</div><div class="topnav-links">{nav}</div></nav>'

def get_badge(row) -> str:
    sec = row["report_section"] or ""
    last = row["last_update"] or ""
    first = last.splitlines()[0] if last else ""
    if sec == "待撥款": return '<span class="badge b-wait">待撥款</span>'
    if "核准" in first: return '<span class="badge b-ok">核准</span>'
    if "婉拒" in first: return '<span class="badge b-rej">婉拒</span>'
    if "補" in first or "缺" in first: return '<span class="badge b-doc">補件</span>'
    if "等保書" in first: return '<span class="badge b-doc">等保書</span>'
    if "照會" in first: return '<span class="badge b-doc">照會</span>'
    return '<span class="badge b-act">進行中</span>'

def render_cust_rows(rows) -> str:
    if not rows: return '<div class="empty-sec">（無資料）</div>'
    html = ""
    for row in rows:
        created = row["created_at"] or ""
        date_str = created[5:10].replace("-","/") if created else ""
        last = row["last_update"] or ""
        first_line = last.splitlines()[0].strip() if last.strip() else ""
        status = extract_status_summary(first_line, row["customer_name"])
        co = row["current_company"] or row["company"] or ""
        badge = get_badge(row)
        if (row["report_section"] or "") == "待撥款":
            amt = row["approved_amount"] or ""
            disb = row["disbursement_date"] or ""
            detail = co + (f" 核准{amt}" if amt else "") + (f" (撥款{disb})" if disb else " (待撥款)")
        else:
            detail = co + (f" · {status}" if status else "")
        html += f'''<div class="cust-row">
          <div style="min-width:0">
            <div class="cust-name">{row["customer_name"]} {badge}</div>
            <div class="cust-detail">{detail}</div>
          </div>
          <div class="cust-date">{date_str}</div>
        </div>'''
    return html

def render_seg(section_map: dict, sections: list, shown: set) -> str:
    html = ""
    for sec in sections:
        if sec in section_map:
            html += f'<div class="sec-hd">{sec}</div>'
            html += render_cust_rows(section_map[sec])
            shown.add(sec)
    return html

def build_row_map(rows) -> dict:
    m = {}
    for r in rows:
        sec = r["report_section"] or r["current_company"] or r["company"] or "送件"
        m.setdefault(sec, []).append(r)
    return m


@app.get("/login", response_class=HTMLResponse)
def login_page(request: Request, error: str = ""):
    err_html = '<div style="background:#fef2f2;color:#dc2626;padding:8px 12px;border-radius:6px;font-size:12px;margin-bottom:14px">密碼錯誤，請再試一次</div>' if error else ""
    return f"""<!DOCTYPE html><html><head>{PAGE_CSS}<title>登入</title></head><body>
    <div style="min-height:100vh;display:flex;align-items:center;justify-content:center">
    <div style="background:#fff;border:1px solid #e5e7eb;border-radius:12px;padding:36px 32px;width:340px;box-shadow:0 4px 24px rgba(0,0,0,.06)">
      <div style="text-align:center;margin-bottom:24px">
        <div style="font-size:22px;font-weight:700;color:#1a1a2e">貸款案件管理</div>
        <div style="font-size:12px;color:#9ca3af;margin-top:6px">請輸入密碼繼續</div>
      </div>
      {err_html}
      <form method="post" action="/login">
        <div style="margin-bottom:14px">
          <input class="input" type="password" name="password" placeholder="輸入密碼" autofocus style="padding:10px 14px;font-size:15px">
        </div>
        <button type="submit" class="btn btn-primary" style="width:100%;justify-content:center;padding:10px;font-size:14px">登入</button>
      </form>
    </div></div></body></html>"""


@app.post("/login")
async def login_post(request: Request):
    from fastapi.responses import RedirectResponse
    form = await request.form()
    pw = form.get("password","")
    if pw == ADMIN_PASSWORD:
        resp = RedirectResponse("/report", status_code=303)
        resp.set_cookie("auth_token", SESSION_ADMIN, max_age=86400*7, httponly=True)
        return resp
    if pw == REPORT_PASSWORD:
        resp = RedirectResponse("/report", status_code=303)
        resp.set_cookie("auth_token", SESSION_NORMAL, max_age=86400*7, httponly=True)
        return resp
    return RedirectResponse("/login?error=1", status_code=303)


@app.get("/logout")
def logout():
    from fastapi.responses import RedirectResponse
    resp = RedirectResponse("/login", status_code=303)
    resp.delete_cookie("auth_token")
    return resp


@app.get("/", response_class=HTMLResponse)
def home_redirect(request: Request):
    from fastapi.responses import RedirectResponse
    return RedirectResponse("/login" if not check_auth(request) else "/report")


@app.get("/report", response_class=HTMLResponse)
def report_web(request: Request):
    from fastapi.responses import RedirectResponse
    role = check_auth(request)
    if not role: return RedirectResponse("/login")

    today = datetime.now().strftime("%m/%d")
    conn = get_conn(); cur = conn.cursor()
    cur.execute("SELECT * FROM groups WHERE group_type='SALES_GROUP' AND is_active=1 ORDER BY group_name")
    groups = cur.fetchall()
    cur.execute("SELECT COUNT(*) as c FROM customers WHERE status='ACTIVE'")
    total_active = cur.fetchone()["c"]
    cur.execute("SELECT COUNT(*) as c FROM customers WHERE status='ACTIVE' AND report_section='待撥款'")
    total_disb = cur.fetchone()["c"]
    cur.execute("SELECT COUNT(*) as c FROM customers WHERE status='ACTIVE' AND (report_section='送件' OR (report_section IS NULL AND current_company IS NULL AND company IS NULL))")
    total_send = cur.fetchone()["c"]
    month_start = datetime.now().strftime("%Y-%m-01")
    cur.execute("SELECT COUNT(*) as c FROM customers WHERE status!='ACTIVE' AND updated_at>=?", (month_start,))
    total_closed = cur.fetchone()["c"]

    groups_html = ""
    for i, grp in enumerate(groups):
        gid, gname = grp["group_id"], grp["group_name"]
        cur.execute("SELECT * FROM customers WHERE source_group_id=? AND status='ACTIVE' ORDER BY updated_at DESC", (gid,))
        all_rows = cur.fetchall()
        count = len(all_rows)
        disb_count = sum(1 for r in all_rows if (r["report_section"] or "") == "待撥款")
        send_count = sum(1 for r in all_rows if (r["report_section"] or r["current_company"] or r["company"] or "送件") == "送件")
        color = GROUP_COLORS[i % len(GROUP_COLORS)]
        section_map = build_row_map(all_rows)
        shown = set()
        seg1 = render_seg(section_map, REPORT_SECTION_1, shown)
        seg2 = render_seg(section_map, REPORT_SECTION_2, shown)
        seg3 = render_seg(section_map, REPORT_SECTION_3, shown)
        extra = render_seg(section_map, [s for s in section_map if s not in shown], set())

        tabs, panels = "", ""
        tab_defs = []
        if seg1: tab_defs.append(("貸款方案", seg1))
        if seg2: tab_defs.append(("民間方案", seg2))
        if seg3: tab_defs.append(("房地", seg3))
        if extra: tab_defs.append(("其他", extra))
        if not tab_defs: tab_defs.append(("貸款方案", '<div class="empty-sec">（目前無有效案件）</div>'))

        for ti, (tlabel, tcontent) in enumerate(tab_defs):
            active = "active" if ti == 0 else ""
            tabs += f'<div class="seg-tab {active}" onclick="switchTab(\'{gid}\',{ti})">{tlabel}</div>'
            panels += f'<div class="seg-panel {active}" id="panel-{gid}-{ti}">{tcontent}</div>'

        pills = f'<span class="gm-pill">{count} 筆</span>'
        if disb_count: pills += f'<span class="gm-pill" style="background:#f3e8ff;color:#7c3aed">待撥款 {disb_count}</span>'
        if send_count: pills += f'<span class="gm-pill" style="background:#fff7ed;color:#c2410c">送件 {send_count}</span>'

        groups_html += f'''
        <div class="group-card">
          <div class="group-hd" onclick="toggleGroup(\'{gid}\')">
            <div class="group-hd-left">
              <div class="group-dot" style="background:{color}"></div>
              <div class="group-title">{gname}</div>
              <div class="group-meta">{pills}</div>
            </div>
            <div class="chevron" id="chev-{gid}">▶</div>
          </div>
          <div class="group-body" id="body-{gid}">
            <div class="seg-tabs">{tabs}</div>
            {panels}
          </div>
        </div>'''

    conn.close()
    return f"""<!DOCTYPE html><html><head>{PAGE_CSS}<title>日報 {today}</title></head><body>
    {make_topnav(role, "report")}
    <div class="page">
      <div class="stats-grid">
        <div class="stat-card"><div class="stat-num" style="color:#1d4ed8">{total_active}</div><div class="stat-lbl">進行中</div></div>
        <div class="stat-card"><div class="stat-num" style="color:#7c3aed">{total_disb}</div><div class="stat-lbl">待撥款</div></div>
        <div class="stat-card"><div class="stat-num" style="color:#c2410c">{total_send}</div><div class="stat-lbl">送件中</div></div>
        <div class="stat-card"><div class="stat-num" style="color:#15803d">{total_closed}</div><div class="stat-lbl">本月結案</div></div>
      </div>
      {groups_html}
    </div>
    <script>
    function toggleGroup(gid){{
      const body = document.getElementById("body-"+gid);
      const chev = document.getElementById("chev-"+gid);
      const open = body.classList.toggle("open");
      chev.textContent = open ? "▼" : "▶";
    }}
    function switchTab(gid, idx){{
      const tabs = document.querySelectorAll(`[id^="panel-${{gid}}-"]`);
      tabs.forEach((p,i) => p.classList.toggle("active", i===idx));
      const tabEls = document.querySelector(`#body-${{gid}} .seg-tabs`).children;
      Array.from(tabEls).forEach((t,i) => t.classList.toggle("active", i===idx));
    }}
    </script>
    </body></html>"""


@app.get("/search", response_class=HTMLResponse)
def search_page(request: Request, q: str = ""):
    from fastapi.responses import RedirectResponse
    role = check_auth(request)
    if not role: return RedirectResponse("/login")

    results_html = ""
    if q:
        conn = get_conn(); cur = conn.cursor()
        cur.execute("SELECT * FROM customers WHERE customer_name LIKE ? ORDER BY status ASC, updated_at DESC", (f"%{q}%",))
        rows = cur.fetchall()
        conn.close()
        if not rows:
            results_html = '<div style="color:#9ca3af;padding:20px 0;text-align:center">找不到客戶：' + q + '</div>'
        for row in rows:
            route_data = parse_route_json(row["route_plan"] or "")
            order, idx = route_data.get("order",[]), route_data.get("current_index",0)
            history = route_data.get("history",[])
            badge = '<span class="badge b-close">已結案</span>' if row["status"]!="ACTIVE" else get_badge(row)
            co = row["current_company"] or row["company"] or ""
            amt = row["approved_amount"] or ""
            disb = row["disbursement_date"] or ""

            route_html = ""
            if order:
                current_co = order[idx] if idx < len(order) else "（已完成）"
                next_co = order[idx+1] if idx+1 < len(order) else ""
                route_html += f'<div class="route-line">目前送件：<b>{current_co}</b> （第{idx+1}/{len(order)}家）</div>'
                if next_co: route_html += f'<div class="route-line">下一家：{next_co}</div>'
                if history:
                    route_html += '<div class="route-line" style="flex-wrap:wrap;gap:4px">歷程：'
                    route_html += " → ".join(f'{h["company"]}({h["status"]})' for h in history[-5:])
                    route_html += '</div>'

            last = (row["last_update"] or "").splitlines()
            last_short = last[-1].strip()[:80] if last else ""
            amount_line = f'核准金額：{amt}' if amt else ""
            disb_line = f'撥款日期：{disb}' if disb else ""

            results_html += f'''<div class="search-result">
              <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:10px">
                <div style="display:flex;align-items:center;gap:8px">
                  <div style="font-size:16px;font-weight:600">{row["customer_name"]}</div>
                  {badge}
                </div>
                <div style="font-size:11px;color:#9ca3af">{get_group_name(row["source_group_id"])}</div>
              </div>
              <div style="display:grid;grid-template-columns:1fr 1fr;gap:6px;font-size:12px;color:#6b7280;margin-bottom:10px">
                {"<div>身分證：" + row["id_no"] + "</div>" if row["id_no"] else ""}
                {"<div>公司：" + co + "</div>" if co else ""}
                {"<div>" + amount_line + "</div>" if amount_line else ""}
                {"<div>" + disb_line + "</div>" if disb_line else ""}
                <div>建立：{row["created_at"][:10] if row["created_at"] else ""}</div>
                <div>更新：{row["updated_at"][:10] if row["updated_at"] else ""}</div>
              </div>
              {route_html}
              {"<div style=\'margin-top:8px;padding:8px;background:#f9fafb;border-radius:6px;font-size:12px;color:#374151\'>" + last_short + "</div>" if last_short else ""}
            </div>'''

    return f"""<!DOCTYPE html><html><head>{PAGE_CSS}<title>查詢客戶</title></head><body>
    {make_topnav(role, "search")}
    <div class="page">
      <form method="get" action="/search">
        <div class="search-bar">
          <input class="input" name="q" value="{q}" placeholder="輸入客戶姓名搜尋..." autofocus style="flex:1">
          <button type="submit" class="btn btn-primary">搜尋</button>
        </div>
      </form>
      {results_html}
    </div></body></html>"""


@app.get("/history", response_class=HTMLResponse)
def history_page(request: Request, group: str = "", status: str = ""):
    from fastapi.responses import RedirectResponse
    role = check_auth(request)
    if not role: return RedirectResponse("/login")

    conn = get_conn(); cur = conn.cursor()
    cur.execute("SELECT * FROM groups WHERE group_type='SALES_GROUP' AND is_active=1 ORDER BY group_name")
    groups = cur.fetchall()

    query = "SELECT * FROM customers WHERE status!='ACTIVE'"
    params = []
    if group:
        query += " AND source_group_id=?"
        params.append(group)
    query += " ORDER BY updated_at DESC LIMIT 200"
    cur.execute(query, params)
    rows = cur.fetchall()
    conn.close()

    group_opts = "<option value=\'\'>全部群組</option>"
    for g in groups:
        sel = "selected" if g["group_id"]==group else ""
        group_opts += f'<option value="{g["group_id"]}" {sel}>{g["group_name"]}</option>'

    rows_html = ""
    if not rows:
        rows_html = '<div style="color:#9ca3af;padding:20px;text-align:center">沒有結案紀錄</div>'
    for row in rows:
        badge = '<span class="badge b-close">已結案</span>'
        co = row["current_company"] or row["company"] or ""
        amt = row["approved_amount"] or ""
        gname = get_group_name(row["source_group_id"])
        updated = row["updated_at"][:10] if row["updated_at"] else ""
        rows_html += f'''<div class="history-row">
          <div>
            <div style="display:flex;align-items:center;gap:6px;margin-bottom:2px">
              <span style="font-weight:500">{row["customer_name"]}</span> {badge}
              <span style="font-size:11px;color:#9ca3af">{gname}</span>
            </div>
            <div style="font-size:11px;color:#9ca3af">{co}{(" · 核准" + amt) if amt else ""}</div>
          </div>
          <div style="font-size:11px;color:#d1d5db">{updated}</div>
        </div>'''

    return f"""<!DOCTYPE html><html><head>{PAGE_CSS}<title>歷史紀錄</title></head><body>
    {make_topnav(role, "history")}
    <div class="page">
      <form method="get" action="/history" style="margin-bottom:16px">
        <div style="display:flex;gap:10px">
          <select class="input" name="group" style="flex:1" onchange="this.form.submit()">{group_opts}</select>
        </div>
      </form>
      <div style="background:#fff;border:1px solid #e5e7eb;border-radius:10px;overflow:hidden">
        <div style="padding:12px 16px;border-bottom:1px solid #f3f4f6;font-size:13px;font-weight:600;color:#6b7280">共 {len(rows)} 筆結案紀錄</div>
        {rows_html}
      </div>
    </div></body></html>"""


@app.get("/admin/groups", response_class=HTMLResponse)
def list_groups(request: Request):
    from fastapi.responses import RedirectResponse
    role = check_auth(request)
    if role != "admin": return RedirectResponse("/login")

    conn = get_conn(); cur = conn.cursor()
    cur.execute("SELECT * FROM groups ORDER BY group_type, group_name")
    rows = cur.fetchall()
    cur.execute("SELECT group_id, group_name FROM groups WHERE group_type='SALES_GROUP' AND is_active=1")
    sales_groups = cur.fetchall()
    conn.close()

    sales_opts = "<option value=\'\'>（無）</option>"
    for sg in sales_groups:
        sales_opts += f'<option value="{sg["group_id"]}">{sg["group_name"]}</option>'

    type_labels = {"SALES_GROUP":"業務群","ADMIN_GROUP":"行政群","A_GROUP":"A群"}
    rows_html = ""
    for r in rows:
        linked_name = ""
        if r["linked_sales_group_id"]:
            conn2 = get_conn(); cur2 = conn2.cursor()
            cur2.execute("SELECT group_name FROM groups WHERE group_id=?", (r["linked_sales_group_id"],))
            ln = cur2.fetchone(); conn2.close()
            linked_name = ln["group_name"] if ln else r["linked_sales_group_id"]
        rows_html += f'''<tr>
          <td style="padding:8px 12px;font-weight:500">{r["group_name"]}</td>
          <td style="padding:8px 12px;color:#6b7280">{type_labels.get(r["group_type"],r["group_type"])}</td>
          <td style="padding:8px 12px;color:#6b7280">{linked_name or "-"}</td>
          <td style="padding:8px 12px">{"✅" if r["is_active"] else "❌"}</td>
          <td style="padding:8px 12px;font-size:11px;color:#9ca3af;font-family:monospace">{r["group_id"]}</td>
        </tr>'''

    return f"""<!DOCTYPE html><html><head>{PAGE_CSS}<title>群組管理</title></head><body>
    {make_topnav(role, "admin")}
    <div class="page">
      <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:16px">
        <h2 style="font-size:18px;font-weight:600">群組管理</h2>
        <button class="btn btn-primary" onclick="document.getElementById('add-modal').classList.add('show')">+ 新增群組</button>
      </div>
      <div style="background:#fff;border:1px solid #e5e7eb;border-radius:10px;overflow:hidden;margin-bottom:20px">
        <table style="width:100%;border-collapse:collapse">
          <thead style="background:#fafafa;border-bottom:1px solid #f3f4f6">
            <tr>
              <th style="padding:8px 12px;text-align:left;font-size:12px;color:#6b7280">名稱</th>
              <th style="padding:8px 12px;text-align:left;font-size:12px;color:#6b7280">類型</th>
              <th style="padding:8px 12px;text-align:left;font-size:12px;color:#6b7280">對應業務群</th>
              <th style="padding:8px 12px;text-align:left;font-size:12px;color:#6b7280">啟用</th>
              <th style="padding:8px 12px;text-align:left;font-size:12px;color:#6b7280">Group ID</th>
            </tr>
          </thead>
          <tbody>{rows_html}</tbody>
        </table>
      </div>

      <div style="background:#fff;border:1px solid #fca5a5;border-radius:10px;padding:16px">
        <div style="font-size:13px;font-weight:600;margin-bottom:8px;color:#dc2626">危險操作</div>
        <div style="font-size:12px;color:#6b7280;margin-bottom:12px">清除所有案件資料，群組設定不受影響。清除後無法復原。</div>
        <button class="btn btn-danger" onclick="doReset()">🗑️ 清除所有測試資料</button>
        <div id="reset-result" style="margin-top:10px;font-size:12px"></div>
      </div>
    </div>

    <div class="modal-bg" id="add-modal" onclick="if(event.target===this)this.classList.remove('show')">
      <div class="modal">
        <div class="modal-title">新增 / 更新群組 <span class="modal-close" onclick="document.getElementById('add-modal').classList.remove('show')">×</span></div>
        <div class="form-row"><label>群組 ID（在群組打 @AI 群組ID 取得）</label><input class="input" id="f_gid" placeholder="Cxxx..."></div>
        <div class="form-row"><label>群組名稱</label><input class="input" id="f_gname" placeholder="例：鉅烽行政群"></div>
        <div class="form-row"><label>群組類型</label>
          <select class="input" id="f_gtype" onchange="document.getElementById('linked-row').style.display=this.value==='ADMIN_GROUP'?'block':'none'">
            <option value="SALES_GROUP">業務群</option>
            <option value="ADMIN_GROUP">行政群</option>
            <option value="A_GROUP">A群/進度群</option>
          </select>
        </div>
        <div class="form-row" id="linked-row" style="display:none"><label>對應業務群（行政群才需要）</label><select class="input" id="f_linked">{sales_opts}</select></div>
        <button class="btn btn-primary" onclick="addGroup()" style="width:100%;justify-content:center">確認新增</button>
        <div id="add-result" style="margin-top:10px;font-size:12px"></div>
      </div>
    </div>

    <script>
    async function addGroup(){{
      const gid=document.getElementById('f_gid').value.trim();
      const gname=document.getElementById('f_gname').value.trim();
      const gtype=document.getElementById('f_gtype').value;
      const linked=document.getElementById('f_linked').value;
      const res=document.getElementById('add-result');
      if(!gid||!gname){{res.className='result-err';res.innerText='群組ID和名稱必填';return}}
      const body={{group_id:gid,group_name:gname,group_type:gtype}};
      if(gtype==='ADMIN_GROUP'&&linked) body.linked_sales_group_id=linked;
      const r=await fetch('/admin/add_group',{{method:'POST',headers:{{\'Content-Type\':\'application/json\'}},body:JSON.stringify(body)}});
      const data=await r.json();
      res.className=data.status==='ok'?'result-ok':'result-err';
      res.innerText=data.message;
      if(data.status==='ok') setTimeout(()=>location.reload(),1500);
    }}
    async function doReset(){{
      if(!confirm('確定要清除所有案件資料嗎？此操作無法復原！')) return;
      const res=document.getElementById('reset-result');
      const r=await fetch('/admin/reset_data',{{method:'POST',headers:{{\'Content-Type\':\'application/json\'}},body:JSON.stringify({{confirm:'yes'}})}});
      const data=await r.json();
      res.style.color=data.status==='ok'?'#15803d':'#dc2626';
      res.innerText=data.message;
    }}
    </script>
    </body></html>"""

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
