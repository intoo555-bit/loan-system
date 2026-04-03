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

# 日報欄位順序（只顯示有客戶的）
REPORT_SECTIONS = [
    "麻吉", "和潤", "中租", "裕融", "21汽車", "亞太", "創鉅", "21",
    "第一", "合信", "興達", "和裕", "鄉民", "喬美",
    "分貝汽車", "分貝機車", "貸救補", "預付手機分期", "融易", "手機分期",
    "送件", "待撥款",
    "銀行", "零卡", "商品貸", "代書", "當舖專案", "核准", "房地",
]

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

DELETE_KEYWORDS = ["結案", "刪掉", "不追了", "全部不送", "已撥款結案"]
BLOCK_KEYWORDS = ["鼎信", "禾基"]

IGNORE_NAME_SET = {
    "信用不良", "不需要了", "不用了", "不要了", "結案", "補件", "核准", "婉拒",
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


def strip_ai_trigger(text: str) -> str:
    return re.sub(r"(@ai助理|#ai助理|@ai|#ai)", "", text or "", flags=re.IGNORECASE).strip()


def has_ai_trigger(text: str) -> bool:
    return bool(re.search(r"@ai|#ai", text or "", re.IGNORECASE))


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
    從訊息抓取核准金額。
    支援格式：核准20萬 / 核准200,000 / 核准200000 / 核准6萬5 / 核准金額20萬
    回傳字串如「20萬」「200,000元」，抓不到回傳空字串
    """
    patterns = [
        r"核[貸准貸]\s*金額?\s*[:：]?\s*(\d+[\d,]*)\s*萬?",
        r"核准\s*(\d+[\d,]*萬?\d*)",
        r"最高核[貸貸]\s*金額?\s*(\d+[\d,]*)\s*萬",
        r"金額\s*[:：]?\s*(\d+[\d,]*)\s*萬?",
    ]
    for pat in patterns:
        m = re.search(pat, text)
        if m:
            raw = m.group(1).replace(",", "")
            # 判斷是否有萬
            after = text[m.end():][:3]
            if "萬" in m.group(0) or "萬" in after:
                # 避免重複加萬
                return f"{raw}萬" if not raw.endswith("萬") else raw
            # 純數字判斷：超過1000當元，否則當萬
            try:
                num = int(raw)
                if num >= 1000:
                    return f"{num:,}元"
                else:
                    return f"{num}萬"
            except:
                return raw
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
        r"(\d{1,2}/\d{1,2})\s*([\w一-鿿]+?)\s*(撥款名單|排撥|撥款)",
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
    return bool(re.search(r"撥款名單|排撥|撥款", text))


def get_admin_group_for_sales(sales_group_id: str) -> Optional[str]:
    """根據業務群ID找對應的行政群ID"""
    conn = get_conn(); cur = conn.cursor()
    cur.execute("""
        SELECT group_id FROM groups
        WHERE group_type='ADMIN_GROUP' AND linked_sales_group_id=? AND is_active=1
        LIMIT 1
    """, (sales_group_id,))
    row = cur.fetchone(); conn.close()
    return row["group_id"] if row else None


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
def generate_report_lines(group_id: str) -> List[str]:
    """產生日報文字，回傳多段（因LINE限制自動分段）"""
    group_name = get_group_name(group_id)
    conn = get_conn(); cur = conn.cursor()
    cur.execute("SELECT * FROM customers WHERE source_group_id=? AND status='ACTIVE' ORDER BY updated_at DESC", (group_id,))
    all_rows = cur.fetchall(); conn.close()

    section_map: Dict[str, List[str]] = {}
    for row in all_rows:
        section = row["report_section"] or row["current_company"] or row["company"] or "送件"
        updated = row["updated_at"] or ""
        date_str = updated[5:10].replace("-", "/") if updated else ""
        company_str = row["current_company"] or row["company"] or ""

        # 日報只取最新訊息第一行作為狀態摘要
        last_update = row["last_update"] or ""
        first_line = last_update.splitlines()[0].strip() if last_update.strip() else ""

        # 從第一行提取狀態關鍵字（去掉姓名、公司、日期，只留狀態）
        status_short = extract_status_summary(first_line, row["customer_name"])

        # 待撥款區用特殊格式
        if section == "待撥款":
            # 用 created_at 當進件日期
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

    output = [f"📊 {group_name} 日報 {datetime.now().strftime('%m/%d')}"]
    shown = set()
    for sec in REPORT_SECTIONS:
        if sec in section_map:
            output.append(sec)
            output.extend(section_map[sec])
            output.append("——————————————")
            shown.add(sec)
    for sec, lines in section_map.items():
        if sec not in shown:
            output.append(sec)
            output.extend(lines)
            output.append("——————————————")

    if len(output) <= 1:
        return [f"📊 {group_name} 日報\n（目前無有效案件）"]

    # 分段 4500 字
    segments, current = [], ""
    for line in output:
        if len(current) + len(line) + 1 > 4500:
            segments.append(current.strip())
            current = line + "\n"
        else:
            current += line + "\n"
    if current.strip():
        segments.append(current.strip())
    return segments


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
    last = (r["last_update"] or "").splitlines()
    if last:
        lines.append(f"最新進度：{last[-1].strip()[:50]}")
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
    if is_approved:
        approved_amount = extract_approved_amount(block_text) or None
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
    解析撥款名單，更新客戶撥款日期，推送到對應行政群。
    格式：
    04/01 21 撥款名單
    吳翰杰
    蔡文杰
    """
    parsed = parse_disbursement_list(text)
    if not parsed:
        reply_text(reply_token, "⚠️ 無法解析撥款名單格式")
        return

    results = []
    notified_admin_groups = set()

    for disb_date, companies in parsed.items():
        for company, names in companies.items():
            for name in names:
                # 找客戶
                rows = find_active_by_name(name)
                if not rows:
                    # 也找待撥款狀態的
                    conn = get_conn(); cur = conn.cursor()
                    cur.execute("""SELECT * FROM customers WHERE customer_name=?
                                   AND report_section='待撥款' ORDER BY updated_at DESC""", (name,))
                    rows = list(cur.fetchall()); conn.close()

                if not rows:
                    results.append(f"⚠️ 找不到客戶：{name}")
                    continue

                target = rows[0]
                # 更新撥款日期，移到待撥款區
                update_customer(
                    target["case_id"],
                    disbursement_date=disb_date,
                    report_section="待撥款",
                    text=f"{name} {company} 撥款{disb_date}",
                    from_group_id=A_GROUP_ID,
                )
                results.append(f"✅ {name} 撥款{disb_date}（{company}）")

                # 推送到對應行政群
                admin_gid = get_admin_group_for_sales(target["source_group_id"])
                if admin_gid and admin_gid not in notified_admin_groups:
                    push_text(admin_gid, text)
                    notified_admin_groups.add(admin_gid)

    if results:
        # 每次最多回5筆，超過只顯示摘要
        if len(results) <= 5:
            reply_text(reply_token, "\n".join(results))
        else:
            ok_count = sum(1 for r in results if r.startswith("✅"))
            fail_count = len(results) - ok_count
            msg = f"✅ 撥款名單處理完成\n共{ok_count}筆成功"
            if fail_count:
                msg += f"，{fail_count}筆找不到客戶"
            reply_text(reply_token, msg)


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
@app.get("/", response_class=HTMLResponse)
def home():
    return """<h2>貸款系統</h2><p>系統正常運作中</p>
    <a href="/report">📊 日報</a> |
    <a href="/admin/groups">群組管理</a> |
    <a href="/admin/reset_data" style="color:red">🗑️ 清除測試資料</a>"""


@app.get("/admin/groups", response_class=HTMLResponse)
def list_groups():
    conn = get_conn(); cur = conn.cursor()
    cur.execute("SELECT * FROM groups ORDER BY group_type, group_name")
    rows = cur.fetchall(); conn.close()
    html = "<h2>群組列表</h2><table border='1' cellpadding='6'>"
    html += "<tr><th>名稱</th><th>類型</th><th>Group ID</th><th>啟用</th></tr>"
    for r in rows:
        html += f"<tr><td>{r['group_name']}</td><td>{r['group_type']}</td><td>{r['group_id']}</td><td>{'✅' if r['is_active'] else '❌'}</td></tr>"
    html += "</table><br><a href='/'>回首頁</a>"
    return html


@app.get("/report", response_class=HTMLResponse)
def report_web():
    conn = get_conn(); cur = conn.cursor()
    cur.execute("SELECT * FROM groups WHERE group_type='SALES_GROUP' AND is_active=1")
    groups = cur.fetchall()
    html = "<h2>📊 日報</h2>"
    for grp in groups:
        gid, gname = grp["group_id"], grp["group_name"]
        html += f"<h3>({gname})</h3>"
        cur.execute("SELECT * FROM customers WHERE source_group_id=? AND status='ACTIVE' ORDER BY updated_at DESC", (gid,))
        all_rows = cur.fetchall()
        section_map: Dict[str, List[str]] = {}
        for row in all_rows:
            section = row["report_section"] or row["current_company"] or row["company"] or "送件"
            updated = row["updated_at"] or ""
            date_str = updated[5:10].replace("-", "/") if updated else ""
            company_str = row["current_company"] or row["company"] or ""
            last_lines = (row["last_update"] or "").splitlines()
            last_short = last_lines[-1].strip()[:40] if last_lines else ""
            line = f"{date_str}-{row['customer_name']}-{company_str}"
            if last_short and last_short not in line:
                line += f"-{last_short}"
            section_map.setdefault(section, []).append(line)
        has_any = False
        shown = set()
        for sec in REPORT_SECTIONS:
            if sec in section_map:
                has_any = True
                html += f"<b>{sec}</b><br>"
                for line in section_map[sec]:
                    html += f"{line}<br>"
                html += "——————————<br>"
                shown.add(sec)
        for sec, lines in section_map.items():
            if sec not in shown:
                has_any = True
                html += f"<b>{sec}</b><br>"
                for line in lines:
                    html += f"{line}<br>"
                html += "——————————<br>"
        if not has_any:
            html += "（目前無有效案件）<br>"
    conn.close()
    return html


@app.get("/search/{name}", response_class=HTMLResponse)
def search_customer_web(name: str):
    conn = get_conn(); cur = conn.cursor()
    cur.execute("SELECT * FROM customers WHERE customer_name LIKE ? ORDER BY updated_at DESC", (f"%{name}%",))
    rows = cur.fetchall(); conn.close()
    if not rows:
        return f"<p>找不到客戶：{name}</p>"
    html = f"<h2>查詢：{name}</h2>"
    for row in rows:
        route_data = parse_route_json(row["route_plan"] or "")
        order, idx = route_data.get("order", []), route_data.get("current_index", 0)
        current = order[idx] if order and idx < len(order) else row["company"] or ""
        badge = "🟢 進行中" if row["status"] == "ACTIVE" else "⚫ 已結案"
        html += f"<div style='border:1px solid #ccc;padding:10px;margin:8px 0;border-radius:8px'>"
        html += f"<b>{row['customer_name']}</b> {badge}<br>"
        html += f"群組：{get_group_name(row['source_group_id'])} | 目前：{current}<br>"
        if order:
            html += f"送件順序：{'/'.join(order)}（第{idx+1}/{len(order)}家）<br>"
        last = (row["last_update"] or "").splitlines()
        if last:
            html += f"最新：{last[-1].strip()[:60]}<br>"
        html += f"更新：{row['updated_at'][:16]}</div>"
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
