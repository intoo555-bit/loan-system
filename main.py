from fastapi import FastAPI, Request, BackgroundTasks
from fastapi.responses import HTMLResponse, JSONResponse
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
    "房地", "新鑫", "核准(房地)",
]
# 合併（用於其他地方參考）
REPORT_SECTIONS = REPORT_SECTION_1 + REPORT_SECTION_2 + REPORT_SECTION_3

# 公司辨識（解析訊息用）
COMPANY_LIST = [
    # 和裕方案
    "和裕商品", "和裕機車", "和裕機",
    # 亞太方案
    "亞太商品", "亞太機車", "亞太工會", "亞太工", "亞太機",
    "手機分期", "貸救補",
    # 分貝方案
    "分貝汽車", "分貝機車", "分貝汽", "分貝機",
    # 21方案：機車/機車25萬/汽車/商品
    "21汽車", "21機車", "21機25萬", "21機25", "21汽", "21機",
    # 創鉅方案：手機/機車
    "創鉅手機", "創鉅機車", "創鉅手", "創鉅機",
    # 麻吉方案：機車/手機
    "麻吉機車", "麻吉手機", "麻吉機", "麻吉手",
    "鄉民", "喬美", "麻吉", "亞太",
    "和裕", "第一", "合信", "興達", "中租", "裕融", "創鉅", "和潤",
    "銀行", "零卡", "商品貸", "代書", "當舖", "融易", "21",
    "新鑫", "房地",
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
# ============================
# 公司別名對照表（完整版）
# ============================
COMPANY_ALIAS = {
    # 亞太相關
    "熊速貸": "亞太商品",
    "工會機車動擔": "亞太工會",
    "機車動擔設定": "亞太機車",
    # 和裕相關
    "維力商品貸": "和裕商品",
    "維力機車專": "和裕機車",
    "維力機車貸": "和裕機車",
    # 英文縮寫
    "TAC": "裕融",
    "EGO": "第一",
    # 融資房地
    "新鑫": "房地",
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
    "NA", "RE", "JCIC", "ID", "CCIS", "TAC", "EGO",
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
# 單公司核准/婉拒格式：03/04-黃娫柔-房地核准20萬
SINGLE_APPROVAL_RE = re.compile(
    r"^\s*(\d{1,4}/\d{1,2}(?:/\d{1,2})?)\s*[-－]\s*([\u4e00-\u9fff]{2,4})\s*[-－]\s*([^\s/\n@-]+?)(核准|婉拒)(\d+(?:\.\d+)?萬)?(?:\s*@AI)?\s*$",
    re.IGNORECASE,
)
# 單公司核准格式：03/04-黃娫柔-房地核准20萬 / 03/04-黃娫柔-新鑫核准20萬
SINGLE_APPROVAL_RE = re.compile(
    r"^\s*(\d{1,4}/\d{1,2}(?:/\d{1,2})?)\s*[-－]\s*([\u4e00-\u9fff]{2,4})\s*[-－]\s*([^\s/\n@-]+?)(核准|婉拒)(\d+(?:\.\d+)?萬)?(?:\s*@AI)?\s*$",
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


def advance_route(route_plan: str, status: str, amount: str = "") -> str:
    data = parse_route_json(route_plan)
    order, idx, history = data.get("order", []), data.get("current_index", 0), data.get("history", [])
    current = order[idx] if 0 <= idx < len(order) else ""
    if current:
        entry = {"company": current, "status": status, "date": now_iso()[:10]}
        if amount:
            entry["amount"] = amount
        history.append(entry)
    data["current_index"] = idx + 1
    data["history"] = history
    return json.dumps(data, ensure_ascii=False)


def get_amount_from_history(route_plan: str, company: str) -> str:
    """從 history 找指定公司的核准金額（支援模糊比對）"""
    data = parse_route_json(route_plan)
    history = data.get("history", [])
    # 先精確比對
    for h in reversed(history):
        if h.get("company", "") == company and h.get("amount"):
            return h["amount"]
    # 再模糊比對（和裕 能匹配 和裕商品/和裕機車，亞太 能匹配 亞太商品/亞太機車）
    for h in reversed(history):
        hc = h.get("company", "")
        if h.get("amount") and (company in hc or hc in company):
            return h["amount"]
    return ""


def update_company_amount_in_history(route_plan: str, company: str, amount: str) -> str:
    """在 history 裡更新或新增指定公司的金額記錄（支援模糊比對）"""
    data = parse_route_json(route_plan)
    history = data.get("history", [])
    # 精確比對
    for h in reversed(history):
        if h.get("company", "") == company:
            h["amount"] = amount
            data["history"] = history
            return json.dumps(data, ensure_ascii=False)
    # 模糊比對
    for h in reversed(history):
        hc = h.get("company", "")
        if company in hc or hc in company:
            h["amount"] = amount
            data["history"] = history
            return json.dumps(data, ensure_ascii=False)
    # 找不到就新增一筆
    history.append({"company": company, "status": "核准", "amount": amount, "date": now_iso()[:10]})
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
        ("birth_date", "TEXT"),
        ("phone", "TEXT"),
        ("email", "TEXT"),
        ("line_id", "TEXT"),
        ("marriage", "TEXT"),
        ("education", "TEXT"),
        ("id_issue_date", "TEXT"),
        ("id_issue_place", "TEXT"),
        ("id_issue_type", "TEXT"),
        ("reg_city", "TEXT"),
        ("reg_district", "TEXT"),
        ("reg_address", "TEXT"),
        ("reg_phone", "TEXT"),
        ("live_city", "TEXT"),
        ("live_district", "TEXT"),
        ("live_address", "TEXT"),
        ("live_phone", "TEXT"),
        ("live_same_as_reg", "TEXT"),
        ("live_status", "TEXT"),
        ("live_years", "TEXT"),
        ("live_months", "TEXT"),
        ("company_name_detail", "TEXT"),
        ("company_industry", "TEXT"),
        ("company_role", "TEXT"),
        ("company_phone_area", "TEXT"),
        ("company_phone_num", "TEXT"),
        ("company_phone_ext", "TEXT"),
        ("company_years", "TEXT"),
        ("company_salary", "TEXT"),
        ("company_city", "TEXT"),
        ("company_district", "TEXT"),
        ("company_address", "TEXT"),
        ("bank_name", "TEXT"),
        ("bank_branch", "TEXT"),
        ("bank_account", "TEXT"),
        ("bank_holder", "TEXT"),
        ("contact1_name", "TEXT"),
        ("contact1_relation", "TEXT"),
        ("contact1_phone", "TEXT"),
        ("contact1_known", "TEXT"),
        ("contact2_name", "TEXT"),
        ("contact2_relation", "TEXT"),
        ("contact2_phone", "TEXT"),
        ("contact2_known", "TEXT"),
        ("eval_fund_need", "TEXT"),
        ("eval_sent_3m", "TEXT"),
        ("eval_labor_ins", "TEXT"),
        ("eval_salary_transfer", "TEXT"),
        ("eval_alert", "TEXT"),
        ("eval_vehicle", "TEXT"),
        ("eval_property", "TEXT"),
        ("eval_credit_card", "TEXT"),
        ("eval_fine", "TEXT"),
        ("eval_fuel_tax", "TEXT"),
        ("eval_note", "TEXT"),
        ("debt_list", "TEXT"),
        ("signature_img", "TEXT"),
        ("selected_plans", "TEXT"),
        ("product_type", "TEXT"),
        ("product_model", "TEXT"),
        ("product_imei", "TEXT"),
        ("vehicle_plate", "TEXT"),
        ("vehicle_type", "TEXT"),
        ("vehicle_owner", "TEXT"),
        ("sales_name", "TEXT"),
        ("created_by_role", "TEXT"),
    ]:
        ensure_column(cur, "customers", col, defn)
    # groups 表新增業務群對應欄位
    ensure_column(cur, "groups", "linked_sales_group_id", "TEXT")
    ensure_column(cur, "groups", "password_hash", "TEXT")
    # settings 表
    cur.execute("""CREATE TABLE IF NOT EXISTS settings (
        key TEXT PRIMARY KEY NOT NULL,
        value TEXT NOT NULL,
        updated_at TEXT NOT NULL
    )""")
    # login_attempts 表
    cur.execute("""CREATE TABLE IF NOT EXISTS login_attempts (
        identifier TEXT PRIMARY KEY NOT NULL,
        attempts INTEGER DEFAULT 0,
        locked_until TEXT DEFAULT '',
        updated_at TEXT NOT NULL
    )""")
    conn.commit()
    conn.close()
    init_settings()


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


def init_settings():
    """初始化預設密碼（只有第一次才設定）"""
    defaults = {
        "admin_pw": hash_pw("admin_secret"),
        "adminB_pw": hash_pw("adminB2026"),
        "report_pw": hash_pw("admin123"),
        "vba_secret": hash_pw("vba_secret_2026"),
    }
    conn = get_conn(); cur = conn.cursor()
    for key, val in defaults.items():
        cur.execute("INSERT OR IGNORE INTO settings (key,value,updated_at) VALUES (?,?,?)",
            (key, val, now_iso()))
    conn.commit(); conn.close()


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
# 公司歸類對照（子分類 → 日報主欄位）
COMPANY_SECTION_MAP = {
    "21機車": "21", "21汽車": "21", "21汽": "21", "21機": "21", "21機25萬": "21",
    "分貝汽": "分貝汽車", "分貝機": "分貝機車",
    "創鉅手機": "創鉅", "創鉅機車": "創鉅",
    "新鑫": "房地",
}

def normalize_section(section: str) -> str:
    """把公司子分類歸到日報主欄位"""
    return COMPANY_SECTION_MAP.get(section, section)


# 公司子分類歸到日報主欄位
COMPANY_SECTION_MAP = {
    # 和裕所有方案 → 全部歸到「和裕」欄位
    "和裕商品": "和裕", "維力商品貸": "和裕",
    "和裕機車": "和裕", "和裕機": "和裕",
    "維力機車專": "和裕", "維力機車貸": "和裕",
    # 亞太所有方案 → 全部歸到「亞太」欄位
    "亞太商品": "亞太", "亞太工會": "亞太", "亞太工": "亞太",
    "亞太機車": "亞太", "亞太機": "亞太",
    # 21方案 → 全部歸到「21」欄位
    "21機車": "21", "21汽車": "21", "21汽": "21",
    "21機": "21", "21機25": "21", "21機25萬": "21",
    # 分貝方案 → 各自欄位
    "分貝汽": "分貝汽車",
    "分貝機": "分貝機車",
    # 創鉅方案 → 全部歸到「創鉅」欄位
    "創鉅手機": "創鉅", "創鉅手": "創鉅",
    "創鉅機車": "創鉅", "創鉅機": "創鉅",
    # 麻吉方案 → 全部歸到「麻吉」欄位
    "麻吉機車": "麻吉", "麻吉機": "麻吉",
    "麻吉手機": "麻吉", "麻吉手": "麻吉",
    # 新鑫 → 房地
    "新鑫": "房地",
}

def normalize_section(section: str) -> str:
    return COMPANY_SECTION_MAP.get(section, section)


def build_section_map(all_rows) -> Dict[str, List[str]]:
    """把客戶列表轉成 section_map"""
    section_map: Dict[str, List[str]] = {}
    for row in all_rows:
        section = row["report_section"] or row["current_company"] or row["company"] or "送件"
        section = normalize_section(section)
        section = normalize_section(section)
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

def parse_single_approval_line(line: str) -> Dict:
    """解析單公司核准/婉拒格式：03/04-黃娫柔-房地核准20萬"""
    m = SINGLE_APPROVAL_RE.match(line.strip())
    if not m:
        return {}
    company = m.group(3).strip()
    # 別名轉換
    for alias, real in COMPANY_ALIAS.items():
        if alias in company:
            company = real
            break
    return {
        "date": m.group(1),
        "name": m.group(2),
        "company": company,
        "status": m.group(4),
        "amount": m.group(5) or "",
    }


def is_single_approval_line(line: str) -> bool:
    return bool(parse_single_approval_line(line))


def parse_single_approval_line(line: str) -> Dict:
    """解析單公司核准/婉拒格式：03/04-黃娫柔-房地核准20萬"""
    m = SINGLE_APPROVAL_RE.match(line.strip())
    if not m:
        return {}
    company = m.group(3).strip()
    for alias, real in COMPANY_ALIAS.items():
        if alias in company:
            company = real
            break
    return {
        "date": m.group(1), "name": m.group(2),
        "company": company, "status": m.group(4), "amount": m.group(5) or "",
    }


def is_single_approval_line(line: str) -> bool:
    return bool(parse_single_approval_line(line))


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
            # 存到 route_plan history 裡對應公司
            new_route = update_company_amount_in_history(new_route, company, quick_amount)
        else:
            ai_amount_needed = True
        new_report_section = "待撥款"

    update_customer(customer["case_id"], company=company, text=block_text,
                    from_group_id=A_GROUP_ID, status=new_status,
                    route_plan=new_route if new_route != route else new_route,
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
    # 核准時在A群顯示金額確認
    if is_approved and approved_amount:
        msg += f"\n💰 核准金額：{approved_amount}（已存入）"

    if ai_amount_needed:
        import threading
        def ai_parse_and_update():
            import asyncio
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            try:
                ai_amount = loop.run_until_complete(extract_approved_amount_with_ai(block_text))
                if ai_amount:
                    # 同時更新 approved_amount 和 route_plan history
                    cur_route = customer["route_plan"] or ""
                    new_r = update_company_amount_in_history(cur_route, company, ai_amount)
                    update_customer(customer["case_id"], approved_amount=ai_amount, route_plan=new_r, from_group_id=A_GROUP_ID)
                    push_text(A_GROUP_ID, f"💰 {customer['customer_name']} {company} 核准金額已辨識：{ai_amount}")
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
                # 優先從 route_plan history 找這家公司的金額，找不到再用 approved_amount
                approved_amount = get_amount_from_history(target["route_plan"] or "", company)
                if not approved_amount:
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
def handle_bc_case_block(block_text, source_group_id, reply_token, source_text="") -> Optional[str]:
    if is_blocked(block_text):
        return "❌ 含禁止關鍵字，已略過"
    if is_route_order_line(extract_first_line(block_text)):
        return handle_route_order_block(block_text, source_group_id, reply_token)
    # 單公司核准/婉拒格式：03/04-黃娫柔-房地核准20萬
    if is_single_approval_line(extract_first_line(block_text)):
        parsed = parse_single_approval_line(extract_first_line(block_text))
        name, company, status, amount = parsed["name"], parsed["company"], parsed["status"], parsed["amount"]
        rows = find_active_by_name(name)
        same = [r for r in rows if r["source_group_id"] == source_group_id and r["status"] == "ACTIVE"]
        target = same[0] if same else (rows[0] if rows else None)
        if not target:
            create_customer_record(name, "", company, source_group_id, block_text)
            return f"🆕 已建立客戶：{name}（{company} {status}）"
        route = target["route_plan"] or ""
        if amount:
            route = update_company_amount_in_history(route, company, amount)
        section = "房地" if "房地" in company or company == "新鑫" else None
        update_customer(target["case_id"], company=company, text=block_text,
                        from_group_id=source_group_id, route_plan=route,
                        report_section=section, approved_amount=amount or None)
        msg = f"已更新：{name} {company} {status}"
        if amount:
            msg += f" {amount}"
        return msg
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
# 密碼工具
# =========================
import hashlib as _hl

def hash_pw(password: str) -> str:
    import os as _os
    salt = _os.urandom(16).hex()
    hashed = _hl.sha256((salt + password).encode()).hexdigest()
    return f"{salt}:{hashed}"

def verify_pw(password: str, stored: str) -> bool:
    try:
        salt, hashed = stored.split(":", 1)
        return _hl.sha256((salt + password).encode()).hexdigest() == hashed
    except:
        return False

def get_setting(key: str) -> str:
    conn = get_conn(); cur = conn.cursor()
    cur.execute("SELECT value FROM settings WHERE key=?", (key,))
    row = cur.fetchone(); conn.close()
    return row["value"] if row else ""

def set_setting(key: str, value: str):
    conn = get_conn(); cur = conn.cursor()
    cur.execute("INSERT OR REPLACE INTO settings (key,value,updated_at) VALUES (?,?,?)",
        (key, value, now_iso()))
    conn.commit(); conn.close()

def get_group_password(group_id: str) -> str:
    conn = get_conn(); cur = conn.cursor()
    cur.execute("SELECT password_hash FROM groups WHERE group_id=?", (group_id,))
    row = cur.fetchone(); conn.close()
    return row["password_hash"] if row and row["password_hash"] else ""

def is_login_locked(identifier: str) -> bool:
    conn = get_conn(); cur = conn.cursor()
    cur.execute("SELECT attempts, locked_until FROM login_attempts WHERE identifier=?", (identifier,))
    row = cur.fetchone(); conn.close()
    if not row: return False
    if row["locked_until"] and row["locked_until"] > now_iso():
        return True
    return False

def record_login_fail(identifier: str):
    conn = get_conn(); cur = conn.cursor()
    cur.execute("SELECT attempts FROM login_attempts WHERE identifier=?", (identifier,))
    row = cur.fetchone()
    if row:
        attempts = row["attempts"] + 1
        locked_until = ""
        if attempts >= 5:
            from datetime import timedelta
            locked_until = (datetime.now() + timedelta(minutes=15)).strftime("%Y-%m-%d %H:%M:%S")
        cur.execute("UPDATE login_attempts SET attempts=?, locked_until=?, updated_at=? WHERE identifier=?",
            (attempts, locked_until, now_iso(), identifier))
    else:
        cur.execute("INSERT INTO login_attempts (identifier,attempts,locked_until,updated_at) VALUES (?,1,'',?)",
            (identifier, now_iso()))
    conn.commit(); conn.close()

def clear_login_fail(identifier: str):
    conn = get_conn(); cur = conn.cursor()
    cur.execute("DELETE FROM login_attempts WHERE identifier=?", (identifier,))
    conn.commit(); conn.close()

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
    """回傳 'admin'/'adminB'/'normal'/'group_xxx'/'' """
    t = request.cookies.get("auth_token","")
    if t == SESSION_ADMIN: return "admin"
    if t == "loan_adminB_2026": return "adminB"
    if t == SESSION_NORMAL: return "normal"
    if t.startswith("loan_group_"): return t.replace("loan_group_","group_")
    return ""

def get_auth_group_id(request: Request) -> str:
    """業務角色回傳其群組ID"""
    role = check_auth(request)
    if role.startswith("group_"):
        return role.replace("group_","")
    return ""

def make_topnav(role: str, active: str) -> str:
    links = [("📊 日報","/report","report"),("🔍 查詢","/search","search"),
             ("📁 歷史","/history","history")]
    if role in ("admin","adminB","normal"):
        links.append(("➕ 新增客戶","/new-customer","new"))
    if role == "admin":
        links += [("⚙️ 群組管理","/admin/groups","admin"),
                  ("🔑 密碼管理","/admin/passwords","passwords"),
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
    err_html = '<div style="background:#fef2f2;color:#dc2626;padding:8px 12px;border-radius:6px;font-size:12px;margin-bottom:14px">密碼錯誤或帳號已鎖定，請稍後再試</div>' if error else ""
    conn = get_conn(); cur = conn.cursor()
    cur.execute("SELECT group_id, group_name FROM groups WHERE group_type='SALES_GROUP' AND is_active=1 ORDER BY group_name")
    sales_groups = cur.fetchall(); conn.close()
    grp_opts = "".join(f'<option value="{g["group_id"]}">{g["group_name"]}</option>' for g in sales_groups)
    return f"""<!DOCTYPE html><html><head>{PAGE_CSS}<title>登入</title></head><body>
    <div style="min-height:100vh;display:flex;align-items:center;justify-content:center;background:#f5f0eb">
    <div style="background:#faf7f4;border:1px solid #ddd5ca;border-radius:12px;padding:36px 32px;width:360px;box-shadow:0 4px 24px rgba(0,0,0,.06)">
      <div style="text-align:center;margin-bottom:24px">
        <div style="font-size:22px;font-weight:700;color:#3a3530">貸款案件管理</div>
        <div style="font-size:12px;color:#9ca3af;margin-top:6px">請選擇身份並輸入密碼</div>
      </div>
      {err_html}
      <form method="post" action="/login">
        <div style="margin-bottom:14px">
          <label style="font-size:12px;color:#5a4e40;font-weight:600;display:block;margin-bottom:6px">身份</label>
          <select name="role" class="input" style="padding:9px 12px" onchange="document.getElementById('grp_sec').style.display=this.value==='group'?'block':'none'">
            <option value="normal">行政A</option>
            <option value="adminB">行政B</option>
            <option value="admin">管理員</option>
            <option value="group">業務（選群組）</option>
          </select>
        </div>
        <div id="grp_sec" style="display:none;margin-bottom:14px">
          <label style="font-size:12px;color:#5a4e40;font-weight:600;display:block;margin-bottom:6px">群組</label>
          <select name="group_id" class="input" style="padding:9px 12px">{grp_opts}</select>
        </div>
        <div style="margin-bottom:16px">
          <label style="font-size:12px;color:#5a4e40;font-weight:600;display:block;margin-bottom:6px">密碼</label>
          <input class="input" type="password" name="password" placeholder="輸入密碼" autofocus style="padding:10px 14px;font-size:15px">
        </div>
        <button type="submit" class="btn btn-primary" style="width:100%;justify-content:center;padding:10px;font-size:14px;background:#6a5e4e;border-color:#6a5e4e">登入</button>
      </form>
    </div></div></body></html>"""


@app.post("/login")
async def login_post(request: Request):
    from fastapi.responses import RedirectResponse
    form = await request.form()
    pw = form.get("password","")
    role = form.get("role","normal")
    group_id = form.get("group_id","")
    identifier = role if role != "group" else f"group_{group_id}"
    if is_login_locked(identifier):
        return RedirectResponse("/login?error=1", status_code=303)
    ok = False
    token = ""
    if role == "admin":
        stored = get_setting("admin_pw")
        ok = verify_pw(pw, stored) if stored else (pw == ADMIN_PASSWORD)
        token = SESSION_ADMIN
    elif role == "adminB":
        stored = get_setting("adminB_pw")
        ok = verify_pw(pw, stored) if stored else (pw == "adminB2026")
        token = "loan_adminB_2026"
    elif role == "normal":
        stored = get_setting("report_pw")
        ok = verify_pw(pw, stored) if stored else (pw == REPORT_PASSWORD)
        token = SESSION_NORMAL
    elif role == "group" and group_id:
        stored = get_group_password(group_id)
        ok = verify_pw(pw, stored) if stored else False
        token = f"loan_group_{group_id}"
    if ok:
        clear_login_fail(identifier)
        resp = RedirectResponse("/report", status_code=303)
        resp.set_cookie("auth_token", token, max_age=86400*7, httponly=True)
        return resp
    record_login_fail(identifier)
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
# VBA 查詢 API
# =========================
@app.get("/api/customer-lookup")
async def customer_lookup(
    request: Request,
    date: str = "",
    name: str = "",
    id_no: str = "",
    secret: str = ""
):
    stored_secret = get_setting("vba_secret")
    if stored_secret:
        vba_ok = verify_pw(secret, stored_secret)
    else:
        vba_ok = (secret == os.getenv("VBA_SECRET", "vba_secret_2026"))
    if not vba_ok:
        return JSONResponse({"ok": False, "error": "無權限"}, status_code=403)
    if not date or not name or not id_no:
        return JSONResponse({"ok": False, "error": "請輸入日期、姓名、身分證"})
    conn = get_conn(); cur = conn.cursor()
    cur.execute("SELECT * FROM customers WHERE customer_name=? AND id_no=? ORDER BY created_at DESC", (name, id_no.upper()))
    rows = cur.fetchall(); conn.close()
    if not rows:
        return JSONResponse({"ok": False, "error": f"找不到客戶：{name} / {id_no}"})
    matched = None
    old_cases = []
    for row in rows:
        created = (row["created_at"] or "")[:10]
        created_fmt = created.replace("-", "/")
        try:
            y = int(created[:4]) - 1911
            created_roc = f"{y}/{created[5:7]}/{created[8:10]}"
        except:
            created_roc = ""
        if date in [created_fmt, created_roc]:
            if row["status"] == "ACTIVE":
                matched = row
            else:
                old_cases.append(row)
    if not matched:
        return JSONResponse({"ok": False, "error": f"找到客戶 {name} 但日期不符，請確認建立日期"})
    d = dict(matched)
    old_warning = ""
    if old_cases:
        oc = old_cases[0]
        old_warning = f"⚠️ 此客戶有舊案記錄\n舊案日期：{(oc['created_at'] or '')[:10]}\n舊案狀態：{oc['status']}\n請確認您填的是新案資料"
    return JSONResponse({
        "ok": True,
        "old_warning": old_warning,
        "data": {
            "customer_name": d.get("customer_name",""),
            "id_no": d.get("id_no",""),
            "birth_date": d.get("birth_date",""),
            "phone": d.get("phone",""),
            "email": d.get("email",""),
            "line_id": d.get("line_id",""),
            "marriage": d.get("marriage",""),
            "education": d.get("education",""),
            "id_issue_date": d.get("id_issue_date",""),
            "id_issue_place": d.get("id_issue_place",""),
            "id_issue_type": d.get("id_issue_type",""),
            "reg_city": d.get("reg_city",""),
            "reg_district": d.get("reg_district",""),
            "reg_address": d.get("reg_address",""),
            "reg_phone": d.get("reg_phone",""),
            "live_same_as_reg": d.get("live_same_as_reg",""),
            "live_city": d.get("live_city",""),
            "live_district": d.get("live_district",""),
            "live_address": d.get("live_address",""),
            "live_phone": d.get("live_phone",""),
            "live_status": d.get("live_status",""),
            "live_years": d.get("live_years",""),
            "live_months": d.get("live_months",""),
            "company_name_detail": d.get("company_name_detail",""),
            "company_industry": d.get("company_industry",""),
            "company_role": d.get("company_role",""),
            "company_phone_area": d.get("company_phone_area",""),
            "company_phone_num": d.get("company_phone_num",""),
            "company_phone_ext": d.get("company_phone_ext",""),
            "company_years": d.get("company_years",""),
            "company_salary": d.get("company_salary",""),
            "company_city": d.get("company_city",""),
            "company_district": d.get("company_district",""),
            "company_address": d.get("company_address",""),
            "bank_name": d.get("bank_name",""),
            "bank_branch": d.get("bank_branch",""),
            "bank_account": d.get("bank_account",""),
            "bank_holder": d.get("bank_holder",""),
            "contact1_name": d.get("contact1_name",""),
            "contact1_relation": d.get("contact1_relation",""),
            "contact1_phone": d.get("contact1_phone",""),
            "contact1_known": d.get("contact1_known",""),
            "contact2_name": d.get("contact2_name",""),
            "contact2_relation": d.get("contact2_relation",""),
            "contact2_phone": d.get("contact2_phone",""),
            "contact2_known": d.get("contact2_known",""),
            "product_model": d.get("product_model",""),
            "product_imei": d.get("product_imei",""),
            "vehicle_plate": d.get("vehicle_plate",""),
            "vehicle_type": d.get("vehicle_type",""),
            "vehicle_owner": d.get("vehicle_owner",""),
        }
    })
    # =========================
# =========================
# 新增客戶網頁（行政A）
# =========================
@app.get("/new-customer", response_class=HTMLResponse)
def new_customer_page(request: Request):
    from fastapi.responses import RedirectResponse
    role = check_auth(request)
    if not role: return RedirectResponse("/login")
    conn = get_conn(); cur = conn.cursor()
    cur.execute("SELECT * FROM groups WHERE is_active=1 ORDER BY group_name")
    groups = cur.fetchall(); conn.close()
    group_opts = "".join(f'<option value="{g["group_id"]}">{g["group_name"]}</option>' for g in groups)
    HTML_PAGE = '<!DOCTYPE html>\n<html lang="zh-TW">\n<head>\n<meta charset="UTF-8">\n<meta name="viewport" content="width=device-width, initial-scale=1.0">\n<title>新增客戶資料</title>\n<style>\n* { box-sizing: border-box; margin: 0; padding: 0; }\nbody { font-family: \'Microsoft JhengHei\', \'PingFang TC\', sans-serif; background: #ece8e2; color: #2c2820; font-size: 14px; }\n.topnav { background: #3a3530; padding: 0 20px; display: flex; align-items: center; height: 50px; gap: 4px; }\n.topnav a { color: #c8bfb5; text-decoration: none; padding: 7px 14px; border-radius: 6px; font-size: 14px; }\n.topnav a.active { background: #7c6f5e; color: #fff; }\n.topnav a:hover { background: #4a4540; color: #fff; }\n.page { max-width: 820px; margin: 24px auto; padding: 0 16px 40px; }\n.page-header { display: flex; justify-content: space-between; align-items: center; margin-bottom: 20px; padding-bottom: 12px; border-bottom: 2px solid #8a7a68; }\nh2 { font-size: 20px; font-weight: 700; color: #2c2820; }\n.card { background: #faf6f2; border-radius: 10px; padding: 18px; margin-bottom: 14px; box-shadow: 0 1px 4px rgba(0,0,0,0.08); border: 1px solid #ddd5ca; }\n.section-title { font-size: 13px; font-weight: 700; color: #5a4e40; margin-bottom: 14px; padding-bottom: 7px; border-bottom: 1px solid #ddd5ca; letter-spacing: 0.5px; }\n.grid2 { display: grid; grid-template-columns: 1fr 1fr; gap: 12px; }\n.grid3 { display: grid; grid-template-columns: 1fr 1fr 1fr; gap: 12px; }\n.grid4 { display: grid; grid-template-columns: 1fr 1fr 1fr 1fr; gap: 12px; }\n.full { grid-column: 1 / -1; }\nlabel { display: block; font-size: 12px; color: #5a4e40; margin-bottom: 5px; font-weight: 600; }\n.req { color: #b84a35; }\ninput, select, textarea {\n  width: 100%; padding: 8px 11px; border: 1px solid #c8bfb5;\n  border-radius: 6px; font-size: 14px; font-family: inherit;\n  background: #fff; color: #2c2820; transition: border 0.15s;\n}\ninput:focus, select:focus, textarea:focus { outline: none; border-color: #7c6f5e; background: #fffcf9; }\ntextarea { resize: vertical; min-height: 65px; }\n.hint { font-size: 12px; color: #8a7a68; margin-top: 4px; }\n.auto-val { font-size: 15px; color: #b84a35; font-weight: 700; margin-top: 5px; }\n.vehicle-card { background: #f0ebe4; border: 1px solid #ccc5ba; border-radius: 8px; padding: 14px; margin-bottom: 10px; position: relative; }\n.vehicle-card .card-label { font-size: 13px; font-weight: 700; color: #5a4e40; margin-bottom: 10px; }\n.remove-btn { position: absolute; top: 12px; right: 12px; background: #f5ddd8; color: #b84a35; border: none; border-radius: 4px; padding: 4px 12px; font-size: 12px; cursor: pointer; font-weight: 600; }\n.debt-header { display: grid; grid-template-columns: 1.4fr 0.7fr 1fr 0.7fr 1.1fr 1fr 30px; gap: 5px; font-size: 12px; color: #5a4e40; font-weight: 600; margin-bottom: 6px; }\n.debt-row { display: grid; grid-template-columns: 1.4fr 0.7fr 1fr 0.7fr 1.1fr 1fr 30px; gap: 5px; margin-bottom: 7px; align-items: center; }\n.debt-row input { font-size: 13px; padding: 6px 8px; }\n.debt-remain { font-size: 13px; font-weight: 700; color: #b84a35; }\n.debt-del { background: #f5ddd8; color: #b84a35; border: none; border-radius: 4px; width: 30px; height: 32px; cursor: pointer; font-size: 15px; }\n.add-btn { background: #e8e2da; color: #4a3e30; border: 1px dashed #a09080; border-radius: 6px; padding: 8px 16px; font-size: 13px; cursor: pointer; margin-top: 8px; font-weight: 600; }\n.add-btn:hover { background: #ddd5ca; }\n.btn-row { display: flex; gap: 10px; margin-top: 22px; flex-wrap: wrap; }\n.btn { padding: 11px 26px; border-radius: 8px; font-size: 15px; font-weight: 700; cursor: pointer; border: none; font-family: inherit; }\n.btn-primary { background: #6a5e4e; color: #fff; }\n.btn-primary:hover { background: #5a4e40; }\n.btn-export { background: #4e7055; color: #fff; }\n.btn-export:hover { background: #3e5e45; }\n.btn-cancel { background: #ddd5ca; color: #4a3e30; }\n.btn-cancel:hover { background: #ccc5ba; }\n.ig { display: flex; gap: 6px; align-items: center; }\n.ig span { font-size: 14px; white-space: nowrap; line-height: 36px; color: #4a3e30; font-weight: 600; }\n</style>\n</head>\n<body>\n<div class="topnav">\n  <a href="#">&#128202; 日報</a>\n  <a href="#">&#128269; 查詢</a>\n  <a href="#" class="active">&#10133; 新增客戶</a>\n</div>\n<div class="page">\n  <div class="page-header">\n    <h2>新增客戶資料</h2>\n    <button class="btn btn-export" onclick="exportPDF()">&#128196; 導出PDF</button>\n  </div>\n  <form id="cf">\n    <div class="card">\n      <div class="section-title">所屬群組</div>\n      <div class="grid2">\n        <div><label>群組</label><select name="grp"><option>B群</option><option>C群</option></select></div>\n        <div><label>業務姓名</label><input name="sales" placeholder="王業務"></div>\n      </div>\n    </div>\n    <div class="card">\n      <div class="section-title">基本資料</div>\n      <div class="grid2">\n        <div><label>客戶姓名 <span class="req">*</span></label><input name="cname" placeholder="王小明"></div>\n        <div><label>身分證字號 <span class="req">*</span></label><input name="idno" placeholder="A123456789" style="text-transform:uppercase"></div>\n        <div><label>出生年月日 <span class="req">*</span></label><input name="birth" placeholder="086/12/15"><div class="hint">民國年：086/12/15</div></div>\n        <div><label>行動電話 <span class="req">*</span></label><input name="phone" placeholder="0912-345678"></div>\n        <div><label>電信業者</label><select name="carrier"><option>中華電信</option><option>遠傳電信</option><option>台灣大哥大</option><option>台灣之星</option><option>亞太電信</option><option>其他</option></select></div>\n        <div><label>Email</label><input name="email" placeholder="example@gmail.com"></div>\n        <div><label>LINE ID</label><input name="line"></div>\n        <div><label>客戶FB</label><input name="fb" placeholder="Facebook名稱"></div>\n        <div><label>婚姻狀態</label><select name="marry"><option>未婚</option><option>已婚</option></select></div>\n        <div><label>最高學歷</label><select name="edu"><option>高中/職</option><option>專科/大學</option><option>研究所以上</option><option>其他</option></select></div>\n      </div>\n    </div>\n    <div class="card">\n      <div class="section-title">身分證發證資料</div>\n      <div class="grid3">\n        <div><label>發證日期 <span class="req">*</span></label><input name="iddate" placeholder="114/03/05"><div class="hint">民國年：114/03/05</div></div>\n        <div><label>發證地 <span class="req">*</span></label><select name="idplace"><option>北市</option><option>新北市</option><option>桃市</option><option>中市</option><option>南市</option><option>高市</option><option>基市</option><option>竹市</option><option>竹縣</option><option>苗縣</option><option>彰縣</option><option>投縣</option><option>雲縣</option><option>嘉市</option><option>嘉縣</option><option>屏縣</option><option>宜縣</option><option>花縣</option><option>東縣</option><option>澎縣</option><option>金門</option><option>連江</option></select></div>\n        <div><label>換補發類別 <span class="req">*</span></label><select name="idtype"><option>初發</option><option>補發</option><option>換發</option></select></div>\n      </div>\n    </div>\n    <div class="card">\n      <div class="section-title">地址資料</div>\n      <div style="font-size:13px;font-weight:700;color:#4a3e30;margin-bottom:10px">戶籍地址</div>\n      <div class="grid3" style="margin-bottom:10px">\n        <div><label>縣市 <span class="req">*</span></label><select name="rcity"><option>台北市</option><option>新北市</option><option>桃園市</option><option>台中市</option><option>台南市</option><option>高雄市</option><option>基隆市</option><option>新竹市</option><option>新竹縣</option><option>苗栗縣</option><option>彰化縣</option><option>南投縣</option><option>雲林縣</option><option>嘉義市</option><option>嘉義縣</option><option>屏東縣</option><option>宜蘭縣</option><option>花蓮縣</option><option>台東縣</option><option>澎湖縣</option><option>金門縣</option><option>連江縣</option></select></div>\n        <div><label>區/鄉鎮</label><input name="rdist" placeholder="苗栗市"></div>\n        <div><label>詳細地址 <span class="req">*</span></label><input name="raddr" placeholder="新東街257號"></div>\n      </div>\n      <div style="margin-bottom:12px"><label>戶籍電話</label><input name="rphone" placeholder="037-123456" style="max-width:200px"></div>\n      <label style="display:flex;align-items:center;gap:8px;font-size:14px;color:#3a3020;margin-bottom:12px;cursor:pointer;font-weight:600">\n        <input type="checkbox" id="sameck" checked onchange="document.getElementById(\'lsec\').style.display=this.checked?\'none\':\'block\'">\n        住家地址與戶籍相同\n      </label>\n      <div id="lsec" style="display:none;margin-bottom:12px">\n        <div style="font-size:13px;font-weight:700;color:#4a3e30;margin-bottom:10px">住家地址</div>\n        <div class="grid3">\n          <div><label>縣市</label><select name="lcity"><option>台北市</option><option>新北市</option><option>桃園市</option><option>台中市</option><option>台南市</option><option>高雄市</option><option>苗栗縣</option><option>其他</option></select></div>\n          <div><label>區/鄉鎮</label><input name="ldist"></div>\n          <div><label>詳細地址</label><input name="laddr"></div>\n        </div>\n      </div>\n      <div class="grid3">\n        <div><label>現住電話</label><input name="lphone" placeholder="037-123456"></div>\n        <div><label>居住狀況</label><select name="lstatus"><option>自有</option><option>配偶</option><option>父母</option><option>親屬</option><option>租屋</option><option>宿舍</option></select></div>\n        <div><label>居住時間</label><div class="ig"><input name="lyear" placeholder="5" style="width:58px"><span>年</span><input name="lmon" placeholder="0" style="width:58px"><span>月</span></div></div>\n      </div>\n    </div>\n    <div class="card">\n      <div class="section-title">職業資料</div>\n      <div class="grid2">\n        <div class="full"><label>公司名稱 <span class="req">*</span></label><input name="cmpname" placeholder="嘉合企業社"></div>\n        <div><label>公司電話 <span class="req">*</span></label><div class="ig"><select name="carea" style="width:82px"><option>02</option><option>03</option><option>037</option><option>04</option><option>049</option><option>05</option><option>06</option><option>07</option><option>08</option><option>089</option></select><input name="cnum" placeholder="1234567"><input name="cext" placeholder="分機" style="width:68px"></div></div>\n        <div><label>職稱</label><input name="crole" placeholder="技工"></div>\n        <div><label>年資</label><div class="ig"><input name="cyear" placeholder="3" style="width:62px"><span>年</span><input name="cmon" placeholder="0" style="width:62px"><span>月</span></div></div>\n        <div><label>月薪（萬）</label><input name="csal" placeholder="3.5"></div>\n        <div class="full"><label>公司地址</label><div style="display:grid;grid-template-columns:1fr 1fr 2fr;gap:8px"><select name="ccity"><option>台北市</option><option>新北市</option><option>桃園市</option><option>台中市</option><option>台南市</option><option>高雄市</option><option>苗栗縣</option><option>其他</option></select><input name="cdist" placeholder="區/鄉鎮"><input name="caddr" placeholder="詳細地址"></div></div>\n      </div>\n    </div>\n    <div class="card">\n      <div class="section-title">聯絡人資料</div>\n      <div style="margin-bottom:14px;padding-bottom:14px;border-bottom:1px solid #ddd5ca">\n        <div style="font-size:13px;color:#5a4e40;font-weight:600;margin-bottom:10px">聯絡人1（需為二等親）</div>\n        <div class="grid4">\n          <div><label>姓名 <span class="req">*</span></label><input name="c1name" placeholder="吳玉英"></div>\n          <div><label>關係</label><input name="c1rel" placeholder="母"></div>\n          <div><label>電話 <span class="req">*</span></label><input name="c1tel" placeholder="0978-055530"></div>\n          <div><label>可知情</label><select name="c1know"><option>可知情</option><option>保密</option><option>無可知情</option></select></div>\n        </div>\n      </div>\n      <div>\n        <div style="font-size:13px;color:#5a4e40;font-weight:600;margin-bottom:10px">聯絡人2</div>\n        <div class="grid4">\n          <div><label>姓名 <span class="req">*</span></label><input name="c2name" placeholder="賴俊明"></div>\n          <div><label>關係</label><input name="c2rel" placeholder="友"></div>\n          <div><label>電話 <span class="req">*</span></label><input name="c2tel" placeholder="0919-616821"></div>\n          <div><label>可知情</label><select name="c2know"><option>可知情</option><option>保密</option><option>無可知情</option></select></div>\n        </div>\n      </div>\n    </div>\n    <div class="card">\n      <div class="section-title">貸款諮詢事項</div>\n      <div class="grid2">\n        <div><label>資金需求</label><input name="efund" placeholder="$100,000"></div>\n        <div><label>近三月是否送件</label><select name="esent"><option>否</option><option>是</option></select></div>\n        <div><label>當鋪私設</label><select name="eprivate"><option>無</option><option>有</option></select></div>\n        <div><label>勞保狀態</label><select name="elabor"><option>公司保</option><option>工會保</option><option>自行投保</option><option>無勞保</option></select></div>\n        <div><label>有無薪轉</label><select name="esal"><option>有薪轉</option><option>無薪轉</option></select></div>\n        <div><label>有無證照</label><select name="elicense"><option>無</option><option>有</option></select></div>\n        <div><label>貸款遲繳</label><select name="elate"><option>無</option><option>有</option></select></div>\n        <div><label>遲繳天數</label><input name="elateday" placeholder="0"></div>\n        <div><label>罰單欠費金額 $</label><input name="efine" placeholder="0" type="number" min="0" oninput="calcF()"></div>\n        <div><label>燃料稅金額 $</label><input name="efuel" placeholder="0" type="number" min="0" oninput="calcF()"></div>\n        <div class="full"><label>欠費總額（自動計算）</label><div class="auto-val" id="tfees">$0</div></div>\n        <div><label>名下信用卡</label><input name="ecard" placeholder="銀行協商"></div>\n        <div><label>有無動產/不動產</label><input name="eprop" placeholder="有機車"></div>\n        <div><label>法學（幾條）</label><input name="elaw" placeholder="共1條"></div>\n        <div class="full"><label>備註</label><textarea name="enote" placeholder="其他說明..."></textarea></div>\n      </div>\n    </div>\n    <div class="card">\n      <div class="section-title">汽機車資料</div>\n      <div id="vlist"></div>\n      <button type="button" class="add-btn" onclick="addV()">&#10133; 新增車輛</button>\n    </div>\n    <div class="card">\n      <div class="section-title">負債明細</div>\n      <div class="debt-header"><div>貸款商家</div><div>期數</div><div>月繳</div><div>已繳期</div><div>剩餘(自動)</div><div>設定日期</div><div></div></div>\n      <div id="dlist"></div>\n      <button type="button" class="add-btn" onclick="addD()">&#10133; 新增負債</button>\n    </div>\n    <div class="btn-row">\n      <button type="submit" class="btn btn-primary">&#9989; 建立客戶</button>\n      <button type="button" class="btn btn-export" onclick="exportPDF()">&#128196; 導出PDF</button>\n      <button type="button" class="btn btn-cancel">取消</button>\n    </div>\n  </form>\n</div>\n<script>\nlet vc=0,dc=0;\nfunction gv(n){const e=document.querySelector(\'[name="\'+n+\'"]\');return e?e.value||\'\':\'\';}\n\nfunction addV(){\n  vc++;const n=vc;\n  const d=document.createElement(\'div\');\n  d.className=\'vehicle-card\';d.id=\'v\'+n;\n  d.innerHTML=\'<div class="card-label">車輛 \'+n+\'</div>\'\n    +\'<button type="button" class="remove-btn" onclick="document.getElementById(\\\'v\'+n+\'\\\').remove()">✕ 移除</button>\'\n    +\'<div class="grid2">\'\n    +\'<div><label>車牌</label><input id="vp\'+n+\'" placeholder="677-NSY"></div>\'\n    +\'<div><label>持有時間</label><input id="vd\'+n+\'" placeholder="3年"></div>\'\n    +\'<div><label>公路動保設定</label><select id="vr\'+n+\'"><option>無</option><option>有</option></select></div>\'\n    +\'<div><label>有無空間</label><select id="vs\'+n+\'"><option>有空間</option><option>無空間</option></select></div>\'\n    +\'</div>\';\n  document.getElementById(\'vlist\').appendChild(d);\n}\n\nfunction addD(){\n  dc++;const n=dc;\n  const r=document.createElement(\'div\');\n  r.className=\'debt-row\';r.id=\'d\'+n;\n  r.innerHTML=\'<input id="dc\'+n+\'" placeholder="裕融">\'\n    +\'<input id="dp\'+n+\'" placeholder="36" type="number" oninput="calcD(\'+n+\')">\'\n    +\'<input id="dm\'+n+\'" placeholder="5265" type="number" oninput="calcD(\'+n+\')">\'\n    +\'<input id="da\'+n+\'" placeholder="20" type="number" oninput="calcD(\'+n+\')">\'\n    +\'<div class="debt-remain" id="dr\'+n+\'">-</div>\'\n    +\'<input id="dd\'+n+\'" placeholder="2023/01">\'\n    +\'<button type="button" class="debt-del" onclick="document.getElementById(\\\'d\'+n+\'\\\').remove()">✕</button>\';\n  document.getElementById(\'dlist\').appendChild(r);\n}\n\nfunction calcD(n){\n  const m=parseFloat(document.getElementById(\'dm\'+n)?.value)||0;\n  const p=parseFloat(document.getElementById(\'dp\'+n)?.value)||0;\n  const a=parseFloat(document.getElementById(\'da\'+n)?.value)||0;\n  const el=document.getElementById(\'dr\'+n);if(!el)return;\n  if(m>0&&p>0){\n    const rem=(m*p)-(m*a);\n    el.textContent=\'$\'+Math.round(rem).toLocaleString();\n    el.style.color=rem>0?\'#b84a35\':\'#4e7055\';\n  }else el.textContent=\'-\';\n}\n\nfunction calcF(){\n  const f=parseFloat(document.querySelector(\'[name="efine"]\')?.value)||0;\n  const u=parseFloat(document.querySelector(\'[name="efuel"]\')?.value)||0;\n  document.getElementById(\'tfees\').textContent=\'$\'+(f+u).toLocaleString();\n}\n\nfunction sec(t){\n  return \'<div style="background:#6a5e4e;color:#fff;font-size:12px;font-weight:700;padding:7px 12px;margin-top:10px;border-radius:4px 4px 0 0;letter-spacing:0.5px">\'+t+\'</div>\'\n        +\'<div style="border:1px solid #ccc5ba;border-top:none;border-radius:0 0 4px 4px;padding:8px 12px;margin-bottom:6px;background:#faf6f2">\';\n}\nfunction fl(l,v){\n  if(!v||v===\'-\'||v===\'0年0月\')return\'\';\n  return \'<div style="display:grid;grid-template-columns:95px 1fr;gap:6px;padding:5px 0;border-bottom:1px solid #ece8e2">\'\n    +\'<span style="font-size:11px;color:#6a5e4e;line-height:1.6;font-weight:600">\'+l+\'</span>\'\n    +\'<span style="font-size:13px;font-weight:500;color:#2c2820">\'+v+\'</span></div>\';\n}\nfunction fl2(items){\n  let h=\'<div style="display:grid;grid-template-columns:1fr 1fr;gap:0 20px">\';\n  items.forEach(function(x){h+=fl(x[0],x[1]);});\n  return h+\'</div>\';\n}\n\nfunction exportPDF(){\n  const name=gv(\'cname\')||\'客戶\';\n  const same=document.getElementById(\'sameck\').checked;\n  const raddr=gv(\'rcity\')+gv(\'rdist\')+gv(\'raddr\');\n  const laddr=same?\'同戶籍\':(gv(\'lcity\')+gv(\'ldist\')+gv(\'laddr\'));\n  const ctel=gv(\'carea\')+\'-\'+gv(\'cnum\')+(gv(\'cext\')?\'#\'+gv(\'cext\'):\'\');\n  const caddr=gv(\'ccity\')+gv(\'cdist\')+gv(\'caddr\');\n  const fine=parseFloat(document.querySelector(\'[name="efine"]\')?.value)||0;\n  const fuel=parseFloat(document.querySelector(\'[name="efuel"]\')?.value)||0;\n  const tot=fine+fuel;\n\n  let vhtml=\'\';\n  for(let i=1;i<=vc;i++){\n    if(!document.getElementById(\'v\'+i))continue;\n    const pl=document.getElementById(\'vp\'+i)?.value||\'-\';\n    const du=document.getElementById(\'vd\'+i)?.value||\'-\';\n    const ro=document.getElementById(\'vr\'+i)?.value||\'-\';\n    const sp=document.getElementById(\'vs\'+i)?.value||\'-\';\n    vhtml+=\'<div style="display:grid;grid-template-columns:1fr 1fr 1fr 1fr;gap:8px;padding:6px 0;border-bottom:1px solid #ece8e2">\'\n      +\'<div><div style="font-size:11px;color:#6a5e4e;font-weight:600">車輛\'+i+\' 車牌</div><div style="font-size:13px;font-weight:500;color:#2c2820">\'+pl+\'</div></div>\'\n      +\'<div><div style="font-size:11px;color:#6a5e4e;font-weight:600">持有時間</div><div style="font-size:13px;font-weight:500;color:#2c2820">\'+du+\'</div></div>\'\n      +\'<div><div style="font-size:11px;color:#6a5e4e;font-weight:600">公路動保</div><div style="font-size:13px;font-weight:500;color:#2c2820">\'+ro+\'</div></div>\'\n      +\'<div><div style="font-size:11px;color:#6a5e4e;font-weight:600">有無空間</div><div style="font-size:13px;font-weight:500;color:#2c2820">\'+sp+\'</div></div>\'\n      +\'</div>\';\n  }\n\n  let dhtml=\'<table style="width:100%;border-collapse:collapse;font-size:12px">\'\n    +\'<tr style="background:#ece8e2">\'\n    +\'<th style="padding:6px 8px;text-align:left;font-weight:700;color:#4a3e30">商家</th>\'\n    +\'<th style="padding:6px 8px;text-align:right;font-weight:700;color:#4a3e30">期數</th>\'\n    +\'<th style="padding:6px 8px;text-align:right;font-weight:700;color:#4a3e30">月繳</th>\'\n    +\'<th style="padding:6px 8px;text-align:right;font-weight:700;color:#4a3e30">已繳期</th>\'\n    +\'<th style="padding:6px 8px;text-align:right;font-weight:700;color:#4a3e30">剩餘金額</th>\'\n    +\'<th style="padding:6px 8px;text-align:left;font-weight:700;color:#4a3e30">設定日期</th></tr>\';\n  let hasD=false;\n  for(let i=1;i<=dc;i++){\n    if(!document.getElementById(\'d\'+i))continue;\n    const co=document.getElementById(\'dc\'+i)?.value||\'\';\n    if(!co)continue;\n    hasD=true;\n    const per=document.getElementById(\'dp\'+i)?.value||\'-\';\n    const mon=document.getElementById(\'dm\'+i)?.value||\'-\';\n    const pai=document.getElementById(\'da\'+i)?.value||\'-\';\n    const rem=document.getElementById(\'dr\'+i)?.textContent||\'-\';\n    const dat=document.getElementById(\'dd\'+i)?.value||\'-\';\n    dhtml+=\'<tr style="border-bottom:1px solid #ece8e2">\'\n      +\'<td style="padding:6px 8px;color:#2c2820">\'+co+\'</td>\'\n      +\'<td style="padding:6px 8px;text-align:right;color:#2c2820">\'+per+\'</td>\'\n      +\'<td style="padding:6px 8px;text-align:right;color:#2c2820">\'+mon+\'</td>\'\n      +\'<td style="padding:6px 8px;text-align:right;color:#2c2820">\'+pai+\'</td>\'\n      +\'<td style="padding:6px 8px;text-align:right;color:#b84a35;font-weight:700">\'+rem+\'</td>\'\n      +\'<td style="padding:6px 8px;color:#2c2820">\'+dat+\'</td></tr>\';\n  }\n  dhtml+=\'</table>\';\n\n  const phdr=function(pt,showbtn){\n    return \'<div style="padding:14px 20px 12px;border-bottom:3px solid #8a7a68;display:flex;justify-content:space-between;align-items:center;background:#faf6f2">\'\n      +\'<div><div style="font-size:11px;color:#8a7a68;font-weight:600">\'+pt+\'</div>\'\n      +\'<div style="font-size:22px;font-weight:700;color:#3a3020;margin:3px 0">\'+name+\'</div>\'\n      +\'<div style="font-size:11px;color:#8a7a68">日期：\'+new Date().toLocaleDateString(\'zh-TW\')+\'\u3000業務：\'+(gv(\'sales\')||\'-\')+\'\u3000群組：\'+(gv(\'grp\')||\'-\')+\'</div></div>\'\n      +(showbtn?\'<button onclick="window.print()" style="background:#4e7055;color:#fff;border:none;padding:9px 20px;border-radius:6px;font-size:14px;cursor:pointer;font-weight:700;font-family:inherit" class="np">&#128190; 存PDF</button>\':\'\')\n      +\'</div>\';\n  };\n\n  const html=\'<!DOCTYPE html><html lang="zh-TW"><head><meta charset="UTF-8"><title>客戶資料表 - \'+name+\'</title>\'\n    +\'<style>*{box-sizing:border-box;margin:0;padding:0;}body{font-family:"Microsoft JhengHei","PingFang TC",sans-serif;background:#faf6f2;color:#2c2820;font-size:14px;}\'\n    +\'@media print{@page{size:A4 portrait;margin:10mm 13mm;}.np{display:none!important;}.pb{page-break-before:always;}}</style>\'\n    +\'</head><body>\'\n    +phdr(\'客戶資料表\u3000第1頁／共2頁\',true)\n    +\'<div style="padding:10px 20px">\'\n    +sec(\'基本資料\')\n    +fl2([[\'姓名\',gv(\'cname\')],[\'身分證\',gv(\'idno\').toUpperCase()],[\'出生年月日\',gv(\'birth\')],[\'行動電話\',gv(\'phone\')],[\'電信業者\',gv(\'carrier\')],[\'Email\',gv(\'email\')],[\'LINE ID\',gv(\'line\')],[\'客戶FB\',gv(\'fb\')],[\'婚姻狀態\',gv(\'marry\')],[\'最高學歷\',gv(\'edu\')]])\n    +\'</div></div>\'\n    +sec(\'身分證發證\')\n    +fl2([[\'發證日期\',gv(\'iddate\')],[\'發證地\',gv(\'idplace\')],[\'換補發類別\',gv(\'idtype\')]])\n    +\'</div></div>\'\n    +sec(\'地址資料\')\n    +fl2([[\'戶籍地址\',raddr],[\'戶籍電話\',gv(\'rphone\')],[\'住家地址\',laddr],[\'現住電話\',gv(\'lphone\')],[\'居住狀況\',gv(\'lstatus\')],[\'居住時間\',gv(\'lyear\')+\'年\'+gv(\'lmon\')+\'月\']])\n    +\'</div></div>\'\n    +sec(\'職業資料\')\n    +fl2([[\'公司名稱\',gv(\'cmpname\')],[\'公司電話\',ctel],[\'職稱\',gv(\'crole\')],[\'年資\',gv(\'cyear\')+\'年\'+gv(\'cmon\')+\'月\'],[\'月薪\',gv(\'csal\')+\'萬\'],[\'公司地址\',caddr]])\n    +\'</div></div>\'\n    +sec(\'聯絡人資料\')\n    +fl2([[\'聯絡人1\',gv(\'c1name\')+\'（\'+gv(\'c1rel\')+\'）\'],[\'電話1\',gv(\'c1tel\')],[\'知情1\',gv(\'c1know\')],[\'聯絡人2\',gv(\'c2name\')+\'（\'+gv(\'c2rel\')+\'）\'],[\'電話2\',gv(\'c2tel\')],[\'知情2\',gv(\'c2know\')]])\n    +\'</div></div>\'\n    +\'</div>\'\n    +\'<div class="pb"></div>\'\n    +phdr(\'客戶資料表\u3000第2頁／共2頁\',false)\n    +\'<div style="padding:10px 20px">\'\n    +sec(\'貸款諮詢事項\')\n    +fl2([[\'資金需求\',gv(\'efund\')],[\'近三月送件\',gv(\'esent\')],[\'當鋪私設\',gv(\'eprivate\')],[\'勞保狀態\',gv(\'elabor\')],[\'有無薪轉\',gv(\'esal\')],[\'有無證照\',gv(\'elicense\')],[\'貸款遲繳\',gv(\'elate\')],[\'遲繳天數\',gv(\'elateday\')],[\'罰單欠費\',\'$\'+fine.toLocaleString()],[\'燃料稅\',\'$\'+fuel.toLocaleString()],[\'欠費總額\',\'$\'+tot.toLocaleString()],[\'名下信用卡\',gv(\'ecard\')],[\'動產/不動產\',gv(\'eprop\')],[\'法學\',gv(\'elaw\')]])\n    +(gv(\'enote\')?fl(\'備註\',gv(\'enote\')):\'\')\n    +\'</div></div>\'\n    +(vhtml?sec(\'汽機車資料\')+vhtml+\'</div></div>\':\'\')\n    +(hasD?sec(\'負債明細\')+dhtml+\'</div></div>\':\'\')\n    +\'</div></body></html>\';\n\n  const w=window.open(\'\',\'_blank\',\'width=880,height=720\');\n  w.document.write(html);\n  w.document.close();\n}\n\naddV();addD();\n</script>\n</body>\n</html>\n'
    HTML_PAGE = HTML_PAGE.replace('<option>B群</option><option>C群</option>', group_opts)
    return HTML_PAGE


@app.post("/new-customer")
async def new_customer_post(request: Request):
    from fastapi.responses import RedirectResponse
    role = check_auth(request)
    if not role: return RedirectResponse("/login")
    form = await request.form()
    f = dict(form)
    name = f.get("cname","").strip()
    id_no = f.get("idno","").strip().upper()
    if not name or not id_no:
        return HTMLResponse("姓名和身分證為必填", status_code=400)
    source_group_id = f.get("grp","")
    live_same = "1" if f.get("sameck") else "0"
    conn = get_conn(); cur = conn.cursor()
    cur.execute("SELECT * FROM customers WHERE id_no=? AND status='ACTIVE'", (id_no,))
    existing = cur.fetchone()
    now = now_iso()
    if existing:
        case_id = existing["case_id"]
    else:
        case_id = short_id()
        cur.execute("""INSERT INTO customers
            (case_id,customer_name,id_no,source_group_id,company,last_update,status,created_at,updated_at)
            VALUES (?,?,?,?,?,?,'ACTIVE',?,?)""",
            (case_id, name, id_no, source_group_id, "", "新增客戶："+name, now, now))
    extra = dict(
        birth_date=f.get("birth",""), phone=f.get("phone",""), email=f.get("email",""),
        line_id=f.get("line",""), marriage=f.get("marry",""), education=f.get("edu",""),
        id_issue_date=f.get("iddate",""), id_issue_place=f.get("idplace",""), id_issue_type=f.get("idtype",""),
        reg_city=f.get("rcity",""), reg_district=f.get("rdist",""), reg_address=f.get("raddr",""), reg_phone=f.get("rphone",""),
        live_same_as_reg=live_same,
        live_city=f.get("lcity","") if live_same=="0" else f.get("rcity",""),
        live_district=f.get("ldist","") if live_same=="0" else f.get("rdist",""),
        live_address=f.get("laddr","") if live_same=="0" else f.get("raddr",""),
        live_phone=f.get("lphone",""), live_status=f.get("lstatus",""),
        live_years=f.get("lyear",""), live_months=f.get("lmon",""),
        company_name_detail=f.get("cmpname",""), company_phone_area=f.get("carea",""),
        company_phone_num=f.get("cnum",""), company_phone_ext=f.get("cext",""),
        company_role=f.get("crole",""), company_years=f.get("cyear",""),
        company_salary=f.get("csal",""), company_city=f.get("ccity",""),
        company_district=f.get("cdist",""), company_address=f.get("caddr",""),
        contact1_name=f.get("c1name",""), contact1_relation=f.get("c1rel",""),
        contact1_phone=f.get("c1tel",""), contact1_known=f.get("c1know",""),
        contact2_name=f.get("c2name",""), contact2_relation=f.get("c2rel",""),
        contact2_phone=f.get("c2tel",""), contact2_known=f.get("c2know",""),
        eval_fund_need=f.get("efund",""), eval_sent_3m=f.get("esent",""),
        eval_labor_ins=f.get("elabor",""), eval_salary_transfer=f.get("esal",""),
        eval_alert=f.get("eprivate",""), eval_credit_card=f.get("ecard",""),
        eval_property=f.get("eprop",""), eval_fine=f.get("efine",""),
        eval_fuel_tax=f.get("efuel",""), eval_late=f.get("elate",""),
        eval_late_days=f.get("elateday",""), eval_note=f.get("enote",""),
        sales_name=f.get("sales",""), source_group_id=source_group_id, created_by_role=role,
    )
    flds = ", ".join(k+" = ?" for k in extra)
    vals = list(extra.values()) + [now_iso(), case_id]
    cur.execute("UPDATE customers SET "+flds+", updated_at=? WHERE case_id=?", vals)
    conn.commit(); conn.close()
    gname = get_group_name(source_group_id)
    push_text(A_GROUP_ID, "✅ 新客戶建立："+name+"（"+gname+"）\n請行政B判別方案")
    return RedirectResponse("/report", status_code=303)

# =========================
# 密碼管理頁面（管理員專用）
# =========================
@app.get("/admin/passwords", response_class=HTMLResponse)
def passwords_page(request: Request):
    from fastapi.responses import RedirectResponse
    role = check_auth(request)
    if role != "admin": return RedirectResponse("/login")
    conn = get_conn(); cur = conn.cursor()
    cur.execute("SELECT group_id, group_name FROM groups WHERE group_type='SALES_GROUP' AND is_active=1 ORDER BY group_name")
    sales_groups = cur.fetchall(); conn.close()
    grp_rows = "".join(f"""
        <div style="display:grid;grid-template-columns:120px 1fr 100px;gap:10px;align-items:end;padding:10px 0;border-bottom:1px solid #e5e7eb">
          <div style="font-size:14px;font-weight:500">{g["group_name"]}</div>
          <input type="password" id="gpw_{g["group_id"]}" class="input" placeholder="輸入新密碼">
          <button onclick="changePw('group','{g["group_id"]}','gpw_{g["group_id"]}')" class="btn btn-primary">更新</button>
        </div>""" for g in sales_groups)
    return f"""<!DOCTYPE html><html><head>{PAGE_CSS}<title>密碼管理</title></head><body>
    {make_topnav("admin","passwords")}
    <div class="page" style="max-width:680px">
      <h2 style="font-size:18px;font-weight:600;margin-bottom:20px">🔑 密碼管理</h2>
      <div id="msg"></div>
      <div style="background:#fff;border:1px solid #e5e7eb;border-radius:10px;padding:20px;margin-bottom:16px">
        <div style="font-size:13px;font-weight:600;color:#6b7280;margin-bottom:14px;padding-bottom:8px;border-bottom:1px solid #f3f4f6">系統角色密碼</div>
        <div style="display:grid;grid-template-columns:120px 1fr 100px;gap:10px;align-items:end;padding:10px 0;border-bottom:1px solid #f3f4f6">
          <div style="font-size:14px;font-weight:500">管理員</div>
          <input type="password" id="admin_pw" class="input" placeholder="輸入新密碼">
          <button onclick="changePw('admin','','admin_pw')" class="btn btn-primary">更新</button>
        </div>
        <div style="display:grid;grid-template-columns:120px 1fr 100px;gap:10px;align-items:end;padding:10px 0;border-bottom:1px solid #f3f4f6">
          <div style="font-size:14px;font-weight:500">行政B</div>
          <input type="password" id="adminB_pw" class="input" placeholder="輸入新密碼">
          <button onclick="changePw('adminB','','adminB_pw')" class="btn btn-primary">更新</button>
        </div>
        <div style="display:grid;grid-template-columns:120px 1fr 100px;gap:10px;align-items:end;padding:10px 0;border-bottom:1px solid #f3f4f6">
          <div style="font-size:14px;font-weight:500">行政A</div>
          <input type="password" id="report_pw" class="input" placeholder="輸入新密碼">
          <button onclick="changePw('normal','','report_pw')" class="btn btn-primary">更新</button>
        </div>
        <div style="display:grid;grid-template-columns:120px 1fr 100px;gap:10px;align-items:end;padding:10px 0">
          <div style="font-size:14px;font-weight:500">VBA密鑰</div>
          <input type="password" id="vba_secret" class="input" placeholder="輸入新密鑰">
          <button onclick="changePw('vba','','vba_secret')" class="btn btn-primary">更新</button>
        </div>
      </div>
      <div style="background:#fff;border:1px solid #e5e7eb;border-radius:10px;padding:20px;margin-bottom:16px">
        <div style="font-size:13px;font-weight:600;color:#6b7280;margin-bottom:14px;padding-bottom:8px;border-bottom:1px solid #f3f4f6">業務群組密碼</div>
        {grp_rows if grp_rows else '<div style="color:#9ca3af;font-size:13px;padding:10px 0">尚無業務群組</div>'}
      </div>
      <div style="background:#fffbeb;border:1px solid #fde68a;border-radius:8px;padding:12px 16px;font-size:13px;color:#92400e">
        ⚠️ 更新密碼後立即生效，請通知相關人員！
      </div>
    </div>
    <script>
    async function changePw(role, groupId, inputId) {{
      const pw = document.getElementById(inputId).value.trim();
      if (!pw) {{ alert("請輸入新密碼"); return; }}
      if (pw.length < 6) {{ alert("密碼至少6個字元"); return; }}
      const r = await fetch("/admin/update-password", {{
        method: "POST",
        headers: {{"Content-Type":"application/json"}},
        body: JSON.stringify({{role, group_id: groupId, password: pw}})
      }});
      const d = await r.json();
      const msg = document.getElementById("msg");
      if (d.ok) {{
        msg.innerHTML = '<div style="background:#dcfce7;color:#15803d;padding:10px 14px;border-radius:6px;margin-bottom:14px">✅ ' + d.message + '</div>';
        document.getElementById(inputId).value = "";
      }} else {{
        msg.innerHTML = '<div style="background:#fef2f2;color:#dc2626;padding:10px 14px;border-radius:6px;margin-bottom:14px">❌ ' + d.message + '</div>';
      }}
      setTimeout(() => msg.innerHTML = "", 3000);
    }}
    </script>
    </body></html>"""


@app.post("/admin/update-password")
async def update_password(request: Request):
    role = check_auth(request)
    if role != "admin":
        return JSONResponse({"ok": False, "message": "無權限"}, status_code=403)
    data = await request.json()
    pw_role = data.get("role","")
    group_id = data.get("group_id","")
    password = data.get("password","").strip()
    if not password or len(password) < 6:
        return JSONResponse({"ok": False, "message": "密碼至少6個字元"})
    hashed = hash_pw(password)
    if pw_role == "admin":
        set_setting("admin_pw", hashed)
        return JSONResponse({"ok": True, "message": "管理員密碼已更新"})
    elif pw_role == "adminB":
        set_setting("adminB_pw", hashed)
        return JSONResponse({"ok": True, "message": "行政B密碼已更新"})
    elif pw_role == "normal":
        set_setting("report_pw", hashed)
        return JSONResponse({"ok": True, "message": "行政A密碼已更新"})
    elif pw_role == "vba":
        set_setting("vba_secret", hashed)
        return JSONResponse({"ok": True, "message": "VBA密鑰已更新"})
    elif pw_role == "group" and group_id:
        conn = get_conn(); cur = conn.cursor()
        cur.execute("UPDATE groups SET password_hash=? WHERE group_id=?", (hashed, group_id))
        conn.commit(); conn.close()
        return JSONResponse({"ok": True, "message": "群組密碼已更新"})
    return JSONResponse({"ok": False, "message": "未知角色"})


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
