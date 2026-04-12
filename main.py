from fastapi import FastAPI, Request, BackgroundTasks
from fastapi.responses import HTMLResponse, JSONResponse, FileResponse
import uvicorn
import requests as req_lib
import os
import re
import sqlite3
import json
import io
from datetime import datetime
import uuid
import secrets
import pathlib
from html import escape as _html_escape
from typing import Optional, List, Dict, Any


def h(val) -> str:
    """HTML escape helper — 防止 XSS（含引號）

    修復 Bug 4：加 quote=True，escape 單引號和雙引號，防止 inline JS/HTML attribute 注入
    修復 Bug 13：用 is None 判斷，避免 0/False 被當空值
    """
    if val is None:
        return ""
    return _html_escape(str(val), quote=True)

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
    "分貝汽車", "分貝機車", "貸救補", "預付手機分期", "融易", "手機分期", "鼎多",
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
    # 亞太方案（含帶金額/工會版本）
    "亞太商品", "亞太機車", "亞太工會", "亞太汽車",
    "亞太工15", "亞太工", "亞太機",
    "亞太25", "亞太15",
    "預付手機原融", "預付手機分期",
    "手機分期", "貸救補",
    "合信機車", "合信手機", "合信二輪",
    # 分貝方案
    "分貝汽車", "分貝機車", "分貝汽", "分貝機",
    # 21方案
    "21汽車", "21機車", "21機25萬", "21機25", "21汽", "21機", "21商品",
    # 創鉅方案
    "創鉅手機", "創鉅機車", "創鉅手", "創鉅機",
    # 麻吉方案
    "麻吉機車", "麻吉手機", "麻吉機", "麻吉手",
    "喬美40", "喬美14",
    "興達機車", "興達機",
    "第一機車", "第一商品",
    "鄉民", "喬美", "麻吉", "亞太",
    "和裕", "第一", "合信", "興達", "中租", "裕融", "裕榮", "創鉅", "和潤",
    "銀行", "零卡", "銀角", "商品貸", "代書", "當舖", "融易",
    "慢點付", "分期趣", "鼎多", "預付手機", "21",
    "新鑫", "房地", "土地",
    "分貝", "鄉", "銀", "C", "商", "代", "研", "當",
]

STATUS_WORDS = [
    "婉拒", "核准", "核準", "待核准", "附條件", "等保書",
    "補件", "補資料", "退件", "不承作", "無額度", "無法承作", "照會",
    "保密", "NA", "待撥款", "可送", "缺資料", "無可知情", "聯絡人皆可知情",
    "補行照", "補照會", "補照片", "補時段", "補案件資料", "補聯徵", "補保人",
    "已補", "待補", "轉", "申覆",
    "派對保", "委對收", "對好", "對保完成", "不收不簽",
    "已撥款", "今日撥款", "今日已撥款", "已加撥", "核准已撥",
    "預計排撥", "排撥",
]

ACTION_KEYWORDS = [
    "結案", "刪掉", "不追了", "全部不送", "已撥款結案",
    "補件", "補資料", "缺資料", "婉拒", "核准", "核準", "待核准", "附條件",
    "照會", "退件", "等保書",
    "不承作", "無額度", "無法承作", "待撥款",
    "補行照", "補照會", "補照片", "補時段", "補案件資料",
    "補聯徵", "補保人", "保密", "無可知情", "聯絡人皆可知情", "已補",
    "補案件", "補", "待補", "重啟", "再辦",
    "派對保", "委對收", "對好", "對保完成", "不收不簽",
    "已撥款", "今日撥款", "今日已撥款", "已加撥", "核准已撥",
    "預計排撥", "排撥",
]

DELETE_KEYWORDS = [
    "結案", "刪掉", "不追了", "全部不送", "已撥款結案",
    "違約金結案", "已支付違約金", "違約金已支付",
    "已收到違約金",  # Bug 14: 正確字
    "以收到違約金",  # Bug 14: 錯字版本，向後相容
    "收到違約金", "違約金已收", "違約金",
]
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
    "亞太(工會)": "亞太工會",
    "亞太(工)": "亞太工會",
    "亞太工會15萬": "亞太工會",
    # 和裕相關
    "維力商品貸": "和裕商品",
    "維力機車專": "和裕機車",
    "維力機車貸": "和裕機車",
    "維力": "和裕",
    # 21 全形/異體字
    "廿一": "21",
    "二十一世紀": "21",
    "２１": "21",
    # 興達/貸救補/歐
    "興機": "興達機車",
    "貸10": "貸救補",
    "貸救": "貸救補",
    "歐": "商品貸",
    # 新新專/維力
    "新新專": "和裕",
    "新新": "和裕",
    # 一路發
    "一路發汽車貸款": "裕融",
    "一路發代償專案": "裕融",
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
# 單公司核准/婉拒格式：03/04-黃娫柔-房地核准20萬 / 03/04-黃娫柔-新鑫核准20萬
SINGLE_APPROVAL_RE = re.compile(
    r"^\s*(\d{1,4}/\d{1,2}(?:/\d{1,2})?)\s*[-－]\s*([\u4e00-\u9fff]{2,4})\s*[-－]\s*([^\s/\n@-]+?)(核准|婉拒)(\d+(?:\.\d+)?萬)?(?:\s*@AI)?\s*$",
    re.IGNORECASE,
)

# ⭐ 照會注意事項偵測（等同已送件）
NOTIFICATION_TRIGGER_RE = re.compile(r"照會注意事項")
EXTRA_COMPANY_RE = re.compile(r"[+＋]\s*([\u4e00-\u9fff0-9]{1,8}?)\s*一起")
# 轉送格式：8/5-戴君哲-轉21 或 8/11-林曉薇-轉麻吉 6/18
TRANSFER_RE = re.compile(
    r"^\s*(\d{1,4}/\d{1,2}(?:/\d{1,2})?)\s*[-－]\s*([\u4e00-\u9fff]{2,4})\s*[-－]\s*轉\s*([^\s@]+)(?:\s.*)?(?:\s*@AI)?\s*$",
    re.IGNORECASE,
)


def is_notification_briefing(text: str) -> bool:
    """是否為照會注意事項格式：第一行姓名（2-4中文字）+ 第二行「照會注意事項」"""
    if not text:
        return False
    lines = [l.strip() for l in text.strip().splitlines() if l.strip()]
    if len(lines) < 2:
        return False
    # 第一行：純中文姓名 2-4 字
    name = lines[0]
    if not re.fullmatch(r"[\u4e00-\u9fff]{2,4}", name):
        return False
    # 第二行：照會注意事項
    if "照會注意事項" not in lines[1]:
        return False
    return True


def extract_extra_company(text: str) -> str:
    """從照會訊息末尾抓「+XXX一起」的額外公司名（原始字串，呼叫處再正規化）"""
    if not text:
        return ""
    m = EXTRA_COMPANY_RE.search(text)
    return m.group(1).strip() if m else ""


def parse_notification_fields(text: str) -> dict:
    """從照會注意事項訊息提取客戶欄位資料"""
    fields = {}
    # 居住年數：居住5年
    m = re.search(r"居住\s*(\d+)\s*年", text)
    if m:
        fields["live_years"] = m.group(1)
    # 居住狀況：父母名下/自有/租屋/配偶
    for status in ["父母", "自有", "租屋", "配偶", "親屬", "宿舍"]:
        if status in text:
            fields["live_status"] = status + ("名下" if "名下" in text else "")
            break
    # 年資：工作年資6年 / 年資6年
    m = re.search(r"(?:工作)?年資\s*(\d+)\s*年", text)
    if m:
        fields["company_years"] = m.group(1)
    # 月薪：月薪10萬 / 月薪50000
    m = re.search(r"月薪\s*(\d+(?:\.\d+)?)\s*萬", text)
    if m:
        fields["company_salary"] = str(int(float(m.group(1)) * 10000))
    else:
        m = re.search(r"月薪\s*(\d{4,})", text)
        if m:
            fields["company_salary"] = m.group(1)
    # 金額/期數：14萬/30期 或 10萬 30期
    m = re.search(r"(\d+(?:\.\d+)?)\s*萬\s*/?\s*(\d+)\s*期", text)
    if m:
        fields["approved_amount"] = f"{m.group(1)}萬"
    # 學歷：大學畢 / 高中 / 專科
    for edu, val in [("大學", "專科/大學"), ("專科", "專科/大學"), ("高中", "高中/職"),
                     ("高職", "高中/職"), ("研究所", "研究所以上"), ("碩士", "研究所以上")]:
        if edu in text:
            fields["education"] = val
            break
    # 資金用途：資金用途：家用
    m = re.search(r"資金用途\s*[：:]\s*(\S+)", text)
    if m:
        fields["fund_use"] = m.group(1)
    return fields


def handle_notification_briefing(block_text: str, source_group_id: str, reply_token: str) -> Optional[str]:
    """處理照會注意事項 — 等同已送件，提取欄位資料更新客戶"""
    lines = block_text.strip().splitlines()
    if not lines:
        return None
    # 第一行是客戶姓名
    name = lines[0].strip()
    if not name:
        return None
    rows = find_active_by_name(name)
    same = [r for r in rows if r["source_group_id"] == source_group_id]
    target = same[0] if same else (rows[0] if rows else None)
    if not target:
        return f"⚠️ 照會注意事項：找不到客戶「{name}」"

    # 提取欄位
    fields = parse_notification_fields(block_text)

    # 更新客戶：標記已送件 + 寫入欄位
    update_fields = {
        "text": block_text,
        "from_group_id": source_group_id,
        "report_section": target.get("current_company") or target.get("company") or "送件",
    }
    if fields.get("approved_amount"):
        update_fields["approved_amount"] = fields["approved_amount"]

    update_customer(target["case_id"], **update_fields)

    # 把照會中解析出的欄位寫入 DB（直接 SQL 更新非核心欄位）
    db_updates = {}
    if fields.get("live_years"):
        db_updates["live_years"] = fields["live_years"]
    if fields.get("live_status"):
        db_updates["live_status"] = fields["live_status"]
    if fields.get("company_years"):
        db_updates["company_years"] = fields["company_years"]
    if fields.get("company_salary"):
        db_updates["company_salary"] = fields["company_salary"]
    if fields.get("education"):
        db_updates["education"] = fields["education"]
    if db_updates:
        with db_conn(commit=True) as conn:
            cur = conn.cursor()
            set_clause = ", ".join(f"{k}=?" for k in db_updates)
            vals = list(db_updates.values()) + [target["case_id"]]
            cur.execute(f"UPDATE customers SET {set_clause} WHERE case_id=?", vals)

    # 回覆
    parsed_info = []
    if fields.get("live_years"):
        parsed_info.append(f"居住{fields['live_years']}年")
    if fields.get("company_years"):
        parsed_info.append(f"年資{fields['company_years']}年")
    if fields.get("company_salary"):
        parsed_info.append(f"月薪{fields['company_salary']}")
    if fields.get("approved_amount"):
        parsed_info.append(f"金額{fields['approved_amount']}")
    if fields.get("education"):
        parsed_info.append(f"學歷{fields['education']}")

    info_str = "、".join(parsed_info) if parsed_info else ""
    msg = f"📋 已收到照會：{name}"
    if info_str:
        msg += f"\n📝 已記錄：{info_str}"
    return msg


# ⭐ 對保 4 子步驟偵測
def detect_pairing_substep(text: str) -> str:
    """偵測對保子步驟，回傳子狀態名稱（空字串=不是對保訊息）

    回傳值：
    - "派對保" - 8a 行政發派對保（含「派對保」「辦理方案」+「對保地區」）
    - "委對收" - 8b 對保員接單回覆「委對收」
    - "約時間" - 8c 對保員回時間+地點
    - "對好"   - 8d 對保完成回報（「對好」「對保完成」「不收不簽」）
    """
    if not text:
        return ""
    if "對好" in text or "對保完成" in text or "不收不簽" in text or "不簽不收" in text:
        return "對好"
    if "委對收" in text:
        return "委對收"
    if "派對保" in text or ("辦理方案" in text and "對保地區" in text):
        return "派對保"
    if ("對保時間" in text or "對保地點" in text or
        ("時間" in text and "地點" in text and len(text) < 100)):
        return "約時間"
    return ""


# =========================
# 基本工具
# =========================
def now_iso() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def short_id() -> str:
    return str(uuid.uuid4())[:8]


def get_conn():
    """取得 DB 連線（向後相容，需手動 close）"""
    pathlib.Path(DB_PATH).parent.mkdir(parents=True, exist_ok=True)
    # Bug 16: timeout 從 10 加到 30 秒，給 BEGIN IMMEDIATE 排隊更多時間
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


from contextlib import contextmanager

@contextmanager
def db_conn(commit: bool = False):
    """DB 連線 context manager（Bug 5/6/16 修復）

    保證：
    - 例外時連線一定會 close（Bug 5 連線洩漏）
    - commit=True 時用 BEGIN IMMEDIATE 立刻拿寫鎖，防 race（Bug 6/16）
    - 例外時自動 rollback
    """
    conn = get_conn()
    try:
        if commit:
            # Bug 16: 切換為手動交易模式，立刻拿寫鎖（避免 read-modify-write race）
            conn.isolation_level = None
            conn.execute("BEGIN IMMEDIATE")
        yield conn
        if commit:
            conn.execute("COMMIT")
    except Exception:
        try:
            if commit:
                conn.execute("ROLLBACK")
        except Exception:
            pass
        raise
    finally:
        try:
            conn.close()
        except Exception:
            pass


# =========================
# Bug 16: 姓名並發鎖
# =========================
import threading as _threading

_name_locks: Dict[str, "_threading.Lock"] = {}
_name_locks_guard = _threading.Lock()


def get_name_lock(name: str):
    """取得姓名鎖（同名訊息排隊處理）

    用法：
        with get_name_lock("王小明"):
            # 同名訊息會排隊
            customer = find_active_by_name(...)
            update_customer(...)

    沒姓名時返回 dummy 鎖（不阻塞任何東西）
    """
    name = (name or "").strip()
    if not name:
        return _DummyLock()
    with _name_locks_guard:
        lock = _name_locks.get(name)
        if lock is None:
            lock = _threading.Lock()
            _name_locks[name] = lock
        return lock


class _DummyLock:
    """空鎖，給沒姓名的訊息用"""
    def __enter__(self):
        return self
    def __exit__(self, *args):
        pass
    def acquire(self, *args, **kwargs):
        return True
    def release(self):
        pass


def normalize_id_no(id_no) -> str:
    """正規化身分證字號（Bug 12 修復）

    處理：
    - None → ""
    - 大小寫不一致 → 全大寫
    - 前後空白 → 去除
    """
    return (id_no or "").strip().upper()


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


def get_all_group_names() -> Dict[str, str]:
    """一次載入所有群組名稱，避免 N+1 查詢"""
    conn = get_conn(); cur = conn.cursor()
    cur.execute("SELECT group_id, group_name FROM groups")
    result = {r["group_id"]: r["group_name"] for r in cur.fetchall()}
    conn.close()
    return result


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
    # Bug 1: 攔截「轉XXX」格式
    if re.match(r"^\s*\d{1,4}/\d{1,2}(?:/\d{1,2})?\s*[-－]\s*[\u4e00-\u9fff]{2,4}\s*[-－]\s*轉", line.strip()):
        return {}
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


def advance_route(route_plan: str, status: str, amount: str = "", disbursed: str = "") -> str:
    # Bug 7: current_index 邊界檢查，防越界後靜默失敗
    data = parse_route_json(route_plan)
    order = data.get("order", []) or []
    idx = data.get("current_index", 0)
    if not isinstance(idx, int) or idx < 0:
        idx = 0
    history = data.get("history", []) or []
    current = order[idx] if 0 <= idx < len(order) else ""
    if current:
        entry = {"company": current, "status": status, "date": now_iso()[:10]}
        if amount:
            entry["amount"] = amount
        if disbursed:
            entry["disbursed"] = disbursed
        history.append(entry)
    # 推進但不超過 order 長度
    data["current_index"] = min(idx + 1, len(order))
    data["history"] = history
    return json.dumps(data, ensure_ascii=False)


def get_all_approved(route_plan: str) -> list:
    """從 history 取得所有核准記錄"""
    data = parse_route_json(route_plan)
    history = data.get("history", [])
    return [h for h in history if h.get("status") in ("核准", "待撥款", "撥款") and h.get("amount")]

def get_all_disbursed(route_plan: str) -> list:
    """從 history 取得所有撥款記錄"""
    data = parse_route_json(route_plan)
    history = data.get("history", [])
    return [h for h in history if h.get("disbursed") or h.get("disbursement_date")]

def get_total_approved_amount(route_plan: str) -> str:
    """計算所有核准金額總計"""
    approved = get_all_approved(route_plan)
    if not approved:
        return ""
    if len(approved) == 1:
        return approved[0].get("amount", "")
    # 多家核准，顯示各家
    parts = []
    for h in approved:
        co = h.get("company", "")
        amt = h.get("amount", "")
        if amt:
            parts.append(f"{co}{amt}")
    return " + ".join(parts)

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


def set_disbursed_in_history(route_plan: str, company: str, disb_date: str) -> str:
    """在 history 裡設定指定公司的撥款日期"""
    data = parse_route_json(route_plan)
    history = data.get("history", [])
    found = False
    for h in reversed(history):
        hc = h.get("company", "")
        if hc == company or company in hc or hc in company:
            h["disbursed"] = disb_date
            found = True
            break
    if not found:
        history.append({"company": company, "status": "撥款", "date": disb_date, "disbursed": disb_date})
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
    return bool(f.get("date") and f.get("name") and f.get("id_no"))


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
    # LINE Quick Reply 上限 13 個按鈕，超過警告 + 截斷
    if len(items) > 13:
        print(f"[reply_quick_reply] 警告：按鈕數 {len(items)} 超過 13，已截斷")
        items = items[:13]
    try:
        req_lib.post(
            "https://api.line.me/v2/bot/message/reply",
            headers={"Authorization": f"Bearer {CHANNEL_ACCESS_TOKEN}", "Content-Type": "application/json"},
            json={"replyToken": reply_token, "messages": [{"type": "text", "text": text[:4900], "quickReply": {"items": items}}]},
            timeout=10,
        )
    except Exception:
        pass


# =========================
# 資料庫
# =========================
def ensure_column(cur, table: str, column: str, definition: str):
    """加欄位（DB migration 用）

    Bug 7: 加 whitelist 防 SQL injection。
    table/column 必須是合法識別字（字母+底線+數字，不能以數字開頭）
    definition 必須是 SQLite 合法型別字串
    """
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", table):
        raise ValueError(f"非法的 table 名稱：{table!r}")
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", column):
        raise ValueError(f"非法的 column 名稱：{column!r}")
    # definition 嚴格白名單：擋 SQL injection。要新增型別請加到這個 set 裡
    _ALLOWED_DEFS = {
        # TEXT
        "TEXT", "TEXT NOT NULL", "TEXT DEFAULT ''", "TEXT DEFAULT '' NOT NULL",
        # INTEGER
        "INTEGER", "INTEGER NOT NULL", "INTEGER DEFAULT 0", "INTEGER DEFAULT 0 NOT NULL",
        "INTEGER DEFAULT 1", "INTEGER DEFAULT 1 NOT NULL",
        # REAL
        "REAL", "REAL NOT NULL", "REAL DEFAULT 0", "REAL DEFAULT 0 NOT NULL",
        # NUMERIC / BLOB / TIMESTAMP
        "NUMERIC", "BLOB",
        "TIMESTAMP", "DATETIME", "DATE",
    }
    if definition not in _ALLOWED_DEFS:
        raise ValueError(
            f"非法的 definition：{definition!r}\n"
            f"請使用以下其中一種，或在 ensure_column 的 _ALLOWED_DEFS 新增：\n"
            f"{sorted(_ALLOWED_DEFS)}"
        )
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
        ("eval_late", "TEXT"),
        ("eval_late_days", "TEXT"),
        ("debt_list", "TEXT"),
        ("eval_note", "TEXT"),
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
        ("adminb_selected_plans", "TEXT"),
        ("adminb_contact_time", "TEXT"),
        ("adminb_bank", "TEXT"),
        ("adminb_branch", "TEXT"),
        ("adminb_account", "TEXT"),
        ("adminb_industry", "TEXT"),
        ("adminb_brand", "TEXT"),
        ("adminb_hr_role", "TEXT"),
        ("adminb_hr_industry", "TEXT"),
        ("signature_applicant", "TEXT"),
        ("signature_legal_rep", "TEXT"),
        ("adminb_role", "TEXT"),
        ("adminb_vehicle_type", "TEXT"),
        ("adminb_engine_no", "TEXT"),
        ("adminb_displacement", "TEXT"),
        ("adminb_color", "TEXT"),
        ("adminb_body_no", "TEXT"),
        ("vehicle_duration", "TEXT"),
        ("vehicle_road_reg", "TEXT"),
        ("vehicle_space", "TEXT"),
        ("adminb_fund_use", "TEXT"),
        ("adminb_product", "TEXT"),
        ("adminb_model", "TEXT"),
        ("adminb_product_name", "TEXT"),
        ("adminb_product_model", "TEXT"),
        ("adminb_21car_project", "TEXT"),
        ("adminb_21car_price", "TEXT"),
        ("adminb_21car_ref_src", "TEXT"),
        ("adminb_21car_ref_price", "TEXT"),
        ("adminb_21car_rate", "TEXT"),
        ("adminb_21car_amount", "TEXT"),
        ("adminb_21car_period", "TEXT"),
        ("adminb_21car_monthly", "TEXT"),
        ("adminb_21car_fund", "TEXT"),
        ("adminb_21car_hascc", "TEXT"),
        ("adminb_mj_brand", "TEXT"),
        ("adminb_mj_model", "TEXT"),
        ("adminb_credit_bank", "TEXT"),
        ("adminb_credit_no", "TEXT"),
        ("adminb_credit_exp", "TEXT"),
        ("adminb_credit_limit", "TEXT"),
        ("adminb_credit_late", "TEXT"),
        ("adminb_credit_pay", "TEXT"),
        ("eval_license", "TEXT"),
        ("eval_law", "TEXT"),
        ("carrier", "TEXT"),
        ("fb", "TEXT"),
        ("company_months", "TEXT"),
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
    # sessions 表（安全 session token）
    cur.execute("""CREATE TABLE IF NOT EXISTS sessions (
        token TEXT PRIMARY KEY NOT NULL,
        role TEXT NOT NULL,
        group_id TEXT DEFAULT '',
        created_at TEXT NOT NULL,
        expires_at TEXT NOT NULL
    )""")
    # 索引
    cur.execute("CREATE INDEX IF NOT EXISTS idx_cust_id_no ON customers(id_no)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_cust_name ON customers(customer_name)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_cust_group ON customers(source_group_id)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_cust_status ON customers(status)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_logs_case ON case_logs(case_id)")
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
    """初始化密碼（Bug 3 修復）

    優先順序：
    1. 已存在密碼 → 不動
    2. 環境變數有設 → 用環境變數的密碼
    3. 都沒有 → 產生隨機密碼並印出 log（不再用硬編碼預設值）
    """
    import secrets as _secrets
    env_var_map = {
        "admin_pw": "ADMIN_PASSWORD",
        "adminB_pw": "ADMINB_PASSWORD",
        "report_pw": "REPORT_PASSWORD",
        "vba_secret": "VBA_SECRET",
    }
    conn = get_conn(); cur = conn.cursor()
    for key, env_var in env_var_map.items():
        cur.execute("SELECT value FROM settings WHERE key=?", (key,))
        if cur.fetchone():
            continue  # 已有密碼，跳過
        env_pw = os.getenv(env_var, "")
        if env_pw:
            pw_to_set = env_pw
            try:
                print(f"[init_settings] {key}: loaded from env {env_var}")
            except Exception:
                pass
        else:
            pw_to_set = _secrets.token_urlsafe(16)
            try:
                print(f"[init_settings] WARNING {key} env not set, generated random password: {pw_to_set}")
                print(f"[init_settings] WARNING please save it and set env var {env_var}")
            except Exception:
                pass
        cur.execute("INSERT INTO settings (key,value,updated_at) VALUES (?,?,?)",
            (key, hash_pw(pw_to_set), now_iso()))
    conn.commit(); conn.close()


# =========================
# DB CRUD
# =========================
def create_customer_record(name, id_no, company, source_group_id, text,
                            route_plan="", current_company="", report_section="") -> str:
    """建立客戶（Bug 5/6/12 修復：context manager + transaction + id_no normalize）"""
    id_no = normalize_id_no(id_no)
    now = now_iso()
    with db_conn(commit=True) as conn:
        cur = conn.cursor()
        # 先找有沒有行政A已填的 PENDING 客戶（同身分證號）
        if id_no:
            cur.execute("SELECT * FROM customers WHERE id_no=? AND status='PENDING' ORDER BY updated_at DESC LIMIT 1", (id_no,))
            pending = cur.fetchone()
        else:
            pending = None
        if pending:
            case_id = pending["case_id"]
            cur.execute("""UPDATE customers SET
                status='ACTIVE', customer_name=?, company=?,
                source_group_id=?, route_plan=?, current_company=?,
                report_section=?, last_update=?, updated_at=?
                WHERE case_id=?""",
                (name, company, source_group_id, route_plan, current_company,
                 report_section, text, now, case_id))
            return case_id
        else:
            # 業務員透過 LINE 建立的新案件直接 ACTIVE（PENDING 是 web /new-customer 表單專用狀態）
            # 已測試：改 ACTIVE 後日報正確顯示，結案流程不受影響
            case_id = short_id()
            cur.execute("""INSERT INTO customers
                (case_id,customer_name,id_no,source_group_id,company,route_plan,current_company,report_section,last_update,status,created_at,updated_at)
                VALUES (?,?,?,?,?,?,?,?,?,'ACTIVE',?,?)""",
                (case_id, name, id_no, source_group_id, company, route_plan, current_company, report_section, text, now, now))
            return case_id


def update_customer(case_id, company=None, text=None, from_group_id="", status=None,
                    name=None, source_group_id=None, route_plan=None,
                    current_company=None, report_section=None,
                    approved_amount=None, disbursement_date=None):
    """更新客戶（Bug 5/6 修復：context manager + transaction）

    UPDATE + INSERT case_logs 包在同一交易內，確保原子性。
    """
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
    with db_conn(commit=True) as conn:
        cur = conn.cursor()
        cur.execute(f"UPDATE customers SET {', '.join(fields)} WHERE case_id=?", values)
        cur.execute("SELECT * FROM customers WHERE case_id=?", (case_id,))
        row = cur.fetchone()
        if row and text is not None:
            cur.execute("INSERT INTO case_logs (case_id,customer_name,id_no,company,message_text,from_group_id,created_at) VALUES (?,?,?,?,?,?,?)",
                (row["case_id"], row["customer_name"], row["id_no"], row["company"], text, from_group_id, now))


def find_active_by_id_no(id_no):
    id_no = normalize_id_no(id_no)
    if not id_no: return None
    with db_conn() as conn:
        cur = conn.cursor()
        cur.execute("SELECT * FROM customers WHERE id_no=? AND status='ACTIVE' ORDER BY updated_at DESC LIMIT 1", (id_no,))
        return cur.fetchone()


def find_active_by_name(name):
    with db_conn() as conn:
        cur = conn.cursor()
        cur.execute("SELECT * FROM customers WHERE customer_name=? AND status='ACTIVE' ORDER BY updated_at DESC", (name,))
        return cur.fetchall()


def find_any_by_name(name):
    with db_conn() as conn:
        cur = conn.cursor()
        cur.execute("SELECT * FROM customers WHERE customer_name=? ORDER BY updated_at DESC", (name,))
        return cur.fetchall()


def find_any_by_id_no(id_no):
    id_no = normalize_id_no(id_no)
    if not id_no: return []
    with db_conn() as conn:
        cur = conn.cursor()
        cur.execute("SELECT * FROM customers WHERE id_no=? ORDER BY updated_at DESC", (id_no,))
        return cur.fetchall()


def save_pending_action(action_id, action_type, payload):
    """Bug 3: 用 db_conn(commit=True) 進 BEGIN IMMEDIATE 防並發競態 + 連線洩漏"""
    with db_conn(commit=True) as conn:
        cur = conn.cursor()
        cur.execute(
            "INSERT OR REPLACE INTO pending_actions (action_id,action_type,payload,created_at) VALUES (?,?,?,?)",
            (action_id, action_type, json.dumps(payload, ensure_ascii=False), now_iso()))


def get_pending_action_and_delete(action_id):
    """Bug 3: 原子化讀取+刪除，防快速重複點擊造成重複處理"""
    with db_conn(commit=True) as conn:
        cur = conn.cursor()
        cur.execute("SELECT * FROM pending_actions WHERE action_id=?", (action_id,))
        row = cur.fetchone()
        if not row:
            return None
        cur.execute("DELETE FROM pending_actions WHERE action_id=?", (action_id,))
        data = dict(row)
        try:
            data["payload"] = json.loads(data["payload"])
        except Exception:
            data["payload"] = {}
        return data


def get_pending_action(action_id):
    """保留供唯讀查詢用（不刪除）。注意：消費按鈕請改用 get_pending_action_and_delete()"""
    with db_conn() as conn:
        cur = conn.cursor()
        cur.execute("SELECT * FROM pending_actions WHERE action_id=?", (action_id,))
        row = cur.fetchone()
        if not row:
            return None
        data = dict(row)
        try:
            data["payload"] = json.loads(data["payload"])
        except Exception:
            data["payload"] = {}
        return data


def delete_pending_action(action_id):
    with db_conn(commit=True) as conn:
        cur = conn.cursor()
        cur.execute("DELETE FROM pending_actions WHERE action_id=?", (action_id,))


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
            except Exception:
                pass

    # 2. 找大數字（超過10000視為元，換算成萬）
    for m in re.finditer(r"(\d{1,3}(?:,\d{3})+|\d{5,})", text):
        num_str = m.group(1).replace(",", "")
        try:
            num = int(num_str)
            if num >= 10000:
                wan = num // 10000
                return f"{wan}萬"
        except Exception:
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

    # 撥款名單標頭正則（有日期）
    DISB_HEADER_RE = re.compile(
        r"(\d{1,2}/\d{1,2})\s*([\w一-鿿]+?)\s*(撥款名單|預計排撥|排撥|今日撥款|商品撥款|機車撥款|汽車撥款|撥款)",
        re.IGNORECASE
    )
    # 無日期標頭：「貸救補 今日撥款」
    DISB_HEADER_NODATE_RE = re.compile(
        r"^([\w一-鿿]+?)\s*(撥款名單|預計排撥|排撥|今日撥款|商品撥款|機車撥款|汽車撥款|撥款)\s*$",
        re.IGNORECASE
    )

    today_date = datetime.now().strftime("%#m/%#d") if os.name == "nt" else datetime.now().strftime("%-m/%-d")

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

        # 無日期標頭（如「貸救補 今日撥款」），用今日日期
        m2 = DISB_HEADER_NODATE_RE.match(line)
        if m2:
            current_date = current_date or today_date
            current_company = m2.group(1).strip()
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
    """判斷是否為撥款名單（多行格式，標頭+人名行）"""
    lines = [l.strip() for l in (text or "").splitlines() if l.strip()]
    if len(lines) < 2:
        return False
    # 至少一行要是標頭（含撥款關鍵詞）
    header_re = re.compile(
        r"(撥款名單|排撥|預計排撥|今日撥款|商品撥款|機車撥款|汽車撥款)"
    )
    has_header = False
    header_idx = -1
    for i, line in enumerate(lines):
        if header_re.search(line):
            has_header = True
            header_idx = i
            break
    if not has_header:
        return False
    # 標頭後面至少要有一行純人名（2-4個中文字）
    name_re = re.compile(r"^[一-鿿]{2,4}$")
    for line in lines[header_idx + 1:]:
        if name_re.match(line):
            return True
    return False


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
    # 帶金額版本 → 主公司
    "亞太工15": "亞太", "亞太25": "亞太", "亞太15": "亞太",
    "亞太汽車": "亞太",
    "21商品": "21",
    "合信機車": "合信", "合信手機": "合信", "合信二輪": "合信",
    "第一商品": "第一", "第一機車": "第一",
    "喬美40": "喬美", "喬美14": "喬美",
    "興達機車": "興達", "興達機": "興達",
    "維力": "和裕",
    "預付手機": "預付手機分期",
    # 新鑫/土地 → 房地
    "新鑫": "房地", "土地": "房地",
    # 民間方案簡稱
    "鄉": "鄉民", "研": "零卡", "當": "當舖專案", "商": "商品貸",
    "銀角": "零卡", "慢點付": "零卡", "分期趣": "零卡",
}

def normalize_section(section: str) -> str:
    return COMPANY_SECTION_MAP.get(section, section)


def build_section_map(all_rows) -> Dict[str, List[str]]:
    """把客戶列表轉成 section_map"""
    section_map: Dict[str, List[str]] = {}
    today_str = datetime.now().strftime("%Y-%m-%d")
    for row in all_rows:
        section = row["report_section"] or row["current_company"] or row["company"] or "送件"
        section = normalize_section(section)  # Bug 20: 移除多餘的第二次呼叫
        created = row["created_at"] or ""
        date_str = created[5:10].replace("-", "/") if created else ""
        company_str = row["current_company"] or row["company"] or ""

        last_update = row["last_update"] or ""
        first_line = last_update.splitlines()[0].strip() if last_update.strip() else ""
        status_short = extract_status_summary(first_line, row["customer_name"])

        if section == "待撥款":
            created = row["created_at"] or ""
            created_date = created[5:10].replace("-", "/") if created else date_str
            amount = row["approved_amount"] or ""
            # 從 route_plan 歷史找核准公司（每家各自顯示撥款狀態）
            approved_list = get_all_approved(row["route_plan"] or "")
            if approved_list:
                parts = []
                for ap in approved_list:
                    co = ap.get('company') or ''
                    amt = ap.get('amount') or ''
                    disb = ap.get('disbursed') or ''
                    if disb:
                        parts.append(f"{co}{amt}(撥款{disb})")
                    else:
                        parts.append(f"{co}{amt}(待撥款)")
                amount_str = "-核准" + "/".join(parts)
            elif amount:
                disb_date = row["disbursement_date"] or ""
                disb_str = f"(撥款{disb_date})" if disb_date else "(待撥款)"
                amount_str = f"-核准{amount}{disb_str}"
            else:
                amount_str = ""
            line = f"{created_date}-{row['customer_name']}-{company_str}{amount_str}"
        else:
            line = f"{date_str}-{row['customer_name']}-{company_str}"
            if status_short:
                line += f"-{status_short}"
        # 今日新進件標記
        if created[:10] == today_str:
            line = "🆕" + line
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

    # 待撥款超過7天提醒（獨立一段）
    overdue_lines = []
    for row in all_rows:
        if (row["report_section"] or "") == "待撥款" and not (row["disbursement_date"] or ""):
            created = row["created_at"] or ""
            if created:
                try:
                    days = (datetime.now() - datetime.fromisoformat(created.replace("Z",""))).days
                    if days >= 7:
                        overdue_lines.append(f"  {row['customer_name']}（{days}天）")
                except Exception:
                    pass
    if overdue_lines:
        segments.append(f"⏰ 待撥款超過7天（{len(overdue_lines)}筆）\n" + "\n".join(overdue_lines))

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
    # Bug 6: 驗證 source_group_id 合法性，避免按鈕回調寫入不存在的群組
    new_g = get_group_name(source_group_id) if source_group_id else ""
    if not source_group_id or not new_g or new_g == "未知群組":
        reply_text(reply_token, "⚠️ 無法轉送：來源群組不存在或尚未註冊，請聯絡管理員")
        return
    action_id = short_id()
    save_pending_action(action_id, "transfer_customer", {
        "case_id": customer["case_id"], "target_group_id": source_group_id,
        "block_text": block_text, "name": extract_name(block_text) or customer["customer_name"],
    })
    old_g = get_group_name(customer["source_group_id"])
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

    # 批次結案：@AI 批次結案\n姓名1\n姓名2\n...
    if clean.startswith("批次結案"):
        rest = clean[len("批次結案"):].strip()
        names = [n.strip() for n in rest.splitlines() if n.strip()]
        if names:
            return {"type": "batch_close", "names": names}

    # 批次婉拒：@AI 批次婉拒\n姓名1\n姓名2\n...
    if clean.startswith("批次婉拒"):
        rest = clean[len("批次婉拒"):].strip()
        names = [n.strip() for n in rest.splitlines() if n.strip()]
        if names:
            return {"type": "batch_reject", "names": names}

    # 待撥款名單：@AI 待撥款
    if re.match(r"^待撥款$", clean):
        return {"type": "pending_disbursement"}

    # 統計：@AI 統計
    if re.match(r"^統計$", clean):
        return {"type": "stats"}

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

    # 修改核准金額：@AI 姓名 公司 核准 金額（姓名和公司之間必須有空格）
    m = re.match(r"^([\u4e00-\u9fff]{2,4})\s+(.+?)\s*核准\s*(.+)$", clean)
    if m:
        return {"type": "update_amount", "name": m.group(1), "company": m.group(2).strip(), "amount": m.group(3).strip()}

    # 改名：@AI 舊名 改名 新名
    m = re.match(r"^([\u4e00-\u9fff]{2,4})\s*改名\s*([\u4e00-\u9fff]{2,4})$", clean)
    if m:
        return {"type": "rename", "old_name": m.group(1), "new_name": m.group(2)}

    # 改身分證：@AI 姓名 改身分證 新ID
    m = re.match(r"^([\u4e00-\u9fff]{2,4})\s*改身分證\s*([A-Z]\d{9})$", clean, re.IGNORECASE)
    if m:
        return {"type": "change_id", "name": m.group(1), "new_id": m.group(2).upper()}

    # 重啟：@AI 姓名 重啟
    m = re.match(r"^([\u4e00-\u9fff]{2,4})\s*重啟$", clean)
    if m:
        return {"type": "reopen", "name": m.group(1)}

    # 結案帶原因：@AI 姓名 結案 原因
    m = re.match(r"^([一-鿿]{2,4})\s*結案\s+(.+)$", clean)
    if m:
        return {"type": "close", "name": m.group(1), "reason": m.group(2).strip()}

    # 結案
    m = re.match(r"^([一-鿿]{2,4})\s*(已結案|結案)$", clean)
    if m:
        return {"type": "close", "name": m.group(1)}

    # 婉拒 轉XXX（跳到指定公司）
    m = re.match(r"^([一-鿿]{2,4})\s*婉拒\s*轉\s*(.+)$", clean)
    if m:
        return {"type": "reject_to", "name": m.group(1), "target": m.group(2).strip()}

    # 婉拒（推到下一家）
    m = re.match(r"^([一-鿿]{2,4})\s*婉拒$", clean)
    if m:
        return {"type": "reject", "name": m.group(1)}

    # 違約金已支付 xxxx（有收到違約金）
    m = re.match(r"^([一-鿿]{2,4})\s*違約金已支付\s*([\d,，]+)", clean)
    if m:
        amt = m.group(2).replace(",","").replace("，","")
        return {"type": "penalty", "name": m.group(1), "penalty": amt}

    # 照會：前後都可以
    # @AI 照會 王小明 / @AI 王小明 照會 / @AI 照會 王小明 21
    m = re.match(r"^照會\s+([\u4e00-\u9fff]{2,4})(?:\s+(.+))?$", clean)
    if m:
        return {"type": "notification", "name": m.group(1), "company": (m.group(2) or "").strip()}
    m = re.match(r"^([\u4e00-\u9fff]{2,4})\s+照會(?:\s+(.+))?$", clean)
    if m:
        return {"type": "notification", "name": m.group(1), "company": (m.group(2) or "").strip()}

    return None


def generate_notification_text(r: dict, company: str = "") -> str:
    """根據客戶 DB 資料自動產生照會注意事項文字"""
    def v(k): return (r.get(k, "") or "").strip()

    name = v("customer_name")
    # 公司：優先用參數，其次 current_company，最後 company
    co = company or v("current_company") or v("company") or ""

    # 居住地：同戶籍→戶籍，否則→現居地
    live_type = "戶籍" if v("live_same_as_reg") == "1" else "現居地"
    live_years = v("live_years") or "0"
    live_status = v("live_status") or "自有"

    # 年資
    co_years = v("company_years") or "0"

    # 月薪：轉成「N萬」或「N萬N」格式
    salary_str = ""
    try:
        sal = float(v("company_salary") or "0")
        if sal >= 10000:
            wan = int(sal // 10000)
            remainder = int((sal % 10000) // 1000)
            salary_str = f"{wan}萬{remainder}" if remainder else f"{wan}萬"
        elif sal > 0:
            salary_str = f"{sal}"
        else:
            salary_str = "0"
    except Exception:
        salary_str = v("company_salary") or "0"

    # 金額
    amount = v("approved_amount") or ""
    # 期數：公司預設
    DEFAULT_PERIODS = {
        "亞太": "30", "亞太商品": "30", "亞太機車15萬": "36", "亞太機車25萬": "48",
        "亞太工會機車": "36", "和裕": "24", "和裕機車": "24", "和裕商品": "24",
        "21": "18", "21機車12萬": "24", "21機車25萬": "48", "21商品": "24",
        "創鉅": "24", "麻吉": "24", "喬美": "30", "第一": "24",
        "貸就補": "24", "貸救補": "24", "手機分期": "18",
    }
    period = DEFAULT_PERIODS.get(co, "24")
    amount_line = f"{amount}/{period}期" if amount else ""

    # 學歷：轉成口語
    edu = v("education")
    EDU_MAP = {"專科/大學": "大學", "專科、大學": "大學", "高中/職": "高中",
               "高中職": "高中", "研究所以上": "研究所", "其他": "高中"}
    edu_spoken = EDU_MAP.get(edu, edu) if edu else "高中"

    # 資金用途
    fund = v("adminb_fund_use") or "家用"

    # 名下車貸狀況：檢查 debt_list
    car_loan_status = "名下無貸款"
    try:
        debt_list = json.loads(v("debt_list")) if v("debt_list") else []
        for d in debt_list:
            co_name = (d.get("co", "") or "").lower()
            dy = d.get("dy", "") or ""
            if any(w in co_name for w in ["車", "機車", "汽車"]):
                if "公路" in dy or "動保" in dy:
                    car_loan_status = "名下車貸正常繳"
                break
    except Exception:
        pass

    # 組合照會文字
    lines = [
        name,
        "照會注意事項",
        f"✅現居住{live_type}地址 居住{live_years}年 {live_status} 工作年資{co_years}年 月薪{salary_str}",
    ]
    if amount_line:
        lines.append(f"✅{amount_line}")
    lines += [
        "✅帳單簡訊條碼繳款",
        "✅ 姓名親簽",
        f"✅學歷詢問到麻煩說{edu_spoken}畢",
        f"✅資金用途 ：{fund}",
        "✅ 詢問任何法學，刑事，一律都說不是自己的",
        "✅假如問到在外面有沒有欠款公司要說沒有！",
        f"✅{car_loan_status}",
        "",
        "⚠️不好的一概否認喔，比如卡循卡債就算清償了，沒拉聯徵，會問的比較多！任何法學 如果詢問麻煩也都否認💯",
        "",
        "💌確認進件照會時間",
        "     🎀白天通知進件 中午或是下午以前注意來電",
    ]
    return "\n".join(lines)


def handle_special_command(cmd: Dict, reply_token: str, group_id: str):
    t = cmd["type"]

    if t == "group_id":
        gname = get_group_name(group_id)
        reply_text(reply_token, f"📋 此群組資訊\n名稱：{gname}\nID：{group_id}")
        return

    if t == "report":
        try:
            segs = generate_report_lines(group_id)
            reply_text(reply_token, segs[0])
            for seg in segs[1:]:
                push_text(group_id, seg)
        except Exception as e:
            import traceback
            traceback.print_exc()
            reply_text(reply_token, f"❌ 日報產生失敗：{type(e).__name__}: {e}")
        return

    if t == "search":
        reply_text(reply_token, search_customer_info(cmd["name"], group_id))
        return

    if t == "batch_close":
        names = cmd["names"]
        results = []
        for name in names:
            rows = find_active_by_name(name)
            same = [r for r in rows if r["source_group_id"] == group_id]
            target = same[0] if same else (rows[0] if rows else None)
            if not target:
                results.append(f"  {name} ❌ 找不到客戶")
            else:
                update_customer(target["case_id"], status="CLOSED",
                                text=f"{name} 結案", from_group_id=group_id)
                push_text(target["source_group_id"], f"{name} 結案")
                results.append(f"  {name} ✅ 已結案")
        ok = sum(1 for r in results if "✅" in r)
        fail = len(results) - ok
        header = f"📋 批次結案 {len(names)} 筆（成功 {ok}"
        if fail:
            header += f"，失敗 {fail}"
        header += "）"
        reply_text(reply_token, header + "\n" + "\n".join(results))
        return

    if t == "batch_reject":
        names = cmd["names"]
        results = []
        for name in names:
            rows = find_active_by_name(name)
            same = [r for r in rows if r["source_group_id"] == group_id]
            target = same[0] if same else (rows[0] if rows else None)
            if not target:
                results.append(f"  {name} ❌ 找不到客戶")
            else:
                route = target["route_plan"] or ""
                current = get_current_company(route)
                next_co = get_next_company(route)
                new_route = advance_route(route, "婉拒")
                update_customer(target["case_id"], route_plan=new_route,
                                current_company=next_co or current,
                                text=f"{name} {current} 婉拒", from_group_id=group_id)
                push_text(target["source_group_id"], f"{name} {current} 婉拒")
                if next_co:
                    results.append(f"  {name} ✅ {current}婉拒 → {next_co}")
                else:
                    results.append(f"  {name} ✅ {current}婉拒（無下一家）")
        ok = sum(1 for r in results if "✅" in r)
        fail = len(results) - ok
        header = f"📋 批次婉拒 {len(names)} 筆（成功 {ok}"
        if fail:
            header += f"，失敗 {fail}"
        header += "）"
        reply_text(reply_token, header + "\n" + "\n".join(results))
        return

    if t == "pending_disbursement":
        conn = get_conn(); cur = conn.cursor()
        cur.execute("SELECT customer_name, current_company, company, approved_amount, route_plan, created_at FROM customers WHERE status='ACTIVE' AND report_section='待撥款' ORDER BY created_at")
        rows = cur.fetchall(); conn.close()
        if not rows:
            reply_text(reply_token, "📋 目前沒有待撥款客戶")
            return
        lines = [f"📋 待撥款名單（{len(rows)} 筆）\n"]
        for r in rows:
            name = r["customer_name"]
            co = r["current_company"] or r["company"] or ""
            amt = r["approved_amount"] or ""
            created = (r["created_at"] or "")[:10]
            approved_list = get_all_approved(r["route_plan"] or "")
            if approved_list:
                parts = [(h.get("company") or "") + (h.get("amount") or "") for h in approved_list]
                amt_str = "/".join(parts)
            elif amt:
                amt_str = amt
            else:
                amt_str = "未知金額"
            lines.append(f"{created}-{name}-核准{amt_str}")
        reply_text(reply_token, "\n".join(lines))
        return

    if t == "stats":
        conn = get_conn(); cur = conn.cursor()
        today = now_iso()[:10]
        month_start = today[:7] + "-01"
        # 今日
        cur.execute("SELECT COUNT(*) as c FROM customers WHERE date(created_at)=?", (today,))
        today_new = cur.fetchone()["c"]
        cur.execute("SELECT COUNT(*) as c FROM customers WHERE status IN ('CLOSED','PENALTY','ABANDONED','REJECTED') AND date(updated_at)=?", (today,))
        today_closed = cur.fetchone()["c"]
        # 本月
        cur.execute("SELECT COUNT(*) as c FROM customers WHERE created_at>=?", (month_start,))
        month_new = cur.fetchone()["c"]
        cur.execute("SELECT COUNT(*) as c FROM customers WHERE status IN ('CLOSED','PENALTY','ABANDONED','REJECTED') AND updated_at>=?", (month_start,))
        month_closed = cur.fetchone()["c"]
        # 核准：從 route_plan 歷史找有核准紀錄的客戶
        cur.execute("SELECT route_plan FROM customers WHERE status='ACTIVE' AND route_plan IS NOT NULL AND route_plan!=''")
        all_routes = cur.fetchall()
        today_approved = 0
        month_approved = 0
        for r in all_routes:
            approved = get_all_approved(r["route_plan"])
            if approved:
                for a in approved:
                    d = a.get("date", "")
                    if d == today:
                        today_approved += 1
                    if d >= month_start:
                        month_approved += 1
        # 待撥款
        cur.execute("SELECT COUNT(*) as c FROM customers WHERE status='ACTIVE' AND report_section='待撥款'")
        total_pending = cur.fetchone()["c"]
        cur.execute("SELECT COUNT(*) as c FROM customers WHERE status='ACTIVE'")
        total_active = cur.fetchone()["c"]
        conn.close()
        msg = (f"📊 統計資訊\n\n"
               f"【今日 {today}】\n"
               f"  新進件：{today_new}\n"
               f"  核准：{today_approved}\n"
               f"  結案：{today_closed}\n\n"
               f"【本月】\n"
               f"  新進件：{month_new}\n"
               f"  核准：{month_approved}\n"
               f"  結案：{month_closed}\n\n"
               f"目前活躍：{total_active}　待撥款：{total_pending}")
        reply_text(reply_token, msg)
        return

    if t == "rename":
        old_name = cmd["old_name"]
        new_name = cmd["new_name"]
        rows = find_active_by_name(old_name)
        if not rows:
            # 也找結案客戶
            conn2 = get_conn(); cur2 = conn2.cursor()
            cur2.execute("SELECT * FROM customers WHERE customer_name=? ORDER BY updated_at DESC LIMIT 1", (old_name,))
            r = cur2.fetchone(); conn2.close()
            if r: rows = [r]
        same = [r for r in rows if r["source_group_id"] == group_id]
        target = same[0] if same else (rows[0] if rows else None)
        if not target:
            reply_text(reply_token, f"❌ 找不到客戶：{old_name}"); return
        conn = get_conn(); cur = conn.cursor()
        cur.execute("UPDATE customers SET customer_name=?, updated_at=? WHERE case_id=?",
                    (new_name, now_iso(), target["case_id"]))
        conn.commit(); conn.close()
        # 寫 case_log
        update_customer(target["case_id"],
                        text=f"{old_name} 改名為 {new_name}",
                        from_group_id=group_id)
        reply_text(reply_token, f"✅ 已將「{old_name}」改名為「{new_name}」")
        return

    if t == "change_id":
        name = cmd["name"]
        new_id = cmd["new_id"]
        rows = find_active_by_name(name)
        same = [r for r in rows if r["source_group_id"] == group_id]
        target = same[0] if same else (rows[0] if rows else None)
        if not target:
            reply_text(reply_token, f"❌ 找不到客戶：{name}"); return
        old_id = target["id_no"] or "無"
        conn = get_conn(); cur = conn.cursor()
        cur.execute("UPDATE customers SET id_no=?, updated_at=? WHERE case_id=?",
                    (new_id, now_iso(), target["case_id"]))
        conn.commit(); conn.close()
        update_customer(target["case_id"],
                        text=f"{name} 身分證 {old_id} → {new_id}",
                        from_group_id=group_id)
        reply_text(reply_token, f"✅ {name} 身分證已更新為 {new_id}")
        return

    if t == "reopen":
        name = cmd["name"]
        conn = get_conn(); cur = conn.cursor()
        cur.execute("SELECT * FROM customers WHERE customer_name=? AND status IN ('CLOSED','PENALTY','ABANDONED','REJECTED') ORDER BY updated_at DESC LIMIT 1", (name,))
        target = cur.fetchone(); conn.close()
        if not target:
            reply_text(reply_token, f"❌ 找不到已結案客戶：{name}"); return
        update_customer(target["case_id"], status="ACTIVE",
                        report_section="",
                        text=f"{name} 重啟案件",
                        from_group_id=group_id)
        reply_text(reply_token, f"✅ {name} 已重啟，恢復到日報")
        return

    if t == "update_amount":
        name = cmd["name"]
        company = cmd["company"]
        amount = cmd["amount"]
        rows = find_active_by_name(name)
        same = [r for r in rows if r["source_group_id"] == group_id]
        target = same[0] if same else (rows[0] if rows else None)
        if not target:
            reply_text(reply_token, f"❌ 找不到客戶：{name}"); return
        route = target["route_plan"] or ""
        new_route = update_company_amount_in_history(route, company, amount)
        update_customer(target["case_id"], route_plan=new_route,
                        approved_amount=amount,
                        text=f"{name} {company} 核准金額修改為 {amount}",
                        from_group_id=group_id)
        reply_text(reply_token, f"✅ {name} {company} 核准金額已更新為 {amount}")
        return

    if t == "close":
        name = cmd["name"]
        reason = cmd.get("reason", "")
        rows = find_active_by_name(name)
        same = [r for r in rows if r["source_group_id"] == group_id]
        target = same[0] if same else (rows[0] if rows else None)
        if not target:
            reply_text(reply_token, f"❌ 找不到客戶：{name}"); return
        close_text = f"{name} 結案（{reason}）" if reason else f"{name} 結案"
        update_customer(target["case_id"], status="CLOSED",
                        text=close_text, from_group_id=group_id)
        push_text(target["source_group_id"], close_text)
        reply_text(reply_token, f"✅ {name} 已結案，從日報移除" + (f"\n原因：{reason}" if reason else ""))
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
        # 回貼業務群
        push_msg = f"{name} {current} 婉拒"
        if next_co:
            push_msg += f"\n➡️ 下一家：{next_co}"
        push_text(target["source_group_id"], push_msg)
        if next_co:
            reply_text(reply_token, f"✅ {name} {current} 婉拒\n➡️ 下一家：{next_co}")
        else:
            reply_text(reply_token, f"✅ {name} {current} 婉拒\n⚠️ 已無下一家送件方案")
        return

    if t == "reject_to":
        name = cmd["name"]
        target_co = cmd["target"]
        rows = find_active_by_name(name)
        same = [r for r in rows if r["source_group_id"] == group_id]
        target = same[0] if same else (rows[0] if rows else None)
        if not target:
            reply_text(reply_token, f"❌ 找不到客戶：{name}"); return
        route = target["route_plan"] or ""
        current = get_current_company(route)
        new_route, ok, err = advance_route_to(route, target_co, "婉拒")
        if not ok:
            reply_text(reply_token, f"❌ {err}\n請確認公司名稱是否正確"); return
        update_customer(target["case_id"], route_plan=new_route,
                        current_company=target_co,
                        text=f"{name} {current} 婉拒，轉送 {target_co}", from_group_id=group_id)
        # 回貼業務群
        push_text(target["source_group_id"], f"{name} {current} 婉拒\n➡️ 跳轉到：{target_co}")
        reply_text(reply_token, f"✅ {name} {current} 婉拒\n➡️ 跳轉到：{target_co}")
        return

    if t == "penalty":
        name = cmd["name"]
        amt = cmd["penalty"]
        rows = find_active_by_name(name)
        same = [r for r in rows if r["source_group_id"] == group_id]
        target = same[0] if same else (rows[0] if rows else None)
        if not target:
            reply_text(reply_token, f"❌ 找不到客戶：{name}"); return
        has_approved = bool(target["approved_amount"])
        reason = "核准後放棄" if has_approved else "辦理中放棄"
        update_customer(target["case_id"], status="PENALTY",
                        text=f"{name} 違約金已支付 ${amt}（{reason}）", from_group_id=group_id)
        push_text(target["source_group_id"], f"{name} 違約金已支付 ${amt}（{reason}）")
        reply_text(reply_token, "✅ " + name + " 已結案\n原因：" + reason + "\n違約金：$" + amt)
        return

    if t == "notification":
        name = cmd["name"]
        company = cmd.get("company", "")
        rows = find_active_by_name(name)
        same = [r for r in rows if r["source_group_id"] == group_id]
        target = same[0] if same else (rows[0] if rows else None)
        if not target:
            reply_text(reply_token, f"❌ 找不到客戶：{name}"); return
        r = dict(target)
        txt = generate_notification_text(r, company)
        reply_text(reply_token, txt)
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
            push_text(target["source_group_id"], f"{name} 已轉送：{current} → {target_co}")
            reply_text(reply_token, f"✅ {name} 已轉送：{current} → {target_co}")
        else:
            next_co = get_next_company(route)
            if not next_co:
                reply_text(reply_token, f"⚠️ {name} 已無下一家送件方案"); return
            new_route = advance_route(route, "轉送")
            update_customer(target["case_id"], route_plan=new_route, current_company=next_co,
                            text=f"{name} 轉下一家→{next_co}", from_group_id=group_id)
            push_text(target["source_group_id"], f"{name} 已轉送：{current} → {next_co}")
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
    name, id_no, company = f.get("name", ""), f.get("id_no", ""), extract_company(block_text)
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


def parse_transfer_line(line: str) -> Dict:
    """解析「8/5-戴君哲-轉21」或「8/11-林曉薇-轉麻吉 6/18」，回傳 dict 或 {}"""
    m = TRANSFER_RE.match(line.strip())
    if not m:
        return {}
    target_co = m.group(3).strip()
    # 透過別名解析
    target_co = COMPANY_ALIAS.get(target_co, target_co)
    return {"date": m.group(1), "name": m.group(2), "target": target_co}


def handle_transfer_block(block_text, source_group_id, reply_token) -> Optional[str]:
    """處理「8/5-戴君哲-轉21」格式的轉送指令"""
    parsed = parse_transfer_line(extract_first_line(block_text))
    if not parsed:
        return None
    name, target_co = parsed["name"], parsed["target"]
    rows = find_active_by_name(name)
    same = [r for r in rows if r["source_group_id"] == source_group_id]
    target = same[0] if same else (rows[0] if rows else None)
    if not target:
        return f"❌ 找不到客戶：{name}"
    route = target["route_plan"] or ""
    current = get_current_company(route)
    new_route, ok, err = advance_route_to(route, target_co, "轉送")
    if not ok:
        # 如果 route_plan 沒有該公司，直接更新 current_company
        update_customer(target["case_id"], current_company=target_co,
                        text=f"{name} 轉{target_co}", from_group_id=source_group_id)
        return f"✅ {name} 已轉送：{current or '無'} → {target_co}"
    update_customer(target["case_id"], route_plan=new_route, current_company=target_co,
                    text=f"{name} 轉{target_co}", from_group_id=source_group_id)
    return f"✅ {name} 已轉送：{current or '無'} → {target_co}"


def handle_a_case_block(block_text, reply_token) -> Optional[str]:
    if is_blocked(block_text):
        return "❌ 含禁止關鍵字，已略過"
    id_no = extract_id_no(block_text)
    name = extract_name(block_text)
    # Bug 16: 用姓名鎖防止同客戶並發更新
    with get_name_lock(name):
        return _handle_a_case_block_locked(block_text, reply_token, id_no, name)


def _handle_a_case_block_locked(block_text, reply_token, id_no, name) -> Optional[str]:
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

    # 核准時抓金額，判斷是否移到待撥款區
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
        # 只有在客戶目前沒有其他公司在送時才移到待撥款
        current_co = customer.get("current_company") or customer.get("company") or ""
        if not current_co or current_co == company:
            # 目前就在核准的公司（或沒有 current_company），再看有沒有下一家
            if not get_next_company(new_route):
                new_report_section = "待撥款"
        # else: 客戶目前在送其他公司，report_section 不動，留在 current_company 區塊

    if is_approved:
        # 核准時：不動 company / current_company（保持送件順序不變）
        # 只更新 route_plan 歷史（記錄哪家核准多少）+ 金額 + 移到待撥款
        update_customer(customer["case_id"], text=block_text,
                        from_group_id=A_GROUP_ID, status=new_status,
                        route_plan=new_route,
                        approved_amount=approved_amount,
                        report_section=new_report_section)
    else:
        # 婉拒/其他：正常更新 company + current_company（推進送件順序）
        update_customer(customer["case_id"], company=company, text=block_text,
                        from_group_id=A_GROUP_ID, status=new_status,
                        route_plan=new_route,
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
        # Bug 16: 只捕獲 id_no/name/case_id/company，不要捕獲整個 customer 物件
        cap_id_no = customer["id_no"]
        cap_name = customer["customer_name"]
        cap_case_id = customer["case_id"]
        cap_company = company

        def ai_parse_and_update():
            import asyncio, traceback
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            try:
                try:
                    ai_amount = loop.run_until_complete(extract_approved_amount_with_ai(block_text))
                except Exception as ex:
                    # Bug 4: Anthropic API 異常或網路錯誤，主動推訊息通知 A 群
                    print(f"[ai_parse_and_update] Anthropic API error: {ex}")
                    traceback.print_exc()
                    push_text(A_GROUP_ID,
                        f"⚠️ {cap_name} AI 金額辨識異常（{type(ex).__name__}），"
                        f"請手動補：{cap_name} 核准金額XX萬@AI")
                    return

                if not ai_amount:
                    push_text(A_GROUP_ID, f"⚠️ {cap_name} 核准金額無法辨識\n請手動補：{cap_name} 核准金額XX萬@AI")
                    return

                # Bug 16: 進姓名鎖，確保不與其他訊息打架
                with get_name_lock(cap_name):
                    # Bug 16: 重新從 DB 讀最新狀態（不用快照）
                    try:
                        fresh = find_active_by_id_no(cap_id_no) if cap_id_no else None
                    except Exception as ex:
                        print(f"[ai_parse_and_update] DB read error: {ex}")
                        push_text(A_GROUP_ID, f"⚠️ {cap_name} 寫入失敗，請手動處理")
                        return
                    if not fresh:
                        push_text(A_GROUP_ID,
                            f"⚠️ {cap_name} AI 辨識完成（金額：{ai_amount}），"
                            f"但客戶在背景處理期間已被結案，未更新。\n"
                            f"如需處理，請手動重啟案件。")
                        return

                    try:
                        cur_route = fresh["route_plan"] or ""
                        new_r = update_company_amount_in_history(cur_route, cap_company, ai_amount)
                        update_customer(fresh["case_id"], approved_amount=ai_amount,
                                        route_plan=new_r, from_group_id=A_GROUP_ID)
                        push_text(A_GROUP_ID, f"💰 {cap_name} {cap_company} 核准金額已辨識：{ai_amount}")
                    except Exception as ex:
                        print(f"[ai_parse_and_update] update error: {ex}")
                        traceback.print_exc()
                        push_text(A_GROUP_ID,
                            f"⚠️ {cap_name} 金額已辨識為 {ai_amount} 但寫入失敗，請手動補")
            except Exception as ex:
                # 最後的防線：任何未預期的例外都不能讓執行緒靜默死掉
                print(f"[ai_parse_and_update] unexpected: {ex}")
                import traceback as _tb; _tb.print_exc()
            finally:
                try:
                    loop.close()
                except Exception:
                    pass
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
        p = a["payload"]; block_text = p.get("block_text", ""); sg = p.get("source_group_id", "")
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
                # Bug 14: 若業務群不存在，顯示 "-" 而非 "未知群組" 以免日報統計出錯
                sales_group_name = get_group_name(target["source_group_id"])
                if sales_group_name == "未知群組":
                    sales_group_name = "-"
                # 優先從 route_plan history 找這家公司的金額，找不到再用 approved_amount
                approved_amount = get_amount_from_history(target["route_plan"] or "", company)
                if not approved_amount:
                    approved_amount = target["approved_amount"] or ""

                # 更新撥款日期（寫到 route_plan history + disbursement_date）
                new_route = set_disbursed_in_history(target["route_plan"] or "", company, disb_date)
                update_customer(
                    target["case_id"],
                    disbursement_date=disb_date,
                    route_plan=new_route,
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


def handle_bc_case_block(block_text, source_group_id, reply_token, source_text="") -> Optional[str]:
    if is_blocked(block_text):
        return "❌ 含禁止關鍵字，已略過"
    # Bug 16: 用姓名鎖防止同客戶並發
    name_for_lock = extract_name(block_text)
    with get_name_lock(name_for_lock):
        return _handle_bc_case_block_locked(block_text, source_group_id, reply_token, source_text)


def _handle_bc_case_block_locked(block_text, source_group_id, reply_token, source_text="") -> Optional[str]:
    # 照會注意事項（等同已送件，提取欄位資料）
    if is_notification_briefing(block_text):
        result = handle_notification_briefing(block_text, source_group_id, reply_token)
        if result:
            return result
    if is_route_order_line(extract_first_line(block_text)):
        return handle_route_order_block(block_text, source_group_id, reply_token)
    # 轉送格式：8/5-戴君哲-轉21
    if parse_transfer_line(extract_first_line(block_text)):
        return handle_transfer_block(block_text, source_group_id, reply_token)
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
    # Bug 12: 防呆檢查 event 型別與必要欄位
    if not isinstance(event, dict):
        return
    if event.get("type") != "message":
        return
    message = event.get("message", {})
    if not isinstance(message, dict) or message.get("type") != "text":
        return
    text = (message.get("text") or "").strip()
    reply_token = event.get("replyToken") or ""
    source = event.get("source") or {}
    if not isinstance(source, dict):
        return
    group_id = source.get("groupId")
    if not text:
        return
    # Bug 2: reply_token 空值時 LINE API 會錯誤，直接忽略該事件
    if not reply_token:
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

    # A 群優先（避免 A 群同時被註冊為 SALES_GROUP 時走錯邏輯）
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

    # Bug 2: 未知群組 — 自動註冊為 UNASSIGNED 待審 + 主動回覆
    if group_id:
        try:
            with db_conn(commit=True) as conn:
                cur = conn.cursor()
                cur.execute("SELECT 1 FROM groups WHERE group_id=?", (group_id,))
                if not cur.fetchone():
                    # 自動建立待審群組
                    cur.execute("""INSERT INTO groups
                        (group_id, group_name, group_type, is_active, created_at)
                        VALUES (?, ?, 'UNASSIGNED', 0, ?)""",
                        (group_id, f"待審群組 {group_id[:8]}", now_iso()))
                    # 主動回覆
                    reply_text(reply_token,
                        f"⚠️ 此群組尚未註冊\n"
                        f"群組 ID：{group_id}\n"
                        f"請管理員到後台「群組管理」設定類型並啟用，"
                        f"設定完成後即可正常處理訊息。")
        except Exception as e:
            print(f"[unknown_group] failed: {e}")


@app.post("/callback")
async def callback(request: Request, background_tasks: BackgroundTasks):
    try:
        body = await request.json()
    except Exception:
        return {"status": "ok"}
    if not isinstance(body, dict):
        return {"status": "ok"}
    events = body.get("events", [])
    if not isinstance(events, list):
        return {"status": "ok"}
    for event in events:
        if not isinstance(event, dict):
            continue
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
    role = check_auth(request)
    if role != "admin":
        return JSONResponse({"status": "error", "message": "無權限"}, status_code=403)
    body = await request.json()
    if body.get("confirm") != "yes":
        return {"status": "error", "message": '請帶 {"confirm": "yes"} 才會執行清除'}
    try:
        with db_conn(commit=True) as conn:
            cur = conn.cursor()
            cur.execute("DELETE FROM customers")
            cur.execute("DELETE FROM case_logs")
            cur.execute("DELETE FROM pending_actions")
            cur.execute("DELETE FROM sqlite_sequence WHERE name IN ('customers','case_logs','pending_actions')")
        return {"status": "ok", "message": "✅ 已清除所有案件資料，群組設定保留"}
    except Exception as e:
        return {"status": "error", "message": str(e)}


@app.get("/admin/reset_data")
def reset_data_page(request: Request):
    """清除測試資料的網頁介面"""
    from fastapi.responses import RedirectResponse
    role = check_auth(request)
    if role != "admin":
        return RedirectResponse("/login", status_code=303)
    return HTMLResponse("""<!DOCTYPE html><html><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>清除測試資料</title></head><body>
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
    </body></html>""")


VALID_GROUP_TYPES = {"SALES_GROUP", "ADMIN_GROUP", "A_GROUP", "UNASSIGNED"}

@app.post("/admin/add_group")
async def add_group(request: Request):
    """新增群組 API（Bug 3 修復：類型 whitelist + 防覆蓋）

    Body: {group_id, group_name, group_type: SALES_GROUP/ADMIN_GROUP/A_GROUP}

    行為：
    - 若 group_id 已存在 → 改用 UPDATE 只更新 name/type/linked，**保留 password_hash**
    - 若不存在 → INSERT 新群組
    - 類型必須在 whitelist 內，否則拒絕
    """
    role = check_auth(request)
    if role != "admin":
        return JSONResponse({"status": "error", "message": "無權限"}, status_code=403)
    body = await request.json()
    gid = body.get("group_id", "").strip()
    gname = body.get("group_name", "").strip()
    gtype = body.get("group_type", "SALES_GROUP").strip()
    if not gid or not gname:
        return {"status": "error", "message": "group_id 和 group_name 必填"}
    # Bug 3-1: 類型 whitelist 驗證
    if gtype not in VALID_GROUP_TYPES:
        return {"status": "error",
                "message": f"無效的群組類型：{gtype}（需為 SALES_GROUP/ADMIN_GROUP/A_GROUP）"}
    linked = body.get("linked_sales_group_id", "").strip()
    try:
        with db_conn(commit=True) as conn:
            cur = conn.cursor()
            # Bug 3-2: 先檢查是否已存在
            cur.execute("SELECT 1 FROM groups WHERE group_id=?", (gid,))
            existing = cur.fetchone()
            if existing:
                # 已存在 → UPDATE 不動 password_hash 和 created_at
                cur.execute("""UPDATE groups
                    SET group_name=?, group_type=?, is_active=1, linked_sales_group_id=?
                    WHERE group_id=?""",
                    (gname, gtype, linked or None, gid))
                msg = f"已更新群組：{gname}（{gtype}）— 密碼保留不動"
            else:
                # 新增
                cur.execute("""INSERT INTO groups
                    (group_id,group_name,group_type,is_active,linked_sales_group_id,created_at)
                    VALUES (?,?,?,1,?,?)""",
                    (gid, gname, gtype, linked or None, now_iso()))
                msg = f"已新增群組：{gname}（{gtype}）— 請到「密碼管理」設定密碼"
            if linked:
                msg += f"，對應業務群：{get_group_name(linked)}"
            return {"status": "ok", "message": msg}
    except Exception as e:
        return {"status": "error", "message": str(e)}



# =========================
# 密碼工具
# =========================
import hashlib as _hl

def hash_pw(password: str) -> str:
    """密碼雜湊 — 使用 PBKDF2-SHA256，20 萬次 iteration（Bug 1 修復）"""
    import os as _os
    salt = _os.urandom(16).hex()
    dk = _hl.pbkdf2_hmac("sha256", password.encode(), bytes.fromhex(salt), 200_000)
    return f"pbkdf2$200000${salt}${dk.hex()}"

def verify_pw(password: str, stored: str) -> bool:
    """驗證密碼 — 支援新格式 PBKDF2 + 舊格式 SHA-256（向後相容）

    舊密碼能繼續登入，但 Web 端建議下次登入成功後重新 hash 升級。
    使用 hmac.compare_digest 防時序攻擊。
    """
    if not stored:
        return False
    try:
        import hmac as _hmac
        # 新格式：pbkdf2$iterations$salt$hash
        if stored.startswith("pbkdf2$"):
            _, iters, salt, hashed = stored.split("$", 3)
            dk = _hl.pbkdf2_hmac("sha256", password.encode(), bytes.fromhex(salt), int(iters))
            return _hmac.compare_digest(dk.hex(), hashed)
        # 舊格式：salt:hash（向後相容）
        if ":" in stored:
            salt, hashed = stored.split(":", 1)
            calc = _hl.sha256((salt + password).encode()).hexdigest()
            return _hmac.compare_digest(calc, hashed)
    except Exception:
        return False
    return False

def needs_pw_upgrade(stored: str) -> bool:
    """檢查密碼是否為舊格式，需要升級到 PBKDF2"""
    return bool(stored) and not stored.startswith("pbkdf2$")

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
    # locked_until 用 datetime 比較而非字串比較，避免 NULL/格式錯誤誤判
    with db_conn() as conn:
        cur = conn.cursor()
        cur.execute("SELECT attempts, locked_until FROM login_attempts WHERE identifier=?", (identifier,))
        row = cur.fetchone()
    if not row:
        return False
    lu = row["locked_until"]
    if not lu:
        return False
    try:
        lu_dt = datetime.strptime(lu, "%Y-%m-%d %H:%M:%S")
        return lu_dt > datetime.now()
    except (ValueError, TypeError):
        return False

def record_login_fail(identifier: str):
    conn = get_conn(); cur = conn.cursor()
    cur.execute("SELECT attempts FROM login_attempts WHERE identifier=?", (identifier,))
    row = cur.fetchone()
    if row:
        attempts = (row["attempts"] or 0) + 1  # 防 NULL
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


def _ensure_sessions_table():
    """確保 sessions 表存在（相容舊版 DB）"""
    try:
        conn = get_conn(); cur = conn.cursor()
        cur.execute("""CREATE TABLE IF NOT EXISTS sessions (
            token TEXT PRIMARY KEY NOT NULL,
            role TEXT NOT NULL,
            group_id TEXT DEFAULT '',
            created_at TEXT NOT NULL,
            expires_at TEXT NOT NULL
        )""")
        conn.commit(); conn.close()
    except Exception:
        pass


def _create_session(role: str, group_id: str = "") -> str:
    """產生隨機 session token 並存入 DB"""
    from datetime import timedelta
    _ensure_sessions_table()
    token = secrets.token_urlsafe(32)
    now = now_iso()
    expires = (datetime.now() + timedelta(days=7)).strftime("%Y-%m-%d %H:%M:%S")
    try:
        conn = get_conn(); cur = conn.cursor()
        cur.execute("INSERT INTO sessions (token,role,group_id,created_at,expires_at) VALUES (?,?,?,?,?)",
            (token, role, group_id, now, expires))
        conn.commit(); conn.close()
    except Exception as e:
        print(f"Session create error: {e}")
    return token


def _is_session_expired(expires_at_str) -> bool:
    """Bug 10: 用 datetime 比較時間，不用字串比較

    NULL/格式錯誤 → 視為過期（安全預設）
    """
    if not expires_at_str:
        return True
    try:
        expires = datetime.strptime(expires_at_str, "%Y-%m-%d %H:%M:%S")
        return expires < datetime.now()
    except (ValueError, TypeError):
        return True


def _lookup_session(token: str) -> Optional[Dict]:
    """查找 session，回傳 {role, group_id} 或 None

    Bug 10/18 修復：用 datetime 比較 + db_conn context manager 防連線洩漏
    """
    if not token:
        return None
    try:
        _ensure_sessions_table()
        with db_conn() as conn:
            cur = conn.cursor()
            cur.execute("SELECT role, group_id, expires_at FROM sessions WHERE token=?", (token,))
            row = cur.fetchone()
        if not row:
            return None
        # Bug 10: 用 datetime 比較
        if _is_session_expired(row["expires_at"]):
            # Bug 18: 用 db_conn 確保例外時連線會關
            with db_conn(commit=True) as conn:
                cur = conn.cursor()
                cur.execute("DELETE FROM sessions WHERE token=?", (token,))
            return None
        return {"role": row["role"], "group_id": row["group_id"]}
    except Exception as e:
        print(f"Session lookup error: {e}")
        return None


def _delete_session(token: str):
    """登出時刪除 session（Bug 18: 用 db_conn 防連線洩漏）"""
    if not token:
        return
    try:
        with db_conn(commit=True) as conn:
            cur = conn.cursor()
            cur.execute("DELETE FROM sessions WHERE token=?", (token,))
    except Exception as e:
        print(f"Session delete error: {e}")

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
.topnav-links{display:flex;gap:4px;align-items:center}
.nl{padding:6px 12px;border-radius:6px;color:rgba(255,255,255,.65);font-size:13px;transition:all .15s;white-space:nowrap}
.nl:hover{background:rgba(255,255,255,.1);color:#fff}
.nl.active{background:rgba(255,255,255,.15);color:#fff;font-weight:500}
.menu-btn{display:none;background:none;border:none;color:#fff;font-size:22px;cursor:pointer;padding:4px 8px}
.mobile-menu{display:none;position:fixed;top:52px;left:0;right:0;background:#1a1a2e;z-index:99;padding:8px 16px 16px;border-bottom:2px solid #6366f1}
.mobile-menu.show{display:flex;flex-direction:column;gap:4px}
.mobile-menu .nl{padding:10px 14px;font-size:14px;border-radius:8px}
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
@media(max-width:768px){
  .topnav{height:52px;padding:0 12px}
  .topnav-links{display:none}
  .menu-btn{display:block}
  .stats-grid{grid-template-columns:repeat(2,1fr);gap:8px}
  .stat-card{padding:12px}
  .page{padding:14px 10px}
  .search-result{padding:12px}
  .modal{width:95%;padding:16px}
  .input{padding:10px 12px;font-size:14px}
  .btn{padding:9px 16px;font-size:14px}
  .btn-primary{padding:10px 18px}
  table{font-size:11px}
  th,td{padding:5px 6px}
  .group-hd{padding:12px 14px}
}
@media(max-width:480px){
  .stats-grid{grid-template-columns:1fr 1fr;gap:6px}
  .stat-num{font-size:20px}
  .stat-lbl{font-size:10px}
  .stat-card{padding:10px}
  .search-bar{flex-direction:column}
  .group-title{font-size:13px}
}
</style>
"""

GROUP_COLORS = ["#1a1a2e","#2d5016","#7c2d12","#1e3a5f","#4a1d6e","#1e4d4d",
                "#78350f","#164e63","#4c1d95","#052e16"]

def check_auth(request: Request) -> str:
    """回傳 'admin'/'adminB'/'normal'/'group_xxx'/'' """
    t = request.cookies.get("auth_token", "")
    session = _lookup_session(t)
    if not session:
        return ""
    role = session["role"]
    if role in ("admin", "adminB", "normal"):
        return role
    if role == "group" and session["group_id"]:
        return f"group_{session['group_id']}"
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
        links.append(("📋 客戶資料庫","/pending-customers","pending"))
        links.append(("➕ 新增客戶","/new-customer","new"))
    if role in ("admin","adminB"):
        links.append(("📋 行政B作業","/adminb","adminb"))
    if role == "admin":
        links += [("⚙️ 群組管理","/admin/groups","admin"),
                  ("🔑 密碼管理","/admin/passwords","passwords"),
                  ("📝 操作紀錄","/admin/logs","logs"),
                  ("💾 下載備份","/admin/download-db","download"),
                  ("🗑️ 清除資料","/admin/reset_data","reset")]
    nav = "".join(f'<a class="nl {"active" if a==active else ""}" href="{u}">{n}</a>'
                  for n,u,a in links)
    nav += '<a class="nl" href="/logout">登出</a>'
    mobile_nav = "".join(f'<a class="nl {"active" if a==active else ""}" href="{u}" onclick="document.getElementById(\'mobileMenu\').classList.remove(\'show\')">{n}</a>'
                  for n,u,a in links)
    mobile_nav += '<a class="nl" href="/logout">登出</a>'
    return (f'<nav class="topnav"><div class="topnav-title">貸款案件管理</div>'
            f'<div class="topnav-links">{nav}</div>'
            f'<button class="menu-btn" onclick="document.getElementById(\'mobileMenu\').classList.toggle(\'show\')">☰</button></nav>'
            f'<div id="mobileMenu" class="mobile-menu">{mobile_nav}</div>')

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

def get_status_type(row) -> str:
    """判斷客戶狀態類型"""
    row = dict(row)
    sec = row["report_section"] or ""
    last = row["last_update"] or ""
    first = last.splitlines()[0] if last else ""
    co = row["current_company"] or row["company"] or ""
    route_data = parse_route_json(row["route_plan"] or "")
    order = route_data.get("order", [])
    # 新客戶：有資料但沒有送件順序和公司
    if not co and not order and not sec:
        return "new"
    # 待撥款
    if sec == "待撥款":
        disbursed = row["disbursement_date"] if row["disbursement_date"] else ""
        if disbursed:
            return "paid_verified"
        return "paid_unverified"
    # 補件
    if any(w in first for w in ["補","缺資料","補件","補資料","補行照","補照會","補照片","補時段","補聯徵","補保人"]):
        return "supplement"
    # 結案
    if row["status"] != "ACTIVE":
        return "closed"
    return "active"

def classify_rows(rows):
    """把客戶分類到各狀態"""
    cats = {"new":[], "supplement":[], "active":[], "paid_unverified":[], "paid_verified":[], "closed":[]}
    for r in rows:
        t = get_status_type(r)
        cats[t].append(r)
    return cats

def render_customer_row(row) -> str:
    """產生單一客戶列"""
    row = dict(row)
    created = row["created_at"] or ""
    date_str = created[5:10].replace("-","/") if created else ""
    last = row["last_update"] or ""
    first_line = last.splitlines()[0].strip() if last.strip() else ""
    status_summary = extract_status_summary(first_line, row["customer_name"])
    co = row["current_company"] or row["company"] or ""
    amt = row["approved_amount"] or ""
    disb = row["disbursement_date"] or ""
    route_data = parse_route_json(row["route_plan"] or "")
    order = route_data.get("order",[])
    idx = route_data.get("current_index",0)

    # 詳細資料 - 支援多家核准
    route_history = route_data.get("history", [])
    approved_list = [rh for rh in route_history if rh.get("status") in ("核准","待撥款","撥款") and rh.get("amount")]
    if (row["report_section"] or "") == "待撥款":
        if len(approved_list) > 1:
            parts = [(rh.get("company") or "") + (rh.get("amount") or "") for rh in approved_list if rh.get("amount")]
            sub = "多家核准：" + " + ".join(parts) + ("（撥款" + disb + "）" if disb else "（待撥款）")
        else:
            sub = co + (f" 核准{amt}" if amt else "") + (f"（撥款{disb}）" if disb else "（待撥款）")
    elif order and idx < len(order):
        next_co = order[idx+1] if idx+1 < len(order) else ""
        # 顯示各家進度摘要
        progress_parts = []
        for rh in route_history[-3:]:
            hco = rh.get("company","")
            hst = rh.get("status","")
            hamt = rh.get("amount","")
            if hst == "核准":
                progress_parts.append(hco + "核准" + hamt)
            elif hst == "婉拒":
                progress_parts.append(hco + "婉拒")
        sub = co + (f" → {next_co}" if next_co else f"（第{idx+1}/{len(order)}家）")
        if progress_parts:
            sub += "　" + " / ".join(progress_parts[-2:])
    else:
        sub = co + (f" · {status_summary}" if status_summary else "")

    # 詳細資料展開內容
    phone = row.get("phone","") or ""
    id_no = row.get("id_no","") or ""
    company = row.get("company_name_detail","") or row.get("company","") or ""
    salary = row.get("company_salary","") or ""
    labor = row.get("eval_labor_ins","") or ""
    fund = row.get("eval_fund_need","") or ""

    progress_html = ('<div style="margin-top:8px;font-size:12px;color:#4a3e30">最新進度：<b style="color:#2c2820">' + h(first_line[:80]) + '</b></div>') if first_line else ""
    detail_html = (
        '<div style="background:#f0ebe4;padding:12px 16px;border-top:1px solid #ddd5ca;">'
        '<div style="display:grid;grid-template-columns:repeat(3,1fr);gap:8px;margin-bottom:6px">'
        '<div><div style="font-size:11px;color:#6a5e4e;font-weight:600">身分證</div><div style="font-size:13px;color:#2c2820;font-weight:500">' + h(id_no or "-") + '</div></div>'
        '<div><div style="font-size:11px;color:#6a5e4e;font-weight:600">電話</div><div style="font-size:13px;color:#2c2820;font-weight:500">' + h(phone or "-") + '</div></div>'
        '<div><div style="font-size:11px;color:#6a5e4e;font-weight:600">公司</div><div style="font-size:13px;color:#2c2820;font-weight:500">' + h(company or "-") + '</div></div>'
        '<div><div style="font-size:11px;color:#6a5e4e;font-weight:600">月薪</div><div style="font-size:13px;color:#2c2820;font-weight:500">' + (h(salary) + "萬" if salary else "-") + '</div></div>'
        '<div><div style="font-size:11px;color:#6a5e4e;font-weight:600">勞保</div><div style="font-size:13px;color:#2c2820;font-weight:500">' + h(labor or "-") + '</div></div>'
        '<div><div style="font-size:11px;color:#6a5e4e;font-weight:600">資金需求</div><div style="font-size:13px;color:#2c2820;font-weight:500">' + h(fund or "-") + '</div></div>'
        '</div>'
        + progress_html
        + '</div>'
    )

    cid = row["case_id"]
    cname = row["customer_name"]
    q = "'"
    row_onclick = 'onclick="togD(' + q + h(cid) + q + ')"'
    return (
        '<div style="border-bottom:1px solid #ddd5ca">'
        + '<div style="display:grid;grid-template-columns:24px 1fr auto;gap:6px;align-items:center;padding:10px 16px">'
        + '<input type="checkbox" class="batch-cb" value="' + h(cid) + '" onclick="event.stopPropagation()" style="width:16px;height:16px;cursor:pointer">'
        + '<div ' + row_onclick + ' style="cursor:pointer">'
        + '<div style="font-size:15px;font-weight:600;color:#1a1208">' + h(cname) + '</div>'
        + '<div style="font-size:13px;color:#4a3e30;margin-top:2px">' + h(sub) + '</div>'
        + '</div>'
        + '<div style="font-size:12px;color:#6a5e4e;white-space:nowrap">' + h(date_str) + '</div>'
        + '</div>'
        + '<div id="dd-' + h(cid) + '" style="display:none">' + detail_html + '</div>'
        + '</div>'
    )

def render_section_block(label, rows, color_bg, color_text, icon) -> str:
    """產生可收合的狀態區塊"""
    if not rows:
        return ""
    count = len(rows)
    sec_id = label.replace(" ","_").replace("/","_")
    rows_html = "".join(render_customer_row(r) for r in rows)
    onclick_js = "togS('" + h(sec_id) + "')"
    return (
        '<div style="border-top:1px solid #ddd5ca">'
        '<div onclick="' + onclick_js + '" style="display:flex;align-items:center;gap:8px;padding:8px 16px;cursor:pointer;background:#ece8e2">'
        '<span style="font-size:13px">' + icon + '</span>'
        '<span style="font-size:13px;font-weight:700;color:' + color_text + '">' + h(label) + '</span>'
        '<span style="font-size:12px;color:#4a3e30;font-weight:500">' + str(count) + '筆</span>'
        '<span id="sa-' + h(sec_id) + '" style="margin-left:auto;font-size:11px;color:#a09080">▶</span>'
        '</div>'
        '<div id="sc-' + h(sec_id) + '" style="display:none">' + rows_html + '</div>'
        '</div>'
    )

def build_row_map(rows) -> dict:
    m = {}
    for r in rows:
        sec = r["report_section"] or r["current_company"] or r["company"] or "送件"
        m.setdefault(sec, []).append(r)
    return m

def render_cust_rows(rows) -> str:
    if not rows: return '<div class="empty-sec">（無資料）</div>'
    return "".join(render_customer_row(r) for r in rows)

def render_seg(section_map: dict, sections: list, shown: set) -> str:
    html = ""
    for sec in sections:
        if sec in section_map:
            html += f'<div class="sec-hd">{h(sec)}</div>'
            html += render_cust_rows(section_map[sec])
            shown.add(sec)
    return html


@app.get("/login", response_class=HTMLResponse)
def login_page(request: Request, error: str = ""):
    if error == "locked":
        err_html = '<div style="background:#fef2f2;color:#dc2626;padding:8px 12px;border-radius:6px;font-size:12px;margin-bottom:14px">帳號已鎖定，請 15 分鐘後再試或聯繫管理員解鎖</div>'
    elif error:
        err_html = '<div style="background:#fef2f2;color:#dc2626;padding:8px 12px;border-radius:6px;font-size:12px;margin-bottom:14px">密碼錯誤，請重新輸入</div>'
    else:
        err_html = ""
    conn = get_conn(); cur = conn.cursor()
    cur.execute("SELECT group_id, group_name FROM groups WHERE group_type='SALES_GROUP' AND is_active=1 ORDER BY group_name")
    sales_groups = cur.fetchall(); conn.close()
    grp_opts = "".join(f'<option value="{h(g["group_id"])}">{h(g["group_name"])}</option>' for g in sales_groups)
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
            <option value="admin">管理員</option>
            <option value="normal">行政A</option>
            <option value="adminB">行政B</option>
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
        return RedirectResponse("/login?error=locked", status_code=303)
    ok = False
    session_role = ""
    session_group = ""
    stored = ""
    if role == "admin":
        stored = get_setting("admin_pw")
        # Bug 3: 移除明文 fallback，沒密碼就拒絕
        ok = verify_pw(pw, stored)
        session_role = "admin"
    elif role == "adminB":
        stored = get_setting("adminB_pw")
        ok = verify_pw(pw, stored)
        session_role = "adminB"
    elif role == "normal":
        stored = get_setting("report_pw")
        ok = verify_pw(pw, stored)
        session_role = "normal"
    elif role == "group" and group_id:
        stored = get_group_password(group_id)
        ok = verify_pw(pw, stored) if stored else False
        session_role = "group"
        session_group = group_id
    if ok:
        clear_login_fail(identifier)
        # Bug 1：密碼舊格式自動升級到 PBKDF2
        if stored and needs_pw_upgrade(stored):
            try:
                if role == "admin":
                    set_setting("admin_pw", hash_pw(pw))
                elif role == "adminB":
                    set_setting("adminB_pw", hash_pw(pw))
                elif role == "normal":
                    set_setting("report_pw", hash_pw(pw))
            except Exception as e:
                print(f"[pw_upgrade] failed: {e}")
        token = _create_session(session_role, session_group)
        resp = RedirectResponse("/report", status_code=303)
        # Bug 2：加 secure 旗標（本地開發可用 ENV=local 關閉）
        _secure = os.getenv("ENV", "production").lower() != "local"
        resp.set_cookie("auth_token", token, max_age=86400*7,
                        httponly=True, samesite="Lax", secure=_secure)
        return resp
    record_login_fail(identifier)
    return RedirectResponse("/login?error=1", status_code=303)


@app.get("/logout")
def logout(request: Request):
    from fastapi.responses import RedirectResponse
    token = request.cookies.get("auth_token", "")
    _delete_session(token)
    resp = RedirectResponse("/login", status_code=303)
    resp.delete_cookie("auth_token")
    return resp


@app.get("/", response_class=HTMLResponse)
def home_redirect(request: Request):
    from fastapi.responses import RedirectResponse
    return RedirectResponse("/login" if not check_auth(request) else "/report")


@app.post("/report/batch-close")
async def report_batch_close(request: Request):
    role = check_auth(request)
    if not role:
        return JSONResponse({"ok": False, "message": "未登入"}, status_code=401)
    data = await request.json()
    case_ids = data.get("case_ids", [])
    if not case_ids:
        return JSONResponse({"ok": False, "message": "未選擇客戶"})
    conn = get_conn(); cur = conn.cursor()
    closed = 0
    for cid in case_ids:
        cur.execute("SELECT * FROM customers WHERE case_id=? AND status='ACTIVE'", (cid,))
        c = cur.fetchone()
        if c:
            update_customer(cid, status="CLOSED", text=f"{c['customer_name']} 網頁批次結案", from_group_id="WEB")
            closed += 1
    conn.close()
    return JSONResponse({"ok": True, "message": f"已結案 {closed} 筆"})


@app.get("/report/export")
def report_export(request: Request):
    from fastapi.responses import StreamingResponse
    role = check_auth(request)
    if not role: return RedirectResponse("/login")
    auth_group = get_auth_group_id(request)
    conn = get_conn(); cur = conn.cursor()
    if auth_group:
        cur.execute("SELECT * FROM customers WHERE source_group_id=? AND status='ACTIVE' ORDER BY created_at DESC", (auth_group,))
    else:
        cur.execute("SELECT * FROM customers WHERE status='ACTIVE' ORDER BY source_group_id, created_at DESC")
    rows = cur.fetchall(); conn.close()
    # CSV 格式
    lines = ["群組,建立日期,姓名,身分證,目前公司,核准金額,撥款日期,狀態"]
    for r in rows:
        gname = get_group_name(r["source_group_id"])
        created = (r["created_at"] or "")[:10]
        co = r["current_company"] or r["company"] or ""
        amt = r["approved_amount"] or ""
        disb = r["disbursement_date"] or ""
        sec = r["report_section"] or ""
        lines.append(f'{gname},{created},{r["customer_name"]},{r["id_no"] or ""},{co},{amt},{disb},{sec}')
    content = "\ufeff" + "\n".join(lines)  # BOM for Excel
    today = datetime.now().strftime("%Y%m%d")
    return StreamingResponse(
        iter([content.encode("utf-8-sig")]),
        media_type="text/csv",
        headers={"Content-Disposition": f'attachment; filename="report_{today}.csv"'}
    )


@app.get("/report", response_class=HTMLResponse)
def report_web(request: Request):
    from fastapi.responses import RedirectResponse
    role = check_auth(request)
    if not role: return RedirectResponse("/login")
    auth_group = get_auth_group_id(request)

    today = datetime.now().strftime("%m/%d")
    conn = get_conn(); cur = conn.cursor()

    # 業務只看自己群組
    if auth_group:
        cur.execute("SELECT * FROM groups WHERE group_id=? AND is_active=1", (auth_group,))
    else:
        cur.execute("SELECT * FROM groups WHERE group_type='SALES_GROUP' AND is_active=1 ORDER BY group_name")
    groups = cur.fetchall()

    # 統計
    total_new = 0; total_supp = 0; total_active = 0; total_unverified = 0; total_closed = 0
    month_start = datetime.now().strftime("%Y-%m-01")
    today_date = datetime.now().strftime("%Y-%m-%d")
    cur.execute("SELECT COUNT(*) as c FROM customers WHERE status IN ('CLOSED','PENALTY','ABANDONED','REJECTED') AND updated_at>=?", (month_start,))
    total_closed = cur.fetchone()["c"]
    cur.execute("SELECT COUNT(*) as c FROM customers WHERE date(created_at)=?", (today_date,))
    today_new = cur.fetchone()["c"]
    cur.execute("SELECT COUNT(*) as c FROM customers WHERE status='ACTIVE' AND report_section='待撥款'")
    total_pending_disb = cur.fetchone()["c"]

    groups_html = ""
    for i, grp in enumerate(groups):
        gid, gname = grp["group_id"], grp["group_name"]
        cur.execute("SELECT * FROM customers WHERE source_group_id=? AND status='ACTIVE' ORDER BY updated_at DESC", (gid,))
        active_rows = cur.fetchall()
        cur.execute("SELECT * FROM customers WHERE source_group_id=? AND status IN ('CLOSED','PENALTY','ABANDONED','REJECTED') AND updated_at>=? ORDER BY updated_at DESC", (gid, month_start))
        closed_rows = cur.fetchall()

        cats = classify_rows(active_rows)
        total_new += len(cats["new"])
        total_supp += len(cats["supplement"])
        total_active += len(cats["active"])
        total_unverified += len(cats["paid_unverified"])

        count = len(active_rows)
        color = GROUP_COLORS[i % len(GROUP_COLORS)]

        # 統計 pills
        pills = f'<span class="gm-pill">{count} 筆</span>'
        if cats["new"]: pills += f'<span class="gm-pill" style="background:#e0f2fe;color:#0369a1">新客戶 {len(cats["new"])}</span>'
        if cats["supplement"]: pills += f'<span class="gm-pill" style="background:#fef9c3;color:#854d0e">補件 {len(cats["supplement"])}</span>'
        if cats["active"]: pills += f'<span class="gm-pill" style="background:#f0fdf4;color:#166534">送件 {len(cats["active"])}</span>'
        if cats["paid_unverified"]: pills += f'<span class="gm-pill" style="background:#fef2f2;color:#991b1b">未對保 {len(cats["paid_unverified"])}</span>'
        if cats["paid_verified"]: pills += f'<span class="gm-pill" style="background:#dcfce7;color:#166534">已對保 {len(cats["paid_verified"])}</span>'

        # 各狀態區塊
        secs_html = ""
        secs_html += render_section_block("新客戶－需排送件順序", cats["new"], "#e0f2fe", "#0369a1", "🆕")
        secs_html += render_section_block("補件中", cats["supplement"], "#fef9c3", "#854d0e", "📋")
        secs_html += render_section_block("送件中", cats["active"], "#f0fdf4", "#166534", "📤")
        secs_html += render_section_block("待撥款－未對保", cats["paid_unverified"], "#fef2f2", "#991b1b", "💰")
        secs_html += render_section_block("待撥款－已對保", cats["paid_verified"], "#dcfce7", "#166534", "✅")
        if closed_rows:
            secs_html += render_section_block("本月結案", closed_rows, "#f8fafc", "#64748b", "📁")
        if not secs_html:
            secs_html = '<div class="empty-sec">（目前無有效案件）</div>'

        toggle_js = "toggleGroup('" + h(gid) + "')"
        groups_html += (
            '<div class="group-card" style="margin-bottom:10px">'
            '<div class="group-hd" onclick="' + toggle_js + '">'
            '<div class="group-hd-left">'
            '<div class="group-dot" style="background:' + color + '"></div>'
            '<div class="group-title">' + h(gname) + '</div>'
            '<div class="group-meta">' + pills + '</div>'
            '</div>'
            '<div class="chevron" id="chev-' + h(gid) + '">▶</div>'
            '</div>'
            '<div class="group-body" id="body-' + h(gid) + '" style="display:none;border-top:1px solid #ddd5ca">' + secs_html + '</div>'
            '</div>'
        )

    conn.close()

    REPORT_CSS = """
    <style>
    :root{--bg1:#faf7f4;--bg2:#f0ebe4;--bg3:#ece8e2;--t1:#2c2820;--t2:#6a5e4e;--t3:#a09080;--bd:#ddd5ca}
    body{background:var(--bg3);color:var(--t1);font-family:'Microsoft JhengHei','PingFang TC',sans-serif;font-size:14px}
    .group-card{background:var(--bg1);border:1px solid var(--bd);border-radius:10px;overflow:hidden;margin-bottom:10px}
    .group-hd{padding:12px 16px;display:flex;align-items:center;justify-content:space-between;cursor:pointer;user-select:none}
    .group-hd:hover{background:var(--bg2)}
    .group-hd-left{display:flex;align-items:center;gap:10px;flex-wrap:wrap}
    .group-dot{width:10px;height:10px;border-radius:50%;flex-shrink:0}
    .group-title{font-size:15px;font-weight:600;color:var(--t1)}
    .group-meta{display:flex;gap:5px;flex-wrap:wrap}
    .gm-pill{padding:2px 8px;border-radius:20px;font-size:11px;font-weight:500}
    .chevron{color:var(--t3);font-size:11px}
    .group-body{display:none;border-top:1px solid var(--bd)}
    .group-body.open{display:block}
    .empty-sec{padding:14px 20px;font-size:12px;color:var(--t3);font-style:italic}
    .stat-card{background:var(--bg1);border:1px solid var(--bd);border-radius:9px;padding:14px 16px}
    .stat-num{font-size:24px;font-weight:600}
    .stat-lbl{font-size:11px;color:var(--t3);margin-top:3px}
    .group-body{overflow-x:auto}
    </style>"""

    return f"""<!DOCTYPE html><html><head>{PAGE_CSS}{REPORT_CSS}<title>日報 {today}</title></head><body>
    {make_topnav(role, "report")}
    <div class="page">
      <div class="stats-grid" style="grid-template-columns:repeat(5,1fr)">
        <div class="stat-card"><div class="stat-num" style="color:#0369a1">{total_new}</div><div class="stat-lbl">🆕 新客戶</div></div>
        <div class="stat-card"><div class="stat-num" style="color:#854d0e">{total_supp}</div><div class="stat-lbl">📋 補件中</div></div>
        <div class="stat-card"><div class="stat-num" style="color:#166534">{total_active}</div><div class="stat-lbl">📤 送件中</div></div>
        <div class="stat-card"><div class="stat-num" style="color:#991b1b">{total_unverified}</div><div class="stat-lbl">💰 未對保</div></div>
        <div class="stat-card"><div class="stat-num" style="color:#64748b">{total_closed}</div><div class="stat-lbl">📁 本月結案</div></div>
        <div class="stat-card"><div class="stat-num" style="color:#7c3aed">{today_new}</div><div class="stat-lbl">📅 今日進件</div></div>
        <div class="stat-card"><div class="stat-num" style="color:#ea580c">{total_pending_disb}</div><div class="stat-lbl">⏳ 待撥款</div></div>
      </div>
      <div style="margin-bottom:14px;text-align:right">
        <a href="/report/export" class="btn" style="background:#e8e2da;color:#4a3e30;font-size:12px;text-decoration:none">📥 匯出 CSV</a>
      </div>
      {groups_html}
    </div>
    <script>
    function toggleGroup(gid){{
      const body=document.getElementById("body-"+gid);
      const chev=document.getElementById("chev-"+gid);
      const open=body.style.display==="none"||body.style.display==="";
      body.style.display=open?"block":"none";
      chev.textContent=open?"▼":"▶";
    }}
    function togS(sid){{
      const el=document.getElementById("sc-"+sid);
      const ar=document.getElementById("sa-"+sid);
      const open=el.style.display==="none"||el.style.display==="";
      el.style.display=open?"block":"none";
      ar.textContent=open?"▼":"▶";
    }}
    function togD(cid){{
      const el=document.getElementById("dd-"+cid);
      el.style.display=el.style.display==="block"?"none":"block";
    }}
    function batchClose(){{
      const cbs=document.querySelectorAll('.batch-cb:checked');
      if(!cbs.length){{alert('請先勾選客戶');return;}}
      if(!confirm('確定要批次結案 '+cbs.length+' 位客戶嗎？'))return;
      const ids=[...cbs].map(c=>c.value);
      fetch('/report/batch-close',{{method:'POST',headers:{{'Content-Type':'application/json'}},body:JSON.stringify({{case_ids:ids}})}})
      .then(r=>r.json()).then(d=>{{alert(d.message);location.reload();}});
    }}
    </script>
    <div style="position:fixed;bottom:20px;right:20px;z-index:999">
      <button onclick="batchClose()" class="btn btn-primary" style="box-shadow:0 4px 12px rgba(0,0,0,.15);font-size:13px;padding:10px 18px">批次結案</button>
    </div>
    </body></html>"""


def _build_timeline(case_id: str) -> str:
    conn = get_conn(); cur = conn.cursor()
    cur.execute("SELECT message_text, from_group_id, created_at FROM case_logs WHERE case_id=? ORDER BY created_at DESC LIMIT 20", (case_id,))
    logs = cur.fetchall(); conn.close()
    if not logs:
        return '<div style="font-size:12px;color:#999">無操作紀錄</div>'
    lines = []
    for log in logs:
        dt = (log["created_at"] or "")[:16].replace("T", " ")
        gname = get_group_name(log["from_group_id"])
        txt = (log["message_text"] or "")[:60]
        lines.append(f'<div style="padding:4px 0;border-bottom:1px solid #f0ebe4;font-size:11px"><span style="color:#999">{h(dt)}</span> <span style="color:#8b7355">{h(gname)}</span> {h(txt)}</div>')
    return "".join(lines)


@app.get("/search", response_class=HTMLResponse)
def search_page(request: Request, q: str = "", grp: str = "", date_from: str = "", date_to: str = ""):
    from fastapi.responses import RedirectResponse
    role = check_auth(request)
    if not role: return RedirectResponse("/login")

    conn2 = get_conn(); cur2 = conn2.cursor()
    cur2.execute("SELECT group_id, group_name FROM groups WHERE group_type='SALES_GROUP' AND is_active=1 ORDER BY group_name")
    all_groups2 = cur2.fetchall(); conn2.close()
    grp_opts2 = "<option value=''>全部群組</option>" + "".join(f'<option value="{h(g["group_id"])}" {"selected" if grp==g["group_id"] else ""}>{h(g["group_name"])}</option>' for g in all_groups2)
    results_html = ""
    if q or grp or date_from or date_to:
        conn = get_conn(); cur = conn.cursor()
        sql2 = "SELECT * FROM customers WHERE 1=1"; params2 = []
        if q: sql2 += " AND (customer_name LIKE ? OR id_no LIKE ?)"; params2 += [f"%{q}%", f"%{q}%"]
        if grp: sql2 += " AND source_group_id=?"; params2.append(grp)
        if date_from: sql2 += " AND DATE(created_at) >= ?"; params2.append(date_from)
        if date_to: sql2 += " AND DATE(created_at) <= ?"; params2.append(date_to)
        sql2 += " ORDER BY status ASC, updated_at DESC"
        cur.execute(sql2, params2)
        rows = cur.fetchall()
        conn.close()
        if not rows:
            results_html = '<div style="color:#9ca3af;padding:20px 0;text-align:center">找不到符合條件的客戶</div>'
        for row in rows:
            route_data = parse_route_json(row["route_plan"] or "")
            order, idx = route_data.get("order",[]), route_data.get("current_index",0)
            history = route_data.get("history",[])
            if row["status"] == "PENDING":
                badge = '<span class="badge b-doc">待處理</span>'
            elif row["status"] != "ACTIVE":
                badge = '<span class="badge b-close">已結案</span>'
            else:
                badge = get_badge(row)
            co = row["current_company"] or row["company"] or ""
            amt = row["approved_amount"] or ""
            disb = row["disbursement_date"] or ""

            route_html = ""
            if order:
                current_co = order[idx] if idx < len(order) else "（已完成）"
                next_co = order[idx+1] if idx+1 < len(order) else ""
                route_html += f'<div class="route-line">目前送件：<b>{h(current_co)}</b> （第{idx+1}/{len(order)}家）</div>'
                if next_co: route_html += f'<div class="route-line">下一家：{h(next_co)}</div>'
                if history:
                    route_html += '<div class="route-line" style="flex-wrap:wrap;gap:4px">歷程：'
                    def _fmt_history(hi):
                        co = h(hi.get("company",""))
                        st = h(hi.get("status",""))
                        amt = hi.get("amount") or ""
                        return f'{co}({st}{amt})' if amt else f'{co}({st})'
                    route_html += " → ".join(_fmt_history(hi) for hi in history[-5:])
                    route_html += '</div>'

            last = (row["last_update"] or "").splitlines()
            last_short = last[-1].strip()[:80] if last else ""
            amount_line = f'核准金額：{amt}' if amt else ""
            disb_line = f'撥款日期：{disb}' if disb else ""
            if role == "admin":
                tl = _build_timeline(row["case_id"])
                timeline_html = '<div onclick="var el=this.nextElementSibling;el.style.display=el.style.display===\'none\'?\'block\':\'none\'" style="cursor:pointer;font-size:12px;color:#8b7355;font-weight:600">▶ 操作歷程</div><div style="display:none;margin-top:6px;max-height:200px;overflow-y:auto">' + tl + '</div>'
            else:
                timeline_html = ""

            results_html += f'''<div class="search-result">
              <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:10px">
                <div style="display:flex;align-items:center;gap:8px">
                  <div style="font-size:16px;font-weight:600">{h(row["customer_name"])}</div>
                  {badge}
                </div>
                <div style="display:flex;align-items:center;gap:8px">
                  <div style="font-size:11px;color:#9ca3af">{h(get_group_name(row["source_group_id"]))}</div>
                  <a href="/edit-pending?case_id={h(row['case_id'])}" style="font-size:11px;color:#0369a1;text-decoration:none">✏️ 編輯</a>
                </div>
              </div>
              <div style="display:grid;grid-template-columns:1fr 1fr;gap:6px;font-size:12px;color:#6b7280;margin-bottom:10px">
                {"<div>身分證：" + h(row["id_no"]) + "</div>" if row["id_no"] else ""}
                {"<div>公司：" + h(co) + "</div>" if co else ""}
                {"<div>" + h(amount_line) + "</div>" if amount_line else ""}
                {"<div>" + h(disb_line) + "</div>" if disb_line else ""}
                <div>建立：{h(row["created_at"][:10]) if row["created_at"] else ""}</div>
                <div>更新：{h(row["updated_at"][:10]) if row["updated_at"] else ""}</div>
              </div>
              {route_html}
              {"<div style=\'margin-top:8px;padding:8px;background:#f9fafb;border-radius:6px;font-size:12px;color:#374151\'>" + h(last_short) + "</div>" if last_short else ""}
              <div style="margin-top:8px">
                {timeline_html}
              </div>
            </div>'''

    return f"""<!DOCTYPE html><html><head>{PAGE_CSS}<title>查詢客戶</title></head><body>
    {make_topnav(role, "search")}
    <div class="page">
      <form method="get" action="/search">
        <div style="background:#faf7f4;border:1px solid #ddd5ca;border-radius:10px;padding:14px 16px;margin-bottom:14px;display:flex;gap:10px;flex-wrap:wrap;align-items:flex-end;">
          <div><div style="font-size:12px;font-weight:600;color:#5a4e40;margin-bottom:3px">姓名／身分證</div><input class="input" name="q" value="{h(q)}" placeholder="搜尋..." autofocus style="width:150px"></div>
          <div><div style="font-size:12px;font-weight:600;color:#5a4e40;margin-bottom:3px">群組</div><select name="grp" class="input" style="width:110px">{grp_opts2}</select></div>
          <div><div style="font-size:12px;font-weight:600;color:#5a4e40;margin-bottom:3px">日期從</div><input type="date" name="date_from" value="{h(date_from)}" class="input" style="width:140px"></div>
          <div><div style="font-size:12px;font-weight:600;color:#5a4e40;margin-bottom:3px">日期至</div><input type="date" name="date_to" value="{h(date_to)}" class="input" style="width:140px"></div>
          <div style="display:flex;gap:6px;align-items:flex-end">
            <button type="submit" class="btn btn-primary">🔍 搜尋</button>
            <a href="/search" class="btn" style="background:#e8e2da;color:#4a3e30;">清除</a>
          </div>
        </div>
      </form>
      {results_html}
    </div></body></html>"""



def apply_adminb_rules(row: dict) -> dict:
    result = {}
    # 年資
    yr = str(row.get("company_years","") or "0").replace("年","").strip()
    try:
        y = float(yr.split()[0]) if yr else 0
        if y < 1:
            result["company_years_display"] = f"客戶填：{yr}年 → 填入：1年"
            result["company_years_val"] = "1"
            result["company_years_adj"] = True
        else:
            result["company_years_display"] = f"填入：{yr}年"
            result["company_years_val"] = yr
            result["company_years_adj"] = False
    except Exception:
        result["company_years_display"] = f"填入：1年（原值：{yr}）"
        result["company_years_val"] = "1"
        result["company_years_adj"] = True
    # 月薪
    sal = str(row.get("company_salary","") or "0")
    try:
        sv = float(sal.replace("萬","").replace("元","").replace(",","").strip())
        if "萬" in sal: sv *= 10000
        if sv < 35000:
            result["salary_display"] = f"客戶填：{sal} → 填入：3.5萬"
            result["salary_val"] = "3.5"
            result["salary_adj"] = True
        else:
            result["salary_display"] = f"填入：{sal}"
            result["salary_val"] = sal
            result["salary_adj"] = False
    except Exception:
        result["salary_display"] = f"填入：3.5萬（原值：{sal}）"
        result["salary_val"] = "3.5"
        result["salary_adj"] = True
    # 居住時間
    ly = str(row.get("live_years","") or "0").replace("年","").strip()
    try:
        lyv = float(ly) if ly else 0
        if lyv < 5:
            result["live_years_display"] = f"客戶填：{ly}年 → 填入：5年"
            result["live_years_val"] = "5"
            result["live_years_adj"] = True
        else:
            result["live_years_display"] = f"填入：{ly}年"
            result["live_years_val"] = ly
            result["live_years_adj"] = False
    except Exception:
        result["live_years_display"] = f"填入：5年（原值：{ly}）"
        result["live_years_val"] = "5"
        result["live_years_adj"] = True
    # 居住狀況
    ls = str(row.get("live_status","") or "")
    rent_kw = ["租","宿舍"]
    own_kw = ["自有","本人名下","配偶名下"]
    warnings = []
    if any(k in ls for k in rent_kw):
        result["live_status_display"] = f"客戶填：{ls} → 填入：親屬（父母或親友）"
        result["live_status_val"] = "親屬"
        result["live_status_adj"] = True
    elif any(k in ls for k in own_kw):
        result["live_status_display"] = f"填入：{ls}"
        result["live_status_val"] = ls
        result["live_status_adj"] = False
        warnings.append("⚠️ 居住狀況「自有」請確認無私設！")
    else:
        result["live_status_display"] = f"填入：{ls}"
        result["live_status_val"] = ls
        result["live_status_adj"] = False
    # 學歷
    edu = str(row.get("education","") or "")
    low_edu = ["國中","國小","小學","其他"]
    if any(k in edu for k in low_edu):
        result["edu_display"] = f"客戶填：{edu} → 填入：高中/職"
        result["edu_val"] = "高中/職"
        result["edu_adj"] = True
    else:
        result["edu_display"] = f"填入：{edu}"
        result["edu_val"] = edu
        result["edu_adj"] = False
    result["warnings"] = warnings
    return result


@app.get("/adminb", response_class=HTMLResponse)
def adminb_page(request: Request, case_id: str = "", saved: str = ""):
    from fastapi.responses import RedirectResponse
    role = check_auth(request)
    if not role: return RedirectResponse("/login")
    if role not in ("admin","adminB"): return RedirectResponse("/report")

    conn = get_conn(); cur = conn.cursor()
    cur.execute("SELECT case_id, customer_name, id_no, source_group_id, created_at, status FROM customers WHERE status IN ('ACTIVE','PENDING') ORDER BY updated_at DESC LIMIT 200")
    all_customers = cur.fetchall()
    customer = None
    if case_id:
        cur.execute("SELECT * FROM customers WHERE case_id=?", (case_id,))
        row = cur.fetchone()
        if row: customer = dict(row)
    conn.close()

    cust_opts = "<option value=''>— 選擇客戶 —</option>"
    for cu in all_customers:
        sel = "selected" if case_id == cu["case_id"] else ""
        gname = get_group_name(cu["source_group_id"])
        tag = "📋表單" if cu["status"]=="PENDING" else "📊日報"
        cust_opts += f'<option value="{h(cu["case_id"])}" data-gid="{h(cu["source_group_id"])}" {sel}>[{tag}] {h(cu["customer_name"])} {h(cu["id_no"] or "")}（{h(gname)}）</option>'

    ADMINB_CSS = """<style>
body{background:#ece8e2;font-family:'Microsoft JhengHei','PingFang TC',sans-serif;font-size:14px;color:#2c2820;}
.ab-card{background:#faf7f4;border:1px solid #ddd5ca;border-radius:10px;padding:16px;margin-bottom:12px;}
.ab-sec{font-size:13px;font-weight:700;color:#4a3e30;margin-bottom:12px;padding-bottom:8px;border-bottom:1px solid #ece8e2;}
.ab-block{border-radius:7px;padding:12px 14px;margin-bottom:10px;}
.ab-lbl{font-size:11px;font-weight:600;color:#5a4e40;margin-bottom:4px;}
.ab-inp{width:100%;padding:7px 10px;border:1px solid #ddd5ca;border-radius:6px;font-size:13px;background:#fff;box-sizing:border-box;color:#1a1208;font-family:inherit;}
.ab-sel{width:100%;padding:7px 10px;border:1px solid #ddd5ca;border-radius:6px;font-size:13px;background:#fff;color:#1a1208;font-family:inherit;}
.ab-g2{display:grid;grid-template-columns:1fr 1fr;gap:10px;}
.ab-g3{display:grid;grid-template-columns:1fr 1fr 1fr;gap:8px;}
.ab-plan-grid{display:grid;grid-template-columns:repeat(3,1fr);gap:8px;margin-bottom:10px;}
.ab-plan{display:flex;align-items:center;gap:8px;padding:10px 12px;background:#fff;border:1px solid #ddd5ca;border-radius:8px;cursor:pointer;}
.ab-plan:has(input:checked){border:2px solid #6a5e4e;background:#f5f0eb;}
.ab-plan input{width:16px;height:16px;accent-color:#6a5e4e;flex-shrink:0;}
.ab-plan-name{font-size:13px;font-weight:600;color:#2c2820;}
.ab-plan-sub{font-size:11px;color:#6a5e4e;}
.rule-item{border-radius:6px;padding:8px 12px;font-size:13px;margin-bottom:6px;}
.rule-adj{background:#fef9c3;color:#854d0e;}
.rule-ok{background:#f0fdf4;color:#166534;}
.btn-save{background:#0369a1;color:#fff;border:none;padding:9px 20px;border-radius:8px;font-size:13px;cursor:pointer;font-weight:600;font-family:inherit;}
.btn-dl{background:#6a5e4e;color:#fff;border:none;padding:9px 16px;border-radius:8px;font-size:13px;cursor:pointer;font-weight:600;font-family:inherit;text-decoration:none;display:inline-block;}
.btn-qm{background:#4e7055;color:#fff;border:none;padding:9px 16px;border-radius:8px;font-size:13px;cursor:pointer;font-weight:600;font-family:inherit;text-decoration:none;display:inline-block;}
.ab-card{overflow-x:auto}
</style>"""

    # 群組選項
    conn2 = get_conn(); cur2 = conn2.cursor()
    cur2.execute("SELECT group_id, group_name FROM groups WHERE group_type='SALES_GROUP' AND is_active=1 ORDER BY group_name")
    ab_groups = cur2.fetchall(); conn2.close()
    grp_filter_opts = '<option value="">全部群組</option>' + "".join(
        f'<option value="{h(g["group_id"])}">{h(g["group_name"])}</option>' for g in ab_groups)

    if not customer:
        # 預先產生 JSON，避免 f-string 大括號衝突
        cust_json = json.dumps([{"id":cu["case_id"],"name":cu["customer_name"],"idno":cu["id_no"] or "","gid":cu["source_group_id"],"gname":get_group_name(cu["source_group_id"])} for cu in all_customers], ensure_ascii=False)
        return """<!DOCTYPE html><html><head>""" + PAGE_CSS + ADMINB_CSS + """<title>行政B作業</title></head><body>
    """ + make_topnav(role,"adminb") + """
    <div class="page">
      <div class="ab-card">
        <div class="ab-sec">選擇客戶</div>
        <div style="display:flex;gap:8px;margin-bottom:10px;flex-wrap:wrap">
          <input type="text" id="ab-search" class="ab-inp" placeholder="輸入姓名或身分證搜尋..." autofocus style="flex:1;min-width:150px">
          <select id="ab-grp" class="ab-sel" style="width:130px">""" + grp_filter_opts + """</select>
          <button onclick="doSearch()" class="btn-save" style="padding:7px 16px">🔍 搜尋</button>
        </div>
        <div id="ab-results" style="max-height:400px;overflow-y:auto"></div>
      </div>
    </div>
    <script>
    const allCusts=""" + cust_json + """;
    document.getElementById('ab-search').addEventListener('keydown',function(e){if(e.key==='Enter')doSearch()});
    function doSearch(){
      const q=document.getElementById('ab-search').value.trim().toLowerCase();
      const g=document.getElementById('ab-grp').value;
      const box=document.getElementById('ab-results');
      if(!q&&!g){box.innerHTML='<div style="color:#999;font-size:13px;padding:12px">請輸入姓名或身分證搜尋</div>';return;}
      const matches=allCusts.filter(c=>{
        const matchQ=!q||c.name.toLowerCase().includes(q)||(c.idno&&c.idno.toLowerCase().includes(q));
        const matchG=!g||c.gid===g;
        return matchQ&&matchG;
      }).slice(0,30);
      if(!matches.length){box.innerHTML='<div style="color:#999;font-size:13px;padding:12px">找不到符合的客戶</div>';return;}
      box.innerHTML=matches.map(c=>'<div onclick="location.href=\\'/adminb?case_id='+c.id+'\\'" style="padding:10px 14px;border-bottom:1px solid #ece8e2;cursor:pointer;display:flex;justify-content:space-between;align-items:center" onmouseover="this.style.background=\\'#f5f0eb\\'" onmouseout="this.style.background=\\'\\'"><div><div style="font-size:14px;font-weight:600">'+c.name+'</div><div style="font-size:12px;color:#6a5e4e">'+(c.idno||'-')+'</div></div><div style="font-size:11px;color:#999">'+c.gname+'</div></div>').join('');
    }
    </script>
    </body></html>"""

    rules = apply_adminb_rules(customer)
    name = customer.get("customer_name","")
    id_no = customer.get("id_no","")
    phone = customer.get("phone","") or "-"
    gname = get_group_name(customer.get("source_group_id",""))
    created = (customer.get("created_at","") or "")[:10]
    co = customer.get("company_name_detail","") or customer.get("company","") or "-"
    salary = customer.get("company_salary","") or "-"
    labor = customer.get("eval_labor_ins","") or "-"
    sel_plans = customer.get("adminb_selected_plans","") or ""

    def rule_item(label, display, adj):
        cls = "rule-adj" if adj else "rule-ok"
        icon = "→" if adj else "✓"
        return f'<div class="rule-item {cls}"><span style="font-size:11px;font-weight:700;">{h(label)}</span>　{icon} {h(display)}</div>'

    saved_html = '<div style="background:#dcfce7;border:1px solid #86efac;border-radius:8px;padding:12px 16px;margin-bottom:12px;font-size:14px;font-weight:600;color:#166534;">✅ 資料已儲存完成</div>' if saved else ""

    rules_html = ""
    rules_html += rule_item("年資", rules["company_years_display"], rules["company_years_adj"])
    rules_html += rule_item("月薪", rules["salary_display"], rules["salary_adj"])
    rules_html += rule_item("居住時間", rules["live_years_display"], rules["live_years_adj"])
    rules_html += rule_item("居住狀況", rules["live_status_display"], rules["live_status_adj"])
    rules_html += rule_item("學歷", rules["edu_display"], rules["edu_adj"])

    warns_html = "".join(
        f'<div style="background:#fef2f2;border:1px solid #fca5a5;border-radius:7px;padding:10px 14px;font-size:13px;font-weight:700;color:#991b1b;margin-top:8px;">{h(w)}</div>'
        for w in rules["warnings"]
    )

    return f"""<!DOCTYPE html><html><head>{PAGE_CSS}{ADMINB_CSS}<title>行政B - {h(name)}</title></head><body>
    {make_topnav(role,"adminb")}
    <div class="page">
    <div style="margin-bottom:12px;">
      <input id="custSearch" class="ab-inp" placeholder="🔍 輸入姓名/群組快速篩選客戶..." style="margin-bottom:6px;">
      <select id="custSelect" class="ab-inp" onchange="if(this.value)location.href='/adminb?case_id='+this.value">{cust_opts}</select>
    </div>
    <div class="ab-card" id="custCard" style="position:sticky;top:0;z-index:20;">
      <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:12px;">
        <div><div style="font-size:20px;font-weight:700;color:#1a1208;">{h(name)}</div>
          <div style="font-size:13px;color:#6a5e4e;">{h(id_no)}　{h(gname)}　{h(created)}</div></div>
        <div style="display:flex;gap:8px;align-items:center">
          <a href="/edit-pending?case_id={h(case_id)}" style="font-size:12px;color:#0369a1;text-decoration:none;padding:4px 12px;border:1px solid #93c5fd;border-radius:20px">✏️ 編輯</a>
          <span style="background:#e0f2fe;color:#0369a1;font-size:12px;padding:4px 12px;border-radius:20px;font-weight:600;">ACTIVE</span>
        </div>
      </div>
      <div style="display:grid;grid-template-columns:repeat(4,1fr);gap:8px;">
        <div><div style="font-size:11px;font-weight:600;color:#6a5e4e;">電話</div><div style="font-size:13px;color:#1a1208;font-weight:500;">{h(phone)}</div></div>
        <div><div style="font-size:11px;font-weight:600;color:#6a5e4e;">公司</div><div style="font-size:13px;color:#1a1208;font-weight:500;">{h(co)}</div></div>
        <div><div style="font-size:11px;font-weight:600;color:#6a5e4e;">月薪</div><div style="font-size:13px;color:#1a1208;font-weight:500;">{h(salary)}</div></div>
        <div><div style="font-size:11px;font-weight:600;color:#6a5e4e;">勞保</div><div style="font-size:13px;color:#1a1208;font-weight:500;">{h(labor)}</div></div>
      </div>
    </div>
    {saved_html}
    <div class="ab-card">
      <div class="ab-sec">確認資料（系統自動調整）</div>
      {rules_html}{warns_html}
    </div>
    <form method="post" action="/adminb/save">
    <input type="hidden" name="case_id" value="{h(case_id)}">
    <div class="ab-card">
      <div class="ab-sec">判別方案（勾選要送的方案）</div>
      <div style="font-size:11px;font-weight:700;color:#6a5e4e;margin-bottom:8px;">Excel 方案</div>
      <div class="ab-plan-grid">
        {"".join(f'<label class="ab-plan"><input type="checkbox" name="plans" value="{v}"><div><div class="ab-plan-name">{n}</div><div class="ab-plan-sub">{s}</div></div></label>' for v,n,s in [
          ("亞太商品","亞太商品","熊速貸 12萬 30期"),("亞太機車15萬","亞太機車15萬","機車動擔 15萬 36期"),
          ("亞太工會機車","亞太工會機車","工會機車動擔 15萬"),("亞太機車25萬","亞太機車25萬","機車動擔 25萬 48期"),
          ("和裕機車","和裕機車","維力機車專 15萬 24期"),("和裕商品","和裕商品","維力商品專 12萬 24期"),
          ("21機車12萬","21機車12萬","12萬 24期"),("21機車25萬","21機車25萬","25萬 48期"),
          ("21商品","21商品","12萬 24期"),("第一","第一","30萬 24期"),("貸就補","貸就補","10萬 24期"),
        ])}
        <label class="ab-plan" style="background:#4e7055;border-color:#3a5040;"><input type="checkbox" name="plans" value="喬美" style="accent-color:#fff;"><div><div class="ab-plan-name" style="color:#fff;">喬美</div><div class="ab-plan-sub" style="color:#c8e6ca;">PDF電子簽名</div></div></label>
      </div>
      <div style="font-size:11px;font-weight:700;color:#6a5e4e;margin-bottom:8px;">填寫表方案</div>
      <div class="ab-plan-grid">
        {"".join(f'<label class="ab-plan"><input type="checkbox" name="plans" value="{v}"><div><div class="ab-plan-name">{n}</div><div class="ab-plan-sub">{s}</div></div></label>' for v,n,s in [
          ("麻吉機車","麻吉機車","10萬 24期"),("麻吉手機","麻吉手機","10萬 24期"),
          ("手機分期","手機分期","填寫表"),("分貝機車","分貝機車","改裝填寫表"),
          ("分貝汽車","分貝汽車","改裝填寫表"),("21汽車","21汽車","貸款100% 利率16%"),
        ])}
      </div>
    </div>
    <div class="ab-card">
      <div class="ab-sec">補充資料</div>
      <div class="ab-block" data-plans="亞太商品,亞太機車15萬,亞太工會機車,亞太機車25萬" style="background:#e0f2fe;">
        <div style="font-size:12px;font-weight:700;color:#0369a1;margin-bottom:10px;">亞太（商品／機車／工會）</div>
        <div class="ab-g3" style="margin-bottom:10px;">
          <div><div class="ab-lbl">資金用途</div><select name="at_fund" class="ab-sel"><option value="">請選擇</option>{"".join(f'<option value="{o}" {"selected" if customer.get("adminb_fund_use","")==o else ""}>{o}</option>' for o in ["I-1教育費","I-2醫藥費","I-3出國旅遊","I-4創業","II-1購買交通工具","II-2購買手機","II-3購買3C產品","III-1交友","III-2健身&醫美","III-3美容課程","IV-1個人理財投資(含不動產、裝修、理財商品)","V-1生活周轉金","V-2整合負債(償還銀行/融資等)"])}</select></div>
          <div><div class="ab-lbl">行業類別</div><select name="at_industry" class="ab-sel"><option value="">請選擇</option>{"".join(f"<option>{o}</option>" for o in ["餐飲與服務業","製造業","建築與營造","軍警與公教","科技與資訊","運輸與物流","金融與保險業","批發與零售業","醫療與教育","農林漁牧業","自由職業","其他"])}</select></div>
          <div><div class="ab-lbl">職務</div><select name="at_role" class="ab-sel"><option value="">請選擇</option>{"".join(f"<option>{o}</option>" for o in ["行政與內勤","勞力與現場","銷售與業務","財務與專業","技術與工程","教學與醫護","管理與經營","自營與自由"])}</select></div>
        </div>
        <div style="font-size:11px;font-weight:700;color:#0369a1;margin-bottom:8px;">機車額外資料</div>
        <div class="ab-g3">
          <div><div class="ab-lbl">廠牌</div><input name="at_brand" class="ab-inp" placeholder="光陽機車" value="{h(customer.get('adminb_brand','') or '')}"></div>
          <div><div class="ab-lbl">牌照號碼</div><input name="at_plate" class="ab-inp" placeholder="MMX-0286" value="{h(customer.get('vehicle_plate','') or '')}"></div>
          <div><div class="ab-lbl">車輛型式</div><input name="at_vtype" class="ab-inp" placeholder="SE22BJ" value="{h(customer.get('adminb_vehicle_type','') or '')}"></div>
          <div><div class="ab-lbl">引擎號碼</div><input name="at_engine" class="ab-inp" placeholder="SE22BJ-105579" value="{h(customer.get('adminb_engine_no','') or '')}"></div>
          <div><div class="ab-lbl">排氣量</div><input name="at_disp" class="ab-inp" placeholder="111" value="{h(customer.get('adminb_displacement','') or '')}"></div>
          <div><div class="ab-lbl">顏色</div><input name="at_color" class="ab-inp" placeholder="綠" value="{h(customer.get('adminb_color','') or '')}"></div>
          <div><div class="ab-lbl">車身號碼</div><input name="at_body" class="ab-inp" placeholder="RFGBK..." value="{h(customer.get('adminb_body_no','') or '')}"></div>
        </div>
      </div>
      <div class="ab-block" data-plans="和裕機車,和裕商品" style="background:#f0fdf4;">
        <div style="font-size:12px;font-weight:700;color:#166534;margin-bottom:10px;">和裕（機車／商品）</div>
        <div class="ab-g2" style="margin-bottom:10px;">
          <div><div class="ab-lbl">行業類別</div><select name="hr_industry" class="ab-sel"><option value="">請選擇</option>{"".join(f'<option {"selected" if customer.get("adminb_hr_industry","")==o else ""}>{o}</option>' for o in ["服務業","餐飲業","科技業","軍人","運輸業","倉儲業","金融業","製造業","營造業","電商網拍業","農狩林牧業","礦業","漁業","證券期貨業","保險業","不動產業","公教人員","水電燃氣業","通信業","社團個人服務","其它"])}</select></div>
          <div><div class="ab-lbl">照會時間</div><input name="hr_contact" class="ab-inp" placeholder="平日下午2-5點" value="{h(customer.get('adminb_contact_time','') or '')}"></div>
        </div>
        <div style="font-size:11px;font-weight:700;color:#166534;margin-bottom:8px;">撥款資料</div>
        <div class="ab-g2" style="margin-bottom:10px;">
          <div><div class="ab-lbl">銀行名稱</div><input name="hr_bank" class="ab-inp" placeholder="台灣銀行" value="{h(customer.get('adminb_bank','') or '')}"></div>
          <div><div class="ab-lbl">分行</div><input name="hr_branch" class="ab-inp" placeholder="苗栗分行" value="{h(customer.get('adminb_branch','') or '')}"></div>
        </div>
        <div style="font-size:11px;font-weight:700;color:#166534;margin-bottom:8px;">商品資料</div>
        <div class="ab-g2">
          <div><div class="ab-lbl">廠牌+商品</div><input name="hr_product" class="ab-inp" placeholder="三陽/安卓手機" value="{h(customer.get('adminb_product','') or '')}"></div>
          <div><div class="ab-lbl">型號或車號</div><input name="hr_model" class="ab-inp" placeholder="677-NSY/OPPO A77" value="{h(customer.get('adminb_model','') or '')}"></div>
        </div>
      </div>
      <div class="ab-block" data-plans="貸就補" style="background:#fef9c3;">
        <div style="font-size:12px;font-weight:700;color:#854d0e;margin-bottom:10px;">貸就補</div>
        <div class="ab-g2">
          <div><div class="ab-lbl">商品名稱</div><input name="lj_pname" class="ab-inp" placeholder="vivo" value="{h(customer.get('adminb_product_name','') or '')}"></div>
          <div><div class="ab-lbl">型號</div><input name="lj_pmodel" class="ab-inp" placeholder="vivo V60" value="{h(customer.get('adminb_product_model','') or '')}"></div>
        </div>
      </div>
      <div class="ab-block" data-plans="麻吉機車,麻吉手機" style="background:#fdf2f8;">
        <div style="font-size:12px;font-weight:700;color:#9d174d;margin-bottom:10px;">麻吉（機車／手機）</div>
        <div class="ab-g2">
          <div><div class="ab-lbl">商品廠牌</div><input name="mj_brand" class="ab-inp" placeholder="山葉 / iPhone" value="{h(customer.get('adminb_mj_brand','') or '')}"></div>
          <div><div class="ab-lbl">型號</div><input name="mj_model" class="ab-inp" placeholder="JQ5-063 / 16 Pro" value="{h(customer.get('adminb_mj_model','') or '')}"></div>
        </div>
      </div>
      <div class="ab-block" data-plans="21汽車" style="background:#fef2f2;">
        <div style="font-size:12px;font-weight:700;color:#991b1b;margin-bottom:10px;">21汽車（利率固定 16%）</div>
        <div class="ab-g2">
          <div><div class="ab-lbl">專案名稱</div><input name="car_proj" class="ab-inp" value="{h(customer.get('adminb_21car_project','') or '')}"></div>
          <div><div class="ab-lbl">車價（實際售價＝參考車價金額共用）</div><input name="car_price" class="ab-inp" value="{h(customer.get('adminb_21car_price','') or '')}"></div>
          <div><div class="ab-lbl">天書</div><input name="car_src" class="ab-inp" value="{h(customer.get('adminb_21car_ref_src','') or '')}"></div>
          <div><div class="ab-lbl">貸款金額</div><input name="car_amt" class="ab-inp" placeholder="50萬" value="{h(customer.get('adminb_21car_amount','') or '')}"></div>
          <div><div class="ab-lbl">期數</div><input name="car_period" class="ab-inp" placeholder="60" value="{h(customer.get('adminb_21car_period','') or '')}"></div>
          <div><div class="ab-lbl">月付金</div><input name="car_monthly" class="ab-inp" placeholder="9500" value="{h(customer.get('adminb_21car_monthly','') or '')}"></div>
          <div><div class="ab-lbl">資金用途</div><input name="car_fund" class="ab-inp" placeholder="購車" value="{h(customer.get('adminb_21car_fund','') or '')}"></div>
          <div><div class="ab-lbl">是否有信用卡</div><select name="car_hascc" class="ab-sel"><option value="">請選</option><option value="有" {"selected" if (customer.get('adminb_21car_hascc','') or '')=='有' else ''}>有</option><option value="無" {"selected" if (customer.get('adminb_21car_hascc','') or '')=='無' else ''}>無</option></select></div>
        </div>
      </div>
      <div class="ab-block" data-plans="喬美" style="background:#ede9fe;">
        <div style="font-size:12px;font-weight:700;color:#5b21b6;margin-bottom:10px;">喬美（PDF電子簽名）</div>
        <div class="ab-g2" style="margin-bottom:10px;">
          <div><div class="ab-lbl">手機型號</div><input name="qm_model" class="ab-inp" placeholder="iPhone 16 Pro Max" value="{h(customer.get('product_model','') or '')}"></div>
          <div><div class="ab-lbl">IMEI</div><input name="qm_imei" class="ab-inp" placeholder="356194482654922" value="{h(customer.get('product_imei','') or '')}"></div>
        </div>
        <div style="background:#fff;border:1px dashed #5b21b6;border-radius:8px;padding:12px;margin:10px 0;">
          <div style="font-size:12px;font-weight:700;color:#5b21b6;margin-bottom:8px;">📝 電子簽名（一鍵簽名+下載 PDF）</div>
          <div style="font-size:11px;color:#6b5b8e;margin-bottom:8px;">說明：下方畫板簽一次，即可同時用於 第1頁「申請人正楷簽名」+ 第2頁「立約定書人」</div>
          <canvas id="qmSignPad" width="500" height="150" style="border:2px solid #5b21b6;background:#fff;cursor:crosshair;display:block;border-radius:6px;width:100%;max-width:500px;"></canvas>
          <div style="margin-top:8px;display:flex;gap:8px;flex-wrap:wrap;">
            <button type="button" onclick="qmClear()" style="padding:6px 14px;background:#888;color:#fff;border:none;border-radius:6px;cursor:pointer;font-size:13px;">🗑 清除</button>
            <button type="button" onclick="qmSignAndDownload()" style="padding:6px 14px;background:#e91e63;color:#fff;border:none;border-radius:6px;cursor:pointer;font-size:13px;font-weight:700;">✍️ 簽名並下載喬美 PDF</button>
          </div>
        </div>
        <div style="font-size:11px;font-weight:700;color:#5b21b6;margin-bottom:8px;">信用資料</div>
        <div class="ab-g3">
          <div><div class="ab-lbl">發卡銀行</div><input name="qm_cbank" class="ab-inp" placeholder="星展銀行" value="{h(customer.get('adminb_credit_bank','') or '')}"></div>
          <div><div class="ab-lbl">卡號</div><input name="qm_cno" class="ab-inp" placeholder="6480-6295-1099-819" value="{h(customer.get('adminb_credit_no','') or '')}"></div>
          <div><div class="ab-lbl">有效日期（年/月）</div><div style="display:flex;gap:6px;"><input name="qm_exp_y" class="ab-inp" placeholder="年" style="width:50%;" value="{h((customer.get('adminb_credit_exp','') or '').split('/')[0] if '/' in (customer.get('adminb_credit_exp','') or '') else '')}"><input name="qm_exp_m" class="ab-inp" placeholder="月" style="width:50%;" value="{h((customer.get('adminb_credit_exp','') or '').split('/')[1] if '/' in (customer.get('adminb_credit_exp','') or '') else '')}"></div></div>
          <div><div class="ab-lbl">額度（萬）</div><input name="qm_climit" class="ab-inp" placeholder="10" value="{h(customer.get('adminb_credit_limit','') or '')}"></div>
          <div><div class="ab-lbl">近二月有無遲繳</div><select name="qm_clate" class="ab-sel"><option value="無">無</option><option value="有" {"selected" if (customer.get('adminb_credit_late','') or '')=='有' else ''}>有</option></select></div>
          <div><div class="ab-lbl">月付金</div><input name="qm_cpay" class="ab-inp" placeholder="3000" value="{h(customer.get('adminb_credit_pay','') or '')}"></div>
        </div>
      </div>
      <div class="ab-block" data-plans="分貝汽車,分貝機車,21汽車" style="background:#ece8e2;">
        <div style="font-size:12px;font-weight:700;color:#4a3e30;margin-bottom:8px;">照會時間（分貝汽車／分貝機車／21汽車）</div>
        <input name="contact_time" class="ab-inp" placeholder="平日下午2-5點" value="{h(customer.get('adminb_contact_time','') or '')}">
      </div>
    </div>
    <div class="ab-card">
      <div class="ab-sec">儲存與下載</div>
      <div style="display:flex;gap:10px;flex-wrap:wrap;align-items:center;">
        <button type="submit" class="btn-save">💾 儲存資料</button>
        <a href="/adminb/download-excel?case_id={h(case_id)}" class="btn-dl" onclick="return confirm('將根據勾選的方案下載 Excel/TXT，確定嗎？')">📥 下載EXCEL/TXT</a>
      </div>
      <div style="font-size:12px;color:#8a7a68;margin-top:8px;">請先儲存資料再下載；喬美 PDF 請使用「補充資料」區的「簽名並下載」按鈕</div>
    </div>

    <script>
    // 喬美簽名 canvas（補充資料區）
    var qmCanvas, qmCtx, qmDrawing = false;
    function qmInit() {{
      qmCanvas = document.getElementById('qmSignPad');
      if (!qmCanvas) return;
      qmCtx = qmCanvas.getContext('2d');
      qmCtx.lineWidth = 2.5;
      qmCtx.lineCap = 'round';
      qmCtx.strokeStyle = '#000';
      function getXY(e) {{
        var r = qmCanvas.getBoundingClientRect();
        var sx = qmCanvas.width / r.width;
        var sy = qmCanvas.height / r.height;
        if (e.touches) {{
          return [(e.touches[0].clientX - r.left) * sx, (e.touches[0].clientY - r.top) * sy];
        }}
        return [(e.clientX - r.left) * sx, (e.clientY - r.top) * sy];
      }}
      qmCanvas.onmousedown = function(e) {{ var p = getXY(e); qmDrawing = true; qmCtx.beginPath(); qmCtx.moveTo(p[0], p[1]); }};
      qmCanvas.onmousemove = function(e) {{ if(!qmDrawing) return; var p = getXY(e); qmCtx.lineTo(p[0], p[1]); qmCtx.stroke(); }};
      qmCanvas.onmouseup = function() {{ qmDrawing = false; }};
      qmCanvas.onmouseleave = function() {{ qmDrawing = false; }};
      qmCanvas.ontouchstart = function(e) {{ e.preventDefault(); var p = getXY(e); qmDrawing = true; qmCtx.beginPath(); qmCtx.moveTo(p[0], p[1]); }};
      qmCanvas.ontouchmove = function(e) {{ e.preventDefault(); if(!qmDrawing) return; var p = getXY(e); qmCtx.lineTo(p[0], p[1]); qmCtx.stroke(); }};
      qmCanvas.ontouchend = function() {{ qmDrawing = false; }};
    }}
    function qmClear() {{
      if (qmCtx) qmCtx.clearRect(0, 0, qmCanvas.width, qmCanvas.height);
    }}
    function qmSignAndDownload() {{
      if (!qmCanvas) {{ alert('簽名版未載入'); return; }}
      var imgData = qmCtx.getImageData(0, 0, qmCanvas.width, qmCanvas.height).data;
      var hasInk = false;
      for (var i = 3; i < imgData.length; i += 40) {{ if (imgData[i] > 0) {{ hasInk = true; break; }} }}
      if (!hasInk) {{ alert('請先簽名'); return; }}
      var dataUrl = qmCanvas.toDataURL('image/png');
      // 1. 先送出整個 adminB 表單（含型號/IMEI/信用卡等補充資料）
      var form = document.querySelector('form');
      var fd = new FormData(form);
      fetch('/adminb/save', {{ method: 'POST', body: fd }})
        .then(function() {{
          // 2. 存簽名
          return fetch('/adminb/save-signature', {{
            method: 'POST',
            headers: {{'Content-Type': 'application/json'}},
            body: JSON.stringify({{case_id: '{h(case_id)}', type: 'both', data: dataUrl}})
          }});
        }})
        .then(r => r.json()).then(d => {{
          if (d.ok) {{
            // 3. 下載 PDF
            window.location.href = '/adminb/download-qiaomei?case_id={h(case_id)}';
          }} else {{
            alert('簽名儲存失敗：' + (d.error || '未知錯誤'));
          }}
        }}).catch(function(e) {{ alert('錯誤：' + e); }});
    }}
    window.addEventListener('DOMContentLoaded', qmInit);
    </script>
    </form>
    </div>
    <script>
    var saved = "{sel_plans}".split(",").filter(Boolean);
    document.querySelectorAll('input[name="plans"]').forEach(function(cb){{
      if(saved.indexOf(cb.value)>=0) cb.checked = true;
    }});
    // === 補充資料區塊依勾選方案顯示/隱藏 ===
    function refreshBlocks() {{
      var checked = [];
      document.querySelectorAll('input[name="plans"]:checked').forEach(function(cb){{
        checked.push(cb.value);
      }});
      document.querySelectorAll('.ab-block[data-plans]').forEach(function(blk){{
        var mine = (blk.getAttribute('data-plans')||'').split(',').filter(Boolean);
        var hit = mine.some(function(p){{ return checked.indexOf(p) >= 0; }});
        blk.style.display = hit ? '' : 'none';
      }});
    }}
    document.querySelectorAll('input[name="plans"]').forEach(function(cb){{
      cb.addEventListener('change', refreshBlocks);
    }});
    refreshBlocks();
    // === 客戶下拉搜尋過濾 ===
    (function(){{
      var si = document.getElementById('custSearch');
      var se = document.getElementById('custSelect');
      if (!si || !se) return;
      var origOpts = Array.prototype.slice.call(se.options);
      si.addEventListener('input', function(){{
        var kw = si.value.trim().toLowerCase();
        se.innerHTML = '';
        origOpts.forEach(function(o){{
          if (!kw || o.text.toLowerCase().indexOf(kw) >= 0 || o.value === '') {{
            se.appendChild(o.cloneNode(true));
          }}
        }});
      }});
    }})();
    </script>
    </body></html>"""


@app.post("/adminb/save")
async def adminb_save(request: Request):
    from fastapi.responses import RedirectResponse
    role = check_auth(request)
    if not role or role not in ("admin","adminB"):
        return RedirectResponse("/login")
    form = await request.form()
    case_id = form.get("case_id","")
    plans = ",".join(form.getlist("plans"))
    ey = form.get("qm_exp_y","")
    em = form.get("qm_exp_m","")
    credit_exp = f"{ey}/{em}" if ey or em else ""
    conn = get_conn(); cur = conn.cursor()
    fields = {
        "adminb_selected_plans": plans,
        "adminb_industry": form.get("at_industry",""),
        "adminb_hr_industry": form.get("hr_industry",""),
        "adminb_brand": form.get("at_brand",""),
        "adminb_role": form.get("at_role",""),
        "adminb_contact_time": form.get("contact_time","") or form.get("hr_contact",""),
        "adminb_bank": form.get("hr_bank",""),
        "adminb_branch": form.get("hr_branch",""),
        "adminb_product": form.get("hr_product",""),
        "adminb_model": form.get("hr_model",""),
        "adminb_product_name": form.get("lj_pname",""),
        "adminb_product_model": form.get("lj_pmodel",""),
        "adminb_21car_project": form.get("car_proj",""),
        "adminb_21car_price": form.get("car_price",""),
        "adminb_21car_ref_src": form.get("car_src",""),
        "adminb_21car_ref_price": form.get("car_refp",""),
        "adminb_21car_rate": "16%",
        "adminb_21car_ref_price": form.get("car_price",""),
        "adminb_21car_amount": form.get("car_amt",""),
        "adminb_21car_period": form.get("car_period",""),
        "adminb_21car_monthly": form.get("car_monthly",""),
        "adminb_21car_fund": form.get("car_fund",""),
        "adminb_21car_hascc": form.get("car_hascc",""),
        "adminb_mj_brand": form.get("mj_brand",""),
        "adminb_mj_model": form.get("mj_model",""),
        "product_model": form.get("qm_model",""),
        "product_imei": form.get("qm_imei",""),
        "adminb_credit_bank": form.get("qm_cbank",""),
        "adminb_credit_no": form.get("qm_cno",""),
        "adminb_credit_exp": credit_exp,
        "adminb_credit_limit": form.get("qm_climit",""),
        "adminb_credit_late": form.get("qm_clate","無"),
        "adminb_credit_pay": form.get("qm_cpay",""),
        "adminb_vehicle_type": form.get("at_vtype",""),
        "adminb_engine_no": form.get("at_engine",""),
        "adminb_displacement": form.get("at_disp",""),
        "adminb_color": form.get("at_color",""),
        "adminb_body_no": form.get("at_body",""),
        "adminb_fund_use": form.get("at_fund",""),
        "vehicle_plate": form.get("at_plate",""),
    }
    # 套用「確認資料」自動調整規則，回寫到主要欄位
    cur.execute("SELECT * FROM customers WHERE case_id=?", (case_id,))
    row = cur.fetchone()
    if row:
        rules = apply_adminb_rules(dict(row))
        fields["company_years"] = rules.get("company_years_val", "")
        fields["company_salary"] = rules.get("salary_val", "")
        fields["live_years"] = rules.get("live_years_val", "")
        fields["live_status"] = rules.get("live_status_val", "")
        fields["education"] = rules.get("edu_val", "")
    set_clause = ", ".join(f"{k}=?" for k in fields)
    vals = list(fields.values()) + [now_iso(), case_id]
    cur.execute(f"UPDATE customers SET {set_clause}, updated_at=? WHERE case_id=?", vals)
    conn.commit(); conn.close()
    return RedirectResponse(f"/adminb?case_id={case_id}&saved=1", status_code=303)




@app.get("/edit-pending", response_class=HTMLResponse)
def edit_pending_get(request: Request, case_id: str = ""):
    from fastapi.responses import RedirectResponse
    role = check_auth(request)
    if not role: return RedirectResponse("/login")
    if not case_id: return RedirectResponse("/pending-customers")
    conn = get_conn(); cur = conn.cursor()
    cur.execute("SELECT * FROM customers WHERE case_id=?", (case_id,))
    row = cur.fetchone()
    if not row: conn.close(); return RedirectResponse("/pending-customers")
    r = dict(row)
    cur.execute("SELECT group_id, group_name FROM groups WHERE group_type='SALES_GROUP' AND is_active=1 ORDER BY group_name")
    all_groups = cur.fetchall()
    conn.close()
    def v(k): return r.get(k,"") or ""
    grp_opts = '<option value="">請選擇</option>' + "".join(f'<option value="{h(g["group_id"])}" {"selected" if v("source_group_id")==g["group_id"] else ""}>{h(g["group_name"])}</option>' for g in all_groups)
    cities = ["台北市","新北市","桃園市","台中市","台南市","高雄市","基隆市","新竹市","新竹縣","苗栗縣","彰化縣","南投縣","雲林縣","嘉義市","嘉義縣","屏東縣","宜蘭縣","花蓮縣","台東縣","澎湖縣","金門縣","連江縣"]
    def csel(nm,val): return f'<select name="{nm}" class="ep">' + "".join(f'<option {"selected" if o==val else ""}>{o}</option>' for o in cities) + "</select>"
    lsame = v("live_same_as_reg")=="1"
    import json as _json
    try:
        debt_data = _json.loads(v("debt_list")) if v("debt_list") else []
    except Exception:
        debt_data = []
    if debt_data:
        debt_rows_html = "".join(f'<div style="padding:6px 0;border-bottom:1px solid #ece8e2;display:grid;grid-template-columns:1fr 1fr 1fr 1fr;gap:4px"><span><b>商家:</b>{h(d.get("co",""))}</span><span><b>金額:</b>{h(d.get("lo",""))}</span><span><b>期數:</b>{h(d.get("pe",""))}期</span><span><b>月繳:</b>{h(d.get("mo",""))}</span><span><b>已繳:</b>{h(d.get("pa",""))}期</span><span><b>剩餘:</b>{h(d.get("re",""))}</span><span><b>日期:</b>{h(d.get("da",""))}</span><span><b>動保:</b>{h(d.get("dy",""))}</span></div>' for d in debt_data)
    else:
        debt_rows_html = '<div style="color:#8a7a68">無負債資料</div>'
    return f"""<!DOCTYPE html><html><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>編輯 {h(v("customer_name"))}</title>
{PAGE_CSS}
<style>
.page{{max-width:860px;margin:24px auto;padding:0 16px 40px;}}
.card{{background:#faf6f2;border-radius:10px;padding:18px;margin-bottom:14px;border:1px solid #ddd5ca;}}
.sec{{font-size:13px;font-weight:700;color:#5a4e40;margin-bottom:12px;padding-bottom:7px;border-bottom:1px solid #ddd5ca;}}
.g2{{display:grid;grid-template-columns:1fr 1fr;gap:12px;}}
.g3{{display:grid;grid-template-columns:1fr 1fr 1fr;gap:10px;}}
label{{display:block;font-size:12px;font-weight:600;color:#5a4e40;margin-bottom:4px;}}
.ep{{width:100%;padding:8px 10px;border:1px solid #c8bfb5;border-radius:6px;font-size:13px;font-family:inherit;background:#fff;box-sizing:border-box;}}
.btn-s{{background:#6a5e4e;color:#fff;border:none;padding:10px 28px;border-radius:8px;font-size:15px;font-weight:700;cursor:pointer;font-family:inherit;}}
.btn-b{{background:#e8e2da;color:#4a3e30;border:1px solid #c8bfb5;padding:10px 20px;border-radius:8px;font-size:14px;text-decoration:none;display:inline-block;}}
</style></head><body>
{make_topnav(role, "edit")}
<div class="page">
<div style="font-size:20px;font-weight:700;margin-bottom:18px;">{h(v("customer_name"))} 資料編輯</div>
<form method="post" action="/edit-pending">
<input type="hidden" name="case_id" value="{h(case_id)}">
<div class="card"><div class="sec">所屬群組</div>
  <div><label>群組</label><select name="grp" class="ep">{grp_opts}</select></div>
</div>
<div class="card"><div class="sec">基本資料</div><div class="g2">
  <div><label>姓名</label><input name="cname" class="ep" value="{h(v("customer_name"))}"></div>
  <div><label>身分證</label><input name="idno" class="ep" value="{h(v("id_no"))}" style="text-transform:uppercase"></div>
  <div><label>出生年月日</label><input name="birth" class="ep" value="{h(v("birth_date"))}"></div>
  <div><label>行動電話</label><input name="phone" class="ep" value="{h(v("phone"))}"></div>
  <div><label>Email</label><input name="email" class="ep" value="{h(v("email"))}"></div>
  <div><label>LINE ID</label><input name="line" class="ep" value="{h(v("line_id"))}"></div>
  <div><label>電信業者</label><select name="carrier" class="ep"><option value="">請選擇</option><option {"selected" if v("carrier")=="中華電信" else ""}>中華電信</option><option {"selected" if v("carrier")=="遠傳電信" else ""}>遠傳電信</option><option {"selected" if v("carrier")=="台灣大哥大" else ""}>台灣大哥大</option><option {"selected" if v("carrier")=="台灣之星" else ""}>台灣之星</option><option {"selected" if v("carrier")=="亞太電信" else ""}>亞太電信</option><option {"selected" if v("carrier")=="其他" else ""}>其他</option></select></div>
  <div><label>客戶FB</label><input name="fb" class="ep" value="{h(v("fb"))}"></div>
  <div><label>婚姻狀態</label><select name="marry" class="ep"><option value="">請選擇</option><option {"selected" if v("marriage")=="未婚" else ""}>未婚</option><option {"selected" if v("marriage")=="已婚" else ""}>已婚</option></select></div>
  <div><label>最高學歷</label><select name="edu" class="ep"><option value="">請選擇</option><option {"selected" if v("education")=="高中/職" else ""}>高中/職</option><option {"selected" if v("education")=="專科/大學" else ""}>專科/大學</option><option {"selected" if v("education")=="研究所以上" else ""}>研究所以上</option><option {"selected" if v("education")=="其他" else ""}>其他</option></select></div>
</div></div>
<div class="card"><div class="sec">身分證發證資料</div><div class="g3">
  <div><label>發證日期</label><input name="iddate" class="ep" value="{h(v("id_issue_date"))}"></div>
  <div><label>發證地</label><select name="idplace" class="ep"><option value="">請選擇</option>{"".join(f'<option {"selected" if v("id_issue_place")==p else ""}>{p}</option>' for p in ["北市","新北市","桃市","中市","南市","高市","基市","竹市","竹縣","苗縣","彰縣","投縣","雲縣","嘉市","嘉縣","屏縣","宜縣","花縣","東縣","澎縣","金門","連江"])}</select></div>
  <div><label>換補發類別</label><select name="idtype" class="ep"><option value="">請選擇</option><option {"selected" if v("id_issue_type")=="初發" else ""}>初發</option><option {"selected" if v("id_issue_type")=="補發" else ""}>補發</option><option {"selected" if v("id_issue_type")=="換發" else ""}>換發</option></select></div>
</div></div>
<div class="card"><div class="sec">地址資料</div>
  <div style="font-size:13px;font-weight:700;color:#4a3e30;margin-bottom:8px">戶籍地址</div>
  <div class="g3" style="margin-bottom:10px"><div><label>縣市</label>{csel("rcity",v("reg_city"))}</div><div><label>區/鄉鎮</label><input name="rdist" class="ep" value="{h(v("reg_district"))}"></div><div><label>詳細地址</label><input name="raddr" class="ep" value="{h(v("reg_address"))}"></div></div>
  <div style="margin-bottom:10px"><label>戶籍電話</label><input name="rphone" class="ep" value="{h(v("reg_phone"))}" style="max-width:200px"></div>
  <label style="display:flex;align-items:center;gap:8px;font-size:14px;color:#3a3020;margin-bottom:10px;cursor:pointer;font-weight:600">
    <input type="checkbox" id="sameck" name="sameck" {"checked" if lsame else ""} onchange="document.getElementById('lsec').style.display=this.checked?'none':'block'">住家地址與戶籍相同</label>
  <div id="lsec" style="{"display:none" if lsame else ""};margin-bottom:10px">
    <div class="g3"><div><label>縣市</label>{csel("lcity",v("live_city"))}</div><div><label>區/鄉鎮</label><input name="ldist" class="ep" value="{h(v("live_district"))}"></div><div><label>詳細地址</label><input name="laddr" class="ep" value="{h(v("live_address"))}"></div></div>
  </div>
  <div class="g3">
    <div><label>現住電話</label><input name="lphone" class="ep" value="{h(v("live_phone"))}"></div>
    <div><label>居住狀況</label><select name="lstatus" class="ep"><option {"selected" if v("live_status")=="自有" else ""}>自有</option><option {"selected" if v("live_status")=="配偶" else ""}>配偶</option><option {"selected" if v("live_status")=="父母" else ""}>父母</option><option {"selected" if v("live_status")=="親屬" else ""}>親屬</option><option {"selected" if v("live_status")=="租屋" else ""}>租屋</option><option {"selected" if v("live_status")=="宿舍" else ""}>宿舍</option></select></div>
    <div><label>居住時間</label><div style="display:flex;gap:6px;align-items:center"><input name="lyear" class="ep" value="{h(v("live_years"))}" style="width:60px"><span>年</span><input name="lmon" class="ep" value="{h(v("live_months"))}" style="width:60px"><span>月</span></div></div>
  </div>
</div>
<div class="card"><div class="sec">職業資料</div><div class="g2">
  <div style="grid-column:1/-1"><label>公司名稱</label><input name="cmpname" class="ep" value="{h(v("company_name_detail"))}"></div>
  <div><label>公司電話</label><div style="display:flex;gap:6px;align-items:center"><select name="carea" class="ep" style="width:82px"><option value="">區碼</option>{"".join(f'<option {"selected" if v("company_phone_area")==a else ""}>{a}</option>' for a in ["02","03","037","04","049","05","06","07","08","089"])}<option {"selected" if v("company_phone_area")=="mobile" else ""} value="mobile">手機</option></select><input name="cnum" class="ep" value="{h(v("company_phone_num"))}"><input name="cext" class="ep" value="{h(v("company_phone_ext"))}" placeholder="分機" style="width:68px"></div></div>
  <div><label>職稱</label><input name="crole" class="ep" value="{h(v("company_role"))}"></div>
  <div><label>年資</label><div style="display:flex;gap:6px;align-items:center"><input name="cyear" class="ep" value="{h(v("company_years"))}" style="width:60px"><span>年</span><input name="cmon" class="ep" value="{h(v("company_months"))}" style="width:60px"><span>月</span></div></div>
  <div><label>月薪</label><input name="csal" class="ep" value="{h(v("company_salary"))}"></div>
  <div style="grid-column:1/-1"><label>公司地址</label><div class="g3">{csel("ccity",v("company_city"))}<input name="cdist" class="ep" value="{h(v("company_district"))}"><input name="caddr" class="ep" value="{h(v("company_address"))}"></div></div>
</div></div>
<div class="card"><div class="sec">聯絡人</div><div class="g2" style="margin-bottom:12px">
  <div><label>聯絡人1</label><input name="c1name" class="ep" value="{h(v("contact1_name"))}"></div>
  <div><label>關係</label><input name="c1rel" class="ep" value="{h(v("contact1_relation"))}"></div>
  <div><label>電話</label><input name="c1tel" class="ep" value="{h(v("contact1_phone"))}"></div>
  <div><label>知情</label><select name="c1know" class="ep"><option {"selected" if v("contact1_known")=="可知情" else ""}>可知情</option><option {"selected" if v("contact1_known")=="保密" else ""}>保密</option><option {"selected" if v("contact1_known")=="無可知情" else ""}>無可知情</option></select></div>
</div><div class="g2">
  <div><label>聯絡人2</label><input name="c2name" class="ep" value="{h(v("contact2_name"))}"></div>
  <div><label>關係</label><input name="c2rel" class="ep" value="{h(v("contact2_relation"))}"></div>
  <div><label>電話</label><input name="c2tel" class="ep" value="{h(v("contact2_phone"))}"></div>
  <div><label>知情</label><select name="c2know" class="ep"><option {"selected" if v("contact2_known")=="可知情" else ""}>可知情</option><option {"selected" if v("contact2_known")=="保密" else ""}>保密</option><option {"selected" if v("contact2_known")=="無可知情" else ""}>無可知情</option></select></div>
</div></div>
<div class="card"><div class="sec">貸款諮詢事項</div><div class="g2">
  <div><label>資金需求</label><input name="efund" class="ep" value="{h(v("eval_fund_need"))}"></div>
  <div><label>近三月送件</label><select name="esent" class="ep"><option {"selected" if v("eval_sent_3m")=="否" else ""}>否</option><option {"selected" if v("eval_sent_3m")=="是" else ""}>是</option></select></div>
  <div><label>當鋪私設</label><select name="eprivate" class="ep"><option {"selected" if v("eval_alert")=="無" else ""}>無</option><option {"selected" if v("eval_alert")=="有" else ""}>有</option></select></div>
  <div><label>勞保狀態</label><select name="elabor" class="ep"><option {"selected" if v("eval_labor_ins")=="公司保" else ""}>公司保</option><option {"selected" if v("eval_labor_ins")=="工會保" else ""}>工會保</option><option {"selected" if v("eval_labor_ins")=="自行投保" else ""}>自行投保</option><option {"selected" if v("eval_labor_ins")=="無勞保" else ""}>無勞保</option></select></div>
  <div><label>有無薪轉</label><select name="esal" class="ep"><option {"selected" if v("eval_salary_transfer")=="有薪轉" else ""}>有薪轉</option><option {"selected" if v("eval_salary_transfer")=="無薪轉" else ""}>無薪轉</option></select></div>
  <div><label>有無證照</label><select name="elicense" class="ep"><option {"selected" if v("eval_license")=="無" else ""}>無</option><option {"selected" if v("eval_license")=="有" else ""}>有</option></select></div>
  <div><label>貸款遲繳</label><select name="elate" class="ep"><option {"selected" if v("eval_late")=="無" else ""}>無</option><option {"selected" if v("eval_late")=="有" else ""}>有</option></select></div>
  <div><label>遲繳天數</label><input name="elateday" class="ep" value="{h(v("eval_late_days"))}"></div>
  <div><label>罰單欠費 $</label><input name="efine" class="ep" value="{h(v("eval_fine"))}"></div>
  <div><label>燃料稅 $</label><input name="efuel" class="ep" value="{h(v("eval_fuel_tax"))}"></div>
  <div><label>名下信用卡</label><input name="ecard" class="ep" value="{h(v("eval_credit_card"))}"></div>
  <div><label>動產/不動產</label><input name="eprop" class="ep" value="{h(v("eval_property"))}"></div>
  <div><label>法學</label><input name="elaw" class="ep" value="{h(v("eval_law"))}"></div>
  <div style="grid-column:1/-1"><label>備註</label><textarea name="enote" class="ep" style="min-height:60px">{h(v("eval_note"))}</textarea></div>
</div></div>
<div class="card"><div class="sec">負債明細</div>
  <div id="ep-debt-list" style="font-size:13px;color:#4a3e30;line-height:2">
    {debt_rows_html}
  </div>
</div>
<div style="display:flex;gap:10px;margin-top:8px">
  <button type="submit" class="btn-s">💾 儲存變更</button>
  <a href="/pending-customers" class="btn-b">取消</a>
</div>
</form></div></body></html>"""


@app.post("/edit-pending")
async def edit_pending_post(request: Request):
    from fastapi.responses import RedirectResponse
    role = check_auth(request)
    if not role: return RedirectResponse("/login")
    form = await request.form()
    f = dict(form)
    case_id = f.get("case_id","")
    if not case_id: return RedirectResponse("/pending-customers")
    live_same = "1" if f.get("sameck") else "0"
    now = now_iso()
    conn = get_conn(); cur = conn.cursor()
    cur.execute("""UPDATE customers SET
        customer_name=?,id_no=?,birth_date=?,phone=?,email=?,line_id=?,
        carrier=?,fb=?,
        source_group_id=?,marriage=?,education=?,
        id_issue_date=?,id_issue_place=?,id_issue_type=?,
        reg_city=?,reg_district=?,reg_address=?,reg_phone=?,live_same_as_reg=?,
        live_city=?,live_district=?,live_address=?,live_phone=?,live_status=?,live_years=?,live_months=?,
        company_name_detail=?,company_phone_area=?,company_phone_num=?,company_phone_ext=?,
        company_role=?,company_years=?,company_months=?,company_salary=?,
        company_city=?,company_district=?,company_address=?,
        contact1_name=?,contact1_relation=?,contact1_phone=?,contact1_known=?,
        contact2_name=?,contact2_relation=?,contact2_phone=?,contact2_known=?,
        updated_at=? WHERE case_id=?""",
        (f.get("cname",""),f.get("idno","").upper(),f.get("birth",""),f.get("phone",""),f.get("email",""),f.get("line",""),
         f.get("carrier",""),f.get("fb",""),
         f.get("grp",""),f.get("marry",""),f.get("edu",""),
         f.get("iddate",""),f.get("idplace",""),f.get("idtype",""),
         f.get("rcity",""),f.get("rdist",""),f.get("raddr",""),f.get("rphone",""),live_same,
         f.get("lcity","") if live_same=="0" else f.get("rcity",""),
         f.get("ldist","") if live_same=="0" else f.get("rdist",""),
         f.get("laddr","") if live_same=="0" else f.get("raddr",""),
         f.get("lphone",""),f.get("lstatus",""),f.get("lyear",""),f.get("lmon",""),
         f.get("cmpname",""),f.get("carea",""),f.get("cnum",""),f.get("cext",""),
         f.get("crole",""),f.get("cyear",""),f.get("cmon",""),f.get("csal",""),
         f.get("ccity",""),f.get("cdist",""),f.get("caddr",""),
         f.get("c1name",""),f.get("c1rel",""),f.get("c1tel",""),f.get("c1know",""),
         f.get("c2name",""),f.get("c2rel",""),f.get("c2tel",""),f.get("c2know",""),
         now, case_id))
    # 更新諮詢事項
    cur.execute("""UPDATE customers SET
        eval_fund_need=?,eval_sent_3m=?,eval_alert=?,eval_labor_ins=?,eval_salary_transfer=?,
        eval_license=?,eval_late=?,eval_late_days=?,eval_fine=?,eval_fuel_tax=?,eval_credit_card=?,eval_property=?,eval_law=?,eval_note=?,
        updated_at=? WHERE case_id=?""",
        (f.get("efund",""),f.get("esent",""),f.get("eprivate",""),f.get("elabor",""),f.get("esal",""),
         f.get("elicense",""),f.get("elate",""),f.get("elateday",""),f.get("efine",""),f.get("efuel",""),f.get("ecard",""),f.get("eprop",""),f.get("elaw",""),f.get("enote",""),
         now, case_id))
    conn.commit(); conn.close()
    return RedirectResponse("/pending-customers", status_code=303)

# 計算客戶資料完整度（填寫欄位比例 0-100）
_COMPLETENESS_EXCLUDE = {
    "id", "case_id", "status", "created_at", "updated_at", "last_update",
    "source_group_id", "route_plan", "current_company", "report_section",
    "customer_name", "created_by_role",
}


def calc_completeness(row: dict) -> int:
    """計算客戶資料填寫率：非空欄位 / 總欄位 × 100"""
    fields = [k for k in row.keys() if k not in _COMPLETENESS_EXCLUDE]
    if not fields:
        return 0
    filled = sum(1 for k in fields if (row.get(k) not in (None, "", "0")))
    return int(round(filled * 100 / len(fields)))


@app.get("/pending-customers", response_class=HTMLResponse)
def pending_customers_page(request: Request, q: str = "", grp: str = "", date_from: str = "", date_to: str = "", page: int = 1):
    from fastapi.responses import RedirectResponse
    role = check_auth(request)
    if not role: return RedirectResponse("/login")
    PAGE_SIZE = 50
    if page < 1: page = 1
    conn = get_conn(); cur = conn.cursor()
    cur.execute("SELECT group_id, group_name FROM groups WHERE group_type='SALES_GROUP' AND is_active=1 ORDER BY group_name")
    all_groups = cur.fetchall()

    where = "WHERE status='PENDING'"
    params: list = []
    if q: where += " AND (customer_name LIKE ? OR id_no LIKE ?)"; params += [f"%{q}%", f"%{q}%"]
    if grp: where += " AND source_group_id=?"; params.append(grp)
    if date_from: where += " AND DATE(created_at) >= ?"; params.append(date_from)
    if date_to: where += " AND DATE(created_at) <= ?"; params.append(date_to)

    cur.execute(f"SELECT COUNT(*) AS c FROM customers {where}", params)
    total = cur.fetchone()["c"]
    total_pages = max(1, (total + PAGE_SIZE - 1) // PAGE_SIZE)
    if page > total_pages: page = total_pages
    offset = (page - 1) * PAGE_SIZE

    cur.execute(f"SELECT * FROM customers {where} ORDER BY created_at DESC LIMIT ? OFFSET ?", params + [PAGE_SIZE, offset])
    rows = [dict(r) for r in cur.fetchall()]
    conn.close()

    # 一次取所有群組名稱（修掉迴圈內 N+1）
    group_name_map = get_all_group_names()

    grp_opts = "<option value=''>全部群組</option>" + "".join(
        f'<option value="{h(g["group_id"])}" {"selected" if grp==g["group_id"] else ""}>{h(g["group_name"])}</option>'
        for g in all_groups
    )

    rows_html = ""
    for r in rows:
        name = r.get("customer_name", "") or ""
        id_no = r.get("id_no", "") or ""
        created = (r.get("created_at", "") or "")[:10]
        gname = group_name_map.get(r.get("source_group_id", ""), "未知群組")
        case_id = r.get("case_id", "")
        pct = calc_completeness(r)
        dot_cls = "dot-green" if pct >= 70 else ("dot-yellow" if pct >= 30 else "dot-red")
        rows_html += f'''<tr>
            <td style="padding:11px 12px;width:36px;"><input type="checkbox" class="case-chk" value="{h(case_id)}"></td>
            <td style="padding:11px 12px;color:#3a2e1c;font-size:13px;font-weight:600;white-space:nowrap;">{h(created)}</td>
            <td style="padding:11px 12px;font-weight:700;font-size:14px;white-space:nowrap;color:#0f0a04;"><span class="dot {dot_cls}" title="完整度 {pct}%"></span>{h(name)}</td>
            <td style="padding:11px 12px;color:#1a1208;font-family:monospace;font-weight:600;">{h(id_no)}</td>
            <td style="padding:11px 12px;color:#1a1208;font-weight:600;">{h(gname)}</td>
            <td style="padding:11px 12px;text-align:center;">
                <a href="/edit-pending?case_id={h(case_id)}" style="background:#3a2e1c;color:#fff;padding:5px 14px;border-radius:6px;font-size:12px;text-decoration:none;font-weight:700;">編輯</a>
            </td>
        </tr>'''
    empty = "" if rows else '<tr><td colspan="6" style="text-align:center;padding:30px;color:#4a3e30;font-weight:600;">目前沒有待確認客戶</td></tr>'

    # 建分頁連結（保留篩選參數）
    from urllib.parse import urlencode
    def page_url(p: int) -> str:
        qs = {"q": q, "grp": grp, "date_from": date_from, "date_to": date_to, "page": p}
        qs = {k: v for k, v in qs.items() if v not in ("", None)}
        return "/pending-customers?" + urlencode(qs)

    pager_html = ""
    if total_pages > 1:
        parts = []
        if page > 1:
            parts.append(f'<a href="{page_url(page-1)}">‹ 上一頁</a>')
        start = max(1, page - 3)
        end = min(total_pages, start + 6)
        start = max(1, end - 6)
        if start > 1:
            parts.append(f'<a href="{page_url(1)}">1</a>')
            if start > 2:
                parts.append('<span style="padding:6px 4px;color:#8a7a68;">…</span>')
        for p in range(start, end + 1):
            cls = ' class="current"' if p == page else ''
            parts.append(f'<a{cls} href="{page_url(p)}">{p}</a>')
        if end < total_pages:
            if end < total_pages - 1:
                parts.append('<span style="padding:6px 4px;color:#8a7a68;">…</span>')
            parts.append(f'<a href="{page_url(total_pages)}">{total_pages}</a>')
        if page < total_pages:
            parts.append(f'<a href="{page_url(page+1)}">下一頁 ›</a>')
        pager_html = '<div class="pager">' + "".join(parts) + '</div>'

    return f"""<!DOCTYPE html><html><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>客戶資料庫</title>
{PAGE_CSS}
<style>
body{{background:#e4ddcd;}}
.page{{max-width:1040px;margin:24px auto;padding:0 16px 40px;color:#1a1208;}}
.card{{background:#f2ecdd;border:1px solid #b8ad9c;border-radius:10px;overflow:hidden;box-shadow:0 1px 3px rgba(60,45,25,0.08);}}
table{{width:100%;border-collapse:collapse;}}
thead tr{{background:#d8ccb0;}}
thead th{{position:sticky;top:0;background:#d8ccb0;z-index:2;border-bottom:2px solid #a89c82;}}
th{{padding:11px 12px;text-align:left;font-size:13px;font-weight:800;color:#1a1208;}}
tbody td{{color:#1a1208;}}
tbody tr{{border-bottom:1px solid #d8ccb0;}}
tbody tr:hover{{background:#ddd5c4;}}
h2{{font-size:19px;font-weight:800;color:#0f0a04;margin-bottom:14px;}}
input,select{{padding:7px 10px;border:1px solid #8a7e68;border-radius:6px;font-size:13px;font-family:inherit;color:#1a1208;background:#f2ede0;}}
input[type=checkbox]{{padding:0;width:16px;height:16px;cursor:pointer;}}
.dot{{display:inline-block;width:10px;height:10px;border-radius:50%;margin-right:7px;vertical-align:middle;}}
.dot-red{{background:#b91c1c;}}
.dot-yellow{{background:#b45309;}}
.dot-green{{background:#15803d;}}
.pager{{text-align:center;padding:16px;}}
.pager a{{display:inline-block;padding:6px 12px;margin:0 2px;border:1px solid #8a7e68;border-radius:6px;color:#1a1208;text-decoration:none;font-size:13px;background:#f2ecdd;font-weight:600;}}
.pager a:hover{{background:#d8ccb0;}}
.pager a.current{{background:#3a2e1c;color:#fff;border-color:#3a2e1c;font-weight:800;}}
.btn-export{{background:#2f5339;color:#fff;border:none;padding:8px 16px;border-radius:6px;font-size:13px;cursor:pointer;font-weight:700;font-family:inherit;}}
.btn-export:disabled{{background:#8a8275;cursor:not-allowed;}}
.filter-box{{background:#f2ecdd !important;border:1px solid #b8ad9c !important;}}
.filter-box label-text{{color:#1a1208 !important;}}
</style></head><body>
{make_topnav(role, "pending")}
<div class="page">
  <h2>客戶資料庫 共 {total} 筆</h2>
  <form method="get" action="/pending-customers">
    <div style="background:#f2ecdd;border:1px solid #b8ad9c;border-radius:10px;padding:14px 16px;margin-bottom:14px;display:flex;gap:10px;flex-wrap:wrap;align-items:flex-end;box-shadow:0 1px 3px rgba(60,45,25,0.08);">
      <div><div style="font-size:13px;font-weight:700;color:#1a1208;margin-bottom:4px">姓名／身分證</div><input name="q" value="{h(q)}" placeholder="搜尋..." style="width:150px"></div>
      <div><div style="font-size:13px;font-weight:700;color:#1a1208;margin-bottom:4px">群組</div><select name="grp" style="width:110px">{grp_opts}</select></div>
      <div><div style="font-size:13px;font-weight:700;color:#1a1208;margin-bottom:4px">日期從</div><input type="date" name="date_from" value="{h(date_from)}" style="width:140px"></div>
      <div><div style="font-size:13px;font-weight:700;color:#1a1208;margin-bottom:4px">日期至</div><input type="date" name="date_to" value="{h(date_to)}" style="width:140px"></div>
      <div style="display:flex;gap:6px;align-items:flex-end">
        <button type="submit" style="background:#3a2e1c;color:#fff;border:none;padding:8px 18px;border-radius:6px;font-size:13px;cursor:pointer;font-weight:700;font-family:inherit">🔍 搜尋</button>
        <a href="/pending-customers" style="background:#d8ccb0;color:#1a1208;border:1px solid #8a7e68;padding:8px 14px;border-radius:6px;font-size:13px;text-decoration:none;font-weight:600;">清除</a>
      </div>
      <div style="margin-left:auto;display:flex;gap:8px;align-items:flex-end;">
        <button type="button" id="btnExport" class="btn-export" disabled onclick="exportPdf()">📄 匯出 PDF（<span id="selCount">0</span>）</button>
      </div>
    </div>
  </form>
  <div class="card"><table>
    <thead><tr>
      <th style="width:36px;"><input type="checkbox" id="chkAll" onclick="toggleAll(this)"></th>
      <th>日期</th><th>姓名</th><th>身分證</th><th>群組</th><th>操作</th>
    </tr></thead>
    <tbody>{rows_html}{empty}</tbody>
  </table></div>
  {pager_html}
</div>
<script>
function updateSelCount() {{
  var n = document.querySelectorAll('.case-chk:checked').length;
  document.getElementById('selCount').textContent = n;
  document.getElementById('btnExport').disabled = (n === 0);
}}
function toggleAll(src) {{
  document.querySelectorAll('.case-chk').forEach(function(c) {{ c.checked = src.checked; }});
  updateSelCount();
}}
document.querySelectorAll('.case-chk').forEach(function(c) {{
  c.addEventListener('change', updateSelCount);
}});
function exportPdf() {{
  var ids = [];
  document.querySelectorAll('.case-chk:checked').forEach(function(c) {{ ids.push(c.value); }});
  if (ids.length === 0) return;
  window.open('/customer-pdf-batch?ids=' + encodeURIComponent(ids.join(',')));
}}
</script>
</body></html>"""


@app.get("/history", response_class=HTMLResponse)
def history_page(request: Request, group: str = "", month: str = "", q: str = ""):
    from fastapi.responses import RedirectResponse
    role = check_auth(request)
    if not role: return RedirectResponse("/login")
    auth_group = get_auth_group_id(request)

    conn = get_conn(); cur = conn.cursor()
    cur.execute("SELECT * FROM groups WHERE group_type='SALES_GROUP' AND is_active=1 ORDER BY group_name")
    groups = cur.fetchall()

    # 業務只看自己群組
    if auth_group and not group:
        group = auth_group

    query = "SELECT * FROM customers WHERE status IN ('CLOSED','PENALTY','ABANDONED','REJECTED')"
    params = []
    if group:
        query += " AND source_group_id=?"
        params.append(group)
    if month:
        query += " AND updated_at LIKE ?"
        params.append(month + "%")
    if q:
        query += " AND (customer_name LIKE ? OR id_no LIKE ?)"
        params.extend([f"%{q}%", f"%{q}%"])
    query += " ORDER BY updated_at DESC LIMIT 300"
    cur.execute(query, params)
    rows = cur.fetchall()
    conn.close()

    # 月份選項（最近6個月）
    from datetime import datetime, timedelta
    month_opts = "<option value=\'\'>全部月份</option>"
    for i in range(6):
        dt = datetime.now().replace(day=1) - timedelta(days=i*28)
        ym = dt.strftime("%Y-%m")
        label = dt.strftime("%Y年%m月")
        sel = "selected" if month == ym else ""
        month_opts += f'<option value="{ym}" {sel}>{label}</option>'

    group_opts = "<option value=\'\'>全部群組</option>"
    for g in groups:
        sel = "selected" if g["group_id"]==group else ""
        group_opts += f'<option value="{h(g["group_id"])}" {sel}>{h(g["group_name"])}</option>'

    def get_close_badge(row):
        st = row.get("status","") or ""
        reason = row.get("close_reason","") or ""
        amt = row.get("approved_amount","") or ""
        penalty = row.get("penalty_amount","") or ""
        if st == "PENALTY":
            if "核准" in reason:
                label = "核准不撥款"
                bg, color = "#fef9c3", "#854d0e"
            else:
                label = "辦理中放棄"
                bg, color = "#fef9c3", "#854d0e"
            return f'<span style="background:{bg};color:{color};font-size:12px;padding:3px 10px;border-radius:20px;font-weight:600">{label}・違約金${h(penalty)}</span>'
        if st == "ABANDONED":
            if "核准" in reason:
                label = "核准不撥款"
            else:
                label = "辦理中放棄"
            return f'<span style="background:#ece8e2;color:#4a3e30;font-size:12px;padding:3px 10px;border-radius:20px;font-weight:600">{label}・未收違約金</span>'
        if st == "CLOSED" and amt:
            return '<span style="background:#dcfce7;color:#166534;font-size:12px;padding:3px 10px;border-radius:20px;font-weight:600">已撥款結案</span>'
        if st == "CLOSED":
            return '<span style="background:#f0fdf4;color:#166534;font-size:12px;padding:3px 10px;border-radius:20px;font-weight:600">結案</span>'
        if st == "REJECTED":
            return '<span style="background:#fee2e2;color:#991b1b;font-size:12px;padding:3px 10px;border-radius:20px;font-weight:600">全數婉拒</span>'
        return '<span style="background:#ece8e2;color:#4a3e30;font-size:12px;padding:3px 10px;border-radius:20px;font-weight:600">結案</span>'

    rows_html = ""
    if not rows:
        rows_html = '<div style="color:#6a5e4e;padding:24px;text-align:center;font-size:14px">沒有結案紀錄</div>'
    for row in rows:
        row = dict(row)
        badge = get_close_badge(row)
        co = row.get("current_company","") or row.get("company","") or ""
        amt = row.get("approved_amount","") or ""
        gname = get_group_name(row["source_group_id"])
        updated = row["updated_at"][:10].replace("-","/") if row["updated_at"] else ""
        # 多家核准
        route_data2 = parse_route_json(row.get("route_plan","") or "")
        all_approved = [rh for rh in route_data2.get("history",[]) if rh.get("status") in ("核准","待撥款","撥款") and rh.get("amount")]
        if len(all_approved) > 1:
            detail = "多家核准：" + " + ".join((rh.get("company") or "") + (rh.get("amount") or "") for rh in all_approved)
        elif all_approved:
            detail = all_approved[0].get("company","") + " 核准" + all_approved[0].get("amount","")
        else:
            detail = co + (" 核准" + amt if amt else "")
        rows_html += (
            '<div style="display:grid;grid-template-columns:1.4fr 1.2fr 1fr 0.7fr 0.8fr;gap:12px;align-items:center;padding:12px 16px;border-bottom:1px solid #ece8e2">'
            + '<div style="font-size:14px;font-weight:600;color:#1a1208">' + h(row["customer_name"]) + '</div>'
            + '<div style="font-size:13px;color:#3a3530">' + h(detail) + '</div>'
            + '<div>' + badge + '</div>'
            + '<div style="font-size:13px;color:#4a3e30">' + h(gname) + '</div>'
            + '<div style="font-size:12px;color:#6a5e4e">' + h(updated) + '</div>'
            + '</div>'
        )

    HIST_CSS = """
    <style>
    body{background:#ece8e2;color:#2c2820;font-family:'Microsoft JhengHei','PingFang TC',sans-serif}
    .input{background:#faf7f4;border:1px solid #ddd5ca;color:#2c2820;border-radius:7px;padding:8px 12px;font-size:14px;font-family:inherit}
    </style>"""

    return f"""<!DOCTYPE html><html><head>{PAGE_CSS}{HIST_CSS}<title>歷史紀錄</title></head><body>
    {make_topnav(role, "history")}
    <div class="page">
      <form method="get" action="/history" style="margin-bottom:16px">
        <div style="display:flex;gap:10px;flex-wrap:wrap;align-items:center">
          <select class="input" name="group" onchange="this.form.submit()" style="min-width:120px">{group_opts}</select>
          <select class="input" name="month" onchange="this.form.submit()" style="min-width:140px">{month_opts}</select>
          <input class="input" name="q" value="{h(q)}" placeholder="搜尋客戶姓名..." style="min-width:160px">
          <button type="submit" class="btn btn-primary" style="white-space:nowrap">搜尋</button>
          <span style="font-size:13px;color:#6a5e4e;margin-left:auto">共 {len(rows)} 筆</span>
        </div>
      </form>
      <div style="background:#faf7f4;border:1px solid #ddd5ca;border-radius:10px;overflow:hidden">
        <div style="display:grid;grid-template-columns:1.4fr 1.2fr 1fr 0.7fr 0.8fr;gap:12px;padding:9px 16px;background:#ece8e2;border-bottom:1px solid #ddd5ca">
          <div style="font-size:12px;font-weight:700;color:#4a3e30">客戶姓名</div>
          <div style="font-size:12px;font-weight:700;color:#4a3e30">方案/金額</div>
          <div style="font-size:12px;font-weight:700;color:#4a3e30">結案原因</div>
          <div style="font-size:12px;font-weight:700;color:#4a3e30">群組</div>
          <div style="font-size:12px;font-weight:700;color:#4a3e30">日期</div>
        </div>
        {rows_html if rows_html else '<div style="color:#6a5e4e;padding:24px;text-align:center;font-size:14px">沒有結案紀錄</div>'}
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
        sales_opts += f'<option value="{h(sg["group_id"])}">{h(sg["group_name"])}</option>'

    type_labels = {"SALES_GROUP":"業務群","ADMIN_GROUP":"行政群","A_GROUP":"A群"}
    rows_html = ""
    for r in rows:
        linked_name = ""
        if r["linked_sales_group_id"]:
            conn2 = get_conn(); cur2 = conn2.cursor()
            cur2.execute("SELECT group_name FROM groups WHERE group_id=?", (r["linked_sales_group_id"],))
            ln = cur2.fetchone(); conn2.close()
            linked_name = ln["group_name"] if ln else r["linked_sales_group_id"]
        edit_btn = f'''<button onclick="openEdit(\'{h(r["group_id"])}\',\'{h(r["group_name"])}\',{1 if r["is_active"] else 0})" class="btn" style="font-size:11px;padding:3px 10px">✏️ 編輯</button>'''
        rows_html += f'''<tr style="border-bottom:1px solid #f3f4f6">
          <td style="padding:8px 12px;font-weight:500">{h(r["group_name"])}</td>
          <td style="padding:8px 12px;color:#6b7280">{h(type_labels.get(r["group_type"],r["group_type"]))}</td>
          <td style="padding:8px 12px;color:#6b7280">{h(linked_name) or "-"}</td>
          <td style="padding:8px 12px">{"✅" if r["is_active"] else "❌"}</td>
          <td style="padding:8px 12px;font-size:11px;color:#9ca3af;font-family:monospace">{h(r["group_id"])}</td>
          <td style="padding:8px 12px">{edit_btn}</td>
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
              <th style="padding:8px 12px;text-align:left;font-size:12px;color:#6b7280"></th>
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

    <div class="modal-bg" id="edit-modal" onclick="if(event.target===this)this.classList.remove('show')">
      <div class="modal">
        <div class="modal-title">編輯群組 <span class="modal-close" onclick="document.getElementById('edit-modal').classList.remove('show')">×</span></div>
        <input type="hidden" id="e_gid">
        <div class="form-row"><label>群組 ID（不可修改）</label><input class="input" id="e_gid_show" disabled style="background:#f9fafb;color:#9ca3af"></div>
        <div class="form-row"><label>群組名稱</label><input class="input" id="e_gname" placeholder="輸入新名稱"></div>
        <div class="form-row"><label>狀態</label>
          <select class="input" id="e_active">
            <option value="1">啟用</option>
            <option value="0">停用</option>
          </select>
        </div>
        <button class="btn btn-primary" onclick="saveEdit()" style="width:100%;justify-content:center">儲存</button>
        <div id="edit-result" style="margin-top:10px;font-size:12px"></div>
      </div>
    </div>

    <script>
    function openEdit(gid, gname, isActive){{
      document.getElementById('e_gid').value = gid;
      document.getElementById('e_gid_show').value = gid;
      document.getElementById('e_gname').value = gname;
      document.getElementById('e_active').value = isActive;
      document.getElementById('edit-result').innerText = '';
      document.getElementById('edit-modal').classList.add('show');
    }}
    async function saveEdit(){{
      const gid = document.getElementById('e_gid').value;
      const gname = document.getElementById('e_gname').value.trim();
      const isActive = document.getElementById('e_active').value;
      const res = document.getElementById('edit-result');
      if(!gname){{res.className='result-err';res.innerText='名稱不可空白';return}}
      const r = await fetch('/admin/update_group', {{
        method:'POST',
        headers:{{'Content-Type':'application/json'}},
        body:JSON.stringify({{group_id:gid, group_name:gname, is_active:parseInt(isActive)}})
      }});
      const data = await r.json();
      res.className = data.status==='ok'?'result-ok':'result-err';
      res.innerText = data.message;
      if(data.status==='ok') setTimeout(()=>location.reload(),1200);
    }}
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
# 更新群組 API
# =========================
@app.post("/admin/update_group")
async def update_group(request: Request):
    role = check_auth(request)
    if role != "admin":
        return JSONResponse({"status":"error","message":"無權限"}, status_code=403)
    data = await request.json()
    gid = data.get("group_id","").strip()
    gname = data.get("group_name","").strip()
    is_active = data.get("is_active", 1)
    if not gid or not gname:
        return JSONResponse({"status":"error","message":"群組ID和名稱必填"})
    try:
        conn = get_conn(); cur = conn.cursor()
        cur.execute("UPDATE groups SET group_name=?, is_active=? WHERE group_id=?",
            (gname, is_active, gid))
        conn.commit(); conn.close()
        return JSONResponse({"status":"ok","message":f"已更新：{gname}"})
    except Exception as e:
        return JSONResponse({"status":"error","message":str(e)})


@app.get("/admin/locked-accounts")
async def get_locked_accounts(request: Request):
    role = check_auth(request)
    if role != "admin":
        return JSONResponse({"ok": False, "locked": []})
    conn = get_conn(); cur = conn.cursor()
    cur.execute("SELECT identifier, attempts, locked_until FROM login_attempts WHERE locked_until > ? ORDER BY locked_until DESC",
        (now_iso(),))
    rows = cur.fetchall(); conn.close()
    locked = [{"identifier": r["identifier"], "attempts": r["attempts"],
               "locked_until": r["locked_until"][11:16] if r["locked_until"] else ""} for r in rows]
    return JSONResponse({"ok": True, "locked": locked})


@app.post("/admin/unlock-account")
async def unlock_account(request: Request):
    role = check_auth(request)
    if role != "admin":
        return JSONResponse({"ok": False, "message": "無權限"})
    data = await request.json()
    identifier = data.get("identifier","").strip()
    if not identifier:
        return JSONResponse({"ok": False, "message": "identifier 必填"})
    conn = get_conn(); cur = conn.cursor()
    cur.execute("DELETE FROM login_attempts WHERE identifier=?", (identifier,))
    conn.commit(); conn.close()
    return JSONResponse({"ok": True, "message": f"已解鎖：{identifier}"})


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
        # Bug 15: 先檢查長度，避免空字串/格式異常時 ValueError 被靜默吞掉
        created_roc = ""
        if len(created) >= 10:
            try:
                y = int(created[:4]) - 1911
                created_roc = f"{y}/{created[5:7]}/{created[8:10]}"
            except (ValueError, IndexError) as e:
                print(f"[date_parse] failed for {created!r}: {e}")
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
    cur.execute("SELECT * FROM groups WHERE is_active=1 AND group_type='SALES_GROUP' ORDER BY group_name")
    groups = cur.fetchall(); conn.close()
    group_opts = '<option value="">請選擇</option>' + "".join(f'<option value="{h(g["group_id"])}">{h(g["group_name"])}</option>' for g in groups)
    grp_map_js = json.dumps({g["group_id"]: g["group_name"] for g in groups}, ensure_ascii=False)
    pdf_js = _PDF_EXPORT_JS.replace("__GRP_MAP__", grp_map_js)
    HTML_PAGE = '<!DOCTYPE html>\n<html lang="zh-TW">\n<head>\n<meta charset="UTF-8">\n<meta name="viewport" content="width=device-width, initial-scale=1.0">\n<title>新增客戶資料</title>\n<style>\n* { box-sizing: border-box; margin: 0; padding: 0; }\nbody { font-family: \'Microsoft JhengHei\', \'PingFang TC\', sans-serif; background: #ece8e2; color: #2c2820; font-size: 14px; }\n.topnav { background: #3a3530; padding: 0 20px; display: flex; align-items: center; height: 50px; gap: 4px; }\n.topnav a { color: #c8bfb5; text-decoration: none; padding: 7px 14px; border-radius: 6px; font-size: 14px; }\n.topnav a.active { background: #7c6f5e; color: #fff; }\n.topnav a:hover { background: #4a4540; color: #fff; }\n.page { max-width: 820px; margin: 24px auto; padding: 0 16px 40px; }\n.page-header { display: flex; justify-content: space-between; align-items: center; margin-bottom: 20px; padding-bottom: 12px; border-bottom: 2px solid #8a7a68; }\nh2 { font-size: 20px; font-weight: 700; color: #2c2820; }\n.card { background: #faf6f2; border-radius: 10px; padding: 18px; margin-bottom: 14px; box-shadow: 0 1px 4px rgba(0,0,0,0.08); border: 1px solid #ddd5ca; }\n.section-title { font-size: 13px; font-weight: 700; color: #5a4e40; margin-bottom: 14px; padding-bottom: 7px; border-bottom: 1px solid #ddd5ca; letter-spacing: 0.5px; }\n.grid2 { display: grid; grid-template-columns: 1fr 1fr; gap: 12px; }\n.grid3 { display: grid; grid-template-columns: 1fr 1fr 1fr; gap: 12px; }\n.grid4 { display: grid; grid-template-columns: 1fr 1fr 1fr 1fr; gap: 12px; }\n.full { grid-column: 1 / -1; }\nlabel { display: block; font-size: 12px; color: #5a4e40; margin-bottom: 5px; font-weight: 600; }\n.req { color: #b84a35; }\ninput, select, textarea {\n  width: 100%; padding: 8px 11px; border: 1px solid #c8bfb5;\n  border-radius: 6px; font-size: 14px; font-family: inherit;\n  background: #fff; color: #2c2820; transition: border 0.15s;\n}\ninput:focus, select:focus, textarea:focus { outline: none; border-color: #7c6f5e; background: #fffcf9; }\ninput::placeholder, textarea::placeholder { color: #c8bfb5; }\ntextarea { resize: vertical; min-height: 65px; }\n.hint { font-size: 12px; color: #8a7a68; margin-top: 4px; }\n.auto-val { font-size: 15px; color: #b84a35; font-weight: 700; margin-top: 5px; }\n.vehicle-card { background: #f0ebe4; border: 1px solid #ccc5ba; border-radius: 8px; padding: 14px; margin-bottom: 10px; position: relative; }\n.vehicle-card .card-label { font-size: 13px; font-weight: 700; color: #5a4e40; margin-bottom: 10px; }\n.remove-btn { position: absolute; top: 12px; right: 12px; background: #f5ddd8; color: #b84a35; border: none; border-radius: 4px; padding: 4px 12px; font-size: 12px; cursor: pointer; font-weight: 600; }\n.debt-header { display: grid; grid-template-columns: 1.2fr 0.8fr 0.6fr 0.8fr 0.6fr 1fr 0.9fr 0.8fr 30px; gap: 5px; font-size: 12px; color: #5a4e40; font-weight: 600; margin-bottom: 6px; }\n.debt-row { display: grid; grid-template-columns: 1.2fr 0.8fr 0.6fr 0.8fr 0.6fr 1fr 0.9fr 0.8fr 30px; gap: 5px; margin-bottom: 7px; align-items: center; }\n.debt-row input { font-size: 13px; padding: 6px 8px; }\n.debt-remain { font-size: 13px; font-weight: 700; color: #b84a35; }\n.debt-del { background: #f5ddd8; color: #b84a35; border: none; border-radius: 4px; width: 30px; height: 32px; cursor: pointer; font-size: 15px; }\n.add-btn { background: #e8e2da; color: #4a3e30; border: 1px dashed #a09080; border-radius: 6px; padding: 8px 16px; font-size: 13px; cursor: pointer; margin-top: 8px; font-weight: 600; }\n.add-btn:hover { background: #ddd5ca; }\n.btn-row { display: flex; gap: 10px; margin-top: 22px; flex-wrap: wrap; }\n.btn { padding: 11px 26px; border-radius: 8px; font-size: 15px; font-weight: 700; cursor: pointer; border: none; font-family: inherit; }\n.btn-primary { background: #6a5e4e; color: #fff; }\n.btn-primary:hover { background: #5a4e40; }\n.btn-export { background: #4e7055; color: #fff; }\n.btn-export:hover { background: #3e5e45; }\n.btn-cancel { background: #ddd5ca; color: #4a3e30; }\n.btn-cancel:hover { background: #ccc5ba; }\n.ig { display: flex; gap: 6px; align-items: center; }\n.ig span { font-size: 14px; white-space: nowrap; line-height: 36px; color: #4a3e30; font-weight: 600; }\n</style>\n</head>\n<body>\n<div class="topnav">\n  <a href="/">&#128202; 日報</a>\n  <a href="/pending-customers">&#128203; 客戶資料庫</a>\n  <a href="/new-customer" class="active">&#10133; 新增客戶</a>\n</div>\n<div class="page">\n  <div class="page-header">\n    <h2>新增客戶資料</h2>\n\n  </div>\n  <form id="cf" method="post" action="/new-customer">\n    <div class="card">\n      <div class="section-title">所屬群組</div>\n      <div>\n        <div><label>群組</label><select name="grp"><option>B群</option><option>C群</option></select></div>\n      </div>\n    </div>\n    <div class="card">\n      <div class="section-title">基本資料</div>\n      <div class="grid2">\n        <div><label>客戶姓名 <span class="req">*</span></label><input name="cname" placeholder="王小明" required></div>\n        <div><label>身分證字號 <span class="req">*</span></label><input name="idno" placeholder="A123456789" style="text-transform:uppercase" required pattern="[A-Za-z][12]\\d{8}" title="格式：英文字母+1或2+8位數字"></div>\n        <div><label>出生年月日 <span class="req">*</span></label><input name="birth" placeholder="086/12/15"><div class="hint">民國年：086/12/15</div></div>\n        <div><label>行動電話 <span class="req">*</span></label><input name="phone" placeholder="0912-345678" pattern="09\\d{8}" title="格式：09開頭共10位數字"></div>\n        <div><label>電信業者</label><select name="carrier"><option value="">--- 請選擇 ---</option><option>中華電信</option><option>遠傳電信</option><option>台灣大哥大</option><option>台灣之星</option><option>亞太電信</option><option>其他</option></select></div>\n        <div><label>Email</label><input name="email" placeholder="example@gmail.com"></div>\n        <div><label>LINE ID</label><input name="line"></div>\n        <div><label>客戶FB</label><input name="fb" placeholder="Facebook名稱"></div>\n        <div><label>婚姻狀態</label><select name="marry"><option value="">--- 請選擇 ---</option><option>未婚</option><option>已婚</option></select></div>\n        <div><label>最高學歷</label><select name="edu"><option value="">--- 請選擇 ---</option><option>高中/職</option><option>專科/大學</option><option>研究所以上</option><option>其他</option></select></div>\n      </div>\n    </div>\n    <div class="card">\n      <div class="section-title">身分證發證資料</div>\n      <div class="grid3">\n        <div><label>發證日期 <span class="req">*</span></label><input name="iddate" placeholder="114/03/05"><div class="hint">民國年：114/03/05</div></div>\n        <div><label>發證地 <span class="req">*</span></label><select name="idplace"><option value="">--- 請選擇 ---</option><option>北市</option><option>新北市</option><option>桃市</option><option>中市</option><option>南市</option><option>高市</option><option>基市</option><option>竹市</option><option>竹縣</option><option>苗縣</option><option>彰縣</option><option>投縣</option><option>雲縣</option><option>嘉市</option><option>嘉縣</option><option>屏縣</option><option>宜縣</option><option>花縣</option><option>東縣</option><option>澎縣</option><option>金門</option><option>連江</option></select></div>\n        <div><label>換補發類別 <span class="req">*</span></label><select name="idtype"><option value="">--- 請選擇 ---</option><option>初發</option><option>補發</option><option>換發</option></select></div>\n      </div>\n    </div>\n    <div class="card">\n      <div class="section-title">地址資料</div>\n      <div style="font-size:13px;font-weight:700;color:#4a3e30;margin-bottom:10px">戶籍地址</div>\n      <div class="grid3" style="margin-bottom:10px">\n        <div><label>縣市 <span class="req">*</span></label><select name="rcity"><option value="">請選擇</option><option>台北市</option><option>新北市</option><option>桃園市</option><option>台中市</option><option>台南市</option><option>高雄市</option><option>基隆市</option><option>新竹市</option><option>新竹縣</option><option>苗栗縣</option><option>彰化縣</option><option>南投縣</option><option>雲林縣</option><option>嘉義市</option><option>嘉義縣</option><option>屏東縣</option><option>宜蘭縣</option><option>花蓮縣</option><option>台東縣</option><option>澎湖縣</option><option>金門縣</option><option>連江縣</option></select></div>\n        <div><label>區/鄉鎮</label><input name="rdist" placeholder="苗栗市"></div>\n        <div><label>詳細地址 <span class="req">*</span></label><input name="raddr" placeholder="新東街257號"></div>\n      </div>\n      <div style="margin-bottom:12px"><label>戶籍電話</label><input name="rphone" placeholder="037-123456" style="max-width:200px"></div>\n      <label style="display:flex;align-items:center;gap:8px;font-size:14px;color:#3a3020;margin-bottom:12px;cursor:pointer;font-weight:600">\n        <input type="checkbox" id="sameck" name="sameck" checked style="width:18px;height:18px;flex-shrink:0;accent-color:#6a5e4e" onchange="document.getElementById(\'lsec\').style.display=this.checked?\'none\':\'block\'">\n        住家地址與戶籍相同\n      </label>\n      <div id="lsec" style="display:none;margin-bottom:12px">\n        <div style="font-size:13px;font-weight:700;color:#4a3e30;margin-bottom:10px">住家地址</div>\n        <div class="grid3">\n          <div><label>縣市</label><select name="lcity"><option value="">請選擇</option><option>台北市</option><option>新北市</option><option>桃園市</option><option>台中市</option><option>台南市</option><option>高雄市</option><option>基隆市</option><option>新竹市</option><option>新竹縣</option><option>苗栗縣</option><option>彰化縣</option><option>南投縣</option><option>雲林縣</option><option>嘉義市</option><option>嘉義縣</option><option>屏東縣</option><option>宜蘭縣</option><option>花蓮縣</option><option>台東縣</option><option>澎湖縣</option><option>金門縣</option><option>連江縣</option></select></div>\n          <div><label>區/鄉鎮</label><input name="ldist"></div>\n          <div><label>詳細地址</label><input name="laddr"></div>\n        </div>\n      </div>\n      <div class="grid3">\n        <div><label>現住電話</label><input name="lphone" placeholder="037-123456"></div>\n        <div><label>居住狀況</label><select name="lstatus"><option value="">--- 請選擇 ---</option><option>自有</option><option>配偶</option><option>父母</option><option>親屬</option><option>租屋</option><option>宿舍</option></select></div>\n        <div><label>居住時間</label><div class="ig"><input name="lyear" placeholder="5" style="width:58px"><span>年</span><input name="lmon" placeholder="0" style="width:58px"><span>月</span></div></div>\n      </div>\n    </div>\n    <div class="card">\n      <div class="section-title">職業資料</div>\n      <div class="grid2">\n        <div class="full"><label>公司名稱 <span class="req">*</span></label><input name="cmpname" placeholder="嘉合企業社"></div>\n        <div><label>公司電話 <span class="req">*</span></label><div class="ig"><select name="carea" style="width:82px"><option value="">區碼</option><option>02</option><option>03</option><option>037</option><option>04</option><option>049</option><option>05</option><option>06</option><option>07</option><option>08</option><option>089</option><option value="mobile">手機</option></select><input name="cnum" placeholder="1234567"><input name="cext" placeholder="分機" style="width:68px"></div></div>\n        <div><label>職稱</label><input name="crole" placeholder="技工"></div>\n        <div><label>年資</label><div class="ig"><input name="cyear" placeholder="3" style="width:62px"><span>年</span><input name="cmon" placeholder="0" style="width:62px"><span>月</span></div></div>\n        <div><label>月薪（萬）</label><input name="csal" placeholder="3.5"></div>\n        <div class="full"><label>公司地址</label><div style="display:grid;grid-template-columns:1fr 1fr 2fr;gap:8px"><select name="ccity"><option value="">請選擇</option><option>台北市</option><option>新北市</option><option>桃園市</option><option>台中市</option><option>台南市</option><option>高雄市</option><option>基隆市</option><option>新竹市</option><option>新竹縣</option><option>苗栗縣</option><option>彰化縣</option><option>南投縣</option><option>雲林縣</option><option>嘉義市</option><option>嘉義縣</option><option>屏東縣</option><option>宜蘭縣</option><option>花蓮縣</option><option>台東縣</option><option>澎湖縣</option><option>金門縣</option><option>連江縣</option></select><input name="cdist" placeholder="區/鄉鎮"><input name="caddr" placeholder="詳細地址"></div></div>\n      </div>\n    </div>\n    <div class="card">\n      <div class="section-title">聯絡人資料</div>\n      <div style="margin-bottom:14px;padding-bottom:14px;border-bottom:1px solid #ddd5ca">\n        <div style="font-size:13px;color:#5a4e40;font-weight:600;margin-bottom:10px">聯絡人1（需為二等親）</div>\n        <div class="grid4">\n          <div><label>姓名 <span class="req">*</span></label><input name="c1name" placeholder="吳玉英"></div>\n          <div><label>關係</label><input name="c1rel" placeholder="母"></div>\n          <div><label>電話 <span class="req">*</span></label><input name="c1tel" placeholder="0978-055530"></div>\n          <div><label>可知情</label><select name="c1know"><option value="">請選擇</option><option>可知情</option><option>保密</option><option>無可知情</option></select></div>\n        </div>\n      </div>\n      <div>\n        <div style="font-size:13px;color:#5a4e40;font-weight:600;margin-bottom:10px">聯絡人2</div>\n        <div class="grid4">\n          <div><label>姓名 <span class="req">*</span></label><input name="c2name" placeholder="賴俊明"></div>\n          <div><label>關係</label><input name="c2rel" placeholder="友"></div>\n          <div><label>電話 <span class="req">*</span></label><input name="c2tel" placeholder="0919-616821"></div>\n          <div><label>可知情</label><select name="c2know"><option value="">請選擇</option><option>可知情</option><option>保密</option><option>無可知情</option></select></div>\n        </div>\n      </div>\n    </div>\n    <div class="card">\n      <div class="section-title">貸款諮詢事項</div>\n      <div class="grid2">\n        <div><label>資金需求</label><input name="efund" placeholder="$100,000"></div>\n        <div><label>近三月是否送件</label><select name="esent"><option value="">請選擇</option><option>否</option><option>是</option></select></div>\n        <div><label>當鋪私設</label><select name="eprivate"><option value="">請選擇</option><option>無</option><option>有</option></select></div>\n        <div><label>勞保狀態</label><select name="elabor"><option value="">請選擇</option><option>公司保</option><option>工會保</option><option>自行投保</option><option>無勞保</option></select></div>\n        <div><label>有無薪轉</label><select name="esal"><option value="">請選擇</option><option>有薪轉</option><option>無薪轉</option></select></div>\n        <div><label>有無證照</label><select name="elicense"><option value="">請選擇</option><option>無</option><option>有</option></select></div>\n        <div><label>貸款遲繳</label><select name="elate"><option value="">請選擇</option><option>無</option><option>有</option></select></div>\n        <div><label>遲繳天數</label><input name="elateday" placeholder="0"></div>\n        <div><label>罰單欠費金額 $</label><input name="efine" placeholder="0" type="number" min="0" oninput="calcF()"></div>\n        <div><label>燃料稅金額 $</label><input name="efuel" placeholder="0" type="number" min="0" oninput="calcF()"></div>\n        <div class="full"><label>欠費總額（自動計算）</label><div class="auto-val" id="tfees">$0</div></div>\n        <div><label>名下信用卡</label><input name="ecard" placeholder="銀行協商"></div>\n        <div><label>有無動產/不動產</label><input name="eprop" placeholder="有機車"></div>\n        <div><label>法學（幾條）</label><input name="elaw" placeholder="共1條"></div>\n        <div class="full"><label>備註</label><textarea name="enote" placeholder="其他說明..."></textarea></div>\n      </div>\n    </div>\n    <div class="card">\n      <div class="section-title">負債明細</div>\n      <div id="dlist"></div>\n      <div style="display:flex;gap:8px;margin-top:8px;flex-wrap:wrap;">\n        <button type="button" class="add-btn" onclick="addD(\'車貸\')">&#10133; 新增車貸</button>\n        <button type="button" class="add-btn" onclick="addD(\'信貸\')">&#10133; 新增信貸/其他</button>\n      </div>\n    </div>\n    <div class="btn-row">\n      <div id="err-box" style="display:none;background:#fef2f2;border:1px solid #fca5a5;border-radius:8px;padding:12px 16px;margin-bottom:12px;"></div><button type="button" class="btn btn-primary" onclick="doSubmit()">&#9989; 建立客戶</button>\n      <button type="button" class="btn btn-cancel" onclick="history.back()">取消</button>\n    </div>\n  </form>\n</div>\n<script>\nlet vc=0,dc=0;\nfunction gv(n){const e=document.querySelector(\'[name="\'+n+\'"]\');return e?e.value||\'\':\'\';}\n\nfunction addD(type){\n  dc++;const n=dc;\n  var isCar=(type===\'車貸\');\n  var r=document.createElement(\'div\');\n  r.id=\'d\'+n;r.className=\'d-row\';\n  var bg=isCar?\'#f2f8f4\':\'#f8f5f1\';\n  var bc=isCar?\'#4e7055\':\'#8a7a68\';\n  r.style.cssText=\'border-radius:8px;margin-bottom:8px;padding:10px 14px;background:\'+bg+\';border-left:4px solid \'+bc+\';\';\n  var lbl=isCar?\'🚗 車貸\':\'💳 信貸/其他\';\n  var lc=isCar?\'#4e7055\':\'#6a5e4e\';\n  var LS=\'font-size:12px;font-weight:500;color:#2c2820;height:17px;\';\n  var IS=\'width:100%;height:34px;padding:0 8px;border:0.5px solid #ddd5ca;border-radius:6px;font-size:13px;font-family:inherit;box-sizing:border-box;background:#fff;color:#2c2820;\';\n  var RS=\'width:100%;height:34px;padding:0 8px;border:0.5px solid #e8c0b0;border-radius:6px;font-size:13px;font-weight:500;color:#b84a35;background:#fff8f5;display:flex;align-items:center;justify-content:flex-end;box-sizing:border-box;\';\n  var SS=\'width:100%;height:34px;padding:0 6px;border:0.5px solid #ddd5ca;border-radius:6px;font-size:13px;font-family:inherit;box-sizing:border-box;background:#fff;color:#2c2820;\';\n  var FS=\'display:flex;flex-direction:column;gap:4px;\';\n  var GS=\'display:grid;grid-template-columns:repeat(5,1fr);gap:8px;margin-bottom:8px;\';\n  var h=\'\';\n  h+=\'<div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:10px;">\';\n  h+=\'<span style="font-size:13px;font-weight:500;color:\'+lc+\';">\'+lbl+\'</span>\';\n  h+=\'<button type="button" onclick="rmD(\'+n+\')" style="background:#f5ddd8;color:#b84a35;border:none;border-radius:5px;width:28px;height:28px;cursor:pointer;font-size:13px;">✕</button>\';\n  h+=\'</div>\';\n  h+=\'<div style="\'+GS+\'">\';\n  h+=\'<div style="\'+FS+\'"><div style="\'+LS+\'">貸款商家</div><input id="dc\'+n+\'" placeholder="裕融" style="\'+IS+\'"></div>\';\n  h+=\'<div style="\'+FS+\'"><div style="\'+LS+\'">貸款金額</div><input id="dl\'+n+\'" placeholder="150000" type="number" style="\'+IS+\'"></div>\';\n  h+=\'<div style="\'+FS+\'"><div style="\'+LS+\'">期數／已繳</div><input id="dp\'+n+\'" placeholder="36 / 0" oninput="calcD(\'+n+\')" style="\'+IS+\'"></div>\';\n  h+=\'<div style="\'+FS+\'"><div style="\'+LS+\'">月繳金額</div><input id="dm\'+n+\'" placeholder="5265" type="number" oninput="calcD(\'+n+\')" style="\'+IS+\'"></div>\';\n  h+=\'<div style="\'+FS+\'"><div style="\'+LS+\'">剩餘金額</div><div id="dr\'+n+\'" style="\'+RS+\'">-</div></div>\';\n  h+=\'</div>\';\n  if(isCar){\n    h+=\'<div style="\'+GS+\'">\';\n    h+=\'<div style="\'+FS+\'"><div style="\'+LS+\'">設定日期（民國）</div><input id="dd\'+n+\'" placeholder="112/01" style="\'+IS+\'"></div>\';\n    h+=\'<div style="\'+FS+\'"><div style="\'+LS+\'">動保／公路</div><select id="dg\'+n+\'" style="\'+SS+\'"><option>無</option><option>公路</option><option>動保</option><option>公路+動保</option></select></div>\';\n    h+=\'<div style="\'+FS+\'"><div style="\'+LS+\'">空間</div><select id="ds\'+n+\'" style="\'+SS+\'"><option>有</option><option>無</option></select></div>\';\n    h+=\'<div style="height:49px;"></div><div style="height:49px;"></div>\';\n    h+=\'</div>\';\n  }else{\n    h+=\'<input id="dd\'+n+\'" value="" style="display:none;"><input id="dg\'+n+\'" value="-" style="display:none;"><input id="ds\'+n+\'" value="-" style="display:none;">\';\n  }\n  r.innerHTML=h;\n  document.getElementById(\'dlist\').appendChild(r);\n}\nfunction rmD(n){\n  var el=document.getElementById(\'d\'+n);if(el)el.remove();\n}\nfunction calcD(n){\n  var m=parseFloat(document.getElementById(\'dm\'+n)?.value)||0;\n  var dpv=(document.getElementById(\'dp\'+n)?.value||\'\').trim();\n  var parts=dpv.split(\'/\');\n  var p=parseFloat(parts[0])||0;\n  var a=parseFloat(parts[1])||0;\n  var el=document.getElementById(\'dr\'+n);if(!el)return;\n  if(m>0&&p>0){\n    var rem=(m*p)-(m*a);\n    el.textContent=\'$\'+Math.round(rem).toLocaleString();\n    el.style.color=rem>0?\'#b84a35\':\'#4e7055\';\n  }else el.textContent=\'-\';\n}\n\nfunction calcF(){\n  const f=parseFloat(document.querySelector(\'[name="efine"]\')?.value)||0;\n  const u=parseFloat(document.querySelector(\'[name="efuel"]\')?.value)||0;\n  document.getElementById(\'tfees\').textContent=\'$\'+(f+u).toLocaleString();\n}\n\nfunction sec(t){\n  return \'<div style="background:#5a4e40;color:#fff;font-size:13px;font-weight:500;padding:7px 12px;border-radius:5px 5px 0 0;letter-spacing:0.5px;">\'+t+\'</div>\'\n    +\'<div style="border:1px solid #ccc5ba;border-top:none;border-radius:0 0 5px 5px;padding:8px 14px;margin-bottom:14px;background:#fdfaf7;">\';\n}\nfunction fl(l,v){\n  if(!v||v===\'-\'||v===\'0年0月\')return\'\';\n  return \'<div style="display:grid;grid-template-columns:110px 1fr;gap:6px;padding:7px 0;border-bottom:0.5px solid #ece8e2;">\'\n    +\'<span style="font-size:13px;color:#5a4e40;font-weight:500;line-height:1.5;">\'+l+\'</span>\'\n    +\'<span style="font-size:14px;font-weight:400;color:#2c2820;line-height:1.5;">\'+v+\'</span></div>\';\n}\nfunction fl2(items){\n  var h=\'<div style="display:grid;grid-template-columns:1fr 1fr;gap:0 28px;">\';\n  items.forEach(function(x){h+=fl(x[0],x[1]);});\n  return h+\'</div>\';\n}\n\nfunction exportPDF(){/* overridden by inject */}\n\n// page-init\n</script>\n</body>\n</html>\n'
    HTML_PAGE = HTML_PAGE.replace('<option>B群</option><option>C群</option>', group_opts)
    # 注入前端驗證JS
    # 把驗證JS注入到原本 HTML_PAGE 的 // page-init 位置
    inject_js = """
function showErr(msgs){
  var b=document.getElementById('err-box');
  b.innerHTML='<div style="color:#991b1b;font-weight:700;margin-bottom:8px;">請修正：</div>'+msgs.map(function(x){return '<div style="color:#b84a35;padding:2px 0;">• '+x+'</div>';}).join('');
  b.style.display='block';b.scrollIntoView({behavior:'smooth',block:'center'});
}
function doSubmit(){
  var e=[];
  var qq=function(s){return (document.querySelector(s)||{value:''}).value;};
  var n=qq('[name="cname"]').trim();
  var id=qq('[name="idno"]').trim().toUpperCase();
  var ph=qq('[name="phone"]').replace(/[- ()]/g,'');
  var em=qq('[name="email"]').trim();
  var li=qq('[name="line"]').trim();
  var ra=qq('[name="raddr"]').trim();
  var sa=document.getElementById('sameck').checked;
  var la=sa?ra:qq('[name="laddr"]').trim();
  var cn=qq('[name="cmpname"]').trim();
  var cp=qq('[name="cnum"]').trim();
  var cc=qq('[name="ccity"]');
  var ca=qq('[name="caddr"]').trim();
  var n1=qq('[name="c1name"]').trim();
  var t1=qq('[name="c1tel"]').replace(/[- ()]/g,'');
  var n2=qq('[name="c2name"]').trim();
  var t2=qq('[name="c2tel"]').replace(/[- ()]/g,'');
  if(!n)e.push('姓名不可空白');
  if(!id)e.push('身分證不可空白');
  else if(!/^[A-Z][0-9]{9}$/.test(id))e.push('身分證格式錯誤');
  if(!ph)e.push('行動電話不可空白');
  else if(ph.length!==10||!/^09/.test(ph))e.push('行動電話10碼');
  if(!em)e.push('Email必填');
  if(!li)e.push('LINE ID必填');
  if(!ra)e.push('戸籍地址不可空白');
  if(!sa&&!la)e.push('居住地址不可空白');
  if(!cn)e.push('公司名稱不可空白');
  if(!cp)e.push('公司電話不可空白');
  if(!cc&&!ca)e.push('公司地址不可空白');
  var selfkw=['自營','自己','自行','自營商','營業中','個人工作室'];
  if(cn&&selfkw.some(function(k){return cn===k||cn.indexOf(k)===0&&cn.length<=4;}))e.push('公司名稱請填明確行業（不可只寫「'+cn+'」）');
  if(!n1)e.push('聯絡人1姓名必填');
  if(!t1)e.push('聯絡人1電話必填');
  else if(t1.length!==10)e.push('聯絡人1電話10碼');
  if(!n2)e.push('聯絡人2姓名必填');
  if(!t2)e.push('聯絡人2電話必填');
  else if(t2.length!==10)e.push('聯絡人2電話10碼');
  if(ph&&t1&&ph===t1)e.push('申請人與聯絡人1電話相同');
  if(ph&&t2&&ph===t2)e.push('申請人與聯絡人2電話相同');
  if(t1&&t2&&t1===t2)e.push('聯絡人1耈2電話相同');
  if(e.length>0){showErr(e);return;}
  var w=[];
  var ls=qq('[name="lstatus"]');
  var rc=qq('[name="rcity"]');
  if(ls==='自有')w.push('居住「自有」請確認無私設');
  var lc=sa?rc:qq('[name="lcity"]');
  if(lc&&cc&&lc!==cc)w.push('居住('+lc+')與公司('+cc+')不同縣市，請確認距離合理');
  w.push('請逐字確認所有欄位填寫正確');
  var b2=document.getElementById('err-box');
  b2.innerHTML='<div style="color:#854d0e;font-weight:700;margin-bottom:8px;">人工確認事項：</div>'+w.map(function(x){return '<div style="color:#854d0e;padding:2px 0;">'+x+'</div>';}).join('')+'<div style="margin-top:10px;"><button onclick="doConfirmSubmit()" style="background:#6a5e4e;color:#fff;border:none;padding:8px 18px;border-radius:6px;cursor:pointer;">確認無誤，送出</button></div>';
  b2.style.display='block';b2.scrollIntoView({behavior:'smooth',block:'center'});
}
function collectDebt(){
  var rows=[];
  for(var i=1;i<=dc;i++){
    var el=document.getElementById('d'+i);
    if(!el)continue;
    var co=document.getElementById('dc'+i)?.value||'';
    if(!co)continue;
    rows.push({
      co:co,
      lo:document.getElementById('dl'+i)?.value||'',
      pe:(function(){var v=(document.getElementById('dp'+i)?.value||'').split('/');return (v[0]||'').trim();})(),
      mo:document.getElementById('dm'+i)?.value||'',
      pa:(function(){var v=(document.getElementById('dp'+i)?.value||'').split('/');return (v[1]||'').trim();})(),
      re:document.getElementById('dr'+i)?.textContent||'',
      da:document.getElementById('dd'+i)?.value||'',
      dy:document.getElementById('dg'+i)?.value||'',
      sp:document.getElementById('ds'+i)?.value||''
    });
  }
  var h=document.getElementById('debt_list_input');
  if(!h){h=document.createElement('input');h.type='hidden';h.name='debt_list';h.id='debt_list_input';document.getElementById('cf').appendChild(h);}
  h.value=JSON.stringify(rows);
}
function doConfirmSubmit(){collectDebt();localStorage.removeItem('nc_draft');document.forms[0].submit();}
function saveForm(){var data={};document.querySelectorAll('#cf input,#cf select,#cf textarea').forEach(function(el){if(!el.name)return;if(el.type==='checkbox')data[el.name]=el.checked;else data[el.name]=el.value;});localStorage.setItem('nc_draft',JSON.stringify(data));}
function restoreForm(){var raw=localStorage.getItem('nc_draft');if(!raw)return;var data=JSON.parse(raw);Object.keys(data).forEach(function(k){var el=document.querySelector('#cf [name="'+k+'"]');if(!el)return;if(el.type==='checkbox')el.checked=data[k];else el.value=data[k];});var ck=document.getElementById('sameck');if(ck)document.getElementById('lsec').style.display=ck.checked?'none':'block';}
window.addEventListener('DOMContentLoaded',function(){restoreForm();document.getElementById('cf').addEventListener('input',saveForm);document.getElementById('cf').addEventListener('change',saveForm);});
// page-init
"""
    HTML_PAGE = HTML_PAGE.replace('// page-init', inject_js + '\n' + (pdf_js or ''))
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
    phone = f.get("phone","").strip().replace("-","").replace(" ","")
    email = f.get("email","").strip()
    line_id = f.get("line","").strip()
    raddr = f.get("raddr","").strip()
    live_same = f.get("sameck","")
    laddr = f.get("laddr","").strip()
    cmpname = f.get("cmpname","").strip()
    cnum = f.get("cnum","").strip()
    caddr = f.get("caddr","").strip()
    c1name = f.get("c1name","").strip()
    c1tel = f.get("c1tel","").strip().replace("-","").replace(" ","")
    c2name = f.get("c2name","").strip()
    c2tel = f.get("c2tel","").strip().replace("-","").replace(" ","")

    import re as _re
    errs = []
    if not name: errs.append("姓名不可空白")
    if not id_no: errs.append("身分證字號不可空白")
    elif not _re.match(r"^[A-Z][0-9]{9}$", id_no): errs.append("身分證字號格式錯誤（1個英文+9個數字）")
    if not phone: errs.append("行動電話不可空白")
    elif len(phone)!=10 or not phone.startswith("09"): errs.append("行動電話需為10碼（09開頭）")
    if not email: errs.append("Email為必填")
    if not line_id: errs.append("LINE ID為必填")
    if not raddr: errs.append("戶籍地址不可空白")
    if not live_same and not laddr: errs.append("居住地址不可空白（或勾選同戶籍）")
    if not cmpname: errs.append("公司名稱不可空白")
    elif cmpname in ["自營","自己","自行"]: errs.append("公司名稱請填明確行業名稱")
    if not cnum: errs.append("公司電話不可空白")
    if not caddr: errs.append("公司地址不可空白")
    if not c1name: errs.append("聯絡人1姓名必填")
    if not c1tel: errs.append("聯絡人1電話必填")
    elif len(c1tel)!=10: errs.append("聯絡人1電話需為10碼")
    if not c2name: errs.append("聯絡人2姓名必填")
    if not c2tel: errs.append("聯絡人2電話必填")
    elif len(c2tel)!=10: errs.append("聯絡人2電話需為10碼")
    # 電話重複
    phones = {"申請人":phone,"聯絡人1":c1tel,"聯絡人2":c2tel}
    pkeys = list(phones.keys())
    for i in range(len(pkeys)):
        for j in range(i+1,len(pkeys)):
            if phones[pkeys[i]] and phones[pkeys[j]] and phones[pkeys[i]]==phones[pkeys[j]]:
                errs.append(f"{pkeys[i]}與{pkeys[j]}電話相同（{phones[pkeys[i]]}），請確認")
    if errs:
        err_html = "<ul>" + "".join(f"<li>{h(e)}</li>" for e in errs) + "</ul>"
        return HTMLResponse(f"""<!DOCTYPE html><html><head><meta charset="UTF-8"><style>body{{font-family:'Microsoft JhengHei',sans-serif;background:#ece8e2;display:flex;justify-content:center;align-items:center;min-height:100vh;margin:0;}}.box{{background:#faf7f4;border:1px solid #ddd5ca;border-radius:12px;padding:32px;max-width:480px;}}.title{{font-size:18px;font-weight:700;color:#991b1b;margin-bottom:12px;}}.btn{{background:#6a5e4e;color:#fff;border:none;padding:10px 24px;border-radius:8px;font-size:14px;cursor:pointer;text-decoration:none;display:inline-block;margin-top:16px;}}</style></head><body><div class="box"><div class="title">⚠️ 請修正以下欄位</div>{err_html}<a href="javascript:history.back()" class="btn">← 返回修正</a></div></body></html>""", status_code=400)
    source_group_id = f.get("grp","")
    live_same = "1" if f.get("sameck") else "0"
    conn = get_conn(); cur = conn.cursor()
    cur.execute("SELECT * FROM customers WHERE id_no=? AND status='ACTIVE'", (id_no,))
    existing = cur.fetchone()
    now = now_iso()
    if existing:
        case_id = existing["case_id"]
        conn.close()
        existing_name = existing["customer_name"]
        return HTMLResponse(f"""<!DOCTYPE html><html><head><meta charset="UTF-8">
<style>body{{font-family:'Microsoft JhengHei',sans-serif;background:#ece8e2;display:flex;justify-content:center;align-items:center;min-height:100vh;margin:0;}}
.box{{background:#faf7f4;border:1px solid #ddd5ca;border-radius:12px;padding:32px;max-width:480px;text-align:center;}}
.icon{{font-size:48px;margin-bottom:16px;}}
.title{{font-size:20px;font-weight:700;color:#991b1b;margin-bottom:8px;}}
.msg{{font-size:14px;color:#4a3e30;margin-bottom:24px;line-height:1.6;}}
.btn{{background:#6a5e4e;color:#fff;border:none;padding:10px 24px;border-radius:8px;font-size:14px;cursor:pointer;text-decoration:none;display:inline-block;margin:4px;}}
.btn2{{background:#e8e2da;color:#4a3e30;border:1px solid #ddd5ca;padding:10px 24px;border-radius:8px;font-size:14px;cursor:pointer;text-decoration:none;display:inline-block;margin:4px;}}
</style></head><body>
<div class="box">
<div class="icon">⚠️</div>
<div class="title">此客戶已存在！</div>
<div class="msg">身分證號 <strong>{h(id_no)}</strong><br>客戶「<strong>{h(existing_name)}</strong>」已在系統中<br>（案件編號：{h(case_id)}）<br><br>資料已自動更新為最新填寫內容。</div>
<a href="/report" class="btn">前往日報查看</a>
<a href="/new-customer" class="btn2">繼續新增其他客戶</a>
</div></body></html>""", status_code=200)
    else:
        case_id = short_id()
        cur.execute("""INSERT INTO customers
            (case_id,customer_name,id_no,source_group_id,company,last_update,status,created_at,updated_at)
            VALUES (?,?,?,?,?,?,'PENDING',?,?)""",
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
        company_months=f.get("cmon",""),
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
        eval_fuel_tax=f.get("efuel",""), eval_late=f.get("elate",""), eval_late_days=f.get("elateday",""), eval_note=f.get("enote",""),
        debt_list=f.get("debt_list",""),
        carrier=f.get("carrier",""), fb=f.get("fb",""),
        sales_name=f.get("sales",""), source_group_id=source_group_id, created_by_role=role,
    )
    flds = ", ".join(k+" = ?" for k in extra)
    vals = list(extra.values()) + [now_iso(), case_id]
    cur.execute("UPDATE customers SET "+flds+", updated_at=? WHERE case_id=?", vals)
    conn.commit(); conn.close()
    gname = get_group_name(source_group_id)
    # 行政A填完 → 顯示提示頁，告知業務去 LINE 建相簿
    return HTMLResponse(f"""<!DOCTYPE html><html><head><meta charset="UTF-8">
<style>body{{font-family:'Microsoft JhengHei',sans-serif;background:#ece8e2;display:flex;justify-content:center;align-items:center;min-height:100vh;margin:0;}}
.box{{background:#faf7f4;border:1px solid #ddd5ca;border-radius:12px;padding:32px;max-width:480px;text-align:center;}}
.title{{font-size:20px;font-weight:700;color:#2c2820;margin-bottom:12px;}}
.msg{{font-size:14px;color:#4a3e40;margin-bottom:8px;line-height:1.8;}}
.note{{background:#fef9c3;border:1px solid #fde047;border-radius:8px;padding:12px;font-size:13px;color:#854d0e;margin-bottom:20px;text-align:left;}}
.btn{{background:#6a5e4e;color:#fff;border:none;padding:10px 24px;border-radius:8px;font-size:14px;cursor:pointer;text-decoration:none;display:inline-block;margin:4px;}}
.btn2{{background:#e8e2da;color:#4a3e30;border:1px solid #ddd5ca;padding:10px 24px;border-radius:8px;font-size:14px;text-decoration:none;display:inline-block;margin:4px;}}
</style></head><body>
<div class="box">
<div style="font-size:48px;margin-bottom:12px;">✅</div>
<div class="title">客戶資料已儲存！</div>
<div class="msg">客戶「<strong>{h(name)}</strong>」資料已完成填寫</div>
<div class="note">
⚠️ 請通知業務：<br>
1. 將客戶證件/勞保/財力證明上傳到 LINE 群組相簿<br>
2. 在 LINE 群組打格式建立客戶：<br>
<strong>{datetime.now().strftime('%y/%m/%d')}-{h(name)}-身分證號/...</strong><br>
完成後客戶才會出現在日報！
</div>
<a href="/new-customer" class="btn">繼續新增客戶</a>
<a href="/pending-customers" class="btn2">查看待確認客戶</a>
</div></body></html>""", status_code=200)

# =========================
# 客戶PDF列印頁面
# =========================
@app.get("/customer-pdf", response_class=HTMLResponse)
def customer_pdf(request: Request, case_id: str = ""):
    from fastapi.responses import RedirectResponse
    role = check_auth(request)
    if not role: return RedirectResponse("/login")
    if not case_id: return RedirectResponse("/pending-customers")
    conn = get_conn(); cur = conn.cursor()
    cur.execute("SELECT * FROM customers WHERE case_id=?", (case_id,))
    row = cur.fetchone(); conn.close()
    if not row: return RedirectResponse("/pending-customers")
    r = dict(row)
    def v(k): return h(r.get(k, "") or "")
    gname = h(get_group_name(r.get("source_group_id", "")))
    created = h((r.get("created_at", "") or "")[:10])

    # Parse debt list
    import json as _json
    try:
        debt_data = _json.loads(r.get("debt_list", "") or "[]") if r.get("debt_list") else []
    except Exception:
        debt_data = []
    debt_html = ""
    if debt_data:
        debt_html = '<table style="width:100%;border-collapse:collapse;font-size:13px;margin-top:8px;">'
        debt_html += '<tr style="background:#e8e2da;font-weight:600;"><th style="padding:6px;border:1px solid #bbb;">貸款商家</th><th style="padding:6px;border:1px solid #bbb;">金額</th><th style="padding:6px;border:1px solid #bbb;">期數/已繳</th><th style="padding:6px;border:1px solid #bbb;">月繳</th><th style="padding:6px;border:1px solid #bbb;">剩餘</th><th style="padding:6px;border:1px solid #bbb;">日期</th><th style="padding:6px;border:1px solid #bbb;">動保</th></tr>'
        for d in debt_data:
            debt_html += f'<tr><td style="padding:5px;border:1px solid #bbb;">{h(d.get("co",""))}</td><td style="padding:5px;border:1px solid #bbb;">{h(d.get("lo",""))}</td><td style="padding:5px;border:1px solid #bbb;">{h(d.get("pe",""))}/{h(d.get("pa",""))}</td><td style="padding:5px;border:1px solid #bbb;">{h(d.get("mo",""))}</td><td style="padding:5px;border:1px solid #bbb;">{h(d.get("re",""))}</td><td style="padding:5px;border:1px solid #bbb;">{h(d.get("da",""))}</td><td style="padding:5px;border:1px solid #bbb;">{h(d.get("dy",""))}</td></tr>'
        debt_html += '</table>'

    lsame = r.get("live_same_as_reg", "") == "1"
    live_addr = f'{v("reg_city")}{v("reg_district")}{v("reg_address")}' if lsame else f'{v("live_city")}{v("live_district")}{v("live_address")}'
    reg_addr = f'{v("reg_city")}{v("reg_district")}{v("reg_address")}'
    company_addr = f'{v("company_city")}{v("company_district")}{v("company_address")}'

    return f"""<!DOCTYPE html><html lang="zh-TW"><head><meta charset="UTF-8">
<title>客戶資料 - {v("customer_name")}</title>
<style>
@media print {{ @page {{ size: A4 portrait; margin: 10mm 12mm; }} .no-print {{ display: none !important; }} }}
* {{ box-sizing: border-box; margin: 0; padding: 0; }}
body {{ font-family: 'Microsoft JhengHei', 'PingFang TC', sans-serif; background: #fff; color: #1a1a1a; font-size: 13px; padding: 20px; }}
.header {{ background: #3a3530; color: #fff; padding: 16px 20px; border-radius: 8px; margin-bottom: 16px; display: flex; justify-content: space-between; align-items: center; }}
.header-name {{ font-size: 22px; font-weight: 700; }}
.header-sub {{ font-size: 12px; color: #c8bfb5; margin-top: 4px; }}
table {{ width: 100%; border-collapse: collapse; margin-bottom: 16px; }}
th, td {{ border: 1px solid #bbb; padding: 7px 10px; font-size: 13px; line-height: 1.5; }}
th {{ background: #f0ebe4; color: #3a3020; font-weight: 700; width: 100px; white-space: nowrap; text-align: left; }}
td {{ background: #fff; }}
.sec {{ background: #3a3530; color: #fff; font-size: 12px; font-weight: 700; padding: 6px 10px; }}
.sec td {{ background: #3a3530; color: #fff; font-weight: 700; }}
</style>
</head><body>
<div class="no-print" style="text-align:center;margin-bottom:16px;">
  <button onclick="window.print()" style="background:#4e7055;color:#fff;border:none;padding:10px 28px;border-radius:6px;font-size:14px;cursor:pointer;font-weight:600;">列印 / 存 PDF</button>
  <button onclick="history.back()" style="background:#6a5e4e;color:#fff;border:none;padding:10px 28px;border-radius:6px;font-size:14px;cursor:pointer;font-weight:600;margin-left:8px;">返回</button>
</div>
<div class="header">
  <div><div class="header-name">{v("customer_name")}</div><div class="header-sub">群組：{gname}　建立日期：{created}</div></div>
  <div style="font-size:13px;color:#c8bfb5;font-weight:600;">客戶資料表</div>
</div>
<table>
<tr class="sec"><td colspan="4">基本資料</td></tr>
<tr><th>姓名</th><td>{v("customer_name")}</td><th>身分證</th><td>{v("id_no")}</td></tr>
<tr><th>出生日期</th><td>{v("birth_date")}</td><th>行動電話</th><td>{v("phone")}</td></tr>
<tr><th>Email</th><td>{v("email")}</td><th>LINE ID</th><td>{v("line_id")}</td></tr>
<tr><th>婚姻</th><td>{v("marriage")}</td><th>學歷</th><td>{v("education")}</td></tr>
<tr class="sec"><td colspan="4">身分證發證</td></tr>
<tr><th>發證日期</th><td>{v("id_issue_date")}</td><th>發證地</th><td>{v("id_issue_place")}</td></tr>
<tr><th>換補發</th><td colspan="3">{v("id_issue_type")}</td></tr>
<tr class="sec"><td colspan="4">地址資料</td></tr>
<tr><th>戶籍地址</th><td colspan="3">{reg_addr}</td></tr>
<tr><th>戶籍電話</th><td>{v("reg_phone")}</td><th>現住電話</th><td>{v("live_phone")}</td></tr>
<tr><th>住家地址</th><td colspan="3">{live_addr}</td></tr>
<tr><th>居住狀況</th><td>{v("live_status")}</td><th>居住時間</th><td>{v("live_years")}年{v("live_months")}月</td></tr>
<tr class="sec"><td colspan="4">職業資料</td></tr>
<tr><th>公司名稱</th><td colspan="3">{v("company_name_detail")}</td></tr>
<tr><th>公司電話</th><td>{v("company_phone_area")}-{v("company_phone_num")}</td><th>職稱</th><td>{v("company_role")}</td></tr>
<tr><th>年資</th><td>{v("company_years")}年</td><th>月薪</th><td>{v("company_salary")}萬</td></tr>
<tr><th>公司地址</th><td colspan="3">{company_addr}</td></tr>
<tr><th>行業</th><td colspan="3">{v("company_industry")}</td></tr>
<tr class="sec"><td colspan="4">聯絡人</td></tr>
<tr><th>聯絡人1</th><td>{v("contact1_name")}（{v("contact1_relation")}）</td><th>電話</th><td>{v("contact1_phone")}</td></tr>
<tr><th>知情</th><td>{v("contact1_known")}</td><th>聯絡人2</th><td>{v("contact2_name")}（{v("contact2_relation")}）</td></tr>
<tr><th>電話</th><td>{v("contact2_phone")}</td><th>知情</th><td>{v("contact2_known")}</td></tr>
</table>
<div style="page-break-before:always;margin-top:20px;"></div>
<table>
<tr class="sec"><td colspan="4">貸款諮詢</td></tr>
<tr><th>資金需求</th><td>{v("eval_fund_need")}</td><th>近三月送件</th><td>{v("eval_sent_3m")}</td></tr>
<tr><th>當鋪私設</th><td>{v("eval_alert")}</td><th>勞保</th><td>{v("eval_labor_ins")}</td></tr>
<tr><th>薪轉</th><td>{v("eval_salary_transfer")}</td><th>遲繳</th><td>{v("eval_late")} {v("eval_late_days")}天</td></tr>
<tr><th>罰單</th><td>{v("eval_fine")}</td><th>燃料稅</th><td>{v("eval_fuel_tax")}</td></tr>
<tr><th>信用卡</th><td>{v("eval_credit_card")}</td><th>動產</th><td>{v("eval_property")}</td></tr>
<tr><th>證照</th><td>{v("eval_license")}</td><th>車輛</th><td>{v("eval_vehicle")}</td></tr>
<tr><th>法學</th><td colspan="3">{v("eval_law")}</td></tr>
{"<tr><th>備註</th><td colspan='3'>" + v("eval_note") + "</td></tr>" if r.get("eval_note") else ""}
</table>
{"<div style='font-size:13px;font-weight:700;color:#3a3530;margin-bottom:8px;'>負債明細</div>" + debt_html if debt_html else ""}
</body></html>"""


def _build_customer_pdf_body(r: dict) -> str:
    """組出單一客戶在 PDF 內的內容（header + 兩頁表格），給單筆與批次共用"""
    def v(k): return h(r.get(k, "") or "")
    gname = h(get_group_name(r.get("source_group_id", "")))
    created = h((r.get("created_at", "") or "")[:10])

    import json as _json
    try:
        debt_data = _json.loads(r.get("debt_list", "") or "[]") if r.get("debt_list") else []
    except Exception:
        debt_data = []
    debt_html = ""
    if debt_data:
        debt_html = '<table style="width:100%;border-collapse:collapse;font-size:13px;margin-top:8px;">'
        debt_html += '<tr style="background:#e8e2da;font-weight:600;"><th style="padding:6px;border:1px solid #bbb;">貸款商家</th><th style="padding:6px;border:1px solid #bbb;">金額</th><th style="padding:6px;border:1px solid #bbb;">期數/已繳</th><th style="padding:6px;border:1px solid #bbb;">月繳</th><th style="padding:6px;border:1px solid #bbb;">剩餘</th><th style="padding:6px;border:1px solid #bbb;">日期</th><th style="padding:6px;border:1px solid #bbb;">動保</th></tr>'
        for d in debt_data:
            debt_html += f'<tr><td style="padding:5px;border:1px solid #bbb;">{h(d.get("co",""))}</td><td style="padding:5px;border:1px solid #bbb;">{h(d.get("lo",""))}</td><td style="padding:5px;border:1px solid #bbb;">{h(d.get("pe",""))}/{h(d.get("pa",""))}</td><td style="padding:5px;border:1px solid #bbb;">{h(d.get("mo",""))}</td><td style="padding:5px;border:1px solid #bbb;">{h(d.get("re",""))}</td><td style="padding:5px;border:1px solid #bbb;">{h(d.get("da",""))}</td><td style="padding:5px;border:1px solid #bbb;">{h(d.get("dy",""))}</td></tr>'
        debt_html += '</table>'

    lsame = r.get("live_same_as_reg", "") == "1"
    live_addr = f'{v("reg_city")}{v("reg_district")}{v("reg_address")}' if lsame else f'{v("live_city")}{v("live_district")}{v("live_address")}'
    reg_addr = f'{v("reg_city")}{v("reg_district")}{v("reg_address")}'
    company_addr = f'{v("company_city")}{v("company_district")}{v("company_address")}'
    note_html = "<tr><th>備註</th><td colspan='3'>" + v("eval_note") + "</td></tr>" if r.get("eval_note") else ""
    debt_section = "<div style='font-size:13px;font-weight:700;color:#3a3530;margin-bottom:8px;'>負債明細</div>" + debt_html if debt_html else ""

    return f"""<div class="header">
  <div><div class="header-name">{v("customer_name")}</div><div class="header-sub">群組：{gname}　建立日期：{created}</div></div>
  <div style="font-size:13px;color:#c8bfb5;font-weight:600;">客戶資料表</div>
</div>
<table>
<tr class="sec"><td colspan="4">基本資料</td></tr>
<tr><th>姓名</th><td>{v("customer_name")}</td><th>身分證</th><td>{v("id_no")}</td></tr>
<tr><th>出生日期</th><td>{v("birth_date")}</td><th>行動電話</th><td>{v("phone")}</td></tr>
<tr><th>Email</th><td>{v("email")}</td><th>LINE ID</th><td>{v("line_id")}</td></tr>
<tr><th>婚姻</th><td>{v("marriage")}</td><th>學歷</th><td>{v("education")}</td></tr>
<tr class="sec"><td colspan="4">身分證發證</td></tr>
<tr><th>發證日期</th><td>{v("id_issue_date")}</td><th>發證地</th><td>{v("id_issue_place")}</td></tr>
<tr><th>換補發</th><td colspan="3">{v("id_issue_type")}</td></tr>
<tr class="sec"><td colspan="4">地址資料</td></tr>
<tr><th>戶籍地址</th><td colspan="3">{reg_addr}</td></tr>
<tr><th>戶籍電話</th><td>{v("reg_phone")}</td><th>現住電話</th><td>{v("live_phone")}</td></tr>
<tr><th>住家地址</th><td colspan="3">{live_addr}</td></tr>
<tr><th>居住狀況</th><td>{v("live_status")}</td><th>居住時間</th><td>{v("live_years")}年{v("live_months")}月</td></tr>
<tr class="sec"><td colspan="4">職業資料</td></tr>
<tr><th>公司名稱</th><td colspan="3">{v("company_name_detail")}</td></tr>
<tr><th>公司電話</th><td>{v("company_phone_area")}-{v("company_phone_num")}</td><th>職稱</th><td>{v("company_role")}</td></tr>
<tr><th>年資</th><td>{v("company_years")}年</td><th>月薪</th><td>{v("company_salary")}萬</td></tr>
<tr><th>公司地址</th><td colspan="3">{company_addr}</td></tr>
<tr><th>行業</th><td colspan="3">{v("company_industry")}</td></tr>
<tr class="sec"><td colspan="4">聯絡人</td></tr>
<tr><th>聯絡人1</th><td>{v("contact1_name")}（{v("contact1_relation")}）</td><th>電話</th><td>{v("contact1_phone")}</td></tr>
<tr><th>知情</th><td>{v("contact1_known")}</td><th>聯絡人2</th><td>{v("contact2_name")}（{v("contact2_relation")}）</td></tr>
<tr><th>電話</th><td>{v("contact2_phone")}</td><th>知情</th><td>{v("contact2_known")}</td></tr>
</table>
<div style="page-break-before:always;margin-top:20px;"></div>
<table>
<tr class="sec"><td colspan="4">貸款諮詢</td></tr>
<tr><th>資金需求</th><td>{v("eval_fund_need")}</td><th>近三月送件</th><td>{v("eval_sent_3m")}</td></tr>
<tr><th>當鋪私設</th><td>{v("eval_alert")}</td><th>勞保</th><td>{v("eval_labor_ins")}</td></tr>
<tr><th>薪轉</th><td>{v("eval_salary_transfer")}</td><th>遲繳</th><td>{v("eval_late")} {v("eval_late_days")}天</td></tr>
<tr><th>罰單</th><td>{v("eval_fine")}</td><th>燃料稅</th><td>{v("eval_fuel_tax")}</td></tr>
<tr><th>信用卡</th><td>{v("eval_credit_card")}</td><th>動產</th><td>{v("eval_property")}</td></tr>
<tr><th>證照</th><td>{v("eval_license")}</td><th>車輛</th><td>{v("eval_vehicle")}</td></tr>
<tr><th>法學</th><td colspan="3">{v("eval_law")}</td></tr>
{note_html}
</table>
{debt_section}"""


_PDF_STYLE = """<style>
@media print { @page { size: A4 portrait; margin: 10mm 12mm; } .no-print { display: none !important; } }
* { box-sizing: border-box; margin: 0; padding: 0; }
body { font-family: 'Microsoft JhengHei', 'PingFang TC', sans-serif; background: #fff; color: #1a1a1a; font-size: 13px; padding: 20px; }
.header { background: #3a3530; color: #fff; padding: 16px 20px; border-radius: 8px; margin-bottom: 16px; display: flex; justify-content: space-between; align-items: center; }
.header-name { font-size: 22px; font-weight: 700; }
.header-sub { font-size: 12px; color: #c8bfb5; margin-top: 4px; }
table { width: 100%; border-collapse: collapse; margin-bottom: 16px; }
th, td { border: 1px solid #bbb; padding: 7px 10px; font-size: 13px; line-height: 1.5; }
th { background: #f0ebe4; color: #3a3020; font-weight: 700; width: 100px; white-space: nowrap; text-align: left; }
td { background: #fff; }
.sec { background: #3a3530; color: #fff; font-size: 12px; font-weight: 700; padding: 6px 10px; }
.sec td { background: #3a3530; color: #fff; font-weight: 700; }
</style>"""


@app.get("/customer-pdf-batch", response_class=HTMLResponse)
def customer_pdf_batch(request: Request, ids: str = ""):
    from fastapi.responses import RedirectResponse
    role = check_auth(request)
    if not role: return RedirectResponse("/login")
    id_list = [i.strip() for i in (ids or "").split(",") if i.strip()]
    if not id_list:
        return RedirectResponse("/pending-customers")
    conn = get_conn(); cur = conn.cursor()
    placeholders = ",".join(["?"] * len(id_list))
    cur.execute(f"SELECT * FROM customers WHERE case_id IN ({placeholders})", id_list)
    rows = cur.fetchall(); conn.close()
    # 依使用者選取順序排序
    row_map = {dict(r)["case_id"]: dict(r) for r in rows}
    ordered = [row_map[i] for i in id_list if i in row_map]
    if not ordered:
        return RedirectResponse("/pending-customers")

    bodies = []
    for idx, r in enumerate(ordered):
        if idx > 0:
            bodies.append('<div style="page-break-before:always;"></div>')
        bodies.append(_build_customer_pdf_body(r))
    body_html = "".join(bodies)

    return f"""<!DOCTYPE html><html lang="zh-TW"><head><meta charset="UTF-8">
<title>客戶資料批次列印（{len(ordered)} 筆）</title>
{_PDF_STYLE}
</head><body>
<div class="no-print" style="text-align:center;margin-bottom:16px;">
  <button onclick="window.print()" style="background:#4e7055;color:#fff;border:none;padding:10px 28px;border-radius:6px;font-size:14px;cursor:pointer;font-weight:600;">列印 / 存 PDF（共 {len(ordered)} 筆）</button>
  <button onclick="window.close()" style="background:#6a5e4e;color:#fff;border:none;padding:10px 28px;border-radius:6px;font-size:14px;cursor:pointer;font-weight:600;margin-left:8px;">關閉</button>
</div>
{body_html}
</body></html>"""


# =========================
# 下載 Excel 申請書
# =========================
@app.post("/adminb/save-signature")
async def adminb_save_signature(request: Request):
    role = check_auth(request)
    if not role or role not in ("admin", "adminB"):
        return JSONResponse({"ok": False, "error": "無權限"}, status_code=403)
    try:
        data = await request.json()
        case_id = data.get("case_id", "")
        sig_type = data.get("type", "")
        sig_data = data.get("data", "")
        if not case_id or not sig_type or not sig_data:
            return JSONResponse({"ok": False, "error": "缺少參數"})
        # sig_type 白名單檢查（防靜默接受非法值）
        if sig_type not in ("applicant", "legal_rep", "both"):
            return JSONResponse({"ok": False, "error": "sig_type 必須是 applicant/legal_rep/both"}, status_code=400)
        conn = get_conn(); cur = conn.cursor()
        if sig_type == "both":
            cur.execute("UPDATE customers SET signature_applicant=?, signature_legal_rep=? WHERE case_id=?",
                        (sig_data, sig_data, case_id))
        elif sig_type == "applicant":
            cur.execute("UPDATE customers SET signature_applicant=? WHERE case_id=?", (sig_data, case_id))
        else:  # legal_rep
            cur.execute("UPDATE customers SET signature_legal_rep=? WHERE case_id=?", (sig_data, case_id))
        conn.commit(); conn.close()
        return JSONResponse({"ok": True})
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)})


@app.get("/adminb/download-qiaomei")
def adminb_download_qiaomei(request: Request, case_id: str = ""):
    from fastapi.responses import StreamingResponse
    role = check_auth(request)
    if not role or role not in ("admin", "adminB"):
        return JSONResponse({"error": "無權限"}, status_code=403)
    if not case_id:
        return JSONResponse({"error": "缺少 case_id"}, status_code=400)
    try:
        conn = get_conn(); cur = conn.cursor()
        cur.execute("SELECT * FROM customers WHERE case_id=?", (case_id,))
        row = cur.fetchone(); conn.close()
        if not row:
            return JSONResponse({"error": "找不到客戶"}, status_code=404)
        r = dict(row)
        try:
            pdf_bytes = _fill_qiaomei_pdf(r)
        except Exception as gen_e:
            import traceback
            tb = traceback.format_exc()
            print(f"喬美 PDF 詳細錯誤:\n{tb}")
            return JSONResponse({"error": f"PDF 生成失敗：{str(gen_e)}"}, status_code=500)
        if not pdf_bytes:
            return JSONResponse({"error": "PDF 生成失敗：範本檔案不存在或內容為空"}, status_code=500)
        from urllib.parse import quote
        fname = quote(f"{r.get('customer_name','')}_喬美申請書.pdf")
        return StreamingResponse(
            io.BytesIO(pdf_bytes),
            media_type="application/pdf",
            headers={"Content-Disposition": f"attachment; filename*=UTF-8''{fname}"}
        )
    except Exception as e:
        import traceback; traceback.print_exc()
        return JSONResponse({"error": f"下載失敗：{str(e)}"}, status_code=500)


def _fill_qiaomei_pdf(r: dict) -> bytes:
    """填入客戶資料到喬美 PDF 範本（reportlab 疊加文字層）"""
    try:
        from pypdf import PdfReader, PdfWriter
        from reportlab.pdfgen import canvas
        from reportlab.pdfbase import pdfmetrics
        from reportlab.pdfbase.cidfonts import UnicodeCIDFont
        from reportlab.lib.pagesizes import A4
        import base64

        # 註冊中文字型
        try:
            pdfmetrics.registerFont(UnicodeCIDFont('STSong-Light'))
            font_name = 'STSong-Light'
        except Exception:
            font_name = 'Helvetica'

        _base = os.path.dirname(os.path.abspath(__file__))
        template_path = os.path.join(_base, "申請書", "喬美3-14萬.pdf")
        if not os.path.exists(template_path):
            return b""

        def v(k): return (r.get(k, "") or "").strip()

        # 公司電話組合（mobile 不顯示）
        co_area = v("company_phone_area")
        co_num = v("company_phone_num")
        if co_area == "mobile":
            co_area = ""
        # 手機（10碼 09 開頭）：不加區碼橫線，純數字
        combined = (co_area + co_num) if co_area else co_num
        if combined.startswith("09") and len(combined.replace("-","")) == 10:
            co_phone = combined.replace("-", "")
        else:
            co_phone = (co_area + "-" + co_num) if co_area and co_num else co_num

        live_same = v("live_same_as_reg") == "1"
        reg_addr = v("reg_city") + v("reg_district") + v("reg_address")
        live_addr = reg_addr if live_same else (v("live_city") + v("live_district") + v("live_address"))

        # 欄位座標表（PDF 點坐標，原點左下，y 反轉）
        # PDF page1 size: 612 x 859
        # 這些座標基於對 PDF 標籤位置的分析，可能需要微調
        page_h = 859
        def yp(top): return page_h - top  # PDF y 反轉

        # === 解析日期為年/月/日 ===
        def parse_ymd(d):
            if not d:
                return ("", "", "")
            parts = d.replace("-", "/").split("/")
            if len(parts) != 3:
                return ("", "", "")
            try:
                y = int(parts[0])
                if y >= 1911:
                    y -= 1911
                return (str(y), str(int(parts[1])), str(int(parts[2])))
            except Exception:
                return ("", "", "")
        b_y, b_m, b_d = parse_ymd(v("birth_date"))
        i_y, i_m, i_d = parse_ymd(v("id_issue_date"))

        from datetime import datetime as _dt
        now = _dt.now()
        ap_y = str(now.year - 1911)
        ap_m = str(now.month)
        ap_d = str(now.day)

        id_no_str = v("id_no").upper()
        qm_model = v("product_model")
        qm_imei = v("product_imei")

        # === 精準座標表（從用戶填好的範本 PDF 提取）===
        # 格式：(x, top, value)
        fields_p1 = [
            # 申請人姓名
            (105, 78, v("customer_name")),
            # 出生日期 年/月/日 (避開範本字 年=249.4 月=265.2 日=284.2)
            (240, 80, b_y),
            (258, 80, b_m),
            (275, 80, b_d),
            # 發證日期 年/月/日 (避開 年=115.4 月=137 日=160.3)
            (98, 129, i_y),
            (122, 129, i_m),
            (145, 129, i_d),
            # 發證地（短碼）
            (241, 128, v("id_issue_place")),
            # 親屬姓名 / 關係 / 電話
            (367, 147, v("contact1_name")),
            (416, 151, v("contact1_relation")),
            (504, 150, v("contact1_phone")),
            # 親友姓名 / 關係+電話
            (367, 196, v("contact2_name")),
            (416, 199, v("contact2_relation")),
            (501, 199, v("contact2_phone")),
            # 戶籍地址
            (107, 201, reg_addr),
            # 住宅地址（同戶籍時清空）
            (151, 223, live_addr if not live_same else ""),
            # E-mail
            (107, 250, v("email")),
            # 戶籍電話
            (101, 276, v("reg_phone")),
            # 行動電話
            (105, 300, v("phone")),
            # LINE ID
            (112, 373, v("line_id")),
            # 公司名稱（長字串自動換較小字體，由下方迴圈處理）
            # (98, 410, company_name_detail) — 用 _LONG_FIELDS 處理
            (188, 412, co_phone),
            (260, 413, v("company_phone_ext")),
            # 公司地址
            (105, 429, v("company_city") + v("company_district") + v("company_address")),
            # 職稱
            (105, 450, v("company_role")),
            # 年資 年 / 月（位置：年=229, 月=267）
            (229, 452, v("company_years")),
            (267, 451, v("company_months")),
            # 月薪
            (110, 472, v("company_salary")),
            # 居住時間 年/月
            (119.8, 326, v("live_years")),
            (174.8, 326, v("live_months")),
            # 信用卡：發卡銀行 / 卡號 / 有效日期年/月 (同卡號行 y=323) / 額度 (下行 y=353)
            (371, 285, v("adminb_credit_bank")),
            (359, 323, v("adminb_credit_no")),
            # 有效日期年：在「年」字 (538.1) 之前
            (505, 323, v("adminb_credit_exp").split("/")[0] if "/" in v("adminb_credit_exp") else ""),
            # 有效日期月：在「年」(538) 和「月」(569) 之間
            (548, 323, v("adminb_credit_exp").split("/")[1] if "/" in v("adminb_credit_exp") else v("adminb_credit_exp")),
            # 額度：在「萬」字 (423.9) 之前的空白
            (380, 353, v("adminb_credit_limit")),
            # 商品名稱（手機型號）+ IMEI
            (93, 565, qm_model),
            (80, 588, qm_imei),
        ]

        sig_app = r.get("signature_applicant", "") or ""
        sig_leg = r.get("signature_legal_rep", "") or ""

        def draw_signature(c, sig_data, x, y, w, h):
            if not sig_data or not sig_data.startswith("data:image"):
                return
            try:
                _, b64 = sig_data.split(",", 1)
                img_bytes = base64.b64decode(b64)
                from reportlab.lib.utils import ImageReader
                img = ImageReader(io.BytesIO(img_bytes))
                c.drawImage(img, x, y, width=w, height=h, mask='auto')
            except Exception as ex:
                print(f"draw_signature error: {ex}")

        # === Page 1 疊加層 ===
        # 用 PDF 實際 mediabox（612.288 x 858.898）
        from pypdf import PdfReader as _PR
        _r = _PR(template_path)
        p1_w = float(_r.pages[0].mediabox.width)
        p1_h = float(_r.pages[0].mediabox.height)
        def yp1(top): return p1_h - top  # PDF y 反轉

        overlay1 = io.BytesIO()
        c1 = canvas.Canvas(overlay1, pagesize=(p1_w, p1_h))
        DEFAULT_FONT_SIZE = 10

        # 一般欄位：pdfplumber top → PDF baseline ≈ top + ascent (≈7.5 for 10pt)
        ASCENT = 7.5
        c1.setFont(font_name, DEFAULT_FONT_SIZE)
        for x, top, val in fields_p1:
            if not val:
                continue
            c1.drawString(x, yp1(top + ASCENT), str(val))

        # === 公司名稱：超長自動縮字體（避免覆蓋電話欄位）===
        co_name = v("company_name_detail")
        if co_name:
            # 公司名稱欄寬約 90pt（x=98 到 188）
            max_w = 88
            fs = DEFAULT_FONT_SIZE
            # CJK 字符約 1 字 = 字體大小寬度
            while fs > 6 and c1.stringWidth(co_name, font_name, fs) > max_w:
                fs -= 0.5
            c1.setFont(font_name, fs)
            c1.drawString(98, yp1(410 + ASCENT), co_name)
            c1.setFont(font_name, DEFAULT_FONT_SIZE)

        # === 身分證字號 10 格（每格中心填一字）===
        # 範本中顯示位置 y=106，x=107 起，每格 19pt
        if id_no_str:
            c1.setFont(font_name, 11)
            for i, ch in enumerate(id_no_str[:10]):
                cx = 107 + i * 19
                c1.drawString(cx, yp1(106 + 8), ch)
            c1.setFont(font_name, DEFAULT_FONT_SIZE)

        # === 勾選方塊（畫 ✓ 勾號；rect 7x7pt）===
        def tick_box(box_x, box_top):
            cx = box_x + 3.5
            ct = box_top + 3.5
            cy = yp1(ct)
            c1.setLineWidth(1.4)
            # 勾號：左下短斜→右上長斜
            c1.line(cx - 3.5, cy, cx - 1, cy - 3)
            c1.line(cx - 1, cy - 3, cx + 4, cy + 4)

        # 換補發：範本沒有 rect，直接在標籤前畫 ✓
        def tick_inline(x, top):
            cy = yp1(top + 3)
            c1.setLineWidth(1.4)
            c1.line(x, cy, x + 2.5, cy - 3)
            c1.line(x + 2.5, cy - 3, x + 7, cy + 4)

        # 換補發 (3 vertical rect at x=268.8): 初=122.2, 換=129.3, 補=136.5
        issue_kind = v("id_issue_type") or v("id_issue_kind")
        if "初" in issue_kind:
            tick_box(268.8, 122.2)
        elif "換" in issue_kind:
            tick_box(268.8, 129.3)
        elif "補" in issue_kind:
            tick_box(268.8, 136.5)

        # 婚姻 (y=153.5): 已婚=105.9, 未婚=142.5, 離婚=177.2, 喪偶=212.6
        marriage = v("marriage")
        if "已婚" in marriage:
            tick_box(105.9, 153.5)
        elif "未婚" in marriage:
            tick_box(142.5, 153.5)
        elif "離" in marriage:
            tick_box(177.2, 153.5)
        elif "喪" in marriage:
            tick_box(212.6, 153.5)

        # 教育 (y=172.7 上排: 碩士=105.9, 大學=153.5; y=183.0 下排: 高中=105.9, 國中=153.5)
        edu = v("education")
        if "研究" in edu or "碩" in edu:
            tick_box(105.9, 172.7)
        elif any(k in edu for k in ("大學","專科")):
            tick_box(153.5, 172.7)
        elif "高中" in edu or "高職" in edu:
            tick_box(105.9, 183.0)
        else:
            tick_box(153.5, 183.0)

        # 居住狀況 (y=342.6: 自有=105.9, 父母/配偶=152.4, 親屬=197.8;
        #          y=353.3: 租屋=105.9, 宿舍=134.5, 借住=162.8)
        live = v("live_status")
        if "自有" in live:
            tick_box(105.9, 342.6)
        elif "父母" in live or "配偶" in live:
            tick_box(152.4, 342.6)
        elif "親屬" in live or "親" in live:
            tick_box(197.8, 342.6)
        elif "租" in live:
            tick_box(105.9, 353.3)
        elif "宿舍" in live:
            tick_box(134.5, 353.3)
        elif "借" in live:
            tick_box(162.8, 353.3)

        # 同戶籍勾選 (y=226.0 x=105.9)
        if live_same:
            tick_box(105.9, 226.0)

        # 申請人正楷簽名（簽名框 rect: x=25.1 top=787.2 268.6x46.7，標籤在 y=791）
        # 簽名圖放在標籤右側，框內
        draw_signature(c1, sig_app, 130, yp1(830), 150, 38)
        # 法定代理人不簽

        c1.showPage()
        c1.save()
        overlay1.seek(0)

        # === Page 2 疊加層 ===
        p2_w = float(_r.pages[1].mediabox.width) if len(_r.pages) >= 2 else 541.68
        p2_h = float(_r.pages[1].mediabox.height) if len(_r.pages) >= 2 else 745.68
        def yp2(top): return p2_h - top
        overlay2 = io.BytesIO()
        c2 = canvas.Canvas(overlay2, pagesize=(p2_w, p2_h))
        # 立約定書人簽名 → 標籤右側（微調對齊）
        draw_signature(c2, sig_app, 320, yp2(728), 110, 18)
        # 日期：今天民國年（範本位置 y=719, x=433/472/505）
        c2.setFont(font_name, 10)
        c2.drawString(433, yp2(719 + 10), ap_y)
        c2.drawString(472, yp2(719 + 10), ap_m)
        c2.drawString(505, yp2(719 + 10), ap_d)
        c2.showPage()
        c2.save()
        overlay2.seek(0)

        # 用 pikepdf 合併（pypdf 對此範本會把所有字元重複輸出）
        # 必須傳 Rectangle 強制使用 mediabox 而非 trimbox（範本 trimbox 會造成 ~10pt 偏移）
        import pikepdf
        src = pikepdf.open(template_path)
        ov1_pdf = pikepdf.open(overlay1)
        rect1 = pikepdf.Rectangle(0, 0, p1_w, p1_h)
        src.pages[0].add_overlay(ov1_pdf.pages[0], rect1)
        if len(src.pages) >= 2:
            ov2_pdf = pikepdf.open(overlay2)
            rect2 = pikepdf.Rectangle(0, 0, p2_w, p2_h)
            src.pages[1].add_overlay(ov2_pdf.pages[0], rect2)

        out = io.BytesIO()
        src.save(out)
        out.seek(0)
        return out.getvalue()
    except Exception as e:
        import traceback; traceback.print_exc()
        print(f"喬美 PDF 生成錯誤: {e}")
        raise  # 讓上層 catch 到具體錯誤


@app.get("/adminb/download-excel")
def adminb_download_excel(request: Request, case_id: str = ""):
    from fastapi.responses import StreamingResponse
    try:
        return _do_download_excel(request, case_id)
    except Exception as e:
        print(f"Download Excel error: {e}")
        import traceback; traceback.print_exc()
        return JSONResponse({"error": f"下載失敗：{str(e)}"}, status_code=500)


def _fill_excel_template(template_path: str, cell_map: dict) -> bytes:
    """
    Fill customer data into Excel template by modifying ONLY sharedStrings.xml.
    All other files (styles, formulas, calcChain, comments, VML, validations) stay untouched.

    cell_map: {"C4": "王小明", "E4": "A123456789", ...}
    Empty string values will clear the cell's text.
    """
    import zipfile
    import re as _re

    with open(template_path, "rb") as f:
        original_bytes = f.read()

    orig_zip = zipfile.ZipFile(io.BytesIO(original_bytes), 'r')
    try:
        return _fill_excel_inner(orig_zip, original_bytes, cell_map, _re)
    finally:
        orig_zip.close()


def _fill_excel_inner(orig_zip, original_bytes, cell_map, _re):
    import zipfile

    # Step 1: Find the FIRST visible sheet's XML file via workbook.xml + rels
    sheet_xml_name = None
    try:
        wb_xml = orig_zip.read('xl/workbook.xml').decode('utf-8')
        rels_xml = orig_zip.read('xl/_rels/workbook.xml.rels').decode('utf-8')
        # Get first sheet's r:id from workbook.xml
        first_sheet = _re.search(r'<sheet\s+name="[^"]*"\s+sheetId="\d+"[^>]*r:id="([^"]+)"', wb_xml)
        if first_sheet:
            rid = first_sheet.group(1)
            # Find the target file for this r:id in rels
            rel_match = _re.search(r'Id="' + _re.escape(rid) + r'"[^>]*Target="([^"]+)"', rels_xml)
            if rel_match:
                target = rel_match.group(1)
                sheet_xml_name = 'xl/' + target if not target.startswith('/') else target.lstrip('/')
    except Exception:
        pass
    # Fallback: if above fails, scan all sheets for the one that has our target cells
    if not sheet_xml_name or sheet_xml_name not in orig_zip.namelist():
        target_cells = set(cell_map.keys())
        for name in sorted(orig_zip.namelist()):
            if _re.match(r'xl/worksheets/sheet\d+\.xml$', name):
                xml_content = orig_zip.read(name).decode('utf-8')
                # Check if this sheet has any of our target cells
                found = sum(1 for c in target_cells if f'r="{c}"' in xml_content)
                if found > len(target_cells) // 2:  # More than half match
                    sheet_xml_name = name
                    break

    if not sheet_xml_name:
        return original_bytes

    sheet_xml = orig_zip.read(sheet_xml_name).decode('utf-8')

    # Parse cells: find <c r="XX" ... t="s"><v>INDEX</v></c>
    # Build map: cell_ref -> shared_string_index
    cell_to_ss_idx = {}
    for m in _re.finditer(r'<c\s+r="([A-Z]+\d+)"[^>]*\s+t="s"[^>]*>\s*<v>(\d+)</v>', sheet_xml):
        cell_ref = m.group(1)
        ss_idx = int(m.group(2))
        cell_to_ss_idx[cell_ref] = ss_idx

    # Step 2: Parse sharedStrings.xml
    ss_xml_name = 'xl/sharedStrings.xml'
    if ss_xml_name not in orig_zip.namelist():
        return original_bytes

    ss_xml = orig_zip.read(ss_xml_name).decode('utf-8')

    # Find all <si>...</si> blocks
    si_blocks = list(_re.finditer(r'(<si>)(.*?)(</si>)', ss_xml, _re.DOTALL))

    # Step 3: 分類 cell_map
    # 策略：不修改現有 shared string（避免破壞下拉選單），
    # 而是新增新的 shared string 並修改 sheet XML 中的引用索引
    ss_cell_changes = {}  # cell_ref -> new_value
    direct_changes = {}   # cell_ref -> new_value
    formula_recalc = []   # cells whose cached <v> should be cleared (force Excel recalc)
    for cell_ref, new_value in cell_map.items():
        if new_value is None:
            continue
        if new_value == "__FORMULA_RECALC__":
            formula_recalc.append(cell_ref)
            continue
        # 強制轉字串（int/float 也要能寫入）
        if not isinstance(new_value, str):
            new_value = str(new_value) if new_value != 0 else ("0" if new_value == 0 else "")
        if cell_ref in cell_to_ss_idx:
            ss_cell_changes[cell_ref] = new_value if new_value else ""
        else:
            direct_changes[cell_ref] = new_value if new_value else ""

    if not ss_cell_changes and not direct_changes and not formula_recalc:
        return original_bytes

    # Step 4: 新增 shared strings + 修改 sheet XML 引用
    if not si_blocks:
        # sharedStrings.xml 沒有 <si> 區塊，跳過 shared string 修改
        return original_bytes
    si_list = [m.group(0) for m in si_blocks]
    new_sheet_xml = sheet_xml
    for cell_ref, new_value in ss_cell_changes.items():
        escaped = new_value.replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;')
        # 新增一個 shared string
        new_idx = len(si_list)
        si_list.append(f'<si><t>{escaped}</t></si>')
        # 修改 sheet XML 中該 cell 的 <v> 指向新索引
        old_idx = cell_to_ss_idx[cell_ref]
        cell_pattern = _re.compile(
            r'(<c\s+r="' + _re.escape(cell_ref) + r'"[^>]*>)\s*<v>' + str(old_idx) + r'</v>\s*(</c>)')
        m = cell_pattern.search(new_sheet_xml)
        if m:
            new_sheet_xml = new_sheet_xml[:m.start()] + m.group(1) + f'<v>{new_idx}</v>' + m.group(2) + new_sheet_xml[m.end():]
    # 更新 sharedStrings：count 是引用總次數（保留原值+新增數），uniqueCount 是 si 數量
    first_si = si_blocks[0].start()
    last_si_end = si_blocks[-1].end()
    new_si_content = ''.join(si_list)
    new_unique = len(si_list)
    added = new_unique - len(si_blocks)  # 新增的 si 數量
    header_xml = ss_xml[:first_si]
    # 取得原始 count 並加上新增數（每個新 si 至少被引用 1 次）
    orig_count_m = _re.search(r'count="(\d+)"', header_xml)
    if orig_count_m:
        new_count = int(orig_count_m.group(1)) + added
        header_xml = _re.sub(r'count="\d+"', f'count="{new_count}"', header_xml)
    header_xml = _re.sub(r'uniqueCount="\d+"', f'uniqueCount="{new_unique}"', header_xml)
    new_ss_xml = header_xml + new_si_content + ss_xml[last_si_end:]

    # Step 4b: Modify sheet XML for direct-value cells
    if not ss_cell_changes:
        new_sheet_xml = sheet_xml  # 只有在 Step 4 沒修改時才重新賦值
    for cell_ref, new_value in direct_changes.items():
        escaped_val = new_value.replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;') if new_value else ""
        # Case 1: 已有值 <c r="G18" s="48"><v>5.4</v></c>
        pattern1 = _re.compile(r'(<c\s+r="' + _re.escape(cell_ref) + r'"[^>]*>)\s*<v>[^<]*</v>\s*(</c>)')
        m = pattern1.search(new_sheet_xml)
        if m:
            if new_value:
                new_sheet_xml = new_sheet_xml[:m.start()] + m.group(1) + f'<v>{escaped_val}</v>' + m.group(2) + new_sheet_xml[m.end():]
            else:
                # 清空：移除 <v> 標籤
                attrs_match = _re.search(r'<c\s+r="' + _re.escape(cell_ref) + r'"([^>]*)>', m.group(1))
                if attrs_match:
                    attrs = _re.sub(r'\s+t="[^"]*"', '', attrs_match.group(1))
                    new_cell = f'<c r="{cell_ref}"{attrs}/>'
                    new_sheet_xml = new_sheet_xml[:m.start()] + new_cell + new_sheet_xml[m.end():]
            continue
        # Case 2: 空的 self-closing <c r="B7" s="77"/>
        pattern2 = _re.compile(r'<c\s+r="' + _re.escape(cell_ref) + r'"([^/]*)/>')
        m2 = pattern2.search(new_sheet_xml)
        if m2 and new_value:  # 只在有值時寫入
            attrs = m2.group(1).strip()
            new_cell = f'<c r="{cell_ref}" {attrs} t="inlineStr"><is><t>{escaped_val}</t></is></c>'
            new_sheet_xml = new_sheet_xml[:m2.start()] + new_cell + new_sheet_xml[m2.end():]

    # Step 4c: 清除公式儲存格的快取值（強制 Excel 重算）
    # <c r="C39" s="99" t="str"><f>C11</f><v>陳耀晨</v></c>
    # → <c r="C39" s="99"><f>C11</f></c>
    if formula_recalc:
        if not ss_cell_changes and not direct_changes:
            new_sheet_xml = sheet_xml
        for cell_ref in formula_recalc:
            # 匹配公式儲存格：含 <f>...</f> 和 <v>...</v>
            pattern = _re.compile(
                r'<c\s+r="' + _re.escape(cell_ref) + r'"([^>]*)>(.*?)<v>[^<]*</v>(.*?)</c>',
                _re.DOTALL
            )
            m = pattern.search(new_sheet_xml)
            if m:
                attrs = m.group(1)
                # 移除 t="..." 屬性（避免類型衝突）
                attrs = _re.sub(r'\s+t="[^"]*"', '', attrs)
                inner = m.group(2) + m.group(3)
                new_cell = f'<c r="{cell_ref}"{attrs}>{inner}</c>'
                new_sheet_xml = new_sheet_xml[:m.start()] + new_cell + new_sheet_xml[m.end():]

    # Step 5: Repackage ZIP
    output_buf = io.BytesIO()
    output_zip = zipfile.ZipFile(output_buf, 'w', zipfile.ZIP_DEFLATED)

    for item in orig_zip.infolist():
        if item.filename == ss_xml_name:
            output_zip.writestr(item, new_ss_xml.encode('utf-8'))
        elif item.filename == sheet_xml_name and (ss_cell_changes or direct_changes or formula_recalc):
            output_zip.writestr(item, new_sheet_xml.encode('utf-8'))
        else:
            output_zip.writestr(item, orig_zip.read(item.filename))

    output_zip.close()
    output_buf.seek(0)
    return output_buf.getvalue()


def _do_download_excel(request: Request, case_id: str):
    from fastapi.responses import StreamingResponse
    role = check_auth(request)
    if not role or role not in ("admin", "adminB"):
        return JSONResponse({"error": "無權限"}, status_code=403)
    if not case_id:
        return JSONResponse({"error": "缺少 case_id"}, status_code=400)

    conn = get_conn(); cur = conn.cursor()
    cur.execute("SELECT * FROM customers WHERE case_id=?", (case_id,))
    row = cur.fetchone(); conn.close()
    if not row:
        return JSONResponse({"error": "找不到客戶"}, status_code=404)

    r = dict(row)
    plans = (r.get("adminb_selected_plans", "") or "").split(",")
    plans = [p.strip() for p in plans if p.strip()]

    if not plans:
        return JSONResponse({"error": "尚未勾選任何方案，請先勾選並儲存"}, status_code=400)

    _base = os.path.dirname(os.path.abspath(__file__))
    PLAN_TEMPLATE_MAP = {
        "亞太商品": os.path.join(_base, "申請書", "亞太商品範本.xlsx"),
        "亞太機車15萬": os.path.join(_base, "申請書", "亞太15萬機車範本.xlsx"),
        "亞太機車25萬": os.path.join(_base, "申請書", "亞太15萬機車範本.xlsx"),
        "亞太工會機車": os.path.join(_base, "申請書", "亞太工會範本.xlsx"),
        "和裕機車": os.path.join(_base, "申請書", "和裕維力貸機車範本).xlsx"),
        "和裕商品": os.path.join(_base, "申請書", "和裕維力貸商品範本.xlsx"),
        "第一": os.path.join(_base, "申請書", "第一申請書範本.xlsx"),
        "貸就補": os.path.join(_base, "申請書", "貸就補範本.xlsx"),
        "21機車12萬": os.path.join(_base, "申請書", "21機車申請書範本xlsx.xlsx"),
        "21機車25萬": os.path.join(_base, "申請書", "21機25萬範本.xlsx"),
        "21商品": os.path.join(_base, "申請書", "21商品範本.xlsx"),
        # TXT 填寫表方案
        "麻吉機車": os.path.join(_base, "申請書", "麻吉機車申請書.txt"),
        "麻吉手機": os.path.join(_base, "申請書", "麻吉手機申請書.txt"),
        "手機分期": os.path.join(_base, "申請書", "手機分期.txt"),
        "分貝機車": os.path.join(_base, "申請書", "分貝機車.txt"),
        "分貝汽車": os.path.join(_base, "申請書", "分貝汽車.txt"),
        "21汽車":   os.path.join(_base, "申請書", "21汽車申請書.txt"),
    }

    files_to_zip = []
    name = r.get("customer_name", "")

    # Build cell data mapping for each plan type
    def _build_cell_map(plan_name, r):
        """Build {cell_ref: value} map. Empty string = clear cell."""
        CITY_TO_CODE = {
            "台北市": "北市", "新北市": "新北市", "桃園市": "桃市", "台中市": "中市",
            "台南市": "南市", "高雄市": "高市", "基隆市": "基市", "新竹市": "竹市",
            "新竹縣": "竹縣", "苗栗縣": "苗縣", "彰化縣": "彰縣", "南投縣": "投縣",
            "雲林縣": "雲縣", "嘉義市": "嘉市", "嘉義縣": "嘉縣", "屏東縣": "屏縣",
            "宜蘭縣": "宜縣", "花蓮縣": "花縣", "台東縣": "東縣", "澎湖縣": "澎縣",
            "金門縣": "金門", "連江縣": "連江",
        }

        def v(key): return (r.get(key, "") or "").strip()

        name = v("customer_name")
        id_no = v("id_no")
        birth = v("birth_date")
        phone = v("phone")
        email = v("email")
        line_id = v("line_id")
        marriage = v("marriage")
        education = v("education")
        id_date = v("id_issue_date")
        id_place = v("id_issue_place")
        id_type = v("id_issue_type")
        reg_addr = v("reg_city") + v("reg_district") + v("reg_address")
        live_same = v("live_same_as_reg") == "1"
        live_addr = reg_addr if live_same else (v("live_city") + v("live_district") + v("live_address"))
        live_status = v("live_status")
        live_years = v("live_years")
        company = v("company_name_detail") or v("company")
        co_phone_area = v("company_phone_area")
        co_phone_num = v("company_phone_num")
        # 手機（mobile）→ 區碼留空，只用號碼
        if co_phone_area == "mobile":
            co_phone_area = ""
        co_phone = (co_phone_area + "-" + co_phone_num) if co_phone_area and co_phone_num else co_phone_num
        co_role = v("company_role")
        co_years = v("company_years")
        co_salary = v("company_salary")
        co_addr = v("company_city") + v("company_district") + v("company_address")
        c1_name = v("contact1_name")
        c1_rel = v("contact1_relation")
        c1_phone = v("contact1_phone")
        c1_known = v("contact1_known")
        c2_name = v("contact2_name")
        c2_rel = v("contact2_relation")
        c2_phone = v("contact2_phone")
        c2_known = v("contact2_known")

        if plan_name == "貸就補":
            # F5 發證地下拉
            valid_lj_place = ["北市","新北市","北縣","基市","宜縣","桃市","桃縣","竹市","竹縣",
                              "苗縣","中市","中縣","嘉市","彰縣","投縣","雲縣","嘉縣","南市",
                              "南縣","高市","高縣","屏縣","東縣","花縣","澎縣","連江","金門"]
            lj_place_map = {"台北市":"北市","桃園市":"桃市","台中市":"中市","台南市":"南市",
                            "高雄市":"高市","基隆市":"基市","新竹市":"竹市","新竹縣":"竹縣",
                            "苗栗縣":"苗縣","彰化縣":"彰縣","南投縣":"投縣","雲林縣":"雲縣",
                            "嘉義市":"嘉市","嘉義縣":"嘉縣","屏東縣":"屏縣","宜蘭縣":"宜縣",
                            "花蓮縣":"花縣","台東縣":"東縣","澎湖縣":"澎縣","金門縣":"金門",
                            "連江縣":"連江","新北市":"新北市"}
            lj_place = lj_place_map.get(id_place, id_place)
            lj_place = lj_place if lj_place in valid_lj_place else ""

            # G5 換補發
            id_type_val = id_type if id_type in ("初發", "換發", "補發") else ""

            # 日期民國 7 位（無斜線）
            def to_roc7(d):
                if not d: return ""
                parts = d.replace("-", "/").split("/")
                if len(parts) != 3: return ""
                try:
                    y = int(parts[0])
                    if y >= 1911:
                        y -= 1911
                    return f"{str(y).zfill(3)}{parts[1].zfill(2)}{parts[2].zfill(2)}"
                except Exception:
                    return ""
            birth_roc7 = to_roc7(birth)
            id_date_roc7 = to_roc7(id_date)

            # 現住電話：優先 live_phone，無則用 reg_phone
            live_phone_lj = v("live_phone") or v("reg_phone")

            # 月薪 E11 純數字（4萬→40000）
            try:
                sal_c = co_salary.replace("萬","").replace(",","").strip() if co_salary else ""
                sn = float(sal_c) if sal_c else 0
                if 0 < sn < 1000:
                    e11_val = int(sn * 10000)
                elif sn >= 1000:
                    e11_val = int(sn)
                else:
                    e11_val = ""
            except Exception:
                e11_val = ""

            # 年資 G11 年數 / I11 月數（純數字）
            try:
                g11_val = int(float(co_years)) if co_years else 0
                co_mos_lj = v("company_months") or "0"
                i11_val = int(float(co_mos_lj)) if co_mos_lj and co_mos_lj != "0" else 0
            except Exception:
                g11_val = 0
                i11_val = 0

            return {
                "C4": name, "E4": id_no, "J4": phone,
                "C5": birth_roc7, "E5": id_date_roc7,
                "F5": lj_place, "G5": id_type_val,
                "J5": live_phone_lj,
                "C7": reg_addr,
                "C8": reg_addr if live_same else live_addr,
                "C9": co_addr,
                "C10": company, "G10": co_phone_area + co_phone_num,
                "J10": v("company_phone_ext"),
                "C11": co_role, "E11": e11_val,
                "G11": g11_val, "I11": i11_val,
                "C18": c1_name, "E18": c1_rel, "H18": c1_phone,
                "C19": c2_name, "E19": c2_rel, "H19": c2_phone,
                # 商品名稱/型號（J7/J8）從貸就補補充資料 lj_pname/lj_pmodel
                "J7": v("adminb_product_name"),
                "J8": v("adminb_product_model"),
            }

        elif plan_name in ("和裕機車", "和裕商品"):
            # === 發證地（F13）必須匹配下拉 ===
            id_place_code = CITY_TO_CODE.get(id_place, id_place)

            # === 婚姻（C13）===
            marriage_val = marriage if marriage in ("已婚", "未婚") else ""

            # === 學歷（C14）必須匹配下拉 ===
            valid_edu_hr = ["高中職", "專科、大學", "研究所以上", "其他"]
            edu_map = {"高中/職": "高中職", "高中": "高中職", "高職": "高中職",
                       "專科/大學": "專科、大學", "大學": "專科、大學", "專科": "專科、大學",
                       "研究所以上": "研究所以上", "研究所": "研究所以上"}
            edu_val = edu_map.get(education, education)
            edu_val = edu_val if edu_val in valid_edu_hr else ""

            # === 公司電話（H17）格式：02-27189890 或 0989-615422 ===
            if co_phone_area and co_phone_num:
                co_phone_fmt = co_phone_area + "-" + co_phone_num
            elif co_phone_num:
                co_phone_fmt = co_phone_num
            else:
                co_phone_fmt = ""

            # === 年資（I18）格式：N年M月（年月字保留，缺少數字以空白代替）===
            co_mos = v("company_months") or "0"
            try:
                yr_int = int(float(co_years)) if co_years else 0
                mo_int = int(float(co_mos)) if co_mos and co_mos != "0" else 0
                yr_str = str(yr_int) if yr_int > 0 else ""
                mo_str = str(mo_int) if mo_int > 0 else ""
                if yr_str or mo_str:
                    years_fmt = f"{yr_str}年{mo_str}月"
                else:
                    years_fmt = ""
            except Exception:
                years_fmt = co_years

            # === 月薪（I19）格式：整數顯示整數（5萬），小數顯示小數（4.5萬）===
            try:
                sal_raw = float(co_salary) if co_salary else 0
                if sal_raw >= 1000:
                    sal_wan = round(sal_raw / 10000, 1)
                else:
                    sal_wan = sal_raw
                if sal_wan:
                    if sal_wan == int(sal_wan):
                        sal_fmt = f"{int(sal_wan)}萬"  # 5萬（非 5.0萬）
                    else:
                        sal_fmt = f"{sal_wan}萬"        # 4.5萬
                else:
                    sal_fmt = ""
            except Exception:
                sal_fmt = co_salary

            # === 行業（G19）必須匹配下拉，從 adminB ===
            valid_hr_ind = ["服務業","餐飲業","科技業","軍人","運輸業","倉儲業","金融業","製造業",
                           "營造業","電商網拍業","農狩林牧業","礦業","漁業","證券期貨業","保險業",
                           "不動產業","公教人員","水電燃氣業","通信業","社團個人服務","其它"]
            # 優先用和裕專屬欄位 adminb_hr_industry，fallback 到 adminb_industry（亞太用）並轉換
            hr_industry = v("adminb_hr_industry") or v("adminb_industry") or ""
            ind_map = {"製造業":"製造業","餐飲與服務業":"服務業","建築與營造":"營造業",
                       "軍警與公教":"公教人員","科技與資訊":"科技業","運輸與物流":"運輸業",
                       "金融與保險業":"金融業","批發與零售業":"服務業","醫療與教育":"服務業",
                       "農林漁牧業":"農狩林牧業","自由職業":"其它"}
            hr_ind_val = ind_map.get(hr_industry, hr_industry)
            hr_ind_val = hr_ind_val if hr_ind_val in valid_hr_ind else ""  # 無值清空

            # === 手機電信（C20）：不在下拉清單就選「其他」 ===
            carrier_raw = v("carrier") or ""
            valid_carriers = ["中華電信", "遠傳電信", "台灣大哥大", "其他"]
            if carrier_raw in valid_carriers:
                carrier_val = carrier_raw
            elif carrier_raw:
                carrier_val = "其他"
            else:
                carrier_val = ""

            # === 聯絡人關係（C24/F24）：常用詞轉換 ===
            valid_hr_rels = ["父母","配偶","子女","兄弟姊妹","朋友","同事","其他"]
            def map_hr_relation(raw):
                if not raw: return ""
                if raw in valid_hr_rels: return raw
                for k in ["夫妻","妻","夫","老婆","老公","太太","先生","配偶"]:
                    if k in raw: return "配偶"
                for k in ["媽媽","爸爸","母親","父親","媽","爸","母","父"]:
                    if k in raw: return "父母"
                for k in ["兒子","女兒","兒","女","子女"]:
                    if k in raw: return "子女"
                for k in ["哥哥","姊姊","姐姐","弟弟","妹妹","哥","姊","姐","弟","妹","兄","兄弟姊妹"]:
                    if k in raw: return "兄弟姊妹"
                for k in ["朋友","友","同學"]:
                    if k in raw: return "朋友"
                for k in ["同事"]:
                    if k in raw: return "同事"
                return raw  # 無法轉換則原值帶入
            c1_rel_hr = map_hr_relation(c1_rel)
            c2_rel_hr = map_hr_relation(c2_rel)

            # === 聯絡人知情（D22/G22）智能判別 ===
            valid_known = ["知情", "保密"]
            def map_known(raw):
                if not raw: return ""
                if raw in valid_known: return raw
                if "知情" in raw or "可知" in raw: return "知情"
                if "保密" in raw or "不知" in raw or "無可" in raw: return "保密"
                return ""
            c1_known_val = map_known(c1_known)
            c2_known_val = map_known(c2_known)

            # === 聯絡人電話格式：0955-389338 ===
            def fmt_phone(p):
                p = (p or "").replace("-", "").replace(" ", "")
                if len(p) == 10 and p.startswith("09"):
                    return p[:4] + "-" + p[4:]
                return p
            c1_ph_fmt = fmt_phone(c1_phone)
            c2_ph_fmt = fmt_phone(c2_phone)

            # === 行動電話（F14）也要格式化 ===
            phone_fmt = fmt_phone(phone)

            result = {
                "C11": name,
                "C39": "__FORMULA_RECALC__",  # 戶名是公式 =C11，清快取值強制重算
                "F11": id_no,
                "C12": birth, "F12": (id_date + " " + id_type) if id_date else "",
                "C13": marriage_val, "F13": id_place_code,
                "C14": edu_val, "F14": phone_fmt,
                "C15": reg_addr, "G15": v("reg_phone") or "",
                "C16": live_addr, "G16": v("live_phone") or "", "H16": "同戶籍" if live_same else "",
                "C17": line_id, "F17": "家用",  # 資金用途固定家用
                "H17": co_phone_fmt,
                "C18": company, "G18": co_role, "I18": years_fmt,
                "C19": co_addr, "I19": sal_fmt,
                "C23": c1_name, "F23": c2_name,
                "C24": c1_rel_hr, "F24": c2_rel_hr,
                "C25": c1_ph_fmt, "F25": c2_ph_fmt,
                "D22": c1_known_val, "G22": c2_known_val,
                # 撥款資訊（無填寫則清空，戶名 C39 不動有公式）
                "C37": v("adminb_bank") or "",
                "C38": v("adminb_branch") or "",
                # 商品資訊（無填寫則清空）
                "C42": v("adminb_product") or "",
                "F42": v("adminb_model") or "",
                # 行業/電信
                "G19": hr_ind_val,
                "C20": carrier_val,
            }
            return result

        elif plan_name in ("亞太商品", "亞太機車15萬", "亞太工會機車", "亞太機車25萬"):
            # === 日期轉換：民國→西元 ===
            def roc_to_ad(date_str):
                """民國日期轉西元：086/12/15 → 1989/12/15"""
                if not date_str:
                    return ""
                parts = date_str.replace("-", "/").split("/")
                if len(parts) == 3:
                    try:
                        y = int(parts[0])
                        if y < 200:  # 是民國年
                            y += 1911
                        return f"{y}/{parts[1].zfill(2)}/{parts[2].zfill(2)}"
                    except Exception:
                        pass
                return date_str  # 已是西元或無法轉換

            birth_ad = roc_to_ad(birth)
            id_date_ad = roc_to_ad(id_date)

            # === 公司電話區碼（B18）：手機時選 0 ===
            if co_phone_area == "mobile" or (co_phone_num and co_phone_num.startswith("09")):
                co_phone_area = "0"

            # === 資金用途（B5）從 adminB 補充資料 ===
            fund_use = v("adminb_fund_use")
            valid_funds = ["I-1教育費","I-2醫藥費","I-3出國旅遊","I-4創業","II-1購買交通工具","II-2購買手機","II-3購買3C產品","III-1交友","III-2健身&醫美","III-3美容課程","IV-1個人理財投資(含不動產、裝修、理財商品)","V-1生活周轉金","V-2整合負債(償還銀行/融資等)"]
            fund_val = fund_use if fund_use in valid_funds else ""

            # === 婚姻（B10）===
            marriage_val = marriage if marriage in ("未婚", "已婚") else ""

            # === 教育程度（D10）必須完全匹配下拉 ===
            valid_edu_at = ["小學/國中", "高中/職", "專科/大學", "研究所以上"]
            if education in valid_edu_at:
                edu_val = education
            else:
                # 轉換常見格式
                edu_at_map = {"國中": "小學/國中", "國小": "小學/國中", "小學": "小學/國中",
                              "高中": "高中/職", "高職": "高中/職", "高中職": "高中/職",
                              "大學": "專科/大學", "專科": "專科/大學", "專科、大學": "專科/大學",
                              "研究所": "研究所以上", "碩士": "研究所以上", "博士": "研究所以上",
                              "其他": "小學/國中"}
                edu_val = edu_at_map.get(education, "")

            # === 發證狀態（F11）===
            id_type_val = id_type if id_type in ("初發", "補發", "換發") else ""

            # === 居住狀況（B14）：智能判別對照 ===
            valid_live = ("自有", "配偶", "親屬", "租屋", "宿舍")
            def map_live_status(raw):
                if not raw: return ""
                if raw in valid_live: return raw
                # 自有相關
                for k in ["自有","本人","名下","自宅","自有房屋"]:
                    if k in raw: return "自有"
                # 配偶相關
                for k in ["配偶","老公","老婆","太太","先生","夫","妻"]:
                    if k in raw: return "配偶"
                # 親屬（含父母/兄弟姊妹/家人/父母名下/租屋/宿舍）
                for k in ["父母","媽媽","爸爸","母親","父親","親屬","親戚","家人","兄","弟","姊","妹","祖","外公","外婆","租","宿舍","借住","寄住"]:
                    if k in raw: return "親屬"
                return ""
            live_status_val = map_live_status(live_status)

            # === 行業（E17）從 adminB，無則不填（D17 是標籤「行業類別」不能動） ===
            industry = v("adminb_industry")
            valid_industries = ["餐飲與服務業","製造業","建築與營造","軍警與公教","科技與資訊","運輸與物流","金融與保險業","批發與零售業","醫療與教育","農林漁牧業","自由職業","其他"]
            industry_val = industry if industry in valid_industries else None  # None = 不動原值

            # === 職務（G17）從 adminB，無選填則清空 ===
            valid_roles = ["行政與內勤","勞力與現場","銷售與業務","財務與專業","技術與工程","教學與醫護","管理與經營","自營與自由"]
            at_role = v("adminb_role")
            role_val = at_role if at_role in valid_roles else ""  # 不在下拉清單中則清空

            # === 聯絡人關係（D21）：常用詞轉換 ===
            valid_rels = ["父母","配偶","子女","兄姊","弟妹","祖父母","旁系血親","姻親","朋友","其他"]
            def map_relation(raw):
                if not raw: return ""
                if raw in valid_rels: return raw
                # 配偶相關
                for k in ["夫妻","妻","夫","老婆","老公","太太","先生","配偶"]:
                    if k in raw: return "配偶"
                # 父母相關
                for k in ["媽媽","爸爸","母親","父親","媽","爸","母","父"]:
                    if k in raw: return "父母"
                # 子女
                for k in ["兒子","女兒","兒","女","子女"]:
                    if k in raw: return "子女"
                # 兄姊
                for k in ["哥哥","姊姊","姐姐","哥","姊","姐","兄"]:
                    if k in raw: return "兄姊"
                # 弟妹
                for k in ["弟弟","妹妹","弟","妹"]:
                    if k in raw: return "弟妹"
                # 祖父母
                for k in ["祖父","祖母","爺爺","奶奶","外公","外婆","祖父母"]:
                    if k in raw: return "祖父母"
                # 旁系血親
                for k in ["叔","伯","姑","舅","姨","堂","表","侄","甥","旁系"]:
                    if k in raw: return "旁系血親"
                # 姻親
                for k in ["公公","婆婆","岳父","岳母","媳","婿","姻"]:
                    if k in raw: return "姻親"
                # 朋友
                for k in ["朋友","友","同事","同學"]:
                    if k in raw: return "朋友"
                return ""
            c1_rel_val = map_relation(c1_rel)

            # === 公司電話區碼（B18）===
            valid_areas = ["0","02","03","037","04","049","05","06","07","08","089","082","083"]
            co_area = co_phone_area if co_phone_area in valid_areas else ""

            # === 年資（G18）：5年6個月→5.6 ===
            try:
                yrs = float(co_years) if co_years else 0
                mos_raw = v("company_months") or "0"
                mos = float(mos_raw) if mos_raw and mos_raw != "0" else 0
                if mos > 0:
                    years_decimal = str(int(yrs)) + "." + str(int(mos))
                else:
                    years_decimal = str(int(yrs)) if yrs == int(yrs) else str(yrs)
            except Exception:
                years_decimal = co_years

            # === 月薪（H18）：58000→5.8萬 ===
            try:
                sal_raw = float(co_salary) if co_salary else 0
                if sal_raw >= 1000:  # 如果是元，轉換為萬
                    sal_wan = round(sal_raw / 10000, 1)
                else:  # 已經是萬
                    sal_wan = sal_raw
                salary_str = str(sal_wan) if sal_wan else ""
            except Exception:
                salary_str = co_salary

            # === 城市：B12/B13/B19 用完整名稱（桃園市非桃市）===
            reg_city_full = v("reg_city")  # 直接用完整名稱
            live_city_full = reg_city_full if live_same else v("live_city")
            co_city_full = v("company_city")

            # === 發證地（D11）用短碼 ===
            id_place_code = CITY_TO_CODE.get(id_place, id_place)

            result = {
                "B5": fund_val if fund_val else None,  # None = 不動
                "B9": name, "D9": id_no, "F9": birth_ad,
                "B10": marriage_val, "D10": edu_val,
                "B11": id_date_ad, "D11": id_place_code, "F11": id_type_val,
                "B12": reg_city_full, "C12": v("reg_district"), "D12": v("reg_address"),
                "B13": live_city_full,
                "C13": v("live_district") if not live_same else v("reg_district"),
                "D13": v("live_address") if not live_same else v("reg_address"),
                "B14": live_status_val, "D14": live_years,
                "B15": phone,
                "B16": email,
                "B17": company,
                "B18": co_area, "C18": co_phone_num, "E18": v("company_phone_ext"),
                "G18": years_decimal, "H18": salary_str,
                "B19": co_city_full, "C19": v("company_district"), "D19": v("company_address"),
                "B21": c1_name, "D21": c1_rel_val,
                "B25": c1_phone,
            }
            # 行業/職務：有值填入，無值清空
            result["E17"] = industry_val if industry_val else ""
            result["G17"] = role_val if role_val else ""
            # 車輛資料：只有機車範本才填，亞太商品不填寫車輛欄位
            if plan_name in ("亞太機車15萬", "亞太工會機車", "亞太機車25萬"):
                result["B7"] = v("adminb_vehicle_type") or ""   # 車輛型式
                result["D7"] = v("adminb_engine_no") or ""      # 引擎號碼
                result["F7"] = v("adminb_displacement") or ""   # 排氣量
                result["H7"] = v("adminb_color") or ""          # 顏色
                result["K2"] = v("adminb_brand") or ""          # 廠牌
                result["K3"] = v("vehicle_plate") or ""         # 牌照號碼
            return result

        elif plan_name == "第一":
            # 換發類型（J6）
            id_type_val = id_type if id_type in ("初發", "換發", "補發") else ""

            # 換證地點（G6）下拉匹配
            valid_dy1_place = ["北市","新北市","北縣","基市","宜縣","桃市","桃縣","竹市","竹縣",
                               "苗縣","中市","中縣","嘉市","彰縣","投縣","雲縣","嘉縣","南市",
                               "南縣","高市","高縣","屏縣","東縣","花縣","澎縣","連江","金門"]
            dy1_place_map = {"台北市":"北市","桃園市":"桃市","台中市":"中市","台南市":"南市",
                             "高雄市":"高市","基隆市":"基市","新竹市":"竹市","新竹縣":"竹縣",
                             "苗栗縣":"苗縣","彰化縣":"彰縣","南投縣":"投縣","雲林縣":"雲縣",
                             "嘉義市":"嘉市","嘉義縣":"嘉縣","屏東縣":"屏縣","宜蘭縣":"宜縣",
                             "花蓮縣":"花縣","台東縣":"東縣","澎湖縣":"澎縣","金門縣":"金門",
                             "連江縣":"連江","新北市":"新北市"}
            raw_place = id_place
            dy1_place = dy1_place_map.get(raw_place, raw_place)
            dy1_place = dy1_place if dy1_place in valid_dy1_place else ""

            # 日期轉民國無斜線 7 位 (113/12/24 → 1131224, 87/02/24 → 0870224)
            def to_roc_7digit(d):
                if not d: return ""
                parts = d.replace("-", "/").split("/")
                if len(parts) != 3: return ""
                try:
                    y = int(parts[0])
                    if y >= 1911:
                        y -= 1911
                    return f"{str(y).zfill(3)}{parts[1].zfill(2)}{parts[2].zfill(2)}"
                except Exception:
                    return ""
            birth_roc = to_roc_7digit(birth)
            id_date_roc = to_roc_7digit(id_date)

            # 公司電話：區碼+號碼+分機 (0223570707#722865)
            co_ext = v("company_phone_ext")
            if co_phone_area and co_phone_num:
                dy1_co_phone = co_phone_area + co_phone_num
            else:
                dy1_co_phone = co_phone_num
            if dy1_co_phone and co_ext:
                dy1_co_phone += "#" + co_ext

            # 年資：M8 年數、O8 月數（純數字）
            try:
                m8_val = int(float(co_years)) if co_years else 0
                co_mos_dy1 = v("company_months") or "0"
                o8_val = int(float(co_mos_dy1)) if co_mos_dy1 and co_mos_dy1 != "0" else 0
            except Exception:
                m8_val = 0
                o8_val = 0

            # 月薪：M9 純數字（4.5萬→45000）
            try:
                sal_c = co_salary.replace("萬","").replace(",","").strip() if co_salary else ""
                sn = float(sal_c) if sal_c else 0
                if 0 < sn < 1000:
                    m9_val = str(int(sn * 10000))
                elif sn >= 1000:
                    m9_val = str(int(sn))
                else:
                    m9_val = ""
            except Exception:
                m9_val = ""

            # 關係智能判別（第一下拉清單）
            valid_dy1_rels = ["父母","夫妻","兄弟姐妹","子女","朋友","同事","祖父母","外祖父母",
                              "孫子女","姪子女","岳父母","女婿","堂兄弟姊","表兄弟姊","伯父母",
                              "叔/嬸","舅/舅媽","姨/姨丈","姑/姑丈","公婆媳","大伯小叔","姑嫂","妯娌"]
            def map_dy1_rel(raw):
                if not raw: return ""
                if raw in valid_dy1_rels: return raw
                # 夫妻
                for k in ["夫妻","配偶","老公","老婆","太太","先生","夫","妻"]:
                    if k in raw: return "夫妻"
                # 父母
                for k in ["媽媽","爸爸","母親","父親","媽","爸","母","父"]:
                    if k in raw: return "父母"
                # 子女
                for k in ["兒子","女兒","兒","女","子女"]:
                    if k in raw: return "子女"
                # 兄弟姐妹
                for k in ["哥哥","姊姊","姐姐","弟弟","妹妹","哥","姊","姐","兄","弟","妹"]:
                    if k in raw: return "兄弟姐妹"
                # 外祖父母
                for k in ["外公","外婆","外祖"]:
                    if k in raw: return "外祖父母"
                # 祖父母
                for k in ["祖父","祖母","爺爺","奶奶"]:
                    if k in raw: return "祖父母"
                # 岳父母
                for k in ["岳父","岳母"]:
                    if k in raw: return "岳父母"
                # 公婆媳
                for k in ["公公","婆婆","媳"]:
                    if k in raw: return "公婆媳"
                # 朋友/同事
                if "同事" in raw: return "同事"
                if "朋友" in raw or "友" in raw or "同學" in raw: return "朋友"
                return ""
            c1_rel_dy1 = map_dy1_rel(c1_rel)
            c2_rel_dy1 = map_dy1_rel(c2_rel)

            return {
                "B5": name, "G5": id_no,
                "B6": id_date_roc, "G6": dy1_place, "J6": id_type_val,
                "B7": birth_roc, "G7": phone,
                "B8": v("reg_phone"), "G8": v("live_phone"),
                "B9": reg_addr,
                "B10": "同上" if live_same else live_addr,
                "M5": company,
                "T6": dy1_co_phone,
                "M7": co_addr,
                "M8": m8_val, "O8": o8_val,
                "M9": m9_val,
                "M13": c1_name, "Q13": c1_rel_dy1, "T13": c1_phone,
                "M14": c2_name, "Q14": c2_rel_dy1, "T14": c2_phone,
            }

        elif plan_name in ("21機車12萬", "21機車25萬", "21商品"):
            # 21 發證地短碼
            CITY_TO_21CODE = {
                "台北市": "北市", "新北市": "北縣", "桃園市": "桃縣", "台中市": "中縣",
                "台南市": "南縣", "高雄市": "高縣", "基隆市": "基市", "新竹市": "竹市",
                "新竹縣": "竹縣", "苗栗縣": "苗縣", "彰化縣": "彰縣", "南投縣": "投縣",
                "雲林縣": "雲縣", "嘉義市": "嘉市", "嘉義縣": "嘉縣", "屏東縣": "屏縣",
                "宜蘭縣": "宜縣", "花蓮縣": "花縣", "台東縣": "東縣", "澎湖縣": "澎縣",
                "金門縣": "金門", "連江縣": "連江",
            }
            id_place_code = CITY_TO_21CODE.get(id_place, id_place) if id_place else ""
            # 補換發（F4 用短碼，實際是 G4）：初發/補發/換發
            id_type_val = id_type if id_type in ("初發", "補發", "換發") else ""
            # 21 地址城市用全名且「台」改「臺」
            def city_to_21full(c):
                if not c: return ""
                return c.replace("台", "臺")
            reg_city_21 = city_to_21full(v("reg_city"))
            live_city_21 = reg_city_21 if live_same else city_to_21full(v("live_city"))

            # 從範本載入 D 欄下拉選項（郵遞區號+鄉鎮區）做智能匹配
            def match_21_district(city_full, district):
                """把區名匹配到「郵遞區號  區名」格式"""
                if not district:
                    return ""
                try:
                    import openpyxl as _opx
                    import warnings as _w
                    _w.filterwarnings("ignore")
                    _tpl = PLAN_TEMPLATE_MAP.get(plan_name)
                    if not _tpl or not os.path.exists(_tpl):
                        return district
                    _wb = _opx.load_workbook(_tpl, data_only=True)
                    if '工作表3' not in _wb.sheetnames:
                        return district
                    _ws = _wb['工作表3']
                    # 找城市對應的欄位
                    city_col = None
                    for c_idx in range(1, 30):
                        col_letter = _opx.utils.get_column_letter(c_idx)
                        if _ws[f'{col_letter}1'].value == city_full:
                            city_col = col_letter
                            break
                    if not city_col:
                        return district
                    # 遍歷該欄尋找匹配
                    for r_idx in range(2, 40):
                        v_cell = _ws[f'{city_col}{r_idx}'].value
                        if v_cell and district in str(v_cell):
                            return str(v_cell)
                    return district
                except Exception:
                    return district

            reg_district_21 = match_21_district(reg_city_21, v("reg_district"))
            live_district_21 = reg_district_21 if live_same else match_21_district(live_city_21, v("live_district"))

            # 日期轉民國格式（帶斜線）
            def to_roc_slash(d):
                if not d: return ""
                parts = d.replace("-", "/").split("/")
                if len(parts) != 3: return d
                try:
                    y = int(parts[0])
                    if y >= 1911:  # 西元
                        y -= 1911
                    return f"{str(y).zfill(3)}/{parts[1].zfill(2)}/{parts[2].zfill(2)}"
                except Exception:
                    return d
            birth_roc = to_roc_slash(birth)
            id_date_roc = to_roc_slash(id_date)

            # 關係智能判別（21 下拉清單）
            valid_21_rels = ["配偶","父母","子女","兄弟姐妹","祖父母","外祖父母","孫子女","外孫子女","配偶之父母","配偶之兄弟姐妹","其他親屬","負責人"]
            def map_21_rel(raw):
                if not raw: return ""
                if raw in valid_21_rels: return raw
                # 配偶
                for k in ["夫妻","配偶","老公","老婆","太太","先生","夫","妻"]:
                    if k in raw: return "配偶"
                # 父母
                for k in ["媽媽","爸爸","母親","父親","媽","爸","母","父"]:
                    if k in raw: return "父母"
                # 子女
                for k in ["兒子","女兒","兒","女","子女"]:
                    if k in raw: return "子女"
                # 兄弟姐妹
                for k in ["哥哥","姊姊","姐姐","弟弟","妹妹","哥","姊","姐","兄","弟","妹"]:
                    if k in raw: return "兄弟姐妹"
                # 外祖父母
                for k in ["外公","外婆","外祖"]:
                    if k in raw: return "外祖父母"
                # 祖父母
                for k in ["祖父","祖母","爺爺","奶奶"]:
                    if k in raw: return "祖父母"
                # 配偶之父母
                for k in ["公公","婆婆","岳父","岳母"]:
                    if k in raw: return "配偶之父母"
                # 其他親屬（含朋友/同事/同學）
                for k in ["朋友","友","同事","同學","叔","伯","姑","舅","姨","堂","表"]:
                    if k in raw: return "其他親屬"
                return ""
            c1_rel_21 = map_21_rel(c1_rel)
            c2_rel_21 = map_21_rel(c2_rel)

            # 年資 G10：偵測儲存格類型，數值型填純數字，文字型填「N年」
            # 月份 H10：固定文字「N月」或「月」
            try:
                yr_int = int(float(co_years)) if co_years else 0
                co_mos_21 = v("company_months") or "0"
                mo_int = int(float(co_mos_21)) if co_mos_21 and co_mos_21 != "0" else 0
                # G10：25萬範本是數值型，其他是文字型
                if plan_name == "21機車25萬":
                    g10_val = str(yr_int) if yr_int > 0 else "0"
                else:
                    g10_val = f"{yr_int}年"
                h10_val = f"{mo_int}月" if mo_int > 0 else "月"
            except Exception:
                g10_val = co_years
                h10_val = "月"

            # 月薪 E10：填純數字（4.5萬→45000）
            try:
                sal_clean = co_salary.replace("萬","").replace(",","").strip() if co_salary else ""
                sal_num = float(sal_clean) if sal_clean else 0
                if sal_num > 0 and sal_num < 1000:  # 萬為單位
                    sal_e10 = str(int(sal_num * 10000))
                elif sal_num >= 1000:
                    sal_e10 = str(int(sal_num))
                else:
                    sal_e10 = ""
            except Exception:
                sal_e10 = co_salary

            return {
                "C3": name, "E3": id_no, "J3": phone,
                "C4": birth_roc, "E4": id_date_roc,
                "F4": id_place_code, "G4": id_type_val,
                "J4": v("live_phone"),
                # 戶籍地址：C6=縣市、D6=鄉鎮區、E6=詳細地址
                "C6": reg_city_21,
                "D6": reg_district_21,
                "E6": v("reg_address"),
                # 住家地址：同戶籍時用戶籍資料
                "C7": reg_city_21 if live_same else live_city_21,
                "D7": reg_district_21 if live_same else live_district_21,
                "E7": v("reg_address") if live_same else v("live_address"),
                "C8": email,
                "C9": company, "G9": co_phone_area + co_phone_num,
                "C10": co_role, "E10": sal_e10,
                "G10": g10_val, "H10": h10_val,
                "C17": c1_name, "E17": c1_rel_21, "H17": c1_phone, "K17": "保密" if c1_known == "保密" else "",
                "C18": c2_name, "E18": c2_rel_21, "H18": c2_phone, "K18": "保密" if c2_known == "保密" else "",
            }

        return {}  # Unknown plan - no data fill

    def _build_txt_content(template_path: str, r: dict) -> str:
        """讀範本逐行，依 label 關鍵字填入客戶資料；無對應的保留範本預設值"""
        def v(k): return (r.get(k, "") or "").strip()

        # ===== 組合常用欄位 =====
        reg_addr = v("reg_city") + v("reg_district") + v("reg_address")
        live_same = v("live_same_as_reg") == "1"
        live_addr = reg_addr if live_same else (v("live_city") + v("live_district") + v("live_address"))
        co_area = v("company_phone_area")
        if co_area == "mobile":
            co_area = ""
        co_num = v("company_phone_num")
        co_phone = (co_area + "-" + co_num) if co_area and co_num else co_num
        co_addr = v("company_city") + v("company_district") + v("company_address")
        # 麻吉商品廠牌/型號 優先用 adminb_mj_*，沒填則 fallback 到 adminb_product_*
        mj_brand = v("adminb_mj_brand") or v("adminb_product_name")
        mj_model = v("adminb_mj_model") or v("adminb_product_model")
        product_brand_model = (mj_brand + " " + mj_model).strip()
        # 居住時間
        live_time = ""
        if v("live_years") or v("live_months"):
            live_time = f'{v("live_years")}年{v("live_months")}月'
        # 年資
        co_years_full = v("company_years")
        if v("company_months"):
            co_years_full += f'年{v("company_months")}月'
        elif co_years_full:
            co_years_full += "年"

        # ===== label 關鍵字 → 值 對應 =====
        # 比對時用 startswith / in 容錯，越長 / 越精確的 key 排前面
        LABEL_MAP = [
            # 21汽車 專屬
            ("專案名稱", v("adminb_21car_project")),
            ("貸款金額.期數.月付金", f'{v("adminb_21car_amount")}/{v("adminb_21car_period")}/{v("adminb_21car_monthly")}'.strip("/")),
            ("貸款成數", "100%"),
            ("實際售價", v("adminb_21car_price")),
            ("車價參考來源", v("adminb_21car_ref_src")),
            ("天書", v("adminb_21car_ref_src")),
            ("參考車價金額", v("adminb_21car_ref_price")),
            ("選擇利率", v("adminb_21car_rate") or "16%"),
            ("是否有信用卡", v("adminb_21car_hascc")),
            # 個人資料
            ("申請人姓名", v("customer_name")),
            ("姓名", v("customer_name")),
            ("名字", v("customer_name")),
            ("身分證字號", v("id_no")),
            ("身分證號", v("id_no")),
            ("身分證", v("id_no")),
            ("出生年月日", v("birth_date")),
            ("出生日期", v("birth_date")),
            ("發證日期", v("id_issue_date")),
            ("發證地", v("id_issue_place")),
            ("發證狀態", v("id_issue_type")),
            ("換補發類別", v("id_issue_type")),
            # 聯絡
            ("E-mail", v("email")),
            ("Email", v("email")),
            ("電子信箱", v("email")),
            ("電子帳單", v("email")),
            ("本人手機電話", v("phone")),
            ("行動電話【電信業者】", f'{v("phone")} {v("carrier")}'.strip()),
            ("行動電話", v("phone")),
            ("門號電信業者", v("carrier")),
            ("電信業者", v("carrier")),
            ("LINE ID", v("line_id")),
            ("LINEID", v("line_id")),
            # 地址 / 電話
            ("戶籍地址", reg_addr),
            ("現居地地址", live_addr),
            ("現居地址", live_addr),
            ("現住地址", live_addr),
            ("住宅地址", live_addr),
            ("戶籍電話", v("reg_phone")),
            ("現住電話", v("live_phone")),
            ("住家電話", v("live_phone")),
            ("住宅電話", v("live_phone")),
            # 居住
            ("居住時間", live_time),
            ("居住狀況", v("live_status")),
            ("最高學歷", v("education")),
            ("教育程度", v("education")),
            ("婚姻狀態", v("marriage")),
            ("婚姻", v("marriage")),
            # 公司
            ("公司名稱", v("company_name_detail") or v("company")),
            ("公司地址", co_addr),
            ("公司分機", v("company_phone_ext")),
            ("公司電話", co_phone),
            ("公司職稱", v("company_role")),
            ("職稱", v("company_role")),
            ("年資【做多久】", co_years_full),
            ("工作幾年", co_years_full),
            ("年資", co_years_full),
            ("月薪多少", v("company_salary")),
            ("月薪", v("company_salary")),
            ("是否有信用卡", v("adminb_credit_bank")),
            # 商品
            ("商品廠牌/型號", product_brand_model),
            ("商品廠牌", product_brand_model),
            # 雜項
            ("可照會時間", v("adminb_contact_time")),
            ("資金用途", v("adminb_21car_fund") or v("adminb_fund_use")),
        ]

        # 聯絡人狀態：當前是否在「親屬/1聯絡人」或「朋友/2聯絡人」段
        contact_state = {"idx": 0}  # 0=未進入, 1=聯絡人1, 2=聯絡人2

        def find_value(label_text):
            """匹配 label，回傳 (是否找到, 值)。"""
            t = label_text.strip()
            nonlocal contact_state
            # 「1聯絡人」「2聯絡人」「親屬聯絡人」「朋友聯絡人」單獨當作姓名欄位
            if t == "1聯絡人" or "親屬聯絡人" in t:
                contact_state["idx"] = 1
                return True, v("contact1_name")
            if t == "2聯絡人" or "朋友聯絡人" in t:
                contact_state["idx"] = 2
                return True, v("contact2_name")

            # 聯絡人段內欄位
            if contact_state["idx"] == 1:
                if "聯絡人姓名" in t: return True, v("contact1_name")
                if t == "姓名": return True, v("contact1_name")
                if "電話" in t: return True, v("contact1_phone")
                if "關係" in t or "稱謂" in t: return True, v("contact1_relation")
                if "知情" in t: return True, v("contact1_known")
            if contact_state["idx"] == 2:
                if "聯絡人姓名" in t: return True, v("contact2_name")
                if t == "姓名": return True, v("contact2_name")
                if "電話" in t: return True, v("contact2_phone")
                if "關係" in t or "稱謂" in t: return True, v("contact2_relation")
                if "知情" in t: return True, v("contact2_known")

            # 一般欄位
            for key, val in LABEL_MAP:
                if key in t:
                    return True, val
            return False, ""

        def detect_state_marker(line):
            """處理沒有 : 的標記行，更新 contact_state。"""
            nonlocal contact_state
            s = line.strip()
            # 1.（...） / 1.姓名 / 親屬聯絡人 / 1聯絡人
            if s.startswith("1.") or "親屬聯絡人" in s or s == "1聯絡人":
                contact_state["idx"] = 1
            elif s.startswith("2.") or "朋友聯絡人" in s or s == "2聯絡人":
                contact_state["idx"] = 2

        # 讀範本，逐行處理
        import re as _re
        with open(template_path, "r", encoding="utf-8") as f:
            content = f.read()

        def process_line(line):
            # 拆分成 [text, sep, text, sep, ...] 保留分隔符
            parts = _re.split(r'([:：])', line)
            n_seps = sum(1 for p in parts if p in (":", "："))
            if n_seps == 0:
                return line

            if n_seps == 1:
                # 單一 label:value — 整行覆寫
                label = parts[0]
                sep = parts[1]
                found, val = find_value(label.strip())
                if found:
                    return f"{label}{sep}{val}"
                return line  # 沒對應 → 保留範本預設

            # 多個 label:value 在同一行（如聯絡人姓名/關係/電話）
            # 值區域 = 下一段的「前置空白」，剩下的是下一個 label
            out = ""
            i = 0
            while i < len(parts):
                if i + 1 < len(parts) and parts[i + 1] in (":", "："):
                    label = parts[i]
                    sep = parts[i + 1]
                    next_seg = parts[i + 2] if i + 2 < len(parts) else ""
                    ws_m = _re.match(r'^[ \t]*', next_seg)
                    ws = ws_m.group(0) if ws_m else ""
                    found, val = find_value(label.strip())
                    if found:
                        out += f"{label}{sep}{val}{ws}"
                    else:
                        out += f"{label}{sep}{ws}"
                    if i + 2 < len(parts):
                        parts[i + 2] = next_seg[len(ws):]
                    i += 2
                else:
                    out += parts[i]
                    i += 1
            return out

        out_lines = []
        for line in content.splitlines():
            detect_state_marker(line)
            out_lines.append(process_line(line))
        return "\n".join(out_lines)

    for plan in plans:
        template_path = PLAN_TEMPLATE_MAP.get(plan)
        if not template_path or not os.path.exists(template_path):
            continue

        try:
            if template_path.lower().endswith(".txt"):
                # TXT 填寫表方案
                txt_content = _build_txt_content(template_path, r)
                files_to_zip.append((f"{name}_{plan}.txt", txt_content.encode("utf-8")))
            else:
                cell_map = _build_cell_map(plan, r)
                if cell_map:
                    filled_bytes = _fill_excel_template(template_path, cell_map)
                else:
                    with open(template_path, "rb") as f:
                        filled_bytes = f.read()
                files_to_zip.append((f"{name}_{plan}.xlsx", filled_bytes))
        except Exception as e:
            print(f"Generate error for {plan}: {e}")
            import traceback; traceback.print_exc()
            continue

    if not files_to_zip:
        return JSONResponse({"error": "沒有可下載的範本檔案，請確認已勾選方案並儲存"}, status_code=400)

    from urllib.parse import quote

    if len(files_to_zip) == 1:
        fname, data = files_to_zip[0]
        encoded_fname = quote(fname)
        mime = "text/plain; charset=utf-8" if fname.lower().endswith(".txt") \
            else "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        return StreamingResponse(
            io.BytesIO(data),
            media_type=mime,
            headers={"Content-Disposition": f"attachment; filename*=UTF-8''{encoded_fname}"}
        )
    else:
        zip_buf = io.BytesIO()
        import zipfile
        with zipfile.ZipFile(zip_buf, 'w', zipfile.ZIP_DEFLATED) as zf:
            for fname, data in files_to_zip:
                zf.writestr(fname, data)
        zip_buf.seek(0)
        zip_fname = quote(f"{name}_申請書.zip")
        return StreamingResponse(
            zip_buf,
            media_type="application/zip",
            headers={"Content-Disposition": f"attachment; filename*=UTF-8''{zip_fname}"}
        )


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
          <div style="font-size:14px;font-weight:500">{h(g["group_name"])}</div>
          <input type="password" id="gpw_{h(g["group_id"])}" class="input" placeholder="輸入新密碼">
          <button onclick="changePw('group','{h(g["group_id"])}','gpw_{h(g["group_id"])}')" class="btn btn-primary">更新</button>
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
      <div style="background:#fff;border:1px solid #e5e7eb;border-radius:10px;padding:20px;margin-bottom:16px">
        <div style="font-size:13px;font-weight:600;color:#6b7280;margin-bottom:14px;padding-bottom:8px;border-bottom:1px solid #f3f4f6">🔓 解鎖帳號</div>
        <div id="locked-list" style="margin-bottom:14px"></div>
        <button class="btn" onclick="loadLocked()" style="font-size:12px">🔄 查看目前鎖定帳號</button>
      </div>
      <div style="background:#fffbeb;border:1px solid #fde68a;border-radius:8px;padding:12px 16px;font-size:13px;color:#92400e">
        ⚠️ 更新密碼後立即生效，請通知相關人員！
      </div>
    </div>
    <script>
    async function loadLocked(){{
      const r = await fetch('/admin/locked-accounts');
      const d = await r.json();
      const el = document.getElementById('locked-list');
      if(!d.locked || d.locked.length === 0){{
        el.innerHTML = '<div style="color:#6b7280;font-size:13px">目前沒有鎖定帳號 ✅</div>';
        return;
      }}
      el.innerHTML = d.locked.map(x => `
        <div style="display:flex;align-items:center;justify-content:space-between;padding:8px 0;border-bottom:1px solid #f3f4f6">
          <div>
            <span style="font-size:13px;font-weight:500">${{x.identifier}}</span>
            <span style="font-size:12px;color:#dc2626;margin-left:8px">失敗${{x.attempts}}次｜鎖定到 ${{x.locked_until}}</span>
          </div>
          <button onclick="unlockAccount('${{x.identifier}}')" class="btn" style="font-size:12px;color:#dc2626;border-color:#fca5a5">解鎖</button>
        </div>`).join('');
    }}
    async function unlockAccount(identifier){{
      const r = await fetch('/admin/unlock-account', {{
        method:'POST',
        headers:{{'Content-Type':'application/json'}},
        body:JSON.stringify({{identifier}})
      }});
      const d = await r.json();
      if(d.ok) loadLocked();
    }}
    window.onload = loadLocked;
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


@app.get("/admin/logs", response_class=HTMLResponse)
def admin_logs_page(request: Request, page: int = 1, q: str = ""):
    from fastapi.responses import RedirectResponse
    role = check_auth(request)
    if role != "admin":
        return RedirectResponse("/login")
    per_page = 50
    offset = (page - 1) * per_page
    conn = get_conn(); cur = conn.cursor()
    if q:
        cur.execute("SELECT COUNT(*) as c FROM case_logs WHERE customer_name LIKE ? OR message_text LIKE ?", (f"%{q}%", f"%{q}%"))
        total = cur.fetchone()["c"]
        cur.execute("SELECT * FROM case_logs WHERE customer_name LIKE ? OR message_text LIKE ? ORDER BY created_at DESC LIMIT ? OFFSET ?", (f"%{q}%", f"%{q}%", per_page, offset))
    else:
        cur.execute("SELECT COUNT(*) as c FROM case_logs")
        total = cur.fetchone()["c"]
        cur.execute("SELECT * FROM case_logs ORDER BY created_at DESC LIMIT ? OFFSET ?", (per_page, offset))
    logs = cur.fetchall(); conn.close()
    total_pages = (total + per_page - 1) // per_page
    rows_html = ""
    for log in logs:
        dt = (log["created_at"] or "")[:19].replace("T", " ")
        gname = get_group_name(log["from_group_id"])
        txt = (log["message_text"] or "")[:80]
        rows_html += f'<tr><td style="white-space:nowrap">{h(dt)}</td><td>{h(log["customer_name"])}</td><td>{h(gname)}</td><td style="word-break:break-all">{h(txt)}</td></tr>'
    if not rows_html:
        rows_html = '<tr><td colspan="4" style="text-align:center;color:#999;padding:20px">沒有操作紀錄</td></tr>'
    # 分頁
    q_param = f"&q={h(q)}" if q else ""
    pag = ""
    if total_pages > 1:
        pag = '<div style="display:flex;gap:6px;justify-content:center;margin-top:14px;flex-wrap:wrap">'
        if page > 1:
            pag += f'<a href="/admin/logs?page={page-1}{q_param}" class="btn" style="background:#e8e2da;color:#4a3e30;font-size:12px">上一頁</a>'
        pag += f'<span style="padding:6px 12px;font-size:12px;color:#6a5e4e">{page}/{total_pages}（共{total}筆）</span>'
        if page < total_pages:
            pag += f'<a href="/admin/logs?page={page+1}{q_param}" class="btn" style="background:#e8e2da;color:#4a3e30;font-size:12px">下一頁</a>'
        pag += '</div>'
    return f"""<!DOCTYPE html><html><head>{PAGE_CSS}<title>操作紀錄</title>
    <style>
    table{{width:100%;border-collapse:collapse;font-size:12px}}
    th{{background:#f0ebe4;color:#4a3e30;padding:8px;text-align:left;font-weight:600;position:sticky;top:0}}
    td{{padding:6px 8px;border-bottom:1px solid #ece8e2}}
    tr:hover{{background:#faf7f4}}
    </style></head><body>
    {make_topnav(role, "logs")}
    <div class="page">
      <h2 style="font-size:18px;font-weight:600;margin-bottom:14px">📝 操作紀錄</h2>
      <form method="get" action="/admin/logs" style="margin-bottom:12px;display:flex;gap:8px">
        <input name="q" value="{h(q)}" placeholder="搜尋客戶名或操作內容..." class="input" style="flex:1" autofocus>
        <button type="submit" class="btn btn-primary" style="font-size:12px">🔍 搜尋</button>
        {"<a href='/admin/logs' class='btn' style='background:#e8e2da;color:#4a3e30;font-size:12px'>清除</a>" if q else ""}
      </form>
      <div style="overflow-x:auto;background:#fff;border:1px solid #ddd5ca;border-radius:10px">
        <table><thead><tr><th>時間</th><th>客戶</th><th>來源群組</th><th>操作內容</th></tr></thead>
        <tbody>{rows_html}</tbody></table>
      </div>
      {pag}
    </div></body></html>"""


@app.get("/admin/download-db")
async def download_db(request: Request):
    role = check_auth(request)
    if role != "admin":
        return RedirectResponse("/login")
    import shutil
    backup_path = DB_PATH + ".backup"
    shutil.copy2(DB_PATH, backup_path)
    today = datetime.now().strftime("%Y%m%d_%H%M")
    return FileResponse(backup_path, filename=f"loan_system_{today}.db",
                        media_type="application/octet-stream")


# ── PDF export JS（必須在 init_db 之前定義，因為 /new-customer 頁面需要引用）──
_PDF_EXPORT_JS = '''
function exportPDF(){
  var grpMap=__GRP_MAP__;
  var grpRaw=gv('grp');
  var grpName=grpMap[grpRaw]||grpRaw;
  var name=gv('cname')||'-';
  var sa=document.getElementById('sameck').checked;
  var raddr=gv('rcity')+gv('rdist')+gv('raddr');
  var laddr=sa?raddr:gv('lcity')+gv('ldist')+gv('laddr');
  var ca=gv('carea'),cn2=gv('cnum'),ce=gv('cext');
  var ctel=(ca&&ca!=='mobile')?ca+'-'+cn2+(ce?' 分機'+ce:''):cn2+(ce?' 分機'+ce:'');
  var caddr=gv('ccity')+gv('cdist')+gv('caddr');
  var fine=parseFloat(gv('efine'))||0;
  var fuel=parseFloat(gv('efuel'))||0;
  var tot=fine+fuel;
  var today=new Date().toLocaleDateString('zh-TW');
  var lyear=gv('lyear'),lmon=gv('lmon');
  var ltime=(lyear||lmon)?(lyear||'0')+'年'+(lmon||'0')+'月':'-';
  var cyear=gv('cyear'),cmon=gv('cmon');
  var ctime=(cyear||cmon)?(cyear||'0')+'年'+(cmon||'0')+'月':'-';
  function r2(l1,v1,l2,v2){return '<tr><th>'+l1+'</th><td>'+(v1||'-')+'</td><th>'+l2+'</th><td>'+(v2||'-')+'</td></tr>';}
  function r1(l,v){return '<tr><th>'+l+'</th><td colspan="3">'+(v||'-')+'</td></tr>';}
  function s3(t,rows){return '<tr class="sh"><th colspan="4">'+t+'</th></tr>'+rows;}
  var trows='';
  trows+=s3('基本資料',
    r2('客戶姓名',gv('cname'),'身分證字號',gv('idno').toUpperCase())+
    r2('出生年月日',gv('birth'),'行動電話',gv('phone'))+
    r2('電信業者',gv('carrier'),'Email',gv('email'))+
    r2('LINE ID',gv('line'),'客戶FB',gv('fb'))+
    r2('婚姻狀態',gv('marry'),'最高學歷',gv('edu')));
  trows+=s3('身分證發證',
    r2('發證日期',gv('iddate'),'發證地',gv('idplace'))+
    r2('換補發類別',gv('idtype'),'',''));
  trows+=s3('地址資料',
    r1('戶籍地址',raddr)+
    r2('戶籍電話',gv('rphone'),'現住電話',gv('lphone'))+
    r1('住家地址',laddr)+
    r2('居住狀況',gv('lstatus'),'居住時間',ltime));
  trows+=s3('職業資料',
    r1('公司名稱',gv('cmpname'))+
    r2('公司電話',ctel,'職稱',gv('crole'))+
    r2('年資',ctime,'月薪',gv('csal')+'萬')+
    r1('公司地址',caddr));
  trows+=s3('聯絡人資料',
    r2('聯絡人1',gv('c1name')+'（'+gv('c1rel')+'）','電話1',gv('c1tel'))+
    r2('知情1',gv('c1know'),'聯絡人2',gv('c2name')+'（'+gv('c2rel')+'）')+
    r2('電話2',gv('c2tel'),'知情2',gv('c2know')));
  trows+=s3('貸款諮詢事項',
    r2('資金需求',gv('efund'),'近三月送件',gv('esent'))+
    r2('當鋪私設',gv('eprivate'),'勞保狀態',gv('elabor'))+
    r2('有無薪轉',gv('esal'),'有無證照',gv('elicense'))+
    r2('貸款遲繳',gv('elate'),'遲繳天數',gv('elateday'))+
    r2('罰單欠費','$'+fine.toLocaleString(),'燃料稅','$'+fuel.toLocaleString())+
    r2('欠費總額','$'+tot.toLocaleString(),'名下信用卡',gv('ecard'))+
    r2('動產/不動產',gv('eprop'),'法學',gv('elaw'))+
    (gv('enote')?r1('備註',gv('enote')):''));
  var drows='';
  for(var ii=1;ii<=dc;ii++){
    var eli=document.getElementById('d'+ii);if(!eli)continue;
    var co2=document.getElementById('dc'+ii);co2=co2?co2.value:'';if(!co2)continue;
    var lo2=document.getElementById('dl'+ii);lo2=lo2?lo2.value:'';
    var dp2=document.getElementById('dp'+ii);dp2=dp2?dp2.value.trim():'';
    var mo2=document.getElementById('dm'+ii);mo2=mo2?mo2.value:'';
    var dr2=document.getElementById('dr'+ii);dr2=dr2?dr2.textContent:'-';
    drows+='<tr><td>'+co2+'</td><td>'+lo2+'</td><td>'+dp2+'</td><td>'+mo2+'</td><td>'+dr2+'</td></tr>';
  }
  if(drows){trows+='<tr class="sh"><th colspan="4">負債明細</th></tr>'+
    '<tr><td colspan="4"><table style="width:100%;border-collapse:collapse;font-size:12px;">'+
    '<tr style="background:#e8e2da;font-weight:600;"><th style="padding:5px;border:1px solid #bbb;">貸款商家</th>'+
    '<th style="padding:5px;border:1px solid #bbb;">貸款金額</th>'+
    '<th style="padding:5px;border:1px solid #bbb;">期數/已繳</th>'+
    '<th style="padding:5px;border:1px solid #bbb;">月繳</th>'+
    '<th style="padding:5px;border:1px solid #bbb;">剩餘</th></tr>'+
    drows+'</table></td></tr>';}
  var css='*{box-sizing:border-box;margin:0;padding:0;}body{font-family:"Microsoft JhengHei","PingFang TC",sans-serif;background:#fff;color:#1a1a1a;font-size:13px;}'+
    '.hdr{background:#3a3530;color:#fff;padding:14px 20px;display:flex;justify-content:space-between;align-items:center;margin-bottom:14px;}'+
    '.hdr-name{font-size:22px;font-weight:700;}.hdr-sub{font-size:12px;color:#c8bfb5;margin-top:4px;}'+
    'table.main{width:100%;border-collapse:collapse;margin-bottom:16px;}'+
    'table.main th,table.main td{border:1px solid #bbb;padding:6px 10px;vertical-align:top;font-size:13px;line-height:1.5;}'+
    'table.main th{background:#f0ebe4;color:#3a3020;font-weight:600;width:90px;white-space:nowrap;}'+
    'table.main td{background:#fff;}tr.sh th{background:#3a3530;color:#fff;font-size:12px;font-weight:700;padding:5px 10px;}'+
    '@media print{@page{size:A4 portrait;margin:10mm 12mm;}.np{display:none!important;}}';
  var html='<!DOCTYPE html><html lang="zh-TW"><head><meta charset="UTF-8">'+
    '<title>客戶資料表 - '+name+'</title><style>'+css+'</style></head><body>'+
    '<div class="hdr"><div><div class="hdr-name">'+name+'</div>'+
    '<div class="hdr-sub">群組：'+grpName+'　日期：'+today+'</div></div>'+
    '<div style="font-size:13px;color:#c8bfb5;font-weight:600;">客戶資料表</div></div>'+
    '<div class="np" style="text-align:center;margin-bottom:12px;">'+
    '<button onclick="window.print()" style="background:#4e7055;color:#fff;border:none;padding:9px 28px;border-radius:6px;font-size:14px;cursor:pointer;font-weight:600;">🖨 列印 / 存PDF</button></div>'+
    '<table class="main">'+trows+'</table></body></html>';
  var w=window.open('','_blank','width=900,height=750');
  w.document.write(html);w.document.close();
}
'''


# =========================
# 啟動（模組載入時就初始化 DB，確保任何情況下都能正常運作）
# =========================
init_db()
seed_groups()


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", 10000)))
