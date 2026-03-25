from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
import uvicorn
import requests
import os
import re
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

# ===== 暫存資料 =====
customers = {}
pending_actions = {}


def today_str():
    return datetime.now().strftime("%m/%d")


def short_id():
    return str(uuid.uuid4())[:8]


def extract_name(text):
    text = text.strip()
    if not text:
        return ""
    first_line = text.splitlines()[0].strip()
    return first_line.split("（")[0].split(" ")[0].strip()


def extract_id_no(text):
    match = re.search(r"[A-Z][12]\d{8}", text.upper())
    return match.group(0) if match else ""


def extract_company(text):
    for c in COMPANY_LIST:
        if c in text:
            return c
    return ""


def get_group_name(group_id):
    if group_id == A_GROUP_ID:
        return "A群"
    if group_id == B_GROUP_ID:
        return "B群"
    if group_id == C_GROUP_ID:
        return "C群"
    return "未知群組"


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
        "messages": [
            {
                "type": "text",
                "text": text
            }
        ]
    }
    requests.post(url, headers=headers, json=data, timeout=10)


def reply_text(reply_token, text):
    if not CHANNEL_ACCESS_TOKEN:
        return

    url = "https://api.line.me/v2/bot/message/reply"
    headers = {
        "Authorization": f"Bearer {CHANNEL_ACCESS_TOKEN}",
        "Content-Type": "application/json"
    }
    data = {
        "replyToken": reply_token,
        "messages": [
            {
                "type": "text",
                "text": text
            }
        ]
    }
    requests.post(url, headers=headers, json=data, timeout=10)


def reply_quick_reply(reply_token, text, items):
    if not CHANNEL_ACCESS_TOKEN:
        return

    url = "https://api.line.me/v2/bot/message/reply"
    headers = {
        "Authorization": f"Bearer {CHANNEL_ACCESS_TOKEN}",
        "Content-Type": "application/json"
    }
    data = {
        "replyToken": reply_token,
        "messages": [
            {
                "type": "text",
                "text": text,
                "quickReply": {
                    "items": items
                }
            }
        ]
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


def split_multi_cases(text):
    """
    支援：
    1. 用單獨一行 "/" 分段
    2. 用空白行分段
    """
    text = text.strip()
    if not text:
        return []

    # 先把單獨一行 / 當作分隔
    text = re.sub(r"\n\s*/\s*\n", "\n<<<SPLIT>>>\n", text)
    # 再把連續空白行當分隔
    text = re.sub(r"\n\s*\n+", "\n<<<SPLIT>>>\n", text)

    parts = [p.strip() for p in text.split("<<<SPLIT>>>") if p.strip()]
    return parts


def is_blocked(text):
    return any(w in text for w in BLOCK_KEYWORDS)


def is_closed_text(text):
    return any(w in text for w in DELETE_KEYWORDS)


def create_customer_record(name, id_no, company, source_group_id, text):
    case_id = short_id()
    customers[case_id] = {
        "case_id": case_id,
        "name": name,
        "id_no": id_no,
        "company": company,
        "source_group_id": source_group_id,
        "last_update": text,
        "date": today_str(),
        "status": "ACTIVE"
    }
    return customers[case_id]


def find_active_by_id_no(id_no):
    for c in customers.values():
        if c["status"] == "ACTIVE" and c["id_no"] == id_no:
            return c
    return None


def find_active_by_name(name):
    return [
        c for c in customers.values()
        if c["status"] == "ACTIVE" and c["name"] == name
    ]


def handle_bc_case_block(block_text, source_group_id, reply_token):
    name = extract_name(block_text)
    id_no = extract_id_no(block_text)
    company = extract_company(block_text)

    if not name:
        return "⚠️ 抓不到客戶姓名"

    if is_blocked(block_text):
        return "❌ 含禁止轉發關鍵字，已攔截"

    # ===== 同身分證優先 =====
    if id_no:
        existing = find_active_by_id_no(id_no)

        if existing:
            # 同一群：直接更新
            if existing["source_group_id"] == source_group_id:
                if company:
                    existing["company"] = company
                existing["last_update"] = block_text
                existing["date"] = today_str()
                return f"🔄 已更新客戶：{name}"

            # 不同群：若有明確轉件，直接轉
            if "轉件" in block_text or "轉" in block_text:
                existing["source_group_id"] = source_group_id
                existing["name"] = name
                if company:
                    existing["company"] = company
                existing["last_update"] = block_text
                existing["date"] = today_str()
                return f"➡️ 已轉移客戶到{get_group_name(source_group_id)}：{name}"

            # 不同群：按鈕確認
            action_id = short_id()
            pending_actions[action_id] = {
                "type": "transfer_customer",
                "case_id": existing["case_id"],
                "target_group_id": source_group_id,
                "text": block_text
            }

            old_group = get_group_name(existing["source_group_id"])
            new_group = get_group_name(source_group_id)

            items = [
                make_quick_reply_item(
                    f"轉到{new_group}",
                    f"CONFIRM_TRANSFER|{action_id}"
                ),
                make_quick_reply_item(
                    "維持原群",
                    f"CANCEL_TRANSFER|{action_id}"
                )
            ]

            reply_quick_reply(
                reply_token,
                f"⚠️ 此客戶已存在於{old_group}，要改到{new_group}嗎？",
                items
            )
            return "QUICK_REPLY_SENT"

    # ===== 沒身分證或找不到同身分證 → 建新客戶 =====
    create_customer_record(name, id_no, company, source_group_id, block_text)
    return f"🆕 已建立客戶：{name}"


def find_customer_for_a_block(block_text):
    name = extract_name(block_text)
    id_no = extract_id_no(block_text)

    # 身分證優先
    if id_no:
        c = find_active_by_id_no(id_no)
        if c:
            return c

    # 姓名次之
    matches = find_active_by_name(name)

    if len(matches) == 1:
        return matches[0]

    if len(matches) > 1:
        return "MULTIPLE"

    return None


def send_ambiguous_case_buttons(reply_token, block_text, matches):
    action_id = short_id()
    pending_actions[action_id] = {
        "type": "route_a_case",
        "text": block_text,
        "choices": [c["case_id"] for c in matches]
    }

    items = []
    for c in matches[:10]:
        label = f"{c['name']}-{c['company'] or '未填'}-{get_group_name(c['source_group_id'])}"
        text = f"SELECT_CASE|{action_id}|{c['case_id']}"
        items.append(make_quick_reply_item(label, text))

    reply_quick_reply(
        reply_token,
        "⚠️ 多筆同名客戶，請選擇要回貼的案件",
        items
    )


def handle_a_case_block(block_text, reply_token):
    if is_blocked(block_text):
        return "❌ 含禁止轉發關鍵字，已攔截"

    customer = find_customer_for_a_block(block_text)

    if customer == "MULTIPLE":
        matches = find_active_by_name(extract_name(block_text))
        send_ambiguous_case_buttons(reply_token, block_text, matches)
        return "QUICK_REPLY_SENT"

    if not customer:
        return "⚠️ 找不到對應客戶"

    customer["last_update"] = block_text
    customer["date"] = today_str()

    company = extract_company(block_text)
    if company:
        customer["company"] = company

    if is_closed_text(block_text):
        customer["status"] = "CLOSED"
        push_text(customer["source_group_id"], f"{block_text}\n（此客戶已結案）")
        return f"✅ 已結案並回貼到{get_group_name(customer['source_group_id'])}"

    push_text(customer["source_group_id"], block_text)
    return f"✅ 已回貼到{get_group_name(customer['source_group_id'])}"


def handle_command_text(text, reply_token):
    # 轉群確認
    if text.startswith("CONFIRM_TRANSFER|"):
        _, action_id = text.split("|", 1)
        action = pending_actions.get(action_id)

        if not action or action["type"] != "transfer_customer":
            reply_text(reply_token, "⚠️ 找不到待確認資料")
            return True

        case_id = action["case_id"]
        target_group_id = action["target_group_id"]

        if case_id not in customers:
            reply_text(reply_token, "⚠️ 原案件不存在")
            return True

        customers[case_id]["source_group_id"] = target_group_id
        customers[case_id]["last_update"] = action["text"]
        customers[case_id]["date"] = today_str()

        reply_text(reply_token, f"✅ 已改到{get_group_name(target_group_id)}")
        del pending_actions[action_id]
        return True

    if text.startswith("CANCEL_TRANSFER|"):
        _, action_id = text.split("|", 1)
        if action_id in pending_actions:
            del pending_actions[action_id]
        reply_text(reply_token, "✅ 已維持原群")
        return True

    # A群同名多筆選擇
    if text.startswith("SELECT_CASE|"):
        _, action_id, case_id = text.split("|", 2)
        action = pending_actions.get(action_id)

        if not action or action["type"] != "route_a_case":
            reply_text(reply_token, "⚠️ 找不到待確認案件")
            return True

        if case_id not in customers:
            reply_text(reply_token, "⚠️ 案件不存在")
            return True

        customer = customers[case_id]
        block_text = action["text"]

        customer["last_update"] = block_text
        customer["date"] = today_str()

        company = extract_company(block_text)
        if company:
            customer["company"] = company

        if is_closed_text(block_text):
            customer["status"] = "CLOSED"
            push_text(customer["source_group_id"], f"{block_text}\n（此客戶已結案）")
            reply_text(reply_token, f"✅ 已結案並回貼到{get_group_name(customer['source_group_id'])}")
        else:
            push_text(customer["source_group_id"], block_text)
            reply_text(reply_token, f"✅ 已回貼到{get_group_name(customer['source_group_id'])}")

        del pending_actions[action_id]
        return True

    return False


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

        # 先處理按鈕命令
        if handle_command_text(text, reply_token):
            continue

        blocks = split_multi_cases(text)

        # ===== B / C 群：建立或更新客戶 =====
        if group_id in [B_GROUP_ID, C_GROUP_ID]:
            results = []
            quick_reply_sent = False

            for block in blocks:
                result = handle_bc_case_block(block, group_id, reply_token)
                if result == "QUICK_REPLY_SENT":
                    quick_reply_sent = True
                    break
                results.append(result)

            if not quick_reply_sent:
                reply_text(reply_token, "\n".join(results))

            continue

        # ===== A 群：多客戶進度回貼 =====
        if group_id == A_GROUP_ID:
            results = []
            quick_reply_sent = False

            for block in blocks:
                result = handle_a_case_block(block, reply_token)
                if result == "QUICK_REPLY_SENT":
                    quick_reply_sent = True
                    break
                results.append(result)

            if not quick_reply_sent:
                reply_text(reply_token, "\n".join(results))

            continue

        reply_text(reply_token, "⚠️ 此群組未設定")

    return {"status": "ok"}


@app.get("/", response_class=HTMLResponse)
def home():
    return """
    <h2>貸款系統</h2>
    <form action="/send">
        <input name="msg" style="width:300px"/>
        <button>送出</button>
    </form>
    <br>
    <a href="/report">看日報</a>
    """


@app.get("/send")
def send(msg: str):
    # 網頁手動測試用，預設當 B群建客戶
    blocks = split_multi_cases(msg)
    results = []
    for block in blocks:
        result = handle_bc_case_block(block, B_GROUP_ID, "TEST")
        if result != "QUICK_REPLY_SENT":
            results.append(result)
    return "\n".join(results)


@app.get("/report", response_class=HTMLResponse)
def report():
    html = "<h2>📊 日報</h2>"

    for company in COMPANY_LIST:
        html += f"<b>{company}</b><br>"
        has_data = False

        for customer in customers.values():
            if customer["status"] != "ACTIVE":
                continue
            if customer["company"] != company:
                continue

            has_data = True
            html += (
                f"{customer['date']}"
                f"-{customer['name']}"
                f"-{customer['company']}"
                f"-{customer['last_update']}"
                f"-{get_group_name(customer['source_group_id'])}"
                f"<br>"
            )

        if not has_data:
            html += "（無資料）<br>"

        html += "——————————<br>"

    return html


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
