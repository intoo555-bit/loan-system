from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
import uvicorn
import requests
import os
import re
from datetime import datetime

app = FastAPI()

CHANNEL_ACCESS_TOKEN = os.getenv("CHANNEL_ACCESS_TOKEN", "")

# ===== 群組 ID =====
A_GROUP_ID = "Cb3579e75c94437ed22aafc7b1f6aecdd"   # A群
B_GROUP_ID = "Cd14f3ee775f1d9f5cfdafb223173cbef"   # B群
C_GROUP_ID = "C1a647fcb29a74842eceeb18e7a53823d"   # C群

customers = {}

COMPANY_LIST = ["亞太", "和裕", "21"]

DELETE_KEYWORDS = ["結案", "刪掉", "不追了", "全部不送", "已撥款結案"]
BLOCK_KEYWORDS = ["鼎信", "禾基"]


def today_str():
    return datetime.now().strftime("%m/%d")


def extract_name(text):
    text = text.strip()
    if not text:
        return ""
    return text.split("（")[0].split(" ")[0].strip()


def extract_id_no(text):
    match = re.search(r"[A-Z][12]\d{8}", text.upper())
    return match.group(0) if match else ""


def extract_company(text):
    for c in COMPANY_LIST:
        if c in text:
            return c
    return ""


def get_source_group_name(group_id):
    if group_id == B_GROUP_ID:
        return "B群"
    if group_id == C_GROUP_ID:
        return "C群"
    if group_id == A_GROUP_ID:
        return "A群"
    return "未知群組"


def push(to_group_id, text):
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


def reply(reply_token, text):
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


def create_or_update_customer(text, source_group_id):
    name = extract_name(text)
    id_no = extract_id_no(text)
    company = extract_company(text)

    if not name:
        return "⚠️ 抓不到客戶姓名"

    # 先擋禁止字
    if any(w in text for w in BLOCK_KEYWORDS):
        return "❌ 含禁止轉發關鍵字，已攔截"

    # 同身分證優先處理
    if id_no:
        for _, customer in customers.items():
            if customer["id_no"] == id_no and customer["status"] == "ACTIVE":
                # 同身分證但不同群：只有明確轉件才改來源群
                if customer["source_group_id"] != source_group_id:
                    if "轉" in text or "轉件" in text:
                        customer["source_group_id"] = source_group_id
                        customer["name"] = name
                        if company:
                            customer["company"] = company
                        customer["last_update"] = text
                        customer["date"] = today_str()
                        return f"➡️ 已轉移客戶到新群：{name}"
                    else:
                        old_group = get_source_group_name(customer["source_group_id"])
                        return f"⚠️ 此客戶已存在於 {old_group}，如需轉件請輸入轉件"

                # 同群正常更新
                customer["name"] = name
                if company:
                    customer["company"] = company
                customer["last_update"] = text
                customer["date"] = today_str()
                return f"🔄 已更新客戶：{name}"

    # 沒身分證時，用姓名 + 來源群找
    for _, customer in customers.items():
        if (
            customer["name"] == name
            and customer["source_group_id"] == source_group_id
            and customer["status"] == "ACTIVE"
            and not customer["id_no"]
        ):
            if company:
                customer["company"] = company
            customer["last_update"] = text
            customer["date"] = today_str()
            return f"🔄 已更新客戶：{name}"

    # 新建
    customer_key = f"{name}_{id_no}_{len(customers)+1}"
    customers[customer_key] = {
        "name": name,
        "id_no": id_no,
        "source_group_id": source_group_id,
        "company": company,
        "last_update": text,
        "date": today_str(),
        "status": "ACTIVE"
    }

    return f"🆕 已建立客戶：{name}"


def find_customer_for_a_group(text):
    name = extract_name(text)
    id_no = extract_id_no(text)

    # 身分證優先
    if id_no:
        for _, customer in customers.items():
            if customer["id_no"] == id_no and customer["status"] == "ACTIVE":
                return customer

    # 沒身分證時，只能用姓名
    matches = []
    for _, customer in customers.items():
        if customer["name"] == name and customer["status"] == "ACTIVE":
            matches.append(customer)

    if len(matches) == 1:
        return matches[0]

    if len(matches) > 1:
        return "MULTIPLE"

    return None


def close_customer(customer):
    customer["status"] = "CLOSED"


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

        # ===== B / C 群：建立客戶 =====
        if group_id in [B_GROUP_ID, C_GROUP_ID]:
            result = create_or_update_customer(text, group_id)
            reply(reply_token, result)
            continue

        # ===== A 群：處理進度並回貼 =====
        if group_id == A_GROUP_ID:
            if any(w in text for w in BLOCK_KEYWORDS):
                reply(reply_token, "❌ 含禁止轉發關鍵字，已攔截")
                continue

            customer = find_customer_for_a_group(text)

            if customer == "MULTIPLE":
                reply(reply_token, "⚠️ 多筆同名客戶，請補身分證")
                continue

            if not customer:
                reply(reply_token, "⚠️ 找不到對應客戶")
                continue

            # 明確結案才關閉
            if any(w in text for w in DELETE_KEYWORDS):
                customer["last_update"] = text
                customer["date"] = today_str()
                close_customer(customer)

                push(
                    customer["source_group_id"],
                    f"{text}\n（此客戶已結案）"
                )
                reply(reply_token, "✅ 已結案並回貼")
                continue

            # 一般更新
            company = extract_company(text)
            if company:
                customer["company"] = company

            customer["last_update"] = text
            customer["date"] = today_str()

            # 不顯示「A群進度回貼」
            push(customer["source_group_id"], text)
            reply(reply_token, "✅ 已回貼到原業務群")
            continue

        # 其他群
        reply(reply_token, "⚠️ 此群組未設定")

    return {"status": "ok"}


@app.get("/", response_class=HTMLResponse)
def home():
    return """
    <h2>貸款系統</h2>
    <form action="/send">
        <input name="msg"/>
        <button>送出</button>
    </form>
    <a href="/report">看日報</a>
    """


@app.get("/send")
def send(msg: str):
    result = create_or_update_customer(msg, B_GROUP_ID)
    return result


@app.get("/report", response_class=HTMLResponse)
def report():
    html = "<h2>📊 日報</h2>"

    for company in COMPANY_LIST:
        html += f"<b>{company}</b><br>"
        has_data = False

        for _, customer in customers.items():
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
                f"-{get_source_group_name(customer['source_group_id'])}"
                f"<br>"
            )

        if not has_data:
            html += "（無資料）<br>"

        html += "——————————<br>"

    return html


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
