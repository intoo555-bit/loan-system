from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
import uvicorn
import requests
import os

app = FastAPI()

CHANNEL_ACCESS_TOKEN = os.getenv("CHANNEL_ACCESS_TOKEN", "")

customers = {}

COMPANY_LIST = ["亞太", "和裕", "21"]

DELETE_KEYWORDS = ["結案", "刪掉", "不追了", "全部不送", "已撥款結案"]
BLOCK_KEYWORDS = ["鼎信", "禾基"]


def extract_name(text):
    return text.split("（")[0].split(" ")[0].strip()


def extract_company(text):
    for c in COMPANY_LIST:
        if c in text:
            return c
    return None


def process_message(text):
    name = extract_name(text)

    if name not in customers:
        customers[name] = {"companies": {}, "closed": False}

    for w in BLOCK_KEYWORDS:
        if w in text:
            return f"❌ 禁止轉發（{w}）"

    for w in DELETE_KEYWORDS:
        if w in text:
            customers[name]["closed"] = True
            customers[name]["companies"] = {}
            return "🗑 已結案"

    company = extract_company(text)

    if "轉" in text and company:
        customers[name]["companies"] = {company: text}
        return f"➡️ 轉到 {company}"

    if company:
        customers[name]["companies"][company] = text
        return f"➕ 更新 {company}"

    return "⚠️ 無法判讀"


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


@app.post("/callback")
async def callback(request: Request):
    body = await request.json()

    for event in body.get("events", []):
        source = event.get("source", {})
        print("GROUP ID:", source.get("groupId"))

        if event.get("type") != "message":
            continue

        message = event.get("message", {})
        if message.get("type") != "text":
            continue

        text = message["text"]
        reply_token = event["replyToken"]

        result = process_message(text)
        reply(reply_token, f"收到：{text}\n結果：{result}")

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
    return process_message(msg)


@app.get("/report", response_class=HTMLResponse)
def report():
    result = "📊 日報<br><br>"
    for c in COMPANY_LIST:
        result += f"{c}<br>"
        for name, data in customers.items():
            if data["closed"]:
                continue
            if c in data["companies"]:
                result += f"{name}｜{data['companies'][c]}<br>"
        result += "——————————<br>"
    return result


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
