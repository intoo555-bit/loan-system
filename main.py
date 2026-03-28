from fastapi import FastAPI, Request
import re

app = FastAPI()

DB = []

A_GROUP_ID = "A"
B_GROUP_ID = "B"
C_GROUP_ID = "C"

STATUS_WORDS = ["結案","補件","婉拒","核准","照會","退件","等保書","缺資料","補資料"]

def push_text(group_id, text):
    print(f"[推送到 {group_id}] {text}")

def extract_name(text):
    text = re.split(r"[:：]|->|結案|補件|婉拒|核准", text, maxsplit=1)[0]
    m = re.search(r'[\u4e00-\u9fa5]{2,3}', text)
    return m.group(0) if m else ""

def split_multi_cases(text):
    return [line.strip() for line in text.split("\n") if line.strip()]

def find_active(name):
    return [x for x in DB if x["name"] == name and x["status"] == "ACTIVE"]

def find_any(name):
    return [x for x in DB if x["name"] == name]

def create_case(name):
    DB.append({"name": name, "status": "ACTIVE"})

def update_case(name, status):
    for x in DB:
        if x["name"] == name:
            x["status"] = status

def handle_bc(block):
    name = extract_name(block)
    if not name:
        return None

    if any(w in block for w in STATUS_WORDS):
        active = find_active(name)

        if active:
            new_status = "CLOSED" if "結案" in block else "ACTIVE"
            update_case(name, new_status)
            push_text(A_GROUP_ID, block)
            return f"已更新客戶：{name}"

        any_rows = find_any(name)
        if any_rows:
            return "⚠️ 已結案，請選擇：重啟 / 新建"

        return f"⚠️ 找不到案件：{name}"

    create_case(name)
    return f"已建立客戶：{name}"

def handle_a(block):
    name = extract_name(block)
    if not name:
        return None

    active = find_active(name)
    if active:
        push_text(B_GROUP_ID, block)
        return f"已回貼：{name}"

    return f"⚠️ 找不到案件：{name}"

@app.post("/callback")
async def callback(request: Request):
    data = await request.json()
    text = data.get("text","")
    group_id = data.get("group_id","")

    clean_text = text.replace("@ai","").replace("@AI","").replace("#ai","").replace("#AI","")
    blocks = split_multi_cases(clean_text)

    results = []

    for block in blocks:
        if group_id in [B_GROUP_ID, C_GROUP_ID]:
            res = handle_bc(block)
        elif group_id == A_GROUP_ID:
            res = handle_a(block)
        else:
            res = None

        if res:
            results.append(res)

    return {"msg":"\n".join(results)}
