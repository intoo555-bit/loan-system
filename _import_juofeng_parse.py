# -*- coding: utf-8 -*-
"""鉅烽 4 月活案 parser — 從 LINE 匯出 txt 抓客戶完整資料。
使用者最新日報（4/27）的活案名單寫死、parser 從 4 月訊息聚合資料。
"""
import re
import json
import os
import sys

sys.stdout.reconfigure(encoding="utf-8")

# 4/27 鉅烽日報活案名單
ACTIVE_NAMES = [
    # 亞太
    "羅衛群",
    # 21
    "許晉晟", "呂佳舫", "莊祿和", "宋仁安", "蔡仁宗",
    "王頌恩", "柯士強", "劉邦丞",
    # 分貝汽車
    "黃彥儒",
    # 待撥款
    "黃世杰", "蔡富翔", "李柏賢",
    # 銀行
    "莊暄",
    # 零卡
    "高玟玲", "黃昱智", "廖瑋傑", "陳紀妤",
    # 核准（已撥款）
    "湯啟誠",
]

# 各客戶在最新日報的狀態（手動對齊 user 提供的 4/27 日報）
# 格式：(current_company, concurrent_list, approved_amount_萬, disb_date, report_section, note)
CUSTOMER_STATE = {
    "羅衛群":  ("亞太", ["喬美"], "", "", "", ""),
    "許晉晟":  ("21", [], "", "", "", "申覆:補1二等知情補足3聯"),
    "呂佳舫":  ("21", [], "", "", "", ""),
    "莊祿和":  ("21", [], "", "", "", ""),
    "宋仁安":  ("21", [], "", "", "", ""),
    "蔡仁宗":  ("21", [], "", "", "", ""),
    "王頌恩":  ("21", [], "", "", "", ""),
    "柯士強":  ("21", [], "", "", "", ""),
    "劉邦丞":  ("21", [], "", "", "", ""),
    "黃彥儒":  ("分貝汽車", [], "", "", "", "補保"),
    # 待撥款（黃世杰 21核准12萬 + C同送）
    "黃世杰":  ("21", ["零卡"], "12", "", "待撥款", ""),
    # 蔡富翔 裕融核准10萬、撥款4/27
    "蔡富翔":  ("裕融", [], "10", "4/27", "待撥款", ""),
    "李柏賢":  ("21", [], "7", "", "待撥款", ""),
    "莊暄":    ("玉山", [], "", "", "", ""),     # 銀行 - 玉山
    # 零卡
    "高玟玲":  ("零卡", [], "", "", "", ""),
    "黃昱智":  ("零卡", [], "", "", "", ""),
    "廖瑋傑":  ("零卡", [], "", "", "", ""),
    "陳紀妤":  ("零卡", [], "", "", "", ""),
    # 核准區（已撥款）
    "湯啟誠":  ("台新", [], "30", "4/24", "待撥款", ""),
}

# 身分證手動補（parser 訊息太長抓不到的）
ID_OVERRIDE = {
    "湯啟誠": "S122302358",
    "莊暄":   "",   # 待補
}

# 建案日期（從日報抓的）
BUILD_DATE = {
    "羅衛群":  "04/27",
    "許晉晟":  "04/15",
    "呂佳舫":  "04/22",
    "莊祿和":  "04/22",
    "宋仁安":  "04/15",
    "蔡仁宗":  "04/24",
    "王頌恩":  "04/27",
    "柯士強":  "04/27",
    "劉邦丞":  "04/24",
    "黃彥儒":  "04/14",
    "黃世杰":  "04/16",
    "蔡富翔":  "04/23",
    "李柏賢":  "04/20",
    "莊暄":    "04/22",
    "高玟玲":  "04/13",
    "黃昱智":  "04/07",
    "廖瑋傑":  "04/17",
    "陳紀妤":  "04/21",
    "湯啟誠":  "04/08",
}


def load_msgs(path):
    """讀 LINE txt、抓 2026/04 訊息 → list of dict"""
    with open(path, encoding="utf-8") as f:
        lines = f.read().splitlines()
    msgs = []
    current_date = ""
    keep = False
    current = None
    for l in lines:
        dm = re.match(r"^(\d{4})\.(\d{2})\.(\d{2})", l)
        if dm:
            keep = dm.group(1) == "2026" and dm.group(2) == "04"
            if keep:
                current_date = f"{dm.group(2)}/{dm.group(3)}"
            if current:
                msgs.append(current)
                current = None
            continue
        if not keep:
            continue
        tm = re.match(r"^(\d{1,2}:\d{2})\s+([^\s]+)\s*(.*)$", l)
        if tm:
            if current:
                msgs.append(current)
            current = {"date": current_date, "time": tm.group(1),
                       "sender": tm.group(2), "text": tm.group(3)}
        else:
            if current:
                current["text"] += "\n" + l
    if current:
        msgs.append(current)
    return msgs


def parse_customer(name, msgs):
    """聚合某客戶的資訊：身分證、送件順序、聯絡人、地址 等"""
    info = {
        "name": name,
        "id_no": "",
        "birth": "",
        "phone": "",
        "company": "",
        "company_phone": "",
        "addr_reg": "",
        "addr_live": "",
        "email": "",
        "build_date": "",
        "route_order": "",
    }
    for m in msgs:
        t = m["text"]
        if name not in t:
            continue
        dt = m["date"]

        # 建案日期（「已建立「XX/X/X-姓名」相簿」）
        bm = re.search(rf"已建立.{{0,3}}(\d{{2,3}})/(\d{{1,2}})/(\d{{1,2}})-{re.escape(name)}", t)
        if bm and not info["build_date"]:
            info["build_date"] = f"{int(bm.group(2)):02d}/{int(bm.group(3)):02d}"

        # 身分證（從短訊息抓）
        if not info["id_no"]:
            idm = re.search(r"([A-Z][12]\d{8})", t)
            if idm and len(t) < 200:
                info["id_no"] = idm.group(1)

        # 完整申請書 block
        if f"申請人姓名：{name}" in t or f"申請人姓名:{name}" in t:
            info.update(parse_application_block(t))

        # 送件順序（4/D-姓名-公司/公司/...）
        rm = re.search(
            rf"\d{{1,2}}/\d{{1,2}}-{re.escape(name)}[\s\-]+((?:[一-鿿]|21|第一)[一-鿿0-9機車商品萬汽分貝鄉銀代手C貸]*(?:/[^\s\n/]+){{1,}})",
            t,
        )
        if rm and not info["route_order"]:
            order = rm.group(1).strip()
            order = re.split(r"[\s(（\-」]", order)[0]
            if order.count("/") >= 1:
                info["route_order"] = order
    return info


def parse_application_block(text):
    """從完整申請書 block 抓個資"""
    out = {}
    m = re.search(r"身分證字號[:：]\s*([A-Z][12]\d{8})", text)
    if m: out["id_no"] = m.group(1)
    m = re.search(r"出生年月日[:：]\s*(\d{2,3}/\d{1,2}/\d{1,2})", text)
    if m: out["birth"] = m.group(1)
    m = re.search(r"行動電話[:：]\s*([\d\-]+)", text)
    if m: out["phone"] = m.group(1)
    m = re.search(r"戶籍地址[:：]\s*([^\n]+)", text)
    if m: out["addr_reg"] = m.group(1).strip()
    m = re.search(r"現居地址[:：]\s*([^\n]+)", text)
    if m: out["addr_live"] = m.group(1).strip()
    m = re.search(r"公司名稱[:：]\s*([^\n]+)", text)
    if m: out["company"] = m.group(1).strip()
    m = re.search(r"公司電話[:：]\s*([\d\-]+)", text)
    if m: out["company_phone"] = m.group(1)
    m = re.search(r"電子信箱[:：]\s*([^\s\n]+)", text)
    if m: out["email"] = m.group(1)
    return out


def main():
    src = r"C:\Users\User\OneDrive\Desktop\[LINE]鉅烽團隊送件客資.txt"
    msgs = load_msgs(src)
    print(f"4 月訊息數: {len(msgs)}", file=sys.stderr)

    customers = []
    for name in ACTIVE_NAMES:
        c = parse_customer(name, msgs)
        if name in ID_OVERRIDE and ID_OVERRIDE[name]:
            c["id_no"] = ID_OVERRIDE[name]
        if name in BUILD_DATE:
            c["build_date"] = BUILD_DATE[name]
        cur, conc, amt, disb, sec, note = CUSTOMER_STATE[name]
        c["final_current"] = cur
        c["final_concurrent"] = conc
        c["final_approved_amount"] = f"{amt}萬" if amt else ""
        c["final_disb_date"] = disb
        c["final_report_section"] = sec
        c["report_note"] = note
        customers.append(c)

    # console preview
    for c in customers:
        concurrent_str = ("+" + "+".join(c["final_concurrent"])) if c["final_concurrent"] else ""
        amt_str = f" 核准{c['final_approved_amount']}" if c["final_approved_amount"] else ""
        disb_str = f" 撥款{c['final_disb_date']}" if c["final_disb_date"] else ""
        section_tag = "💰待撥款 " if c["final_report_section"] == "待撥款" else "         "
        print(f"{section_tag}{c['name']:6s} {c['id_no'] or '❌':12s} | {c['final_current']}{concurrent_str}{amt_str}{disb_str} | 送:{c['route_order'] or '-'}")

    # 輸出 JSON
    out_path = os.path.join(os.path.dirname(__file__), "juofeng_apr_import.json")
    export = []
    for c in customers:
        export.append({
            "name": c["name"],
            "id_no": c["id_no"],
            "birth": c["birth"],
            "phone": c["phone"],
            "email": c["email"],
            "company_name": c["company"],
            "company_phone": c["company_phone"],
            "addr_reg": c["addr_reg"],
            "addr_live": c["addr_live"],
            "build_date": c["build_date"],
            "route_order": c["route_order"],
            "current_company": c["final_current"],
            "concurrent": c["final_concurrent"],
            "approved_amount": c["final_approved_amount"],
            "disb_date": c["final_disb_date"],
            "report_section": c["final_report_section"],
            "report_note": c["report_note"],
            "extra_approved": [],
            "company_notes": {},
        })
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(export, f, ensure_ascii=False, indent=2)
    print(f"\n已輸出 JSON: {out_path}", file=sys.stderr)


if __name__ == "__main__":
    main()
