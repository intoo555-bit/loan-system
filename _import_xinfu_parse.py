# -*- coding: utf-8 -*-
"""幸福貸（鉅烽團隊）4 月活案 parser — 從 LINE 匯出 txt 抓客戶完整資料。
最新狀態日報（4/29）的活案名單寫死、parser 從 LINE 訊息聚合資料。
"""
import re
import json
import os
import sys

sys.stdout.reconfigure(encoding="utf-8")

# 4/29 幸福貸日報活案名單
ACTIVE_NAMES = [
    # 麻吉
    "黃俊銘", "王家森",
    # 亞太
    "黃佳宸",
    # 21
    "林芳婕", "陳明陽", "朱有健", "邱唸淑", "祝惠美", "宋品淇",
    "蔡建富", "普恩生", "黃柏智", "蘭姿儀", "陳黃韻荃", "游惠卿", "謝伯偉",
    # 貸救補
    "葉招盛",
    # 手機分期
    "陳諍蘭",
    # 送件
    "黃文駿", "黃李義",
    # 待撥款
    "蘇翎毓", "黃秀霞", "田笠農", "賴均奕", "邱芷妤", "程翔暉",
    "張正宏", "黃舒婷", "何恭帆", "黃祥彬",
    # 零卡
    "鄭雅妍", "趙宥媗", "張雅雯", "蔡易澄", "林昌頡", "曾俞方",
    "李逸璿", "黃聖家", "鄭智謙", "洪靚怡", "張睿穎",
    # 當鋪 核准
    "林建辰",
    # 房地 核准
    "黃月美",
]

# (current, concurrent, approved_萬, disb, report_section, note, status)
CUSTOMER_STATE = {
    # 麻吉
    "黃俊銘":   ("麻吉",     [],      "",   "",     "",      "更換聯+補知聯時段(後手在審)", "ACTIVE"),
    "王家森":   ("麻吉",     [],      "",   "",     "",      "補保",                          "ACTIVE"),
    # 亞太
    "黃佳宸":   ("亞太",     [],      "",   "",     "",      "",                              "ACTIVE"),
    # 21
    "林芳婕":   ("21",       [],      "",   "",     "",      "已補時段",                      "ACTIVE"),
    "陳明陽":   ("21",       [],      "",   "",     "",      "已補申覆+ID",                   "ACTIVE"),
    "朱有健":   ("21",       [],      "",   "",     "",      "婉拒/已補申覆",                 "ACTIVE"),
    "邱唸淑":   ("21",       [],      "",   "",     "",      "婉拒/補申覆",                   "ACTIVE"),
    "祝惠美":   ("21",       [],      "",   "",     "",      "",                              "ACTIVE"),
    "宋品淇":   ("21",       [],      "",   "",     "",      "婉拒/已補申覆",                 "ACTIVE"),
    "蔡建富":   ("21",       [],      "",   "",     "",      "已補1知聯補足3聯",              "ACTIVE"),
    "普恩生":   ("21",       [],      "",   "",     "",      "",                              "ACTIVE"),
    "黃柏智":   ("21",       [],      "",   "",     "",      "已更正友手機+補足3聯",          "ACTIVE"),
    "蘭姿儀":   ("21",       [],      "",   "",     "",      "",                              "ACTIVE"),
    "陳黃韻荃": ("21",       [],      "",   "",     "",      "",                              "ACTIVE"),
    "游惠卿":   ("21",       [],      "",   "",     "",      "",                              "ACTIVE"),
    "謝伯偉":   ("21",       [],      "",   "",     "",      "",                              "ACTIVE"),
    # 貸救補
    "葉招盛":   ("貸救補",   [],      "",   "",     "",      "",                              "ACTIVE"),
    # 手機分期
    "陳諍蘭":   ("手機分期", [],      "",   "",     "",      "已補時段",                      "ACTIVE"),
    # 送件區
    "黃文駿":   ("",         [],      "",   "",     "送件",  "確認資料",                      "ACTIVE"),
    "黃李義":   ("第一",     ["喬美"], "",   "",     "送件",  "缺繳息",                        "ACTIVE"),
    # 待撥款
    "蘇翎毓":   ("21",       [],      "6",  "",     "待撥款", "",                              "ACTIVE"),
    "黃秀霞":   ("裕融",     [],      "10", "",     "待撥款", "",                              "ACTIVE"),
    "田笠農":   ("21",       [],      "8",  "4/27", "待撥款", "",                              "ACTIVE"),
    "賴均奕":   ("亞太",     [],      "25", "",     "待撥款", "",                              "ACTIVE"),
    "邱芷妤":   ("21",       [],      "10", "4/27", "待撥款", "",                              "ACTIVE"),
    "程翔暉":   ("21",       ["零卡"], "7",  "4/27", "待撥款", "",                              "ACTIVE"),
    "張正宏":   ("21",       [],      "7",  "4/28", "待撥款", "",                              "ACTIVE"),
    "黃舒婷":   ("亞太",     [],      "12", "4/28", "待撥款", "",                              "ACTIVE"),
    "何恭帆":   ("21",       [],      "7",  "",     "待撥款", "",                              "ACTIVE"),
    "黃祥彬":   ("鄉民",     [],      "8",  "",     "待撥款", "",                              "ACTIVE"),
    # 零卡
    "鄭雅妍":   ("零卡",     [],      "",   "",     "",      "",                              "ACTIVE"),
    "趙宥媗":   ("零卡",     [],      "",   "",     "",      "",                              "ACTIVE"),
    "張雅雯":   ("零卡",     [],      "",   "",     "",      "",                              "ACTIVE"),
    "蔡易澄":   ("零卡",     [],      "",   "",     "",      "",                              "ACTIVE"),
    "林昌頡":   ("零卡",     [],      "",   "",     "",      "",                              "ACTIVE"),
    "曾俞方":   ("零卡",     [],      "",   "",     "",      "",                              "ACTIVE"),
    "李逸璿":   ("零卡",     [],      "",   "",     "",      "",                              "ACTIVE"),
    "黃聖家":   ("零卡",     [],      "",   "",     "",      "",                              "ACTIVE"),
    "鄭智謙":   ("零卡",     [],      "",   "",     "",      "",                              "ACTIVE"),
    "洪靚怡":   ("零卡",     [],      "",   "",     "",      "",                              "ACTIVE"),
    "張睿穎":   ("零卡",     [],      "",   "",     "",      "",                              "ACTIVE"),
    # 當鋪 核准
    "林建辰":   ("當鋪",     [],      "7",  "4/27", "核准",  "",                              "ACTIVE"),
    # 房地 核准
    "黃月美":   ("民間房地", [],      "20", "",     "核准(房地)", "",                          "ACTIVE"),
}

# 建案日期（從日報抓的、民國年自動轉）
BUILD_DATE = {
    "黃俊銘": "04/14", "王家森": "04/28",
    "黃佳宸": "04/08",
    "林芳婕": "04/21", "陳明陽": "04/21", "朱有健": "04/27", "邱唸淑": "04/28",
    "祝惠美": "04/23", "宋品淇": "03/20", "蔡建富": "04/28", "普恩生": "04/28",
    "黃柏智": "04/28", "蘭姿儀": "04/28", "陳黃韻荃": "04/28", "游惠卿": "04/29", "謝伯偉": "04/27",
    "葉招盛": "04/17",
    "陳諍蘭": "04/27",
    "黃文駿": "04/22", "黃李義": "04/27",
    "蘇翎毓": "03/25", "黃秀霞": "04/20", "田笠農": "04/21", "賴均奕": "04/14",
    "邱芷妤": "04/21", "程翔暉": "04/15", "張正宏": "04/20", "黃舒婷": "04/22",
    "何恭帆": "04/17", "黃祥彬": "03/24",
    "鄭雅妍": "04/02", "趙宥媗": "04/20", "張雅雯": "04/14", "蔡易澄": "04/08",
    "林昌頡": "04/14", "曾俞方": "04/22", "李逸璿": "04/21", "黃聖家": "04/22",
    "鄭智謙": "04/28", "洪靚怡": "03/10", "張睿穎": "04/16",
    "林建辰": "03/24",
    "黃月美": "03/02",
}

ID_OVERRIDE = {}


def load_msgs(path):
    with open(path, encoding="utf-8") as f:
        lines = f.read().splitlines()
    msgs = []
    current_date = ""
    keep = False
    current = None
    for l in lines:
        # 日期行可能是 2025.MM.DD 或 2026.MM.DD
        dm = re.match(r"^(\d{4})\.(\d{2})\.(\d{2})", l)
        if dm:
            year, mm, dd = dm.group(1), dm.group(2), dm.group(3)
            # 只抓 2026 整年資料（活案應該都在 2026）
            keep = year == "2026"
            if keep:
                current_date = f"{mm}/{dd}"
            if current:
                msgs.append(current); current = None
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
    info = {"name": name, "id_no": "", "birth": "", "phone": "",
            "company": "", "company_phone": "", "addr_reg": "", "addr_live": "",
            "email": "", "build_date": "", "route_order": ""}
    for m in msgs:
        t = m["text"]
        if name not in t:
            continue
        # 建案日期：「已建立「115/4/14-黃俊銘」相簿」
        bm = re.search(rf"已建立.{{0,3}}(\d{{2,3}})/(\d{{1,2}})/(\d{{1,2}})-{re.escape(name)}", t)
        if bm and not info["build_date"]:
            info["build_date"] = f"{int(bm.group(2)):02d}/{int(bm.group(3)):02d}"
        # 身分證（短訊息）
        if not info["id_no"]:
            idm = re.search(r"([A-Z][12]\d{8})", t)
            if idm and len(t) < 200:
                info["id_no"] = idm.group(1)
        # 完整申請書 block
        if f"申請人姓名：{name}" in t or f"申請人姓名:{name}" in t:
            info.update(parse_application_block(t))
        # 送件順序
        rm = re.search(
            rf"\d{{1,2}}/\d{{1,2}}-{re.escape(name)}[\s\-]+((?:[一-鿿]|21|第一)[一-鿿0-9機車商品萬汽分貝鄉銀代手C貸研當]*(?:/[^\s\n/]+){{1,}})",
            t,
        )
        if rm and not info["route_order"]:
            order = rm.group(1).strip()
            order = re.split(r"[\s(（\-」]", order)[0]
            if order.count("/") >= 1:
                info["route_order"] = order
    return info


def parse_application_block(text):
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
    src = r"C:\Users\User\OneDrive\Desktop\[LINE]幸福貸-鉅烽.txt"
    msgs = load_msgs(src)
    print(f"2026 訊息數: {len(msgs)}", file=sys.stderr)

    customers = []
    for name in ACTIVE_NAMES:
        c = parse_customer(name, msgs)
        if name in ID_OVERRIDE and ID_OVERRIDE[name]:
            c["id_no"] = ID_OVERRIDE[name]
        if name in BUILD_DATE:
            c["build_date"] = BUILD_DATE[name]
        cur, conc, amt, disb, sec, note, st = CUSTOMER_STATE[name]
        c["final_current"] = cur
        c["final_concurrent"] = conc
        c["final_approved_amount"] = f"{amt}萬" if amt else ""
        c["final_disb_date"] = disb
        c["final_report_section"] = sec
        c["report_note"] = note
        c["final_status"] = st
        customers.append(c)

    for c in customers:
        concurrent_str = ("+" + "+".join(c["final_concurrent"])) if c["final_concurrent"] else ""
        amt_str = f" 核准{c['final_approved_amount']}" if c["final_approved_amount"] else ""
        disb_str = f" 撥款{c['final_disb_date']}" if c["final_disb_date"] else ""
        st_tag = "💰待撥款 " if c["final_report_section"] == "待撥款" else "         "
        print(f"{st_tag}{c['name']:6s} {c['id_no'] or '❌':12s} | {c['final_current']}{concurrent_str}{amt_str}{disb_str}")

    out_path = os.path.join(os.path.dirname(__file__), "xinfu_apr_import.json")
    export = []
    for c in customers:
        export.append({
            "name": c["name"], "id_no": c["id_no"], "birth": c["birth"],
            "phone": c["phone"], "email": c["email"],
            "company_name": c["company"], "company_phone": c["company_phone"],
            "addr_reg": c["addr_reg"], "addr_live": c["addr_live"],
            "build_date": c["build_date"], "route_order": c["route_order"],
            "current_company": c["final_current"],
            "concurrent": c["final_concurrent"],
            "approved_amount": c["final_approved_amount"],
            "disb_date": c["final_disb_date"],
            "report_section": c["final_report_section"],
            "report_note": c["report_note"],
            "status": c["final_status"],
        })
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(export, f, ensure_ascii=False, indent=2)
    no_id = sum(1 for c in customers if not c["id_no"])
    print(f"\n已輸出 JSON: {out_path}", file=sys.stderr)
    print(f"共 {len(customers)} 筆、抓不到身分證：{no_id} 筆", file=sys.stderr)


if __name__ == "__main__":
    main()
