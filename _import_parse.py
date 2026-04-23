# -*- coding: utf-8 -*-
"""精雕版 parser：從 LINE 匯出 txt 抓 4 月活案完整資料。"""
import re
import json
import sys
from collections import defaultdict

sys.stdout.reconfigure(encoding="utf-8")

# 日報活案名單（使用者 4/23 最新版）
ACTIVE_NAMES = [
    "鍾明玉", "趙書明", "趙慧羚", "林志鴻", "張心悅", "蔡建林", "翁慧娟", "楊秋緣", "許詩雅", "陳裕仁",
    "曾陳銓", "田福賓", "楊家榮", "邱張菁", "洪慧瑜", "賴詩婷", "陳信全", "李淑樺", "羅坤芃", "謝美芃",
    "風傳龍", "朱志明", "許志成", "葉玥樂", "柯忻恩", "劉娟伶", "曾月英", "邱子勛", "陳渃玹", "楊創富",
    "陳美旺", "劉祐騰", "鄭佳怡", "謝緯達", "郭曉芸", "蕭傳義", "張庚輝", "吳麗貞", "盧姿樺", "熊高玲",
    # 零卡 13 筆
    "王家齊", "江淑芳", "高峰瑛", "王思婷", "李宜芳", "黃晏柔", "陳玥汝", "陳弈叡", "呂昶宗",
    "吳宗燁", "黃文忠", "張維鋼", "黎俊志",
]


def load_msgs(path):
    """讀 LINE txt，切成 message 物件（date, time, sender, text），抓 4 月。"""
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
            current = {"date": current_date, "time": tm.group(1), "sender": tm.group(2), "text": tm.group(3)}
        else:
            if current:
                current["text"] += "\n" + l
    if current:
        msgs.append(current)
    return msgs


def is_report_dump(text):
    if "(勞工紓困)" in text:
        return True
    if text.count("——") >= 3:
        return True
    return False


def parse_customer(name, msgs):
    """聚合某客戶的資訊。"""
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
        "approvals": [],   # [{"company":"", "amount":"", "period":"", "monthly":""}]
        "rejects": [],
        "signings": [],    # [{"area":"", "time":"", "location":""}]
        "last_status": "",
        "timeline": [],
    }
    for m in msgs:
        t = m["text"]
        if name not in t:
            continue
        dt = m["date"]
        info["timeline"].append(f"[{dt} {m['time']}] {m['sender']} {t[:80]}")

        # 建案日期（「已建立「XX/X/X-姓名」相簿」）
        bm = re.search(rf"已建立.{{0,3}}(\d{{2,3}})/(\d{{1,2}})/(\d{{1,2}})-{re.escape(name)}", t)
        if bm and not info["build_date"]:
            info["build_date"] = f"{int(bm.group(2)):02d}/{int(bm.group(3)):02d}"

        # 身分證（從「姓名 身分證 注記」這種短訊息抓）
        if not info["id_no"]:
            idm = re.search(r"([A-Z][12]\d{8})", t)
            if idm and len(t) < 150:  # 短訊息比較可靠
                info["id_no"] = idm.group(1)

        # 完整申請書 block（「申請人姓名：陳泓夫」）
        if f"申請人姓名：{name}" in t or f"申請人姓名:{name}" in t:
            info.update(parse_application_block(t))

        # 送件順序（「4/21-姓名-第一/21機25/亞太25/...」）
        # 公司名第一段必須是中文或「21」開頭，避免抓到身分證
        rm = re.search(
            rf"\d{{1,2}}/\d{{1,2}}-{re.escape(name)}[\s\-]+((?:[\u4e00-\u9fff]|21|第一)[\u4e00-\u9fff0-9機車商品萬汽分貝鄉銀代手C貸]*(?:/[^\s\n/]+){{1,}})",
            t,
        )
        if rm and not info["route_order"]:
            order = rm.group(1).strip()
            # 剝掉尾端中括號、狀態、中文字尾綴（如「的相簿刪除」）
            order = re.split(r"[\s(（\-」]", order)[0]
            # 再次過濾：只保留 /… 結構
            if order.count("/") >= 1:
                info["route_order"] = order

        # 核准：三種格式
        # (A) 緊接：姓名 公司 核准 N萬/M期/P金額（簡短訊息）
        # (B) 有插字：姓名 公司 核准 【X】... 最高核貸金額 N萬/M期/P金額
        # (C) 日報片段：4/D-姓名-公司-核准N萬
        cm = re.findall(
            rf"\d{{1,2}}/\d{{1,2}}-{re.escape(name)}-(?:轉)?([\u4e00-\u9fff0-9]+?)-核准(\d+(?:\.\d+)?)萬",
            t,
        )
        for co, amt in cm:
            if not any(a["company"] == co and a["amount"] == amt for a in info["approvals"]):
                info["approvals"].append({
                    "company": co, "amount": amt, "period": "", "monthly": "", "date": dt
                })
        if m["sender"] == "大幫手" and "核准" in t and name in t and "未核准" not in t and "待核准" not in t and "審核未通過" not in t:
            flat = re.sub(r"\s+", " ", t)
            # (A) 緊接
            am = re.search(
                rf"{re.escape(name)}\s*[\(（]?([\u4e00-\u9fff0-9維力商品貸]+?)[\)）]?\s+核准\s+(\d+(?:\.\d+)?)\s*[萬W]\s*(?:/\s*(\d+)\s*[期N])?\s*(?:/\s*P?(\d+))?",
                flat,
            )
            # (B) 穿插 200 字內找金額
            if not am:
                am = re.search(
                    rf"{re.escape(name)}\s*[\(（]?([\u4e00-\u9fff0-9維力商品貸]+?)[\)）]?\s+核准.{{0,200}}?(\d+(?:\.\d+)?)\s*[萬W]\s*/\s*(\d+)\s*[期N]\s*/\s*P?(\d+)",
                    flat,
                )
            if am:
                co = am.group(1).strip()
                if co not in ("已", "待", "需", "補", "轉"):
                    amt = am.group(2)
                    period = am.group(3) or ""
                    monthly = am.group(4) or ""
                    if not any(a["company"] == co and a["amount"] == amt for a in info["approvals"]):
                        info["approvals"].append({
                            "company": co, "amount": amt, "period": period, "monthly": monthly, "date": dt
                        })

        # 婉拒
        if m["sender"] == "大幫手" and "婉拒" in t and name in t:
            rjm = re.search(rf"{re.escape(name)}\s*([\u4e00-\u9fff]+?)\s*婉拒", t)
            if rjm:
                info["rejects"].append(rjm.group(1))

        # 派保/對保（「XX 派保 客戶：姓名 對保地點：YY」或「對保 時間/地點」）
        if "派保" in t or "對保" in t:
            am = re.search(r"對保地[區點][:：]\s*([^\s\n]+)", t)
            if am and name in t:
                info["signings"].append({"area": am.group(1), "date": dt})

    return info


def parse_application_block(text):
    """從完整申請書 block 抓個資。"""
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


# 使用者補的身分證（parser 抓不到的）
ID_OVERRIDE = {
    "陳信全": "I100247622",
    "賴詩婷": "B222030953",
}

# 日報 4/23 版「待撥款」區的正確狀態（姓名→[公司, 金額(萬), 撥款日期 or None]）
# 從使用者貼的日報人工整理：待撥款的 current=該公司、report_section=待撥款
PENDING_DISB = {
    "賴詩婷":  ("21", "8", None),
    "邱子勛":  ("喬美", "10", "4/21"),
    "陳渃玹":  ("手機分期", "2", "4/20"),
    "楊創富":  ("亞太", "12", "4/20"),   # 另有第一核准 28萬
    "陳美旺":  ("21", "7", "4/20"),
    "劉祐騰":  ("21", "7", "4/20"),
    "鄭佳怡":  ("21", "7", "4/22"),
    "謝緯達":  ("亞太", "12", "4/21"),
    "郭曉芸":  ("21", "7", "4/23"),
    "蔡建林":  ("和裕", "5", None),
    "蕭傳義":  ("亞太", "12", "4/23"),
    "洪慧瑜":  ("21", "25", None),
    "張庚輝":  ("21", "7", None),
    "吳麗貞":  ("喬美", "10", None),
    "謝美芃":  ("和裕", "6", None),
    "盧姿樺":  ("第一", "30", None),
}

# 日報目前 current_company（未核准、送件中）
ACTIVE_CO = {
    "鍾明玉": "21", "趙書明": "21", "趙慧羚": "21", "林志鴻": "21", "張心悅": "21",
    "翁慧娟": "21", "楊秋緣": "21", "許詩雅": "21", "陳裕仁": "21", "曾陳銓": "21",
    "田福賓": "21", "楊家榮": "21", "邱張菁": "21",
    "陳信全": "貸救補",  # 貸救補 4萬 待轉核
    "李淑樺": "和裕",    # 也送喬美
    "羅坤芃": "鄉民",
    "風傳龍": "喬美",
    "朱志明": "分貝汽車",
    "許志成": "分貝機車",
    "葉玥樂": "貸救補",
    "柯忻恩": "手機分期",
    "劉娟伶": "手機分期",
    "曾月英": "分貝機車",
    "熊高玲": "房地",
    # 零卡
    "王家齊": "零卡", "江淑芳": "零卡", "高峰瑛": "零卡", "王思婷": "零卡", "李宜芳": "零卡",
    "黃晏柔": "零卡", "陳玥汝": "零卡", "陳弈叡": "零卡", "呂昶宗": "零卡",
    "吳宗燁": "零卡", "黃文忠": "零卡", "張維鋼": "零卡", "黎俊志": "零卡",
}

# 同送公司（concurrent）— 日報顯示在多個區塊的姓名
CONCURRENT_CO = {
    "李淑樺": ["喬美"],     # current 和裕、同送喬美
    "謝美芃": ["喬美"],     # current 和裕(核准6萬)、同送喬美
    "陳信全": ["和裕"],     # current 貸救補、同送和裕
}


def parse_last_report_status(msgs):
    """找最後一份 (勞工紓困) 日報，抓每位客戶的狀態備註。
    回傳 {name: status_text}"""
    # 找最後一則含「(勞工紓困)」的大幫手訊息
    last_dump = None
    for m in msgs:
        if "(勞工紓困)" in m["text"] and m["sender"] == "大幫手":
            last_dump = m
    if not last_dump:
        return {}
    text = last_dump["text"]
    # 逐行 parse
    status_map = {}
    for line in text.splitlines():
        line = line.strip()
        # 匹配：{日期}-{姓名}-{公司...}-{備註}
        # 公司後面可能有「-」接備註，備註直到行尾或括號
        m = re.match(r"^\d{1,2}/\d{1,2}-([\u4e00-\u9fff]{2,5})-(.+?)$", line)
        if not m:
            continue
        name = m.group(1)
        rest = m.group(2).strip()
        # rest 可能是「21-已補申覆資料」「轉21-補時段」「第一-核准20萬-已補母時段」
        # 把第一個 - 前當公司，後面當備註
        parts = rest.split("-", 1)
        if len(parts) >= 2:
            note = parts[1].strip()
            status_map[name] = note
        elif rest:
            status_map[name] = rest  # 只有公司沒備註
    return status_map


def main():
    msgs = load_msgs("C:/Users/User/OneDrive/Desktop/[LINE]勞工紓困貸-鉅烽.txt")
    print(f"4 月訊息數: {len(msgs)}", file=sys.stderr)
    status_map = parse_last_report_status(msgs)
    print(f"日報備註抓到 {len(status_map)} 位", file=sys.stderr)

    customers = []
    for name in ACTIVE_NAMES:
        c = parse_customer(name, msgs)
        # 套用身分證 override
        if name in ID_OVERRIDE:
            c["id_no"] = ID_OVERRIDE[name]
        # 日報備註
        c["report_note"] = status_map.get(name, "")
        # 決定 current_company / report_section / approved
        if name in PENDING_DISB:
            co, amt, dd = PENDING_DISB[name]
            c["final_current"] = co
            c["final_approved_amount"] = f"{amt}萬"
            c["final_disb_date"] = dd
            c["final_report_section"] = "待撥款"
        elif name in ACTIVE_CO:
            c["final_current"] = ACTIVE_CO[name]
            c["final_approved_amount"] = ""
            c["final_disb_date"] = ""
            c["final_report_section"] = ""
        else:
            c["final_current"] = ""
            c["final_approved_amount"] = ""
            c["final_disb_date"] = ""
            c["final_report_section"] = ""
        c["final_concurrent"] = CONCURRENT_CO.get(name, [])
        customers.append(c)

    # 印給使用者看
    for c in customers:
        concurrent_str = ("+" + "+".join(c["final_concurrent"])) if c["final_concurrent"] else ""
        amt_str = f" 核准{c['final_approved_amount']}" if c["final_approved_amount"] else ""
        disb_str = f" 撥款{c['final_disb_date']}" if c["final_disb_date"] else ""
        section_tag = "💰待撥款 " if c["final_report_section"] == "待撥款" else "         "
        print(f"{section_tag}{c['name']:6s} {c['id_no'] or '❌':12s} | {c['final_current']}{concurrent_str}{amt_str}{disb_str} | 送:{c['route_order'] or '-'}")

    # 輸出 JSON
    import os
    out_path = os.path.join(os.path.dirname(__file__), "apr_loan_import.json")
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
            "signings": c["signings"],
            "report_note": c.get("report_note", ""),
        })
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(export, f, ensure_ascii=False, indent=2)
    print(f"\n已輸出 JSON: {out_path}", file=sys.stderr)


if __name__ == "__main__":
    main()
