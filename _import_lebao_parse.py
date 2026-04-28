# -*- coding: utf-8 -*-
"""樂保 4 月活案 parser — 60 位活案 + 1 位違約金結案。
資料來源：[LINE]💰老闆娘帶著飛🛫️.txt（4 月訊息）+ 4/28 最新日報。
"""
import re
import json
import os
import sys

sys.stdout.reconfigure(encoding="utf-8")

# 4/28 樂保最新日報活案名單（含 1 位 PENALTY 結案）
ACTIVE_NAMES = [
    # 裕融 / 21汽車
    "陳榮宏", "侯佳菱",
    # 亞太
    "莊珉語", "胡正浩", "蕭嘉宏", "許力元", "郭政達",
    # 21
    "余佳祥", "蔡宗欽", "劉佳欣", "蔡峰榮", "張文馨", "陳柏安", "潘俐蒨",
    "鄭俊明", "黃祖瑞", "雷秉華", "張竣翔",
    # 鄉民 / 喬美
    "高振原", "蘇洏潁",
    # 分貝
    "羅婉婷", "朱婉蓁", "陳莉雯",
    # 貸救補
    "蕭柏鈞", "李品葳", "王照雄", "邱微馨", "溫彥誠",
    # 送件區
    "潘建宇",
    # 待撥款
    "黃春蓮", "陳資仁", "陳冠和", "陳漢聰", "彭宣惟", "廖保勳", "葉家豪",
    "巫文斌", "陳冠羽", "周美玲", "朱芳葦", "陳家輝", "潘信宏", "劉英璋",
    "陳志龍", "陳麗珠", "林絡語", "呂雨柔", "陳嘉純", "吳崇聖", "施承志",
    # 零卡
    "張容粸", "陳冠伶", "詹博翔", "陳筱琪", "田惠秋", "蔣能靜", "李福來",
    "許程闈", "姚珊妮",
    # 房地（待撥款）
    "許建文",
    # 違約金結案（PENALTY、不在日報但要建紀錄）
    "邱柏翰",
]

# 各客戶在最新日報的狀態
# (current, concurrent_list, approved_萬, disb_date, report_section, note, status)
# status 預設 ACTIVE、特殊狀態（PENALTY）顯式標
CUSTOMER_STATE = {
    # 裕融
    "陳榮宏":  ("裕融", [], "", "", "", "", "ACTIVE"),
    "侯佳菱":  ("21汽車", [], "", "", "", "補保", "ACTIVE"),
    # 亞太
    "莊珉語":  ("亞太", [], "", "", "", "", "ACTIVE"),
    "胡正浩":  ("亞太", ["21"], "", "", "", "圖", "ACTIVE"),
    "蕭嘉宏":  ("亞太", [], "", "", "", "", "ACTIVE"),
    "許力元":  ("亞太", ["21"], "", "", "", "", "ACTIVE"),
    "郭政達":  ("亞太", ["喬美"], "", "", "", "", "ACTIVE"),
    # 21
    "余佳祥":  ("21", [], "", "", "", "申覆:補職電時段", "ACTIVE"),
    "蔡宗欽":  ("21", [], "", "", "", "已補時段+確認", "ACTIVE"),
    "劉佳欣":  ("21", [], "", "", "", "補時段", "ACTIVE"),
    "蔡峰榮":  ("21", [], "", "", "", "申覆:更換知聯", "ACTIVE"),
    "張文馨":  ("21", [], "", "", "", "已補時段", "ACTIVE"),
    "陳柏安":  ("21", [], "", "", "", "補時段", "ACTIVE"),
    "潘俐蒨":  ("21", [], "", "", "", "已補時段", "ACTIVE"),
    "鄭俊明":  ("21", [], "", "", "", "已補申覆資料", "ACTIVE"),
    "黃祖瑞":  ("21", [], "", "", "", "補時段", "ACTIVE"),
    "雷秉華":  ("21", ["喬美"], "", "", "", "", "ACTIVE"),
    "張竣翔":  ("21", [], "", "", "", "", "ACTIVE"),
    # 鄉民
    "高振原":  ("鄉民", [], "", "", "", "缺資料+確認", "ACTIVE"),
    # 喬美 + 待撥款 21（蘇洏潁 是同送 + 21 已核准）
    "蘇洏潁":  ("21", ["喬美"], "25", "", "待撥款", "", "ACTIVE"),
    # 分貝
    "羅婉婷":  ("分貝汽車", [], "", "", "", "缺繳息", "ACTIVE"),
    "朱婉蓁":  ("分貝汽車", [], "", "", "", "補條碼+收據", "ACTIVE"),
    "陳莉雯":  ("分貝機車", [], "", "", "", "補職電時段+1二等知情", "ACTIVE"),
    # 貸救補
    "蕭柏鈞":  ("貸救補", [], "", "", "", "補協商繳息明細+往存", "ACTIVE"),
    "李品葳":  ("貸救補", [], "5", "", "", "待轉核", "ACTIVE"),
    "王照雄":  ("貸救補", [], "", "", "", "已更換二等親聯", "ACTIVE"),
    "邱微馨":  ("貸救補", [], "", "", "", "已補時段", "ACTIVE"),
    "溫彥誠":  ("貸救補", [], "", "", "", "", "ACTIVE"),
    # 送件區
    "潘建宇":  ("和裕", ["21"], "", "", "", "缺PDF", "ACTIVE"),
    # 待撥款（23 行、葉家豪和裕+21 雙核准）
    "黃春蓮":  ("裕融", [], "15", "", "待撥款", "", "ACTIVE"),
    "陳資仁":  ("麻吉", [], "8", "", "待撥款", "", "ACTIVE"),
    "陳冠和":  ("21", [], "25", "", "待撥款", "", "ACTIVE"),
    "陳漢聰":  ("21", [], "6", "4/27", "待撥款", "", "ACTIVE"),
    "彭宣惟":  ("21", [], "25", "", "待撥款", "", "ACTIVE"),
    "廖保勳":  ("貸救補", [], "4", "", "待撥款", "", "ACTIVE"),
    "葉家豪":  ("和裕", ["21"], "8", "", "待撥款", "", "ACTIVE"),
    "巫文斌":  ("手機分期", [], "2", "", "待撥款", "", "ACTIVE"),
    "陳冠羽":  ("21", [], "25", "", "待撥款", "", "ACTIVE"),
    "周美玲":  ("21", [], "7", "", "待撥款", "", "ACTIVE"),
    "朱芳葦":  ("21", [], "6", "", "待撥款", "", "ACTIVE"),
    "陳家輝":  ("21", [], "6", "", "待撥款", "", "ACTIVE"),
    "潘信宏":  ("21", [], "8", "4/27", "待撥款", "", "ACTIVE"),
    "劉英璋":  ("21", [], "7", "4/28", "待撥款", "", "ACTIVE"),
    "陳志龍":  ("21", [], "7", "4/28", "待撥款", "", "ACTIVE"),
    "陳麗珠":  ("21", [], "6", "4/28", "待撥款", "", "ACTIVE"),
    "林絡語":  ("21", [], "7", "4/27", "待撥款", "", "ACTIVE"),
    "呂雨柔":  ("21", [], "6", "", "待撥款", "", "ACTIVE"),
    "陳嘉純":  ("和裕", [], "6", "", "待撥款", "", "ACTIVE"),
    "吳崇聖":  ("21", [], "6", "", "待撥款", "", "ACTIVE"),
    "施承志":  ("21", [], "25", "", "待撥款", "", "ACTIVE"),
    # 零卡
    "張容粸":  ("零卡", [], "", "", "", "", "ACTIVE"),
    "陳冠伶":  ("零卡", [], "", "", "", "", "ACTIVE"),
    "詹博翔":  ("零卡", [], "", "", "", "", "ACTIVE"),
    "陳筱琪":  ("零卡", [], "", "", "", "", "ACTIVE"),
    "田惠秋":  ("零卡", [], "", "", "", "", "ACTIVE"),
    "蔣能靜":  ("零卡", [], "", "", "", "", "ACTIVE"),
    "李福來":  ("零卡", [], "", "", "", "", "ACTIVE"),
    "許程闈":  ("零卡", [], "", "", "", "", "ACTIVE"),
    "姚珊妮":  ("零卡", [], "", "", "", "", "ACTIVE"),
    # 房地（待撥款 等違約金）
    "許建文":  ("房地", [], "25", "", "待撥款", "等違約金", "ACTIVE"),
    # 違約金結案（PENALTY）
    "邱柏翰":  ("房地", [], "50", "", "", "違約金結案", "PENALTY"),
}

# 葉家豪 21 也核准 6 萬（extra_approved）
EXTRA_APPROVED = {
    "葉家豪": [("21", "6")],
}

# 建案日期（日報日期）
BUILD_DATE = {
    "陳榮宏": "04/27", "侯佳菱": "04/21",
    "莊珉語": "04/27", "胡正浩": "04/24", "蕭嘉宏": "04/27", "許力元": "04/27", "郭政達": "04/28",
    "余佳祥": "03/23", "蔡宗欽": "04/07", "劉佳欣": "04/10", "蔡峰榮": "04/15",
    "張文馨": "04/21", "陳柏安": "04/20", "潘俐蒨": "04/23", "鄭俊明": "04/24",
    "黃祖瑞": "04/24", "雷秉華": "04/27", "張竣翔": "04/27",
    "高振原": "04/16", "蘇洏潁": "04/20",
    "羅婉婷": "04/14", "朱婉蓁": "04/22", "陳莉雯": "04/15",
    "蕭柏鈞": "04/13", "李品葳": "03/31", "王照雄": "04/20", "邱微馨": "04/13", "溫彥誠": "04/09",
    "潘建宇": "04/15",
    "黃春蓮": "02/10", "陳資仁": "03/20", "陳冠和": "04/07", "陳漢聰": "04/02",
    "彭宣惟": "04/13", "廖保勳": "03/24", "葉家豪": "04/16", "巫文斌": "04/14",
    "陳冠羽": "04/17", "周美玲": "04/07", "朱芳葦": "04/14", "陳家輝": "04/14",
    "潘信宏": "04/20", "劉英璋": "04/13", "陳志龍": "04/02", "陳麗珠": "04/17",
    "林絡語": "04/17", "呂雨柔": "04/20", "陳嘉純": "04/21", "吳崇聖": "04/14", "施承志": "04/23",
    "張容粸": "04/02", "陳冠伶": "04/10", "詹博翔": "04/17", "陳筱琪": "04/01",
    "田惠秋": "03/30", "蔣能靜": "04/20", "李福來": "04/20", "許程闈": "04/21", "姚珊妮": "04/27",
    "許建文": "01/29",
    "邱柏翰": "03/12",
}

# 身分證手動補（parser 抓不到的、訊息 >200 字或建案前後散）
ID_OVERRIDE = {
    "余佳祥": "J121683384",
    "黃春蓮": "S222077656",
    "陳資仁": "K120545224",
    "廖保勳": "P124176997",
    "許建文": "L122664286",
    "邱柏翰": "S124861289",
}


def load_msgs(path):
    """讀 LINE txt、抓 2026/04 訊息"""
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
    info = {
        "name": name, "id_no": "", "birth": "", "phone": "",
        "company": "", "company_phone": "", "addr_reg": "", "addr_live": "",
        "email": "", "build_date": "", "route_order": "",
    }
    for m in msgs:
        t = m["text"]
        if name not in t:
            continue
        # 建案日期
        bm = re.search(rf"已建立.{{0,3}}(\d{{2,3}})/(\d{{1,2}})/(\d{{1,2}})-{re.escape(name)}", t)
        if bm and not info["build_date"]:
            info["build_date"] = f"{int(bm.group(2)):02d}/{int(bm.group(3)):02d}"
        # 身分證
        if not info["id_no"]:
            idm = re.search(r"([A-Z][12]\d{8})", t)
            if idm and len(t) < 200:
                info["id_no"] = idm.group(1)
        # 完整申請書 block
        if f"申請人姓名：{name}" in t or f"申請人姓名:{name}" in t:
            info.update(parse_application_block(t))
        # 送件順序
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
    return out


def main():
    src = r"C:\Users\User\OneDrive\Desktop\[LINE]💰老闆娘帶著飛🛫️.txt"
    msgs = load_msgs(src)
    print(f"4 月訊息數: {len(msgs)}", file=sys.stderr)

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
        c["extra_approved"] = EXTRA_APPROVED.get(name, [])
        customers.append(c)

    # console preview
    for c in customers:
        concurrent_str = ("+" + "+".join(c["final_concurrent"])) if c["final_concurrent"] else ""
        amt_str = f" 核准{c['final_approved_amount']}" if c["final_approved_amount"] else ""
        disb_str = f" 撥款{c['final_disb_date']}" if c["final_disb_date"] else ""
        st_tag = "💸違約結 " if c["final_status"] == "PENALTY" else (
                 "💰待撥款 " if c["final_report_section"] == "待撥款" else "         ")
        print(f"{st_tag}{c['name']:6s} {c['id_no'] or '❌':12s} | {c['final_current']}{concurrent_str}{amt_str}{disb_str}")

    # 輸出 JSON
    out_path = os.path.join(os.path.dirname(__file__), "lebao_apr_import.json")
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
            "status": c["final_status"],
            "extra_approved": c.get("extra_approved", []),
            "company_notes": {},
        })
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(export, f, ensure_ascii=False, indent=2)
    print(f"\n已輸出 JSON: {out_path}", file=sys.stderr)


if __name__ == "__main__":
    main()
