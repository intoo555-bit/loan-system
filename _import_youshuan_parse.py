# -*- coding: utf-8 -*-
"""勞工優選 4 月活案 parser — 23 位活案。"""
import re
import json
import os
import sys

sys.stdout.reconfigure(encoding="utf-8")

ACTIVE_NAMES = [
    # 亞太
    "方侑浚", "蔡昀汝",
    # 21
    "唐千崎", "李翎瑜", "李宜蓁", "黃詩婷", "陳彥璋", "蘇政銘", "溫玉芬",
    # 第一
    "彭泓揚",
    # 鄉民
    "張禹安",
    # 分貝機車
    "李淑君", "林雅婷",
    # 手機分期
    "范育菱",
    # 送件區（route 設好但還沒被處理）
    "張宇鋒",
    # 待撥款（其中彭泓揚跟 21 有 dual approve）
    "李珂徵", "廖志仁", "蘇培鳳", "潘音霈", "蔡志豪",
    # 銀行
    "陳淑慧",
    # 零卡
    "蘇峻毅", "黃文樟",
]

# (current, concurrent_list, approved_萬, disb_date, report_section, note, status)
CUSTOMER_STATE = {
    "方侑浚":  ("亞太", [], "", "", "", "已補時段", "ACTIVE"),
    "蔡昀汝":  ("亞太", [], "", "", "", "", "ACTIVE"),
    "唐千崎":  ("21", [], "", "", "", "補申時段+更正聯2手機+補足3聯", "ACTIVE"),
    "李翎瑜":  ("21", [], "", "", "", "補申覆資料", "ACTIVE"),
    "李宜蓁":  ("21", ["和裕"], "", "", "", "已補申覆資料", "ACTIVE"),
    "黃詩婷":  ("21", [], "", "", "", "已補時段", "ACTIVE"),
    "陳彥璋":  ("21", [], "", "", "", "", "ACTIVE"),
    "蘇政銘":  ("21", [], "", "", "", "", "ACTIVE"),
    "溫玉芬":  ("21", [], "", "", "", "", "ACTIVE"),
    # 彭泓揚 21核准20萬待撥款（current）+ 第一26萬待核准（concurrent + company_status）
    "彭泓揚":  ("21", ["第一"], "20", "", "待撥款", "", "ACTIVE"),
    "張禹安":  ("鄉民", [], "", "", "", "缺資料+確認", "ACTIVE"),
    "李淑君":  ("分貝機車", [], "", "", "", "缺繳息", "ACTIVE"),
    "林雅婷":  ("分貝機車", [], "", "", "", "已補勞保+條碼+收據", "ACTIVE"),
    "范育菱":  ("手機分期", [], "", "", "", "補時段", "ACTIVE"),
    # 張宇鋒：送件順序設了但還沒被 A 群處理（送件區塊、明確設 report_section="送件"）
    "張宇鋒":  ("喬美", [], "", "", "送件", "", "ACTIVE"),
    # 待撥款
    "李珂徵":  ("21", [], "7", "4/27", "待撥款", "", "ACTIVE"),
    "廖志仁":  ("21", [], "6", "", "待撥款", "", "ACTIVE"),
    "蘇培鳳":  ("21", [], "7", "", "待撥款", "", "ACTIVE"),
    "潘音霈":  ("21", [], "25", "", "待撥款", "", "ACTIVE"),
    "蔡志豪":  ("21", [], "12", "", "待撥款", "", "ACTIVE"),
    "陳淑慧":  ("銀行", [], "", "", "", "", "ACTIVE"),
    "蘇峻毅":  ("零卡", [], "", "", "", "", "ACTIVE"),
    "黃文樟":  ("零卡", [], "", "", "", "", "ACTIVE"),
}

# 彭泓揚 21核准20萬主、第一26萬待核准 → company_status[第一]
EXTRA_APPROVED = {}

# 各客戶獨立公司備註（日報該家區塊顯示對應狀態）
COMPANY_NOTES = {
    "李宜蓁":  {"21": "已補申覆資料", "和裕": "已補時段"},
    "彭泓揚":  {"第一": "彭泓揚 第一 核准26萬-待核准"},
}

# 建案日期
BUILD_DATE = {
    "方侑浚": "04/13", "蔡昀汝": "04/27",
    "唐千崎": "04/07", "李翎瑜": "04/16", "李宜蓁": "04/23",
    "黃詩婷": "04/24", "陳彥璋": "04/27", "蘇政銘": "04/27", "溫玉芬": "04/28",
    "彭泓揚": "04/17",
    "張禹安": "04/27",
    "李淑君": "03/18", "林雅婷": "03/19",
    "范育菱": "03/26",
    "張宇鋒": "04/24",
    "李珂徵": "04/08", "廖志仁": "03/13", "蘇培鳳": "04/13",
    "潘音霈": "04/23", "蔡志豪": "04/23",
    "陳淑慧": "04/17",
    "蘇峻毅": "04/20", "黃文樟": "04/27",
}

ID_OVERRIDE = {
    "李淑君": "E221586762",
    "林雅婷": "T223598806",
}


def load_msgs(path):
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
        bm = re.search(rf"已建立.{{0,3}}(\d{{2,3}})/(\d{{1,2}})/(\d{{1,2}})-{re.escape(name)}", t)
        if bm and not info["build_date"]:
            info["build_date"] = f"{int(bm.group(2)):02d}/{int(bm.group(3)):02d}"
        if not info["id_no"]:
            idm = re.search(r"([A-Z][12]\d{8})", t)
            if idm and len(t) < 200:
                info["id_no"] = idm.group(1)
        if f"申請人姓名：{name}" in t:
            inner = parse_application_block(t)
            info.update(inner)
        rm = re.search(
            rf"\d{{1,2}}/\d{{1,2}}-{re.escape(name)}[\s\-]+((?:[一-鿿]|21|第一)[一-鿿0-9機車商品萬汽分貝鄉銀代手C貸]*(?:/[^\s\n/]+){{1,}})",
            t)
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
    m = re.search(r"行動電話[:：]\s*([\d\-]+)", text)
    if m: out["phone"] = m.group(1)
    return out


def main():
    src = r"C:\Users\User\OneDrive\Desktop\[LINE]勞工優選.txt"
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
        c["company_notes"] = COMPANY_NOTES.get(name, {})
        customers.append(c)

    for c in customers:
        concurrent_str = ("+" + "+".join(c["final_concurrent"])) if c["final_concurrent"] else ""
        amt_str = f" 核准{c['final_approved_amount']}" if c["final_approved_amount"] else ""
        disb_str = f" 撥款{c['final_disb_date']}" if c["final_disb_date"] else ""
        st_tag = "💰待撥款 " if c["final_report_section"] == "待撥款" else "         "
        print(f"{st_tag}{c['name']:6s} {c['id_no'] or '❌':12s} | {c['final_current']}{concurrent_str}{amt_str}{disb_str}")

    out_path = os.path.join(os.path.dirname(__file__), "youshuan_apr_import.json")
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
            "extra_approved": c["extra_approved"],
            "company_notes": c["company_notes"],
        })
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(export, f, ensure_ascii=False, indent=2)
    print(f"\n已輸出 JSON: {out_path}", file=sys.stderr)


if __name__ == "__main__":
    main()
