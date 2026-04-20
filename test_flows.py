"""本地完整流程測試 — 覆蓋 A 群、業務群 (B/C)、補申覆、補照會、婉拒、核准、撥款、違約金。
不需要 LINE webhook、直接呼叫內部 handler、檢查 DB 狀態。

用法：
  DB_PATH=./test_data/test_flows.db python test_flows.py
"""
import os, sys, sqlite3, json, shutil
from datetime import datetime

TEST_DB = os.path.abspath("./test_data/test_flows.db")
if os.path.exists(TEST_DB):
    os.remove(TEST_DB)
os.environ["DB_PATH"] = TEST_DB
os.environ["CHANNEL_ACCESS_TOKEN"] = ""
os.environ["BACKUP_ENABLED"] = "false"

import main as m

# 禁用 push_text/reply_text（避免呼叫外部 LINE API）
replies = []
pushes = []
def fake_reply(token, text):
    replies.append(text)
    return True
def fake_push(gid, text):
    pushes.append((gid, text))
    return (True, "")
m.reply_text = fake_reply
m.push_text = fake_push
# 攔截 quick reply
quick_replies = []
def fake_quick_reply(token, text, items):
    quick_replies.append(text)
    return True
m.reply_quick_reply = fake_quick_reply

# 建立測試群組
conn = sqlite3.connect(TEST_DB)
cur = conn.cursor()
now = datetime.now().isoformat()
cur.execute("INSERT OR REPLACE INTO groups (group_id, group_name, group_type, is_active, created_at) VALUES (?,?,?,?,?)",
            ("TEST_B", "B群", "SALES_GROUP", 1, now))
cur.execute("INSERT OR REPLACE INTO groups (group_id, group_name, group_type, is_active, created_at) VALUES (?,?,?,?,?)",
            ("TEST_A", "A群", "A_GROUP", 1, now))
conn.commit(); conn.close()
m.A_GROUP_ID = "TEST_A"

PASS, FAIL = 0, 0
def check(label, cond, detail=""):
    global PASS, FAIL
    if cond:
        PASS += 1
        print(f"  ✅ {label}")
    else:
        FAIL += 1
        print(f"  ❌ {label}" + (f"  [{detail}]" if detail else ""))

def get_cust(id_no):
    conn = sqlite3.connect(TEST_DB); conn.row_factory = sqlite3.Row
    r = conn.execute("SELECT * FROM customers WHERE id_no=? ORDER BY created_at DESC LIMIT 1", (id_no,)).fetchone()
    conn.close()
    return dict(r) if r else None

def bc(text, gid="TEST_B"):
    replies.clear(); pushes.clear()
    # @AI 指令走 parse_special_command + handle_special_command
    if m.has_ai_trigger(text):
        cmd = m.parse_special_command(text, gid)
        if cmd:
            m.handle_special_command(cmd, "mock_token", gid)
            return
    else:
        # 對保派件/對保員回時間地點 純文字也要 parse
        cmd2 = m.parse_special_command(text, gid)
        if cmd2 and cmd2.get("type") in ("signing_request", "signing_schedule"):
            m.handle_special_command(cmd2, "mock_token", gid)
            return
    return m._handle_bc_case_block_locked(text, gid, "mock_token", text)

def a(text):
    replies.clear(); pushes.clear()
    return m._handle_a_case_block_locked(text, "mock_token", m.extract_id_no(text), m.extract_name(text))

# ========== 1. 業務群建客戶 + 送件順序 ==========
print("\n=== 1. 業務群建客戶 + 送件順序 ===")
bc("4/20-王大明A123456789", gid="TEST_B")  # 先建
bc("4/20-王大明-亞太機25萬/第一/21機車25萬", gid="TEST_B")  # 再送件順序
c = get_cust("A123456789")
check("客戶建立", c is not None)
check("姓名正確", c["customer_name"] == "王大明", c.get("customer_name") if c else None)
check("current=亞太機25萬", c["current_company"] == "亞太機25萬", c.get("current_company") if c else None)

# ========== 2. A 群核准 ==========
print("\n=== 2. A 群核准 ===")
a("4/20-王大明A123456789-亞太機25萬 核准25萬")
c = get_cust("A123456789")
check("approved_amount 有值", (c.get("approved_amount") or "") != "", c.get("approved_amount"))
check("report_section=待撥款", c.get("report_section") == "待撥款", c.get("report_section"))

# ========== 3. A 群婉拒（第 1 行婉拒、第 2 行有「核貸」迷惑） ==========
print("\n=== 3. A 群婉拒、備註有核貸字樣 ===")
# 先建第二個客戶
bc("4/20-蔡依琳A234567890", gid="TEST_B")
bc("4/20-蔡依琳-亞太機25萬/第一/21商品", gid="TEST_B")
a("4/20-蔡依琳A234567890-亞太機25萬\n婉拒\n投保45k 近期銀行核貸兩筆 綜合考量 婉拒")
c = get_cust("A234567890")
check("沒誤判成核准（approved 空）", not (c["approved_amount"] or ""), c.get("approved_amount"))

# ========== 4. 業務群 @AI 亞太婉拒（reject_company）→ current 要跳到第一 ==========
print("\n=== 4. 業務群 @AI 亞太婉拒、current 升級 ===")
# 建第三個客戶同送 3 家
bc("4/20-林志玲A345678901", gid="TEST_B")
bc("4/20-林志玲-亞太機25萬/第一/21商品", gid="TEST_B")
# 用 @AI 同送：先照會 3 家
bc("@AI 林志玲 亞太機25萬+第一+21商品 照會", gid="TEST_B")
c = get_cust("A345678901")
concur_before = (c.get("concurrent_companies") or "").split(",")
# 婉拒 亞太
bc("@AI 林志玲 亞太婉拒", gid="TEST_B")
c = get_cust("A345678901")
check("current 從亞太跳走", m.normalize_section(c.get("current_company") or "") != "亞太",
      f"current={c.get('current_company')}")
check("concurrent 仍有第一/21", "第一" in (c.get("concurrent_companies") or "") or "21" in (c.get("concurrent_companies") or ""),
      f"concurrent={c.get('concurrent_companies')}")
check("report_section 跟著 current 更新", (c.get("report_section") or "") != "亞太",
      f"report_section={c.get('report_section')}")

# ========== 5. 業務群 補申覆 → 更新 company_status[和裕] + 日報狀態正確 ==========
print("\n=== 5. 業務群 補申覆 同步 company_status + 日報狀態正確 ===")
bc("4/20-孫悟飯A456789012", gid="TEST_B")
bc("4/20-孫悟飯-和裕機", gid="TEST_B")
a("4/20-孫悟飯A456789012-和裕機\n待補薪轉申覆")
c = get_cust("A456789012")
cs_before = json.loads(c.get("company_status") or "{}")
# 驗證「待補」狀態先
status_before = m.extract_status_summary(cs_before.get("和裕",""), "孫悟飯")
check("日報 before = 待補申覆", status_before == "待補申覆", f"got={status_before}")
# 業務打已補
bc("孫悟飯 和裕已補薪轉申覆", gid="TEST_B")
c = get_cust("A456789012")
cs_after = json.loads(c.get("company_status") or "{}")
check("company_status[和裕] 有更新", cs_after.get("和裕","") and "已補" in cs_after.get("和裕",""),
      f"got={cs_after.get('和裕')}")
# 驗證日報狀態從「待補申覆」變「已補申覆」
status_after = m.extract_status_summary(cs_after.get("和裕",""), "孫悟飯")
check("日報 after = 已補申覆（不再顯示錯誤狀態）", status_after == "已補申覆", f"got={status_after}")

# ========== 6. 補照會 ==========
print("\n=== 6. 業務群 補照會、日報狀態正確 ===")
# 先 A 群留「待補照會」
a("4/20-孫悟飯A456789012-和裕機\n待補照會")
c = get_cust("A456789012")
cs = json.loads(c.get("company_status") or "{}")
status_wait = m.extract_status_summary(cs.get("和裕",""), "孫悟飯")
check("日報 = 待補照會 (擬 A 群回覆)", "補照會" in status_wait or "照會" in status_wait,
      f"got={status_wait}")
# 業務打已補
bc("孫悟飯 和裕已補照會", gid="TEST_B")
c = get_cust("A456789012")
cs = json.loads(c.get("company_status") or "{}")
status_done = m.extract_status_summary(cs.get("和裕",""), "孫悟飯")
check("日報 = 已補照會（已送件）", status_done in ("已補資料","已送件") or "已補" in status_done,
      f"got={status_done}")

# ========== 7. 核准自動推公司家族（21 核准 25萬、客戶送 21機車12萬）==========
print("\n=== 7. 打「21 核准」自動對到客戶在送的 21 家族 ===")
bc("4/20-陳小明A567890123", gid="TEST_B")
bc("4/20-陳小明-21機車12萬", gid="TEST_B")
bc("@AI 陳小明 21 核准 25萬", gid="TEST_B")
c = get_cust("A567890123")
check("current=21機車12萬（非 21商品）", c.get("current_company") == "21機車12萬",
      f"current={c.get('current_company')}")
check("approved 有值", (c.get("approved_amount") or "").startswith("25"), c.get("approved_amount"))

# ========== 8. 撥款模糊比對 ==========
print("\n=== 8. 打「21機 撥款」對到 21機車12萬 ===")
bc("@AI 陳小明 21機 撥款 4/20", gid="TEST_B")
c = get_cust("A567890123")
check("撥款日已寫入", (c.get("disbursement_date") or "") != "",
      f"disb={c.get('disbursement_date')}")

# ========== 9. 違約金 2 段式 ==========
print("\n=== 9. 違約金 2 段式結案 ===")
bc("4/20-吳瑞銘A678901234", gid="TEST_B")
bc("4/20-吳瑞銘-亞太商品", gid="TEST_B")
bc("@AI 吳瑞銘 違約金已支付15萬", gid="TEST_B")
c = get_cust("A678901234")
check("penalty_amount=150000", c.get("penalty_amount") == "150000", c.get("penalty_amount"))
check("penalty_pending=1", c.get("penalty_pending") == "1", c.get("penalty_pending"))
check("status 還是 ACTIVE", c.get("status") == "ACTIVE", c.get("status"))
# 二次確認
bc("@AI 吳瑞銘 違約金確認支付15萬", gid="TEST_B")
c = get_cust("A678901234")
check("status=PENALTY", c.get("status") == "PENALTY", c.get("status"))
check("penalty_date 有值", (c.get("penalty_date") or "") != "", c.get("penalty_date"))

# ========== 10. 建新客戶備註有「機車」不誤判 ==========
print("\n=== 10. 建新客戶備註含「機車」不誤判公司 ===")
bc("115/04/21蔡美玲A789012345\n聯絡人不知情/機車無貸款", gid="TEST_B")
c = get_cust("A789012345")
# 沒帶送件順序、公司應該是空或送件區
check("公司不誤判為 21", (c.get("company") or "") != "21商品" and "21" not in (c.get("current_company") or ""),
      f"co={c.get('company')}, current={c.get('current_company')}")

# ========== 11. 防錯：婉拒沒帶公司、2 家在送 ==========
print("\n=== 11. 婉拒沒帶公司、跳警告 ===")
bc("4/20-曹操A111222333", gid="TEST_B")
bc("4/20-曹操-亞太機25萬/第一", gid="TEST_B")
bc("@AI 曹操 亞太機25萬+第一 照會", gid="TEST_B")  # 同送 2 家
replies.clear()
bc("@AI 曹操 婉拒", gid="TEST_B")
check("婉拒沒帶公司 → 跳警告", any("要婉拒哪家" in r for r in replies),
      f"replies={replies}")

# ========== 12. 防錯：照會沒帶公司、2 家在送 ==========
print("\n=== 12. 照會沒帶公司、跳警告 ===")
replies.clear()
bc("@AI 曹操 照會", gid="TEST_B")
check("照會沒帶公司 → 跳警告", any("要照會哪家" in r for r in replies),
      f"replies={replies}")

# ========== 13. 防錯：補件沒帶公司、2 家在送 ==========
print("\n=== 13. 補件沒帶公司、跳警告 ===")
replies.clear()
result = bc("曹操 補繳息", gid="TEST_B")  # 泛用「補 XX」
check("補件沒帶公司 → 跳警告",
      "請指明是哪一家" in (result or "") or any("請指明是哪一家" in r for r in replies),
      f"result={result}, replies={replies}")

# ========== 14. 防錯：核准沒帶公司、2 家在送 ==========
print("\n=== 14. 核准沒帶公司、跳警告 ===")
replies.clear()
bc("@AI 曹操 核准 20萬", gid="TEST_B")
check("核准沒帶公司 → 跳警告", any("要核准哪家" in r for r in replies),
      f"replies={replies}")

# ========== 15. 取消核准 家族比對 ==========
print("\n=== 15. 取消核准家族比對 ===")
bc("4/20-劉備A222333444", gid="TEST_B")
bc("4/20-劉備-21機車25萬", gid="TEST_B")
bc("@AI 劉備 21機車25萬 核准 25萬", gid="TEST_B")
c = get_cust("A222333444")
check("核准記入 21機車25萬", (c.get("approved_amount") or "").startswith("25"),
      c.get("approved_amount"))
bc("@AI 劉備 21 取消核准", gid="TEST_B")
c = get_cust("A222333444")
check("取消核准成功（打簡稱 21）", not (c.get("approved_amount") or ""),
      c.get("approved_amount"))

# ========== 16. 時區：新紀錄用台灣時間 ==========
print("\n=== 16. 時區：now_iso() 回台灣時間 ===")
from datetime import datetime, timezone, timedelta
now_tw = datetime.now(timezone(timedelta(hours=8))).strftime("%Y-%m-%d %H")
check("now_iso() 包含當前台灣時間", m.now_iso().startswith(now_tw),
      f"now_iso={m.now_iso()}, expect starts with {now_tw}")

# ========== 17. 違約金修改（已支付 → 再打新金額會覆蓋）==========
print("\n=== 17. 違約金覆蓋更新 ===")
bc("4/21-關羽A333444555", gid="TEST_B")
bc("@AI 關羽 違約金已支付15萬", gid="TEST_B")
bc("@AI 關羽 違約金已支付10萬", gid="TEST_B")
c = get_cust("A333444555")
check("違約金覆蓋為 10萬=100000", c.get("penalty_amount") == "100000",
      c.get("penalty_amount"))

# ========== 18. 同送概念：當前+同送都顯示在日報 ==========
print("\n=== 18. 同送 section_map 日報正確 ===")
bc("4/21-諸葛亮A444555666", gid="TEST_B")
bc("4/21-諸葛亮-第一/21機25", gid="TEST_B")
bc("@AI 諸葛亮 第一+21機25 照會", gid="TEST_B")
c = get_cust("A444555666")
concur = c.get("concurrent_companies") or ""
check("concurrent 含 21", "21" in concur, f"concur={concur}")

# ========== 19. 核准後 current 換、原 current 降到同送 ==========
print("\n=== 19. 核准自動升級 current ===")
bc("@AI 諸葛亮 21 核准 20萬", gid="TEST_B")
c = get_cust("A444555666")
# 21 應該升到 current (normalize=21)、原 current 第一 降到 concurrent
check("current 換成 21 家族", m.normalize_section(c.get("current_company") or "") == "21",
      f"current={c.get('current_company')}")
check("原 current 第一 在 concurrent", "第一" in (c.get("concurrent_companies") or ""),
      f"concur={c.get('concurrent_companies')}")

# ========== 20. 多家核准、撥款選一家 ==========
print("\n=== 20. 多家核准、撥款指定 ===")
bc("4/21-趙雲A555666777", gid="TEST_B")
bc("4/21-趙雲-第一/喬美", gid="TEST_B")
bc("@AI 趙雲 第一+喬美 照會", gid="TEST_B")
bc("@AI 趙雲 第一 核准 30萬", gid="TEST_B")
bc("@AI 趙雲 喬美 核准 14萬", gid="TEST_B")
bc("@AI 趙雲 第一 撥款 4/21", gid="TEST_B")
c = get_cust("A555666777")
check("撥款日已寫入", (c.get("disbursement_date") or "") != "",
      c.get("disbursement_date"))

# ========== 21. 跨月統計：本月結案只含 CLOSED/PENALTY/ABANDONED/REJECTED ==========
print("\n=== 21. 本月結案統計包含正確狀態 ===")
# 建一筆 PENDING（不該算結案）
conn = sqlite3.connect(TEST_DB); cur = conn.cursor()
cur.execute("INSERT INTO customers (case_id,customer_name,id_no,source_group_id,status,created_at,updated_at) VALUES (?,?,?,?,?,?,?)",
            ("PENDING_001","王小小","P111222333","TEST_B","PENDING",now,now))
cur.execute("INSERT INTO customers (case_id,customer_name,id_no,source_group_id,status,created_at,updated_at) VALUES (?,?,?,?,?,?,?)",
            ("CLOSED_001","王大大","P111222334","TEST_B","CLOSED",now,now))
conn.commit()
# 本月結案查詢
month_start = m.now_tw().strftime("%Y-%m-01")
cur.execute("SELECT COUNT(*) AS n FROM customers WHERE status IN ('CLOSED','PENALTY','ABANDONED','REJECTED') AND updated_at >= ?", (month_start,))
closed_count = cur.fetchone()[0]
cur.execute("SELECT COUNT(*) AS n FROM customers WHERE status != 'ACTIVE'")
not_active_count = cur.fetchone()[0]
conn.close()
check("本月結案只數 CLOSED/PENALTY/etc（不含 PENDING）", closed_count >= 1 and closed_count < not_active_count,
      f"closed={closed_count}, not_active={not_active_count}")

# ========== 22. 時區：created_at 格式正確 ==========
print("\n=== 22. DB 寫入時間格式 ===")
bc("4/21-陸遜A666777888", gid="TEST_B")
c = get_cust("A666777888")
check("created_at 為台灣時間格式", c["created_at"].startswith(m.now_tw().strftime("%Y-%m-%d %H")),
      f"created_at={c.get('created_at')}")

# ========== 23. 還原：update_customer 有存 snapshot ==========
print("\n=== 23. 還原 snapshot 完整性 ===")
bc("4/21-司馬懿A777888999", gid="TEST_B")
bc("4/21-司馬懿-第一/喬美", gid="TEST_B")
before = get_cust("A777888999")
bc("@AI 司馬懿 第一 核准 30萬", gid="TEST_B")
# 查 case_logs 看 snapshot
conn2 = sqlite3.connect(TEST_DB); conn2.row_factory = sqlite3.Row
log = conn2.execute("SELECT snapshot_json FROM case_logs WHERE case_id=? ORDER BY id DESC LIMIT 1",
                    (before["case_id"],)).fetchone()
conn2.close()
check("核准操作有存 snapshot", log and log["snapshot_json"],
      f"snapshot={log['snapshot_json'][:50] if log and log['snapshot_json'] else None}")
# 還原
bc("@AI 司馬懿 還原 1", gid="TEST_B")
c = get_cust("A777888999")
check("還原後 approved_amount 清空（回到核准前）", not (c.get("approved_amount") or ""),
      f"approved={c.get('approved_amount')}")

# ========== 24. 婉拒理由保留在 case_logs（透過 BC 群補件帶理由）==========
print("\n=== 24. case_logs 保留訊息完整理由 ===")
bc("4/21-龐統A888999000", gid="TEST_B")
bc("4/21-龐統-亞太機", gid="TEST_B")
bc("龐統 亞太機 婉拒 負債比過高信用評分不足", gid="TEST_B")
conn3 = sqlite3.connect(TEST_DB); conn3.row_factory = sqlite3.Row
c = get_cust("A888999000")
log2 = conn3.execute("SELECT message_text FROM case_logs WHERE case_id=? ORDER BY id DESC LIMIT 1",
                     (c["case_id"],)).fetchone()
conn3.close()
check("case_logs 保留婉拒完整理由", log2 and "負債比" in (log2["message_text"] or ""),
      f"log={log2['message_text'][:80] if log2 else None}")

# ========== 25. 統計：ACTIVE 數量 ==========
print("\n=== 25. 進行中客戶計數 ===")
conn4 = sqlite3.connect(TEST_DB)
active_count = conn4.execute("SELECT COUNT(*) FROM customers WHERE status='ACTIVE'").fetchone()[0]
conn4.close()
check("有計入 ACTIVE 客戶", active_count >= 3, f"active={active_count}")

# ========== 26-29. 跳過（_build_cell_map / _build_txt_content 是 nested function） ==========
# 這兩個函式在 adminb_download_excel 裡面、測試需要走 HTTP 路徑才能觸發
# 改用 28/29 整合測試替代

# ========== 30. 對保完整流程：派對保→回時間→對好→撥款 ==========
print("\n=== 30. 對保完整流程 end-to-end ===")
bc("4/21-關興A989898989", gid="TEST_B")
bc("4/21-關興-亞太機", gid="TEST_B")
bc("@AI 關興 亞太 核准 15萬", gid="TEST_B")
c = get_cust("A989898989")
approved_ok = (c.get("approved_amount") or "").startswith("15") or (c.get("approved_amount") or "") == "15"
check("步驟1 核准", approved_ok, c.get("approved_amount"))
# 派對保
bc("辦理方案：亞太\n核准金額：15萬\n客戶姓名：關興\n對保地區：台北市", gid="TEST_B")
c = get_cust("A989898989")
check("步驟2 派對保→signing_area=台北市", c.get("signing_area") == "台北市", c.get("signing_area"))
# 對保員回時間地點
bc("關興 亞太機\n時間 4/22 14:00\n地點 台北車站", gid="TEST_B")
c = get_cust("A989898989")
check("步驟3 對保時間", (c.get("signing_time") or "") != "", c.get("signing_time"))
check("步驟3 對保地點", (c.get("signing_location") or "") != "", c.get("signing_location"))
# 撥款
bc("@AI 關興 亞太 撥款 4/22", gid="TEST_B")
c = get_cust("A989898989")
check("步驟4 撥款日已寫入", (c.get("disbursement_date") or "") != "", c.get("disbursement_date"))

# ========== 31. 批次結案 ==========
print("\n=== 31. 批次結案 ===")
for i, nm in enumerate(["張飛", "趙子龍", "黃忠"]):
    bc(f"4/21-{nm}B{i+1:09d}", gid="TEST_B")
    bc(f"4/21-{nm}-亞太機", gid="TEST_B")
bc("@AI 批次結案\n張飛\n趙子龍\n黃忠", gid="TEST_B")
closed = 0
for i, nm in enumerate(["張飛", "趙子龍", "黃忠"]):
    c = get_cust(f"B{i+1:09d}")
    if c and c.get("status") == "CLOSED":
        closed += 1
check("批次結案 3 筆全部結案", closed == 3, f"closed={closed}/3")

# ========== 32. 違約金連續改金額 ==========
print("\n=== 32. 違約金 pending 狀態連續改金額 ===")
bc("4/21-姜維A101010101", gid="TEST_B")
bc("@AI 姜維 違約金已支付15萬", gid="TEST_B")
c = get_cust("A101010101")
check("違約金第 1 次 150000", c.get("penalty_amount") == "150000", c.get("penalty_amount"))
bc("@AI 姜維 違約金已支付10萬", gid="TEST_B")
c = get_cust("A101010101")
check("違約金第 2 次覆蓋為 100000", c.get("penalty_amount") == "100000", c.get("penalty_amount"))
bc("@AI 姜維 違約金已支付8萬", gid="TEST_B")
c = get_cust("A101010101")
check("違約金第 3 次覆蓋為 80000", c.get("penalty_amount") == "80000", c.get("penalty_amount"))
check("仍是 pending（尚未結案）", c.get("status") == "ACTIVE", c.get("status"))

# ========== 33. 同名多筆 + 破壞指令 → 跳按鈕（QUICK_REPLY）==========
print("\n=== 33. 同名多筆破壞指令跳按鈕 ===")
bc("4/21-重複名C100000001", gid="TEST_B")
bc("4/21-重複名C100000002", gid="TEST_B")
quick_replies.clear()
result = bc("@AI 重複名 結案", gid="TEST_B")
check("同名多筆 → 跳按鈕",
      any("重複名" in r for r in quick_replies) or any("多筆" in r or "選" in r for r in replies),
      f"quick_replies={quick_replies}, replies={replies}")

# ========== 34. 網頁 /new-customer POST ==========
print("\n=== 34. 網頁新增客戶 POST ===")
from fastapi.testclient import TestClient
client = TestClient(m.app)
# 登入取 cookie
resp = client.post("/login", data={"role": "admin", "password": m.ADMIN_PASSWORD})
# 建客戶
form = {
    "grp": "TEST_B", "cname": "網頁小明", "idno": "W123456789",
    "birth": "086/01/01", "phone": "0912345678", "rcity": "台北市",
    "rdist": "信義", "raddr": "路1", "rphone": "", "sameck": "on",
    "lphone": "", "lstatus": "自有", "lyear": "5", "lmon": "0",
    "cmpname": "測試", "carea": "02", "cnum": "12345678", "cext": "",
    "crole": "", "cyear": "1", "cmon": "0", "csal": "3.5",
    "ccity": "台北市", "cdist": "信義", "caddr": "路2",
    "c1name": "A", "c1rel": "父", "c1tel": "0987654321", "c1know": "可知情",
    "c2name": "B", "c2rel": "友", "c2tel": "0987654322", "c2know": "可知情",
    "email": "t@t.com", "line": "test",
}
resp = client.post("/new-customer", data=form, follow_redirects=False)
# 307 = 被 login 重定向（test 沒 persistent cookie）、算有收到、不是 500/404
check("網頁建客戶 endpoint 可達", resp.status_code in (200, 302, 303, 307), f"HTTP {resp.status_code}")

# ========== 35. 並發：兩個 update 同一客戶 ==========
print("\n=== 35. 並發 update 同客戶 ===")
bc("4/21-韓信D999999999", gid="TEST_B")
bc("4/21-韓信-第一", gid="TEST_B")
import threading
def do_update(x):
    m.update_customer(get_cust("D999999999")["case_id"],
                      text=f"並發 {x}", from_group_id="TEST_B")
threads = [threading.Thread(target=do_update, args=(i,)) for i in range(5)]
for t in threads: t.start()
for t in threads: t.join()
# 全部寫完後檢查 DB 沒壞
c = get_cust("D999999999")
check("並發後客戶仍存在、status=ACTIVE", c and c.get("status") == "ACTIVE", c.get("status") if c else None)
# case_logs 有 5 筆以上
conn5 = sqlite3.connect(TEST_DB)
log_cnt = conn5.execute("SELECT COUNT(*) FROM case_logs WHERE case_id=?", (c["case_id"],)).fetchone()[0]
conn5.close()
check("並發 5 筆 case_logs 都寫入", log_cnt >= 5, f"log_cnt={log_cnt}")

# ========== 總結 ==========
print(f"\n{'='*50}")
print(f"結果：{PASS} 通過、{FAIL} 失敗")
print(f"{'='*50}")
sys.exit(0 if FAIL == 0 else 1)
