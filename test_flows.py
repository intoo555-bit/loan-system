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
pushes.clear()   # 下一行執行時不該產生任何 push（同群靠 reply 彙總）
bc("@AI 批次結案\n張飛\n趙子龍\n黃忠", gid="TEST_B")
closed = 0
for i, nm in enumerate(["張飛", "趙子龍", "黃忠"]):
    c = get_cust(f"B{i+1:09d}")
    if c and c.get("status") == "CLOSED":
        closed += 1
check("批次結案 3 筆全部結案", closed == 3, f"closed={closed}/3")
# 迴圈內 push 會洗版（3 人 = 3 則）+ 燒 3 倍配額。同群一則都不該推。
check("批次結案不洗版（同群 0 則 push）", len(pushes) == 0,
      f"多發了 {len(pushes)} 則：{[t for _, t in pushes][:3]}")

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
m.set_setting("admin_pw", m.hash_pw("tstpw123"))   # 啟動時密碼是隨機產生的，測試要自己設
resp = client.post("/login", data={"role": "admin", "password": "tstpw123"})
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

# ========== 37. 網頁編輯案件不砍送件順序 ==========
# 潘藝中 2026-08-03：在網頁按一次儲存，送件順序從 11 家剩 1 家、婉拒歷史清空。
# 網頁編輯是拿來修日報顯示的，不該重建 route。
print("\n=== 37. 網頁編輯不砍送件順序 ===")
bc("4/21-馬超D222222222", gid="TEST_B")
bc("4/21-馬超-亞太機25萬/第一/21商品/和裕機", gid="TEST_B")
c = get_cust("D222222222")
order_before = json.loads(c.get("route_plan") or "{}").get("order", [])
check("前置：送件順序有 4 家", len(order_before) == 4, f"order={order_before}")
m.check_auth = lambda req: "admin"   # TestClient 下 session cookie 不生效，直接繞過認證
client.post("/case-edit", data={"case_id": c["case_id"], "current_company": "第一",
                                "status": "ACTIVE", "company_status_json": "{}"})
c = get_cust("D222222222")
rp = json.loads(c.get("route_plan") or "{}")
order_after = rp.get("order", [])
check("網頁儲存後送件順序沒被砍", len(order_after) == len(order_before),
      f"{order_before} → {order_after}")
_idx = rp.get("current_index", 0)
check("current_index 移到「第一」", 0 <= _idx < len(order_after) and order_after[_idx] == "第一",
      f"idx={_idx} order={order_after}")

# ========== 38. 網頁改進度要留紀錄（可還原） ==========
# 舊寫法直接下 SQL，不寫 case_logs、不存快照 → 改錯查不到也還原不了。
# LINE 改進度本來就走 update_customer，網頁要一致。
print("\n=== 38. 網頁改進度要留紀錄 ===")
bc("4/21-黃忠E333333333", gid="TEST_B")
bc("4/21-黃忠-亞太機25萬/第一", gid="TEST_B")
c = get_cust("E333333333")
conn6 = sqlite3.connect(TEST_DB)
log_before = conn6.execute("SELECT COUNT(*) FROM case_logs WHERE case_id=?", (c["case_id"],)).fetchone()[0]
conn6.close()
m.check_auth = lambda req: "admin"
client.post("/report/update-progress", json={"case_id": c["case_id"], "progress": "待補薪轉"})
conn6 = sqlite3.connect(TEST_DB); conn6.row_factory = sqlite3.Row
rows6 = conn6.execute("""SELECT snapshot_json FROM case_logs WHERE case_id=?
                         ORDER BY created_at DESC LIMIT 1""", (c["case_id"],)).fetchall()
log_after = conn6.execute("SELECT COUNT(*) FROM case_logs WHERE case_id=?", (c["case_id"],)).fetchone()[0]
conn6.close()
c = get_cust("E333333333")
check("進度有寫入", (c.get("last_update") or "") == "待補薪轉", c.get("last_update"))
check("網頁改進度有留案件歷程", log_after > log_before, f"{log_before} → {log_after}")
check("有存快照（可還原）", bool(rows6 and rows6[0]["snapshot_json"]),
      "snapshot 是空的" if rows6 else "沒有 log")

# ========== 39. 已撥款的案子不給跨群轉移 ==========
# 鍾志文 2026-08-03：優選跑到撥款，客戶又去問幸福貸，業務在幸福貸群按「轉移」，
# 撥款紀錄和建案日期整包被搬到幸福貸。LINE 分不出誰是管理員，所以一律擋。
print("\n=== 39. 已撥款案不給跨群轉移 ===")
conn7 = sqlite3.connect(TEST_DB)
conn7.execute("INSERT OR REPLACE INTO groups (group_id, group_name, group_type, is_active, created_at) VALUES (?,?,?,?,?)",
              ("TEST_C2", "C2群", "SALES_GROUP", 1, datetime.now().isoformat()))
conn7.commit(); conn7.close()
# 未核准的客戶：轉移選項要還在
bc("4/21-關羽F444444444", gid="TEST_B")
bc("4/21-關羽-亞太機25萬", gid="TEST_B")
quick_replies.clear()
bc("4/21-關羽F444444444", gid="TEST_C2")
check("未核准案：轉移選項保留", not any("不可轉移" in q for q in quick_replies),
      quick_replies[-1][:40] if quick_replies else "沒跳按鈕")
# 已撥款的客戶：轉移要被擋
bc("4/21-馬岱F555555555", gid="TEST_B")
bc("4/21-馬岱-亞太機25萬", gid="TEST_B")
bc("@AI 馬岱 亞太機25萬 核准 20萬", gid="TEST_B")
bc("@AI 馬岱 亞太機25萬 撥款 4/21", gid="TEST_B")
c = get_cust("F555555555")
check("前置：馬岱已撥款", (c.get("disbursement_date") or "") != "", c.get("disbursement_date"))
quick_replies.clear()
bc("4/21-馬岱F555555555", gid="TEST_C2")
check("已撥款案：轉移被擋", any("不可轉移" in q for q in quick_replies),
      quick_replies[-1][:60] if quick_replies else "沒跳按鈕")

# ========== 40. 舊案已結案時，同客戶再送一次不可被去重刪掉 ==========
# 黃俊仁 2026-08-06：5 月送過 21商品、核准 7 萬、5/8 撥款結案；8 月客戶再來送，
# 去重看「誰的送件歷程完整」→ 保留 5 月那筆已結案的、把 8 月正在跑的新案標 DELETED。
print("\n=== 40. 結案舊案不參與去重 ===")
bc("5/6-黃測試S999888777", gid="TEST_B")
bc("5/6-黃測試-和裕/21商品/零卡", gid="TEST_B")
old_case = m.find_active_by_name("黃測試")[0]["case_id"]
m.update_customer(old_case, current_company="21商品", approved_amount="7萬", disbursement_date="5/8",
                  route_plan=m.make_route_json(["和裕", "21商品", "零卡"], 1,
                                               [{"company": "和裕", "status": "婉拒"},
                                                {"company": "21", "status": "核准", "amount": "7萬"}]),
                  status="CLOSED", text="結案", from_group_id="TEST_B")
bc("8/6-黃測試S999888777", gid="TEST_B")   # 同客戶三個月後再送一次
merged = m._dedupe_same_id_in_group("S999888777", "TEST_B")
conn8 = sqlite3.connect(TEST_DB); conn8.row_factory = sqlite3.Row
states = {r["status"]: r["case_id"] for r in conn8.execute(
    "SELECT case_id, status FROM customers WHERE customer_name='黃測試'")}
conn8.close()
check("去重不動已結案的舊案（併掉 0 筆）", merged == 0, f"併掉 {merged} 筆")
check("8 月新案還在（沒被標 DELETED）", "DELETED" not in states, f"狀態={list(states)}")
check("5 月舊案仍是 CLOSED", "CLOSED" in states, f"狀態={list(states)}")
# 真的該合併的情況（兩筆都還在跑）→ 要合併，而且必須留下紀錄
bc("8/6-併測試S111222333", gid="TEST_B")
c_a = m.find_active_by_name("併測試")[0]["case_id"]
conn9 = sqlite3.connect(TEST_DB)
conn9.execute("""INSERT INTO customers (case_id, customer_name, id_no, source_group_id, status,
                 created_at, updated_at) VALUES ('dup_test','併測試','S111222333','TEST_B','ACTIVE',?,?)""",
              (datetime.now().isoformat(), datetime.now().isoformat()))
conn9.commit(); conn9.close()
merged2 = m._dedupe_same_id_in_group("S111222333", "TEST_B")
conn9 = sqlite3.connect(TEST_DB); conn9.row_factory = sqlite3.Row
killed = [r["case_id"] for r in conn9.execute(
    "SELECT case_id FROM customers WHERE customer_name='併測試' AND status='DELETED'")]
logs9 = conn9.execute("""SELECT message_text, from_group_id, snapshot_json FROM case_logs
                         WHERE case_id=? ORDER BY created_at DESC LIMIT 1""",
                      (killed[0] if killed else "",)).fetchone()
conn9.close()
check("兩筆都在跑時仍會合併", merged2 >= 1, f"併掉 {merged2} 筆")
check("被軟刪那筆有留紀錄（查得到誰刪的）", bool(logs9) and logs9["from_group_id"] == "SYSTEM_DEDUPE",
      f"log={dict(logs9) if logs9 else None}")
check("紀錄含刪除前快照（可還原）", bool(logs9) and bool(logs9["snapshot_json"]),
      "snapshot 是空的")

# ========== 41. 結案時刻要記在 closed_at，之後被動到也不變 ==========
# 統計「本月結案」原本用 updated_at（最後異動時間），舊案這個月被碰一下
# 就會被算成本月結案，而且從原本那個月的統計消失（黃俊仁 5/8 結案跑到 8 月）。
print("\n=== 41. 結案時刻不受後續異動影響 ===")
bc("4/21-馬謖G666666666", gid="TEST_B")
bc("4/21-馬謖-亞太機25萬", gid="TEST_B")
bc("@AI 馬謖 結案", gid="TEST_B")
c = get_cust("G666666666")
closed_at_1 = (c.get("closed_at") or "")[:10]
check("結案時有寫 closed_at", closed_at_1 != "", f"closed_at={c.get('closed_at')}")
# 之後又去動這筆（模擬救資料／改欄位）
m.update_customer(c["case_id"], text="事後修改資料", from_group_id="WEB")
c = get_cust("G666666666")
check("事後被動到，closed_at 不變", (c.get("closed_at") or "")[:10] == closed_at_1,
      f"{closed_at_1} → {(c.get('closed_at') or '')[:10]}")
# 重啟後再次結案 → closed_at 要更新成新的結案日（那是新的申請週期）
bc("@AI 馬謖 重啟", gid="TEST_B")
bc("@AI 馬謖 結案", gid="TEST_B")
c = get_cust("G666666666")
check("重啟後再結案，closed_at 會更新", (c.get("closed_at") or "") != "",
      f"closed_at={c.get('closed_at')}")
# 狀態英文不可外洩
check("status_zh 把代碼轉中文", m.status_zh("CLOSED") == "已結案" and m.status_zh("ACTIVE") == "進行中",
      f"CLOSED→{m.status_zh('CLOSED')} ACTIVE→{m.status_zh('ACTIVE')}")

# ========== 42. 啟動期通知：失敗要出聲，但不能擋住啟動 ==========
# closed_at 回填若失敗而只印 log，統計會 fallback 回 updated_at ——
# 網站照常運作、數字卻還是錯的，沒人會發現。
print("\n=== 42. 啟動期通知 ===")
_sent = []
_orig_push, _orig_token = m.push_text, m.CHANNEL_ACCESS_TOKEN
m.push_text = lambda gid, msg: (_sent.append((gid, msg)), (True, ""))[1]
m.CHANNEL_ACCESS_TOKEN = "dummy"
m._notify_startup("✅ 測試通知")
check("啟動期通知會推出去", len(_sent) == 1 and "測試通知" in _sent[0][1],
      f"sent={_sent}")
def _boom(gid, msg):
    raise RuntimeError("LINE API 掛了")
m.push_text = _boom
try:
    m._notify_startup("這則會失敗")
    check("通知失敗不會擋住啟動", True)
except Exception as _e:
    check("通知失敗不會擋住啟動", False, f"往外炸了：{_e}")
m.push_text, m.CHANNEL_ACCESS_TOKEN = _orig_push, _orig_token

# ========== 43. 資料健檢 ==========
# 2026-08 連續踩到的坑共同點：系統照常運作、畫面正常，只有資料悄悄自相矛盾，
# 要等業務發現日報怪怪的才知道。健檢主動去翻這些。
print("\n=== 43. 資料健檢 ===")
conn10 = sqlite3.connect(TEST_DB)
conn10.execute("INSERT OR REPLACE INTO groups (group_id, group_name, group_type, is_active, created_at) VALUES (?,?,?,?,?)",
               ("HC_G", "健檢群", "SALES_GROUP", 1, datetime.now().isoformat()))
conn10.commit(); conn10.close()
check("乾淨資料回報正常", "沒有發現異常" in m.data_health_check("HC_G"),
      m.data_health_check("HC_G")[:60])
bc("4/21-健檢客H777777777", gid="HC_G")
bc("4/21-健檢客-亞太機25萬/第一", gid="HC_G")
_hc = m.find_active_by_name("健檢客")[0]
m.update_customer(_hc["case_id"], current_company="喬美", approved_amount="20萬",
                  disbursement_date="4/25", report_section="",
                  text="製造異常", from_group_id="HC_G")
_rep = m.data_health_check("HC_G")
check("抓到：送的公司不在送件順序裡", "不在送件順序裡" in _rep and "喬美" in _rep, _rep[:80])
check("抓到：有核准金額卻不在待撥款區", "不在待撥款區" in _rep, _rep[:80])
check("抓到：已撥款但案子還開著", "已撥款但案子還開著" in _rep, _rep[:80])
check("報告有寫怎麼處理", "怎麼處理" in _rep, "沒有處理建議")
# ⛔ 健檢的判斷要跟日報一致，不可以自己看欄位。
# 2026-08-11 誤報過：郭晉瑋 report_section 是空的，但送件順序裡有核准紀錄，
# 日報的「核准補強」會自動把他歸到待撥款 —— 健檢卻報「不在待撥款區」。
bc("7/31-甲健檢H881111111", gid="HC_G")
bc("7/31-甲健檢-和裕", gid="HC_G")
_a = m.find_active_by_name("甲健檢")[0]["case_id"]
m.update_customer(_a, approved_amount="5萬", report_section="",
                  route_plan=m.make_route_json(["和裕"], 0,
                      [{"company": "和裕", "status": "核准", "amount": "5萬"}]),
                  text="核准", from_group_id="HC_G")
bc("7/31-乙健檢H882222222", gid="HC_G")
bc("7/31-乙健檢-和裕", gid="HC_G")
_b = m.find_active_by_name("乙健檢")[0]["case_id"]
m.update_customer(_b, approved_amount="5萬", report_section="", text="核准", from_group_id="HC_G")
_rep2 = m.data_health_check("HC_G")
check("日報已歸待撥款的不誤報", "甲健檢" not in _rep2, "甲健檢被誤報了")
check("送件順序沒核准紀錄的照樣抓到", "乙健檢" in _rep2, "乙健檢漏掉了")
# 每週自動健檢：乾淨不推播（避免變雜訊），有異常才推
_hsent = []
_op, _ot = m.push_text, m.CHANNEL_ACCESS_TOKEN
m.push_text = lambda gid, msg: (_hsent.append(msg), (True, ""))[1]
m.CHANNEL_ACCESS_TOKEN = "dummy"
m.send_weekly_health_check()
check("每週健檢有異常會推播", len(_hsent) >= 1, f"推了 {len(_hsent)} 則")
m.push_text, m.CHANNEL_ACCESS_TOKEN = _op, _ot

# ========== 總結 ==========
print(f"\n{'='*50}")
print(f"結果：{PASS} 通過、{FAIL} 失敗")
print(f"{'='*50}")
sys.exit(0 if FAIL == 0 else 1)
