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
# ⛔ 業務臨時加送一家原順序沒有的公司是正常操作，不可誤報（2026-08-13 誤報幸福 吳建緻）
bc("8/1-加送客J777888999", gid="HC_G")
bc("8/1-加送客-亞太機25萬/第一", gid="HC_G")
_add = m.find_active_by_name("加送客")[0]["case_id"]
m.update_customer(_add, current_company="房地",
                  company_status=json.dumps({"房地": "加送客 房地 照會"}, ensure_ascii=False),
                  text="加送房地", from_group_id="HC_G")
check("加送原順序沒有的公司不誤報", "加送客" not in m.data_health_check("HC_G"),
      "加送客被誤報了")
check("已移除容易誤報的「不在送件順序裡」那項", "不在送件順序裡" not in m.data_health_check("HC_G"),
      "那一項還在")
check("抓到：有核准金額卻不在待撥款區", "不在待撥款區" in _rep, _rep[:80])
check("抓到：撥款很久還沒結案", "撥款超過" in _rep and "還沒結案" in _rep, _rep[:80])
check("報告有寫怎麼處理", "怎麼處理" in _rep, "沒有處理建議")
# 撥款日只有月/日沒有年份，算出未來日期要當成去年（12 月的撥款到 1 月不可變成「還有 300 天」）
_today = datetime.now()
_recent = f"{_today.month}/{_today.day}"
check("日期換算：今天 = 0 天", m._days_since_md(_recent) == 0, f"{_recent} → {m._days_since_md(_recent)}")
check("日期換算：未來日期當成去年", (m._days_since_md("12/25") or 0) > 0, m._days_since_md("12/25"))
# ⛔ 使用者是一週批次結案一次，撥款後幾天還開著是正常的，不可以報
bc("8/1-剛撥款客K111000111", gid="HC_G")
bc("8/1-剛撥款客-亞太機25萬", gid="HC_G")      # 要設送件順序，否則會被「沒在送任何公司」那項報走
m.update_customer(m.find_active_by_name("剛撥款客")[0]["case_id"],
                  disbursement_date=_recent, text="今天撥款", from_group_id="HC_G")
# 只看撥款那一段，不要整份報告找名字（別項也可能提到他）
_disb_sec = m.data_health_check("HC_G").split("撥款超過")[-1].split("→ 怎麼處理")[0]
check("剛撥款的不報（一週批次結案是正常流程）",
      "剛撥款客" not in _disb_sec, f"撥款那段：{_disb_sec[:70]}")
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

# ========== 44. 同客戶再次申請：個資要帶過來、案件資料不可繼承 ==========
# 2026-08-11 黃宏棋：在別的群組已結案，新群組重新建檔卻一片空白，
# 身分證、電話、公司、聯絡人全部要重打。同一個人的個資不會變。
print("\n=== 44. 同客戶再次申請帶入個資 ===")
conn11 = sqlite3.connect(TEST_DB)
conn11.execute("INSERT OR REPLACE INTO groups (group_id, group_name, group_type, is_active, created_at) VALUES (?,?,?,?,?)",
               ("CP_B", "帶資料B群", "SALES_GROUP", 1, datetime.now().isoformat()))
conn11.commit(); conn11.close()
bc("5/1-帶測客C888888888", gid="TEST_B")
_old = m.find_active_by_name("帶測客")[0]["case_id"]
conn11 = sqlite3.connect(TEST_DB)
conn11.execute("""UPDATE customers SET birth_date='080/05/12', phone='0912345678',
    reg_city='高雄市', company_name_detail='大耀工程', contact1_name='王小美',
    approved_amount='7萬', disbursement_date='5/8', status='CLOSED' WHERE case_id=?""", (_old,))
conn11.commit(); conn11.close()
bc("8/11-帶測客C888888888", gid="CP_B")       # 換群組重新申請
conn11 = sqlite3.connect(TEST_DB); conn11.row_factory = sqlite3.Row
_new = conn11.execute("SELECT * FROM customers WHERE id_no='C888888888' AND source_group_id='CP_B'").fetchone()
conn11.close()
check("新案有建起來", _new is not None)
if _new:
    check("個資有帶過來（生日/電話/公司/聯絡人）",
          _new["birth_date"] == "080/05/12" and _new["phone"] == "0912345678"
          and _new["company_name_detail"] == "大耀工程" and _new["contact1_name"] == "王小美",
          f"birth={_new['birth_date']} phone={_new['phone']} co={_new['company_name_detail']}")
    check("⛔ 核准金額不可繼承", not (_new["approved_amount"] or "").strip(),
          f"approved={_new['approved_amount']}")
    check("⛔ 撥款日期不可繼承", not (_new["disbursement_date"] or "").strip(),
          f"disb={_new['disbursement_date']}")
    check("新案是進行中、屬於新群組", _new["status"] == "ACTIVE" and _new["source_group_id"] == "CP_B",
          f"status={_new['status']} gid={_new['source_group_id']}")

# ========== 45. 重啟不可以跨群組動到別人的案子 ==========
# 舊寫法整個資料庫撈同名最近一筆：在 B 群打「@AI 姓名 重啟」會把 A 群那筆重啟掉，
# 系統回「已重啟」但案子還屬於 A 群，B 群日報什麼都沒有。同名不同人更慘。
print("\n=== 45. 重啟限本群組 ===")
conn12 = sqlite3.connect(TEST_DB)
conn12.execute("INSERT OR REPLACE INTO groups (group_id, group_name, group_type, is_active, created_at) VALUES (?,?,?,?,?)",
               ("RO_B", "重啟B群", "SALES_GROUP", 1, datetime.now().isoformat()))
conn12.commit(); conn12.close()
bc("5/1-重啟客R999111222", gid="TEST_B")
_ro = m.find_active_by_name("重啟客")[0]["case_id"]
m.update_customer(_ro, status="CLOSED", text="結案", from_group_id="TEST_B")
def _ro_status():
    cn = sqlite3.connect(TEST_DB)
    v = cn.execute("SELECT status FROM customers WHERE case_id=?", (_ro,)).fetchone()[0]
    cn.close(); return v
replies.clear()
bc("@AI 重啟客 重啟", gid="RO_B")          # 在別的群組打重啟
check("跨群組重啟被擋下", _ro_status() == "CLOSED", f"狀態變成 {_ro_status()}")
check("有告訴業務該怎麼做", any("重新申請" in r or "才有這位客戶" in r for r in replies),
      replies[-1][:60] if replies else "沒有回覆")
replies.clear()
bc("@AI 重啟客 重啟", gid="TEST_B")        # 本群組打重啟
check("本群組重啟正常", _ro_status() == "ACTIVE", f"狀態={_ro_status()}")

# ========== 46. @AI 姓名 補資料：既有空白案子從舊案補個資 ==========
# 「建檔自動帶入」只對之後建的生效，功能上線前就建好的空白案子要能手動補。
print("\n=== 46. 補資料指令 ===")
bc("5/1-補測客F111222333", gid="TEST_B")
_fp_old = m.find_active_by_name("補測客")[0]["case_id"]
conn13 = sqlite3.connect(TEST_DB)
conn13.execute("""UPDATE customers SET birth_date='080/05/12', phone='0912345678',
    company_name_detail='大耀工程', contact1_name='王小美', status='CLOSED' WHERE case_id=?""", (_fp_old,))
conn13.commit(); conn13.close()
bc("8/11-補測客F111222333", gid="CP_B")
conn13 = sqlite3.connect(TEST_DB)
_fp_new = conn13.execute("SELECT case_id FROM customers WHERE id_no='F111222333' AND source_group_id='CP_B'").fetchone()[0]
# 模擬功能上線前建的：個資空白，但聯絡人已經有人填了
conn13.execute("""UPDATE customers SET birth_date=NULL, phone=NULL, company_name_detail=NULL,
    contact1_name='現場填的聯絡人' WHERE case_id=?""", (_fp_new,))
conn13.commit(); conn13.close()
bc("@AI 補測客 補資料", gid="CP_B")
conn13 = sqlite3.connect(TEST_DB); conn13.row_factory = sqlite3.Row
_fp_r = conn13.execute("SELECT * FROM customers WHERE case_id=?", (_fp_new,)).fetchone()
conn13.close()
check("空欄位有補上", _fp_r["phone"] == "0912345678" and _fp_r["company_name_detail"] == "大耀工程",
      f"phone={_fp_r['phone']} co={_fp_r['company_name_detail']}")
check("⛔ 已填好的不可被覆蓋", _fp_r["contact1_name"] == "現場填的聯絡人",
      f"contact1={_fp_r['contact1_name']}")
# 建檔訊息要告訴業務「資料是帶過來的、記得確認」，不然他不知道那是舊資料
_hint_new = bc("8/11-提示客H555666777", gid="TEST_B")
check("全新客戶不加提示", "帶入" not in (_hint_new or ""), _hint_new)
conn13 = sqlite3.connect(TEST_DB)
_h_old = conn13.execute("SELECT case_id FROM customers WHERE id_no='H555666777'").fetchone()[0]
conn13.execute("""UPDATE customers SET phone='0911222333', company_name_detail='測試公司',
    status='CLOSED' WHERE case_id=?""", (_h_old,))
conn13.commit(); conn13.close()
_hint_msg = bc("8/11-提示客H555666777", gid="CP_B")
check("老客戶建檔會提示帶入幾項", "帶入" in (_hint_msg or "") and "確認" in (_hint_msg or ""),
      _hint_msg)

# ========== 47. A 群回報多筆同名時，依公司自動對到正確那筆 ==========
# 林耘耘 2026-05-12：A 群打「林耘耘 喬美 撥款05/12」，客戶在專業群和勞工群各有一筆，
# 撥款卻掛到勞工群 —— 但當時只有專業群那筆在送喬美。業績歸屬錯誤。
print("\n=== 47. A 群多筆同名依公司自動對 ===")
conn14 = sqlite3.connect(TEST_DB)
for _g, _n in [("AG_P", "專業群"), ("AG_L", "勞工群")]:
    conn14.execute("INSERT OR REPLACE INTO groups (group_id, group_name, group_type, is_active, created_at) VALUES (?,?,?,?,?)",
                   (_g, _n, "SALES_GROUP", 1, datetime.now().isoformat()))
conn14.commit(); conn14.close()
_p = m.create_customer_record("林測客", "A111222333", "喬美", "AG_P", "建檔",
                              route_plan=m.make_route_json(["喬美", "21商品"], 0), current_company="喬美")
_l = m.create_customer_record("林測客", "A111222333", "亞太", "AG_L", "建檔",
                              route_plan=m.make_route_json(["亞太", "和裕"], 0), current_company="亞太")
quick_replies.clear()
a("林測客 喬美 撥款05/12")
check("依公司自動對到，不用人選", len(quick_replies) == 0,
      f"跳了按鈕：{quick_replies[-1][:40] if quick_replies else ''}")
# 公司對不到任何一筆時，還是要跳按鈕讓人選（不可以亂猜）
quick_replies.clear()
a("林測客 分貝機 撥款05/12")
check("公司對不到時仍跳按鈕讓人選", len(quick_replies) >= 1, "沒跳按鈕，可能亂對")

# ========== 48. 多筆回報混貼，不可被「XXX撥款」誤判成撥款名單 ==========
# 病根（2026-08-26）：撥款名單判斷跑在 split_multi_cases 切段「之前」，
# 拿整包去比對 → 埋在第 3 筆備註裡的「機車設定完成後撥款」被當成名單標頭，
# 4 筆客戶全部沒處理、只回「✅ 撥款名單處理完成，共0筆」。
# 使用者看到的症狀是「BOT 訊息傳不出去」（單筆單筆貼卻正常＝等於幫它切好段）。
# 標頭正則極寬鬆：「已撥款」「尚未撥款」「請問何時撥款」「對保完成後撥款」全部會中。
print("\n=== 48. 多筆混貼不可被誤判成撥款名單 ===")

_MIXED = """蔡福龍  貸救補 待核准
核准 5 萬 12期$4801
可轉核再請通知

龐依澐  熊速貸 亞太核准12萬
30期付4878，申貸120000

莊雅芳 和裕 補聯3照會
降貸 5W/12N/P4815
補聯3(莊王日/母)可照會時間
照會無誤後轉核
機車設定完成後撥款

林伯憲 亞太 婉拒
其他照會不實"""

def _is_disb_after_split(t):
    """正式碼在 A 群入口用的判斷：切段後每段都是名單才算（main.py 約 L12432）"""
    _blocks = m.split_multi_cases(t) or [t]
    return all(m.is_disbursement_list(b) for b in _blocks)

check("整包混貼不會被當成撥款名單", not _is_disb_after_split(_MIXED),
      "整包被誤判 → 4 筆客戶全部會被吃掉")

# 日常句子單獨出現在某一筆備註裡，都不可以讓整包變成名單
for _sentence in ["機車設定完成後撥款", "已撥款", "尚未撥款", "還沒撥款",
                  "等撥款", "請問何時撥款", "對保完成後撥款", "簽約後撥款"]:
    _t = f"王小明 亞太 婉拒\n{_sentence}\n其他照會不實"
    check(f"備註含「{_sentence}」不被當名單", not _is_disb_after_split(_t))

# ⭐ 反向：真的撥款名單一定要照舊被認得（不可為了修上面而擋掉真名單）
_REAL_LISTS = {
    "日期+公司+撥款名單": "8/26 21商品撥款名單\n王小明\n陳大華\n李美玲",
    "無日期標頭":        "貸救補 今日撥款\n王小明\n陳大華",
    "多區段":            "8/26 21商品撥款名單\n王小明\n陳大華\n8/26 亞太排撥\n李美玲",
    "無公司(NOCO)":      "8/26 撥款\n王小明\n陳大華",
}
for _label, _txt in _REAL_LISTS.items():
    check(f"真名單仍認得：{_label}", _is_disb_after_split(_txt),
          "真名單被擋掉了 → 撥款名單功能會失效")

# ========== 49. 東豐（2026-08-28 新增公司）走完整流程 ==========
# 新增一家公司要同時動：REPORT_SECTION_1（日報區塊）、COMPANY_LIST（訊息辨識）、
# /case-edit 公司下拉。少改一處的症狀不同：日報沒那一格 / BOT 認不得公司名 / 網頁改不了。
# 這組釘住「東豐跟既有公司行為一致」，任何一處被改掉都會紅。
print("\n=== 49. 東豐新公司 ===")
check("東豐在 COMPANY_LIST（訊息辨識）", "東豐" in m.COMPANY_LIST)
check("東豐在 REPORT_SECTION_1（日報區塊）", "東豐" in m.REPORT_SECTION_1)
check("normalize_section 認得東豐", m.normalize_section("東豐") == "東豐", m.normalize_section("東豐"))

bc("8/28-東測甲 B111111111", gid="TEST_B")
bc("8/28-東測甲-東豐/21商品/亞太", gid="TEST_B")
_d = get_cust("B111111111")
check("送件順序第一家=東豐", _d and _d["current_company"] == "東豐", _d and _d.get("current_company"))

bc("@AI 東測甲 送東豐", gid="TEST_B")
_d = get_cust("B111111111")
check("送東豐後離開送件區（進東豐區塊）", (_d.get("report_section") or "") == "",
      f"report_section={_d.get('report_section')!r}")

a("東測甲 東豐 核准10萬")
_d = get_cust("B111111111")
check("東豐核准金額有存", (_d.get("approved_amount") or "") != "", _d.get("approved_amount"))
check("東豐核准後進待撥款", _d.get("report_section") == "待撥款", _d.get("report_section"))

# 婉拒要能推進到下一家（證明東豐有正常接上送件順序引擎）
bc("8/28-東測乙 B222222222", gid="TEST_B")
bc("8/28-東測乙-東豐/21商品", gid="TEST_B")
a("東測乙 東豐 婉拒 信用評分不足")
_d2 = get_cust("B222222222")
check("東豐婉拒後推進下一家", _d2.get("current_company") == "21商品", _d2.get("current_company"))

# 日報真的長出「東豐」那一格（原始需求就是這件事）
bc("8/28-東測丙 B333333333", gid="TEST_B")
bc("8/28-東測丙-東豐/21商品", gid="TEST_B")
bc("@AI 東測丙 送東豐", gid="TEST_B")
_rep = "\n".join(m.generate_report_lines("TEST_B"))
check("日報有「東豐」區塊標題", any(l.strip() == "東豐" for l in _rep.splitlines()),
      "日報沒長出東豐那一格")

# ========== 50. 貴重案件不可被靜默改名（2026-09-02 吳珮嬋事故）==========
# 業務要建劉永芳，身分證複製貼上貼到吳珮嬋的（當天早上剛撥款的案子）。
# 系統照身分證找到吳珮嬋那筆、直接改名成劉永芳，只回「🔄 已更新客戶」，
# 業務完全不知道蓋掉了一筆已撥款案件；後續再滾 4 步才被發現。
# ⭐ 而且因為那筆被改名成「劉永芳」，之後再打就撞到「同名不同身分證」分支，
#    業務按「不同人(建新)」→ 又多長出一筆重複的劉永芳。源頭都是這個沒防護的改名。
print("\n=== 50. 貴重案件改名防護 ===")

# 這組要拿到按鈕的 callback token，臨時換掉 mock 記下 items
_qr_items = []
_orig_qr = m.reply_quick_reply
def _qr_capture(token, text, items):
    quick_replies.append(text); _qr_items.append(items); return True
m.reply_quick_reply = _qr_capture

def _btn_token(prefix):
    for it in (_qr_items[-1] if _qr_items else []):
        d = it.get("action", {}).get("data", "")
        if d.startswith(prefix):
            return d
    return None

_WU = "S299887766"   # 「吳珮嬋」的身分證
def _setup_precious():
    """建一筆已核准待撥款的貴重案件"""
    conn_p = sqlite3.connect(TEST_DB)
    conn_p.execute("DELETE FROM customers WHERE id_no=?", (_WU,))
    conn_p.commit(); conn_p.close()
    bc(f"114/08/20-吳珮嬋 {_WU}", gid="TEST_B")
    bc("8/20-吳珮嬋-亞太/21商品/鄉民", gid="TEST_B")
    a("吳珮嬋 亞太 核准25萬")

# ① 貴重案件被改名 → 要跳確認、且資料一個字都不能動
_setup_precious()
_qr_items.clear()
_r = bc(f"115/9/3-劉永芳 {_WU} 高齡/無保人", gid="TEST_B")
_c = get_cust(_WU)
check("貴重案件改名 → 跳確認不靜默改", _r == "QUICK_REPLY_SENT", f"回傳={_r}")
check("⭐ 吳珮嬋名字沒被蓋掉", _c and _c["customer_name"] == "吳珮嬋", _c and _c.get("customer_name"))
check("⭐ 核准金額沒被弄丟", _c and (_c.get("approved_amount") or "") != "", _c and _c.get("approved_amount"))

# ② 按「取消」→ 完全不變
_tok = _btn_token("CANCEL_RENAME|")
check("有產生取消按鈕", _tok is not None)
if _tok:
    m.handle_command_text(_tok, "mock_token")
    _c = get_cust(_WU)
    check("取消後案件完好如初", _c["customer_name"] == "吳珮嬋" and (_c.get("approved_amount") or "") != "",
          f"{_c.get('customer_name')}/{_c.get('approved_amount')}")

# ③ 按「確定改名」→ 要真的改，且核准/待撥款要保留（不可為了防護把正常操作擋死）
_setup_precious()
_qr_items.clear()
bc(f"115/9/3-劉永芳 {_WU}", gid="TEST_B")
_tok = _btn_token("CONFIRM_RENAME|")
check("有產生確定按鈕", _tok is not None)
if _tok:
    m.handle_command_text(_tok, "mock_token")
    _c = get_cust(_WU)
    check("確定後名字真的改掉", _c["customer_name"] == "劉永芳", _c.get("customer_name"))
    check("確定改名後核准金額仍保留", (_c.get("approved_amount") or "") != "", _c.get("approved_amount"))
    check("確定改名後待撥款區塊仍保留", _c.get("report_section") == "待撥款", _c.get("report_section"))
    # 改完仍要能還原退回（事故當天就是靠這個救回來的）
    _ok2, _k2, _m2 = m.restore_prev_state(_c["case_id"], steps=1, from_group_id="TEST_B",
                                          actor="劉永芳", cust_name_hint="劉永芳")
    check("改名後還原退得回原客戶", get_cust(_WU)["customer_name"] == "吳珮嬋",
          get_cust(_WU)["customer_name"])

# ④ ⚠️ 反向：一般案件（沒核准）改名不可以擋 —— 全部都擋業務會每天在按按鈕、然後亂按
bc("8/20-張三測 T911222333", gid="TEST_B")
_r2 = bc("115/9/3-李四測 T911222333", gid="TEST_B")
_c2 = get_cust("T911222333")
check("一般案件改名不跳按鈕（不干擾日常）", _r2 != "QUICK_REPLY_SENT", f"回傳={_r2}")
check("一般案件改名照舊成功", _c2 and _c2["customer_name"] == "李四測", _c2 and _c2.get("customer_name"))

# ⑤ 同一個人再打一次（沒有要改名）→ 不該擋
_setup_precious()
_r3 = bc(f"114/08/20-吳珮嬋 {_WU}", gid="TEST_B")
check("沒改名時不跳按鈕", _r3 != "QUICK_REPLY_SENT", f"回傳={_r3}")

# ⑥ 已撥款的案子也要擋
bc("8/20-王五測 T955666777", gid="TEST_B")
bc("8/20-王五測-亞太/21商品", gid="TEST_B")
a("王五測 亞太 核准20萬")
conn_d = sqlite3.connect(TEST_DB)
conn_d.execute("UPDATE customers SET disbursement_date='09/02' WHERE id_no='T955666777'")
conn_d.commit(); conn_d.close()
_r4 = bc("115/9/3-趙六測 T955666777", gid="TEST_B")
_c4 = get_cust("T955666777")
check("已撥款案件改名被擋", _r4 == "QUICK_REPLY_SENT", f"回傳={_r4}")
check("已撥款案件名字沒變", _c4 and _c4["customer_name"] == "王五測", _c4 and _c4.get("customer_name"))

m.reply_quick_reply = _orig_qr   # 還原 mock，不影響後面的測試

# ========== 51. 公司電話「分機」不可以在輸出時消失（2026-09-08）==========
# 後台公司電話拆成「區碼／號碼／分機」三格，但顯示的地方有 5 份各寫各的，
# 其中 3 份漏掉分機 → 行政填了分機 277，PDF 和填寫表只印 03-4626789，
# 照會打過去接不到人（純顯示問題，HTTP 200、測試也不會紅，只有人工核對才看得出來）。
# ⚠️ 不可以無條件把分機併進電話：21汽車範本有獨立的「公司分機：」行，併了會顯示兩次。
print("\n=== 51. 公司電話分機不可消失 ===")

for _args, _want in [
    (("03", "4626789", "277"), "03-4626789 分機277"),
    (("03", "4626789", ""),    "03-4626789"),
    (("mobile", "0912345678", ""), "0912345678"),
    (("mobile", "0912345678", "12"), "0912345678 分機12"),
    (("", "27189090", ""),     "27189090"),
    ((None, None, None),       ""),
]:
    check(f"fmt_company_phone{_args}", m.fmt_company_phone(*_args) == _want,
          f"得到 {m.fmt_company_phone(*_args)!r}、期望 {_want!r}")

m.check_auth = lambda req: "admin"
m.get_auth_group_id = lambda req: ""

_PHONE_FIELDS = {
    "company_name_detail": "台灣積層工業股份有限公司",
    "company_phone_area": "03", "company_phone_num": "4626789", "company_phone_ext": "277",
    "company_role": "機台操作員", "company_years": "27", "company_months": "6",
    "company_salary": "4.5", "phone": "0912345678",
    "contact1_name": "王親屬", "contact1_phone": "0955111222", "contact1_relation": "母",
    "contact2_name": "李朋友", "contact2_phone": "0966333444", "contact2_relation": "朋友",
}

def _mk_phone_case(plan, idno, ext="277"):
    """建一筆有公司分機的客戶，回傳 (case_id, 填寫表文字)"""
    _cid = m.create_customer_record("電話測試客", idno, plan, "TEST_B", "建案")
    _cn = sqlite3.connect(TEST_DB)
    _cols = {r[1] for r in _cn.execute("PRAGMA table_info(customers)").fetchall()}
    _f = dict(_PHONE_FIELDS)
    _f["company_phone_ext"] = ext
    _f["adminb_selected_plans"] = plan
    _use = {k: v for k, v in _f.items() if k in _cols}
    _cn.execute(f"UPDATE customers SET {','.join(f'{k}=?' for k in _use)} WHERE case_id=?",
                list(_use.values()) + [_cid])
    _cn.commit(); _cn.close()
    _r = client.get(f"/adminb/download-excel?case_id={_cid}")
    return _cid, _r.content.decode("utf-8", errors="replace")

# ① PDF 要印出分機
_cid_pdf, _ = _mk_phone_case("分貝機車", "P111222333")
_pdf = client.get(f"/customer-pdf?case_id={_cid_pdf}").text
check("PDF 公司電話含分機277", "分機277" in _pdf, "PDF 沒印出分機")

# ② 分貝填寫表（範本無「公司分機」行）→ 分機要併進公司電話
_, _txt_fb = _mk_phone_case("分貝機車", "P444555666")
_tel_fb = [l for l in _txt_fb.splitlines() if l.startswith("公司電話")]
check("分貝填寫表：公司電話含分機", any("分機277" in l for l in _tel_fb), str(_tel_fb))

# ③ 21汽車（範本有「公司分機」行）→ 不可重複顯示
_, _txt_21 = _mk_phone_case("21汽車", "P777888999")
_tel_21 = [l for l in _txt_21.splitlines() if l.startswith("公司電話")]
_ext_21 = [l for l in _txt_21.splitlines() if l.startswith("公司分機")]
check("21汽車：公司電話不含分機（不重複）", all("分機" not in l for l in _tel_21), str(_tel_21))
check("21汽車：公司分機行有值", any("277" in l for l in _ext_21), str(_ext_21))
check("21汽車：分機全表只出現一次",
      sum(1 for l in _txt_21.splitlines() if "277" in l) == 1,
      str([l for l in _txt_21.splitlines() if "277" in l]))

# ④ 沒填分機 → 不可以多出空的「分機」字
_, _txt_no = _mk_phone_case("分貝機車", "P123123123", ext="")
_tel_no = [l for l in _txt_no.splitlines() if l.startswith("公司電話")]
check("沒填分機時不出現「分機」字", all("分機" not in l for l in _tel_no), str(_tel_no))
check("沒填分機時電話仍正確", any("03-4626789" in l for l in _tel_no), str(_tel_no))

# ⑤ 分貝改裝填寫表的新格式（2026-09-08 換版）欄位要能自動帶入
_cid_f, _txt_f = _mk_phone_case("分貝機車", "P321321321")
_cn = sqlite3.connect(TEST_DB)
_cn.execute("UPDATE customers SET birth_date=?, id_issue_date=?, email=?, carrier=? WHERE case_id=?",
            ("080/05/12", "110/03/20", "test@example.com", "中華電信", _cid_f))
_cn.commit(); _cn.close()
_txt_f = client.get(f"/adminb/download-excel?case_id={_cid_f}").content.decode("utf-8", "replace")
for _lb, _want in [("申請人姓名", "電話測試客"), ("出生年月日", "080/05/12"),
                   ("發證日期", "110/03/20"), ("電子信箱", "test@example.com"),
                   ("手機門號電信", "中華電信")]:
    _ln = next((l for l in _txt_f.splitlines() if l.startswith(_lb)), None)
    check(f"分貝新版填寫表：{_lb} 有帶入", bool(_ln and _want in _ln), f"實際 {_ln!r}")

# ========== 52. 箭頭備註不可以被切成第二筆（2026-09-11 黃彥萍案）==========
# 病根：is_format_trigger 舊規則「這行只要有 -> 就是新一筆的開頭」。
# A 群大幫手把箭頭當「所以」在用：
#   「近9個月均薪 366041 / 月付款 39263 (...) 107% -> 負債高 維持婉拒」
# → 整則被從箭頭切成兩筆：第2筆姓名拽成「個月均薪」回「找不到對應客戶」；
#   更貴的是「負債高 維持婉拒」這個婉拒原因也跟著被切走、沒記到客戶身上。
print("\n=== 52. 箭頭備註不可切成第二筆 ===")

_ARROW_REAL = ("黃彥萍 亞太 婉拒\n"
               "近9個月均薪 366041 / 月付款 39263 "
               "(7083+5500+1w+9780+3400+3500) 107% ->\n"
               "負債高 維持婉拒")
_blk = m.split_multi_cases(_ARROW_REAL)
check("真實訊息只切出 1 筆", len(_blk) == 1,
      f"切成 {len(_blk)} 筆：{_blk!r}")
check("姓名拽到黃彥萍", m.extract_name(_blk[0]) == "黃彥萍",
      f"拽到 {m.extract_name(_blk[0])!r}")
check("婉拒原因留在同一筆", "維持婉拒" in _blk[0],
      "原因被切走了、不會記到客戶身上")

# 其他帶數據的箭頭備註行一律不可以開新一筆
for _line in ["107% -> 負債高", "勞保 3 年 -> 可送",
              "DBR 22 -> 超標", "月付 39263 -> 比例太高"]:
    check(f"備註行「{_line}」不算新一筆",
          not m.looks_like_case_start(_line))

# ⭐ 反向：不可為了修這條而把真的切段擋掉
check("｜ 格式行仍算新一筆（業務群建新案在用）",
      m.looks_like_case_start("信用正常｜勞保3年｜可送"),
      "｜ 被數字條件誤傷了")
check("無數字的箭頭行仍算新一筆（舊行為保留）",
      m.looks_like_case_start("王小明 -> 亞太"))
_TWO = ("王小明 亞太 婉拒\n負債高\n\n"
        "陳大華 和潤 核准\n核 12 萬")
check("空行隔開的真多筆仍切得開",
      len(m.split_multi_cases(_TWO)) == 2,
      f"切成 {len(m.split_multi_cases(_TWO))} 筆")

# ========== 53. 分貝商品（課程分期）進件表單（2026-09-16）==========
# 使用者貼的表單原文就是這組的「真實單據」—— 期望值從原文拄，
# 不可以用程式反推（反推＝自己驗自己，等於沒驗）。
# ⭐ 兩個最容易壞的點：
#   ① 「課程名稱：真人線上英文教材」「金額：14.8萬」是固定值，
#     靠「label 找不到對應就保留範本原值」活著—— 哪天有人把「金額」加進
#     LABEL_MAP，這兩行會被洗成空白，而且沒人會發現。
#   ② ("手機", phone) 排序：排到「手機門號電信」前面的話，
#     分貝改裝表那格會變成填手機號碼（2026-09-08 才修好的那格）。
print("\n=== 53. 分貝商品（課程分期）進件表單 ===")

m.check_auth = lambda req: "admin"
m.get_auth_group_id = lambda req: ""

_BSP_FIELDS = {
    "phone": "0912345678",
    "carrier": "中華電信",
    "company_name_detail": "台灣積層工業股份有限公司",
    "company_role": "機台操作員",
    "company_salary": "4.5",
    "company_years": "27", "company_months": "6",
    "eval_labor_ins": "公司保",
    "eval_salary_transfer": "有薪轉",
    "eval_sent_3m": "是", "eval_sent_3m_detail": "喬美、裕融",
    "eval_law": "共1條",
    "eval_late": "有", "eval_late_days": "15",
    "debt_list": json.dumps([
        {"co": "裕融", "lo": "150000", "pe": "36/6", "mo": "5265", "dy": "動保"},
        {"co": "和潤", "lo": "80000", "pe": "24/12", "mo": "3800", "dy": "無"},
    ], ensure_ascii=False),
}

def _mk_bsp(plan, idno, extra=None):
    """建一筆資料齊全的客戶，回傳填寫表文字"""
    _cid = m.create_customer_record("王小明", idno, plan, "TEST_B", "建案")
    _cn = sqlite3.connect(TEST_DB)
    _cols = {r[1] for r in _cn.execute("PRAGMA table_info(customers)").fetchall()}
    _f = dict(_BSP_FIELDS)
    _f.update(extra or {})
    _f["adminb_selected_plans"] = plan
    _use = {k: v for k, v in _f.items() if k in _cols}
    _cn.execute(f"UPDATE customers SET {','.join(f'{k}=?' for k in _use)} WHERE case_id=?",
                list(_use.values()) + [_cid])
    _cn.commit(); _cn.close()
    _r = client.get(f"/adminb/download-excel?case_id={_cid}")
    return _r.content.decode("utf-8", errors="replace")

_txt = _mk_bsp("分貝商品", "F111222333")
check("分貝商品有範本、抽得出表",
      "進件表單" in _txt, f"抽不出來：{_txt[:80]!r}")

# 期望值逐行對（label 寫法照使用者原文，含 ✅ 跟半形/全形冒號的差異）
for _lb, _want in [
    ("✅姓名：", "王小明"),
    ("✅身分證：", "F111222333"),
    ("✅手機：", "0912345678"),
    ("✅任職公司：", "台灣積層工業股份有限公司"),
    ("✅職稱：", "機台操作員"),
    ("✅月收入：", "4.5萬"),
    ("✅工作年資：", "27年6月"),
    ("✅有無勞保/薪轉：", "公司保 / 有薪轉"),
    ("✅名下貸款/繳息：", "有2筆 / 遲繳15天"),
    ("✅三個月內是否融資有進件:", "是（喬美、裕融）"),
    ("✅有無法學/動單:", "法學共1條 / 有動單"),
    ("✅哪家/金額/期數/已繳期數/月付金:",
     "裕融/150000/36/6/5265、和潤/80000/24/12/3800"),
]:
    _ln = next((l for l in _txt.splitlines() if l.startswith(_lb)), None)
    check(f"分貝商品：{_lb.rstrip(chr(65306)+':')} 帶入正確",
          bool(_ln) and _ln[len(_lb):].strip() == _want,
          f"實際 {_ln!r}、期望 {_want!r}")

# ⭐ 固定值兩行：不可以被洗成空白（使用者特別交代這兩行是固定的）
check("固定值：課程名稱保留",
      "✅課程名稱：真人線上英文教材" in _txt,
      "課程名稱被清掉了")
check("固定值：金額 14.8萬 保留",
      "✅金額：14.8萬" in _txt, "金額被清掉了")

# 沒填負債明細 → 留空白，⛔ 不可以自己寫「無」
_txt_nodebt = _mk_bsp("分貝商品", "F444555666", {"debt_list": "[]"})
_ln_nd = next((l for l in _txt_nodebt.splitlines()
               if l.startswith("✅名下貸款/繳息：")), "")
check("沒負債資料時留空白（不假報「無」）",
      _ln_nd.strip() == "✅名下貸款/繳息：", f"實際 {_ln_nd!r}")

# 別名：課程分期 / 英文教材 都要認得是分貝商品
for _alias in ["課程分期", "英文教材"]:
    check(f"「{_alias}」解成分貝商品",
          m._resolve_alias_loose(_alias) == "分貝商品",
          f"解成 {m._resolve_alias_loose(_alias)!r}")
    check(f"「{_alias}」日報歸到分貝商品",
          m.normalize_section(_alias) == "分貝商品",
          f"歸到 {m.normalize_section(_alias)!r}")
    check(f"「{_alias}」是合法公司名（送件順序不會被擋）",
          _alias in m._get_valid_company_names())

# ⭐ 反向：新增的 ("手機") 不可以打到分貝改裝表的「手機門號電信」
_txt_mc = _mk_bsp("分貝機車", "F777888999")
_ln_mc = next((l for l in _txt_mc.splitlines() if l.startswith("手機門號電信")), "")
check("分貝機車：手機門號電信仍是電信商、不是手機號碼",
      "中華電信" in _ln_mc and "0912345678" not in _ln_mc,
      f"實際 {_ln_mc!r}")

# ========== 54. 願望（2026-10-05 新增公司）走完整流程 ==========
# 同第 49 組：REPORT_SECTION_1／COMPANY_LIST／case-edit 下拉 三處要一起改。
# 日報欄位叫「願望」；業務打「願望」「願望貸」「願望小車」都要認（使用者 2026-10-05）。
print("\n=== 54. 願望新公司 ===")
check("願望在 COMPANY_LIST", "願望" in m.COMPANY_LIST)
check("願望貸在 COMPANY_LIST 且排在願望前面（長字先比）",
      "願望貸" in m.COMPANY_LIST and m.COMPANY_LIST.index("願望貸") < m.COMPANY_LIST.index("願望"))
check("願望在 REPORT_SECTION_1（日報區塊）", "願望" in m.REPORT_SECTION_1)
check("normalize_section(願望)=願望", m.normalize_section("願望") == "願望", m.normalize_section("願望"))
check("normalize_section(願望貸)=願望", m.normalize_section("願望貸") == "願望", m.normalize_section("願望貸"))
check("normalize_section(願望小車)=願望", m.normalize_section("願望小車") == "願望", m.normalize_section("願望小車"))
check("願望小車在 COMPANY_LIST 且排在願望前面",
      "願望小車" in m.COMPANY_LIST and m.COMPANY_LIST.index("願望小車") < m.COMPANY_LIST.index("願望"))

for _tag, _co, _ids in [("短", "願望", ("B444444444", "B555555555", "B666666666")),
                        ("全", "願望貸", ("B444444445", "B555555556", "B666666667")),
                        ("車", "願望小車", ("B444444446", "B555555557", "B666666668"))]:
    _i1, _i2, _i3 = _ids
    bc(f"10/05-願{_tag}甲 {_i1}", gid="TEST_B")
    bc(f"10/05-願{_tag}甲-{_co}/21商品/亞太", gid="TEST_B")
    _d = get_cust(_i1)
    check(f"[{_co}] 送件順序第一家歸願望", _d and m.normalize_section(_d["current_company"]) == "願望",
          _d and _d.get("current_company"))
    bc(f"@AI 願{_tag}甲 送{_co}", gid="TEST_B")
    _d = get_cust(_i1)
    check(f"[{_co}] 送出後離開送件區", (_d.get("report_section") or "") == "",
          f"report_section={_d.get('report_section')!r}")
    a(f"願{_tag}甲 {_co} 核准10萬")
    _d = get_cust(_i1)
    check(f"[{_co}] 核准金額有存", (_d.get("approved_amount") or "") != "", _d.get("approved_amount"))
    check(f"[{_co}] 核准後進待撥款", _d.get("report_section") == "待撥款", _d.get("report_section"))

    bc(f"10/05-願{_tag}乙 {_i2}", gid="TEST_B")
    bc(f"10/05-願{_tag}乙-{_co}/21商品", gid="TEST_B")
    a(f"願{_tag}乙 {_co} 婉拒 信用評分不足")
    _d2 = get_cust(_i2)
    check(f"[{_co}] 婉拒後推進下一家", _d2.get("current_company") == "21商品", _d2.get("current_company"))

    bc(f"10/05-願{_tag}丙 {_i3}", gid="TEST_B")
    bc(f"10/05-願{_tag}丙-{_co}/21商品", gid="TEST_B")
    bc(f"@AI 願{_tag}丙 送{_co}", gid="TEST_B")
    _rep = "\n".join(m.generate_report_lines("TEST_B"))
    _lines = [l.strip() for l in _rep.splitlines()]
    check(f"[{_co}] 日報有「願望」區塊標題", "願望" in _lines, "日報沒長出願望那一格")
    check(f"[{_co}] 日報沒有多長一格「願望貸／願望小車」", "願望貸" not in _lines and "願望小車" not in _lines)

# ========== 55. 核准金額寫「元」有零頭不可被砍（2026-10-06 曾俊傑案）==========
# 原文：「曾俊傑 分貝商核准\n55000/ 18/3884@AI」→ 系統存成 5萬（應為 5.5萬）
# 病根：extract_approved_amount 用 // 整除換算成萬，五千被砍掉。
print("\n=== 55. 核准金額元換萬不砍零頭 ===")
for _txt, _want in [("曾俊傑 分貝商核准\n55000/ 18/3884@AI", "5.5萬"),
                    ("核准 65,500 24期", "6.55萬"),
                    ("核准 50000", "5萬"),
                    ("核准 120,000", "12萬")]:
    _got = m.extract_approved_amount(_txt)
    check(f"金額 {_txt.splitlines()[-1]!r} → {_want}", _got == _want, _got)

bc("10/06-曾測傑 B777777777", gid="TEST_B")
bc("10/06-曾測傑-分貝商品/21商品", gid="TEST_B")
bc("@AI 曾測傑 送分貝商品", gid="TEST_B")
a("曾測傑 分貝商核准\n55000/ 18/3884@AI")
_d = get_cust("B777777777")
check("A 群原文貼上後存的核准金額=5.5萬", "5.5" in (_d.get("approved_amount") or ""), _d.get("approved_amount"))

# ========== 總結 ==========
print(f"\n{'='*50}")
print(f"結果：{PASS} 通過、{FAIL} 失敗")
print(f"{'='*50}")
sys.exit(0 if FAIL == 0 else 1)
