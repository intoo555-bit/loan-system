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
_REAL_CHECK_AUTH = m.check_auth   # 後面很多測試會把登入檢查換成假的，要測真登入時用這個

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

# ========== 56. 合併重複頁：舊客再送件的已結案舊案不可列成「會被合併掉」（2026-10-07）==========
# 邱芷妤／李宜芳等：同群組同身分證兩筆都已結案（5 月一次、8～9 月又一次），
# 預覽頁 _find_dup_sets 沒看狀態 → 列成「31 筆會被合併掉」，跟實際合併（只動進行中）對不上。
print("\n=== 56. 合併重複預覽不列已結案舊案 ===")
_now = datetime.now().isoformat()
_c = sqlite3.connect(TEST_DB)
for _cid, _nid, _st in [("rep_old1", "R100200300", "CLOSED"), ("rep_old2", "R100200300", "CLOSED"),
                        ("rep_new1", "R100200301", "CLOSED"), ("rep_new2", "R100200301", "ACTIVE"),
                        ("dup_a1",  "R100200302", "ACTIVE"), ("dup_a2",  "R100200302", "PENDING")]:
    _c.execute("""INSERT INTO customers (case_id, customer_name, id_no, source_group_id, status, created_at, updated_at)
                  VALUES (?,?,?,?,?,?,?)""", (_cid, "合併測", _nid, "TEST_B", _st, _now, _now))
_c.commit(); _c.close()
_sets = {nid: rs for gid, nid, rs in m._find_dup_sets() if gid == "TEST_B"}
check("兩次都已結案（舊客再送件）→ 不列為重複", "R100200300" not in _sets)
check("舊案結案＋新案進行中 → 不列為重複", "R100200301" not in _sets)
check("兩筆都在進行中 → 仍列為重複", "R100200302" in _sets, list(_sets))
check("預覽清單裡沒有任何已結案的案子",
      all(r["status"] in m._DEDUPE_STATUSES for rs in _sets.values() for r in rs),
      [r["status"] for rs in _sets.values() for r in rs])
# 預覽說幾筆會被併掉，實際按下去就要併掉幾筆
_preview = sum(len(rs) - 1 for rs in _sets.values())
_actual = sum(m._dedupe_same_id_in_group(nid, "TEST_B") for nid in list(_sets))
check("預覽頁筆數 = 實際合併筆數", _preview == _actual, f"預覽 {_preview}、實際 {_actual}")

# ========== 57. business_ai 寫入窗口 /api/customer-upsert + 查詢多回案件歷程（2026-10-07）==========
# 使用者裁示：有進行中案子 → 只補空格、不一樣的不改並推提醒給該業務群；沒有 → 建「待確認」。
print("\n=== 57. business_ai 寫入窗口 ===")
from fastapi.testclient import TestClient
_tc = TestClient(m.app)
m.set_setting("vba_secret", m.hash_pw("t57secret"))
def _up(body):
    return _tc.post("/api/customer-upsert", json=body).json()

check("密碼錯 → 擋下", _tc.post("/api/customer-upsert",
      json={"secret": "wrong", "id_no": "U100000001"}).status_code == 403)

# ── ② 全新客戶 → 待確認 ──
_r = _up({"secret": "t57secret", "id_no": "u100000001", "customer_name": "線上甲",
          "fields": {"birth_date": "民國 70 年 3 月 15 日", "phone": "0912-345-678",
                     "company_name_detail": "大明工程行", "contact1_name": "線上媽",
                     "approved_amount": "99萬", "route_plan": "{}", "status": "CLOSED"}})
_d = get_cust("U100000001")
check("新客戶建成待確認", _r.get("action") == "created" and _d and _d["status"] == "PENDING", (_r, _d and _d.get("status")))
check("生日轉成行政格式 070/03/15", _d.get("birth_date") == "070/03/15", _d.get("birth_date"))
check("手機去掉橫線", _d.get("phone") == "0912345678", _d.get("phone"))
check("公司名稱有寫入", _d.get("company_name_detail") == "大明工程行")
check("⛔ 案件欄位（核准金額/送件順序/狀態）不可從外部寫入",
      not (_d.get("approved_amount") or "") and not (_d.get("route_plan") or "")
      and set(_r.get("ignored", [])) >= {"approved_amount", "route_plan", "status"}, _r.get("ignored"))

# 業務之後在 LINE 建檔 → 接上那筆待確認、資料都在
bc("10/7-線上甲U100000001", gid="TEST_B")
_d = get_cust("U100000001")
check("LINE 建檔接上待確認那筆（變進行中、公司還在）",
      _d["status"] == "ACTIVE" and _d.get("company_name_detail") == "大明工程行"
      and _d["source_group_id"] == "TEST_B", (_d["status"], _d.get("company_name_detail")))

# ── ① 已有進行中案子：只補空格，不一樣的不改、推提醒 ──
pushes.clear()
_r = _up({"secret": "t57secret", "id_no": "U100000001", "customer_name": "線上甲",
          "fields": {"phone": "0912345678",              # 一樣（只是格式不同）→ 不算不一樣
                     "company_name_detail": "新光保全",   # 換工作 → 不改、要提醒
                     "company_role": "警衛",              # 原本空白 → 補上
                     "contact1_name": "線上媽"}})          # 一樣
_d = get_cust("U100000001")
check("回 updated", _r.get("action") == "updated", _r)
check("空白欄位補上（職稱）", _d.get("company_role") == "警衛", _d.get("company_role"))
check("不一樣的不改（公司還是大明工程行）", _d.get("company_name_detail") == "大明工程行", _d.get("company_name_detail"))
check("不一樣的只有公司一項（格式不同的手機不算）",
      [c["field"] for c in _r.get("conflicts", [])] == ["company_name_detail"], _r.get("conflicts"))
_p = [t for g, t in pushes if g == "TEST_B"]
check("推一則提醒到該業務群組", len(_p) == 1, pushes)
check("提醒內容是中文、有新舊值、沒有欄位英文名",
      bool(_p) and "大明工程行" in _p[0] and "新光保全" in _p[0] and "公司" in _p[0]
      and "company_name_detail" not in _p[0], _p[:1])
_c = sqlite3.connect(TEST_DB); _c.row_factory = sqlite3.Row
_lg = _c.execute("SELECT message_text, from_group_id FROM case_logs WHERE id_no='U100000001' ORDER BY id DESC LIMIT 1").fetchone()
_c.close()
check("案件歷程有記（補了什麼、哪裡不一樣）",
      _lg and _lg["from_group_id"] == "BUSINESS_AI" and "警衛" in _lg["message_text"] and "新光保全" in _lg["message_text"],
      _lg and dict(_lg))

# 全部一樣 → 不推提醒
pushes.clear()
_r = _up({"secret": "t57secret", "id_no": "U100000001", "fields": {"company_role": "警衛"}})
check("資料都一樣 → 不推提醒", not pushes and not _r.get("conflicts"), (pushes, _r))

# ── 舊客：以前結案過，這次沒填的從舊案帶 ──
bc("5/1-線上乙U100000002", gid="TEST_B")
_old = get_cust("U100000002")["case_id"]
_c = sqlite3.connect(TEST_DB)
_c.execute("UPDATE customers SET contact2_name='舊聯絡人', company_name_detail='舊公司', status='CLOSED' WHERE case_id=?", (_old,))
_c.commit(); _c.close()
_r = _up({"secret": "t57secret", "id_no": "U100000002", "customer_name": "線上乙",
          "fields": {"company_name_detail": "新公司"}})
_c = sqlite3.connect(TEST_DB); _c.row_factory = sqlite3.Row
_d = dict(_c.execute("SELECT * FROM customers WHERE case_id=?", (_r.get("case_id", ""),)).fetchone() or {})
_c.close()
check("舊客再申請 → 新建待確認（不動結案舊案）", _r.get("action") == "created" and _d["case_id"] != _old, _r)
check("客戶這次填的優先（新公司）", _d.get("company_name_detail") == "新公司", _d.get("company_name_detail"))
check("沒填的從舊案帶（聯絡人2）", _d.get("contact2_name") == "舊聯絡人", _d.get("contact2_name"))

# ── 查詢窗口多回案件歷程 ──
_st = _tc.get("/api/customer-status", params={"id_no": "U100000001", "secret": "t57secret"}).json()
_logs = _st.get("case_logs") or []
check("customer-status 有回案件歷程", len(_logs) >= 2, _st.keys())
check("案件歷程來源是中文、沒有群組 ID",
      all(x["source"] in ("線上申請書", "A群（行政）", "網頁", "系統", "B群", "業務群") for x in _logs)
      and not any("TEST_" in json.dumps(x, ensure_ascii=False) for x in _logs), _logs[:3])
check("案件歷程新的在前", _logs and _logs[0]["source"] == "線上申請書", _logs[:1])

# ========== 58. 建檔日期打錯（115 打成 15）：提醒要講清楚「不用改」（2026-10-08 李冠偉案）==========
# 業務打的日期不會存（建檔日期一律用實際建檔當天），舊提醒卻問「確認無誤？」又只教改身分證/改名，
# 業務以為要改卻找不到方法。
print("\n=== 58. 建檔日期打錯的提醒 ===")
_ret = bc("15/10/8-李測偉 L126040353\n機車無貸款 NSG-8722\n汽車不提供", gid="TEST_B")
_out = "\n".join([str(_ret or "")] + [str(x) for x in replies])
_d = get_cust("L126040353")
check("客戶照樣建立", _d is not None and _d["status"] == "ACTIVE")
check("建檔日期用今天（不是 15 年）", _d and (_d["created_at"] or "")[:10] == m.now_tw().strftime("%Y-%m-%d"), _d and _d["created_at"])
check("提醒猜出正確日期 115/10/8", "115/10/8" in _out, _out)
check("提醒講明不用改", "不用改" in _out, _out)
check("不再問「確認無誤」、不再叫人改身分證/改名", "確認無誤" not in _out and "改身分證" not in _out, _out)
_rep = "\n".join(m.generate_report_lines("TEST_B"))
check("日報顯示今天的日期", f"{m.now_tw().strftime('%m/%d')}-李測偉" in _rep, [l for l in _rep.splitlines() if "李測偉" in l])

# ========== 59. 查詢窗口多回客戶資料表欄位（2026-10-08 business_ai 要求：AI 不重問客戶）==========
print("\n=== 59. customer-status 回客戶資料表欄位 ===")
m.set_setting("vba_secret", m.hash_pw("t59secret"))
bc("10/8-查詢甲V100000001", gid="TEST_B")
_c = sqlite3.connect(TEST_DB)
_c.execute("""UPDATE customers SET company_name_detail='大明工程行', company_role='技工', company_years='3',
              phone='0912345678', contact1_name='查詢媽', contact1_phone='0922333444',
              reg_city='台中市', eval_fine='1200', eval_fuel_tax='800' WHERE id_no='V100000001'""")
_c.commit(); _c.close()
_st = TestClient(m.app).get("/api/customer-status", params={"id_no": "V100000001", "secret": "t59secret"}).json()
check("每個約定的欄位名都有回（business_ai 照名字接）",
      all(k in _st for k in m._STATUS_API_PERSONAL_KEYS + ["eval_total_fee"]),
      [k for k in m._STATUS_API_PERSONAL_KEYS + ["eval_total_fee"] if k not in _st])
check("工作資料有回（AI 一直重問工作的原因）",
      _st.get("company_name_detail") == "大明工程行" and _st.get("company_role") == "技工", _st.get("company_name_detail"))
check("聯絡人有回", _st.get("contact1_name") == "查詢媽" and _st.get("contact1_phone") == "0922333444")
check("欠費總額 = 罰單 + 燃料稅 = 2000", _st.get("eval_total_fee") == "2000", _st.get("eval_total_fee"))
check("沒填的回空字串（不是 None）", _st.get("contact2_name") == "", repr(_st.get("contact2_name")))
check("原本的欄位沒被蓋掉", _st.get("exists") is True and "case_logs" in _st and "company_salary" in _st)
check("密碼錯 → 擋下",
      TestClient(m.app).get("/api/customer-status", params={"id_no": "V100000001", "secret": "x"}).status_code == 403)
# 都沒填欠費 → 空字串，不能回 0（AI 會以為問過了、沒欠費）
check("欠費都沒填 → 空字串", m._fee_total({}) == "" and m._fee_total({"eval_fine": "$1,500"}) == "1500")

# ========== 60. 待確認資料在 LINE 建檔 → 建立日期改成建檔那天（2026-10-08 李冠偉案）==========
# 行政 10/02 在網頁先填（PENDING），業務 10/08 才在 LINE 建檔，日報卻顯示 10/02。
# 使用者裁示：一律依建檔那天。
print("\n=== 60. 待確認轉正式：日期用建檔那天 ===")
_today = m.now_tw().strftime("%Y-%m-%d")
_c = sqlite3.connect(TEST_DB)
_c.execute("""INSERT INTO customers (case_id,customer_name,id_no,source_group_id,company,status,company_name_detail,created_at,updated_at)
              VALUES ('pend60','預填甲','W100000001','','','PENDING','預填公司','2026-10-02T15:00:00','2026-10-02T15:00:00')""")
_c.commit(); _c.close()
_ret = bc("15/10/8-預填甲 W100000001", gid="TEST_B")
_d = get_cust("W100000001")
check("接上待確認那筆（同一筆、資料還在）",
      _d["case_id"] == "pend60" and _d["status"] == "ACTIVE" and _d.get("company_name_detail") == "預填公司",
      (_d["case_id"], _d["status"]))
check("建立日期改成今天（不是 10/02）", (_d["created_at"] or "")[:10] == _today, _d["created_at"])
_rep = "\n".join(m.generate_report_lines("TEST_B"))
check("日報顯示今天", f"{m.now_tw().strftime('%m/%d')}-預填甲" in _rep and "10/02-預填甲" not in _rep,
      [l for l in _rep.splitlines() if "預填甲" in l])
check("機器人提醒寫的日期跟日報一致", f"日報的建檔日期是 {m.now_tw().strftime('%m/%d')}" in str(_ret), _ret)
_c = sqlite3.connect(TEST_DB); _c.row_factory = sqlite3.Row
_lg = [r["message_text"] for r in _c.execute("SELECT message_text FROM case_logs WHERE case_id='pend60'")]
_c.close()
check("案件歷程記下原本 10/02 填寫", any("2026-10-02 填寫" in t for t in _lg), _lg)

# ========== 61. 南亞（2026-10-08 新增公司）走完整流程 ==========
# 日報欄位叫「南亞」；業務打「南亞」「南亞機車」「南亞汽車」都要認、都歸同一格（使用者 2026-10-08）。
print("\n=== 61. 南亞新公司 ===")
check("南亞在 REPORT_SECTION_1（日報區塊）", "南亞" in m.REPORT_SECTION_1)
for _v in ("南亞機車", "南亞汽車"):
    check(f"{_v} 在 COMPANY_LIST 且排在南亞前面（長字先比）",
          _v in m.COMPANY_LIST and m.COMPANY_LIST.index(_v) < m.COMPANY_LIST.index("南亞"))
    check(f"normalize_section({_v})=南亞", m.normalize_section(_v) == "南亞", m.normalize_section(_v))
check("normalize_section(南亞)=南亞", m.normalize_section("南亞") == "南亞", m.normalize_section("南亞"))

for _tag, _co, _ids in [("短", "南亞",     ("D200000001", "D200000002", "D200000003")),
                        ("機", "南亞機車", ("D200000011", "D200000012", "D200000013")),
                        ("汽", "南亞汽車", ("D200000021", "D200000022", "D200000023"))]:
    _i1, _i2, _i3 = _ids
    bc(f"10/08-南{_tag}甲 {_i1}", gid="TEST_B")
    bc(f"10/08-南{_tag}甲-{_co}/21商品/亞太", gid="TEST_B")
    _d = get_cust(_i1)
    check(f"[{_co}] 送件順序第一家歸南亞", _d and m.normalize_section(_d["current_company"]) == "南亞",
          _d and _d.get("current_company"))
    bc(f"@AI 南{_tag}甲 送{_co}", gid="TEST_B")
    _d = get_cust(_i1)
    check(f"[{_co}] 送出後離開送件區", (_d.get("report_section") or "") == "",
          f"report_section={_d.get('report_section')!r}")
    a(f"南{_tag}甲 {_co} 核准10萬")
    _d = get_cust(_i1)
    check(f"[{_co}] 核准金額有存", (_d.get("approved_amount") or "") != "", _d.get("approved_amount"))
    check(f"[{_co}] 核准後進待撥款", _d.get("report_section") == "待撥款", _d.get("report_section"))

    bc(f"10/08-南{_tag}乙 {_i2}", gid="TEST_B")
    bc(f"10/08-南{_tag}乙-{_co}/21商品", gid="TEST_B")
    a(f"南{_tag}乙 {_co} 婉拒 信用評分不足")
    _d2 = get_cust(_i2)
    check(f"[{_co}] 婉拒後推進下一家", _d2.get("current_company") == "21商品", _d2.get("current_company"))

    bc(f"10/08-南{_tag}丙 {_i3}", gid="TEST_B")
    bc(f"10/08-南{_tag}丙-{_co}/21商品", gid="TEST_B")
    bc(f"@AI 南{_tag}丙 送{_co}", gid="TEST_B")
    _lines = [l.strip() for l in "\n".join(m.generate_report_lines("TEST_B")).splitlines()]
    check(f"[{_co}] 日報有「南亞」區塊標題", "南亞" in _lines, "日報沒長出南亞那一格")
    check(f"[{_co}] 日報沒有多長一格「南亞機車／南亞汽車」",
          "南亞機車" not in _lines and "南亞汽車" not in _lines)

# ========== 62. 線上申請書提醒不可以充滿「假的不一樣」（2026-10-09 卓泰禎測試）==========
# 真實提醒 15 項裡大部分是寫法不同：台/臺、弟/弟弟、父母名下/父母、500000 vs 30～50 萬、
# 描述型欄位各說各話。業務看到這種會麻痺 → 真的換工作時也不看了。
# 下面的值照抄那則真實提醒（系統值 / 客戶填的值），身分證用假的。
print("\n=== 62. 線上申請書提醒只留真的不一樣 ===")
m.set_setting("vba_secret", m.hash_pw("t57secret"))   # 第 59 組換過密碼，這裡設回來
bc("10/9-卓測禎X100000001", gid="TEST_B")
_c = sqlite3.connect(TEST_DB)
_c.execute("""UPDATE customers SET birth_date='069/06/21', fb='卓小宇', live_city='台中市', reg_city='台中市',
              company_city='台中市', live_status='父母名下', company_years='18', company_salary='4',
              eval_fund_need='500000', contact1_relation='弟', contact2_relation='父',
              eval_sent_3m_detail='和潤車貸過件', eval_credit_card='有卡', eval_property='1汽 1機 /有房',
              eval_late='無' WHERE id_no='X100000001'""")
_c.commit(); _c.close()
pushes.clear()
_r = _up({"secret": "t57secret", "id_no": "X100000001", "customer_name": "卓測禎",
          "fields": {"birth_date": "083/12/04", "fb": "黃小小", "live_city": "臺中市", "reg_city": "臺中市",
                     "company_city": "臺中市", "live_status": "父母", "company_years": "10",
                     "company_salary": "45000", "eval_fund_need": "30～50 萬",
                     "contact1_relation": "弟弟", "contact2_relation": "爸爸",
                     "eval_sent_3m_detail": "融資也正常/無", "eval_credit_card": "無",
                     "eval_property": "無（問到時客戶只回有汽車）", "eval_late": "正常，無呆帳/協商"}})
_cf = sorted(c["field"] for c in _r.get("conflicts", []))
check("只剩真的不一樣：生日、FB、年資、月薪",
      _cf == sorted(["birth_date", "fb", "company_years", "company_salary"]), _cf)
_p = [t for g, t in pushes if g == "TEST_B"]
check("月薪用萬顯示（4萬 → 4.5萬），不是 4 → 45000",
      bool(_p) and "4萬" in _p[0] and "4.5萬" in _p[0] and "45000" not in _p[0], _p[:1])
check("台/臺 不算不一樣", "臺中市" not in (_p[0] if _p else ""), _p[:1])
_d = get_cust("X100000001")
check("描述型欄位不提醒、也不蓋掉（行政寫的還在）",
      _d.get("eval_sent_3m_detail") == "和潤車貸過件" and _d.get("eval_credit_card") == "有卡", _d.get("eval_credit_card"))
# 範圍判斷
check("50 萬 落在 30～50 萬 → 一樣", m._apply_same("500000", "30～50 萬", "eval_fund_need"))
check("80 萬 不在 30～50 萬 → 不一樣", not m._apply_same("800000", "30～50 萬", "eval_fund_need"))
check("關係：媽媽＝母、老公≠父", m._apply_same("母", "媽媽", "contact1_relation")
      and not m._apply_same("父", "老公", "contact1_relation"))

# ========== 63. 亞太新規則（2026-10-13 使用者口述）==========
# 亞太機車／汽車 25萬：持有 1 年、車齡 15 年內
# 車齡超過 15 年、持有 1 年以上、名下無動擔 → 可送 15 萬（機車汽車都可以）
print("\n=== 63. 亞太汽車25萬／老車15萬 ===")
def _apt(extra):
    c = {"birth_date": "080/01/01", "id_no": "A123456789", "contact1_relation": "母",
         "eval_labor_ins": "公司保", "eval_alert_warning": "否"}
    c.update(extra)
    return {r["company"]: r["eligible"] is not False for r in m.evaluate_case(c)["results"]}
_y = m.now_tw().year
_new_car = _apt({"eval_property": "1汽", "unloan_vehicles": json.dumps([{"type": "汽車", "year": str(_y - 8)}])})
_old_car = _apt({"eval_property": "1汽", "unloan_vehicles": json.dumps([{"type": "汽車", "year": str(_y - 20)}])})
_old_moto = _apt({"unloan_vehicles": json.dumps([{"type": "機車", "year": str(_y - 20)}])})
_old_moto_lien = _apt({"debt_list": json.dumps([{"co": "裕融機車", "dy": "動保", "sp": "有", "yr": str(_y - 20)}])})
check("車齡 8 年汽車 → 可送亞太汽車25萬", _new_car["亞太汽車25萬"])
check("車齡 8 年汽車 → 不走老車15萬（該送 25 萬）", not _new_car["亞太老車15萬"])
check("車齡 20 年汽車 → 不可送 25 萬", not _old_car["亞太汽車25萬"])
check("車齡 20 年汽車、無動擔 → 可送老車15萬", _old_car["亞太老車15萬"])
check("車齡 20 年機車、無動擔 → 可送老車15萬", _old_moto["亞太老車15萬"])
check("車齡 20 年機車、有動擔 → 不可送老車15萬", not _old_moto_lien["亞太老車15萬"])
_pr = {p["company"]: p["priority"] for p in m.PLAN_ELIGIBILITY_RULES}
check("金額高先送：汽車25萬排在老車15萬前面", _pr["亞太汽車25萬"] > _pr["亞太老車15萬"])
check("日報歸亞太", m.normalize_section("亞太汽車25萬") == "亞太" and m.normalize_section("亞太老車15萬") == "亞太")

# ========== 64. 東豐／願望（2026-10-13 使用者貼公告）==========
print("\n=== 64. 東豐小額信貸／願望小車 ===")
def _ev(c):
    return {r["company"]: r["eligible"] is not False for r in m.evaluate_case(c)["results"]}
_b = {"birth_date": "080/01/01", "id_no": "A123456789", "contact1_relation": "母", "contact2_relation": "父",
      "unloan_vehicles": json.dumps([{"type": "機車", "year": str(_y - 5)}])}
# 東豐：同業繳息半年 / 現職勞保 擇一（使用者裁示）
check("東豐：無勞保但同業繳息 8 期 → 可送", _ev(dict(_b, eval_labor_ins="無", debt_list=json.dumps([{"co": "裕融機車", "pa": "8"}])))["東豐小額信貸"])
check("東豐：公司保、沒有繳息紀錄 → 可送", _ev(dict(_b, eval_labor_ins="公司保"))["東豐小額信貸"])
check("東豐：無勞保、繳息 3 期 → 不可送", not _ev(dict(_b, eval_labor_ins="無", debt_list=json.dumps([{"co": "裕融機車", "pa": "3"}])))["東豐小額信貸"])
check("東豐：56 歲 → 不可送", not _ev(dict(_b, eval_labor_ins="公司保", birth_date="059/01/01"))["東豐小額信貸"])
# 願望：公司保 25 萬 / 工會 15 萬 / 有呆帳不送 / 當鋪可代償
_w = _ev(dict(_b, eval_labor_ins="公司保"))
check("願望：公司保 → 可送 25 萬", _w["願望小車25萬"])
check("願望：公司保 → 不走工會 15 萬", not _w["願望工會15萬"])
check("願望：工會保 → 可送工會 15 萬", _ev(dict(_b, eval_labor_ins="工會保"))["願望工會15萬"])
check("願望：信用卡寫呆帳 → 不可送", not _ev(dict(_b, eval_labor_ins="公司保", eval_credit_card="國泰呆帳"))["願望小車25萬"])
check("願望：寫「無呆帳」不能誤判", _ev(dict(_b, eval_labor_ins="公司保", eval_note="無呆帳"))["願望小車25萬"])
check("願望：66 歲 → 不可送", not _ev(dict(_b, eval_labor_ins="公司保", birth_date="049/01/01"))["願望小車25萬"])
check("願望：當鋪私設也能送（當鋪可代償）", "願望小車25萬" in m.PRIVATE_OK_PLANS)
# 分貝：2026-10-13 使用者「分貝現在改規則不用一定要二等親 現金都可以」→ 警示戶直接可送、不用再確認
_fb = [r for r in m.evaluate_case(dict(_b, eval_alert_warning="是"))["results"] if r["company"] == "分貝商品"][0]
_fb_ck = [ch for ch in _fb["checks"] if "警示戶" in ch["label"]][0]
check("分貝商品：警示戶 → 這條直接通過（不用再確認撥二等親）", _fb_ck["status"] == "pass")
check("分貝商品：警示戶 → 可送", _fb["eligible"] is not False)
# 年齡提醒只在需要時跳（2026-10-13 使用者：35 歲還打驚嘆號沒道理）
def _age_ck(birth, plan):
    r = [x for x in m.evaluate_case(dict(_b, birth_date=birth))["results"] if x["company"] == plan][0]
    return [c for c in r["checks"] if "年齡" in c["label"]][0]["status"]
_ny = _y - 1911
check("21商品：35 歲 → 年齡直接 ✅", _age_ck(f"{_ny-35:03d}/01/01", "21商品") == "pass")
check("21商品：56 歲 → 年齡 ⚠️（要補保人）", _age_ck(f"{_ny-56:03d}/01/01", "21商品") == "manual")
check("願望：35 歲 → 年齡直接 ✅", _age_ck(f"{_ny-35:03d}/01/01", "願望小車25萬") == "pass")
check("願望：19 歲 → 年齡 ⚠️（要補保人）", _age_ck(f"{_ny-19:03d}/01/01", "願望小車25萬") == "manual")
# 和裕：一開始照排、亞太婉拒過才跳（2026-10-13 使用者）
def _hy_ck(rp):
    r = [x for x in m.evaluate_case(dict(_b, route_plan=rp))["results"] if x["company"] == "和裕機車"][0]
    return [c for c in r["checks"] if "亞太" in c["label"]][0]["status"]
check("和裕：亞太沒婉拒 → 這條直接 ✅", _hy_ck(json.dumps({"order": ["亞太", "和裕"], "current_index": 0, "history": []})) == "pass")
check("和裕：亞太機車25萬婉拒過 → ❌ 跳過", _hy_ck(json.dumps({"order": ["亞太機車25萬", "和裕"], "current_index": 1,
      "history": [{"company": "亞太機車25萬", "status": "婉拒", "date": "2026-10-13"}]}, ensure_ascii=False)) == "fail")
# 規則表 A/B 類（2026-10-13）：系統判斷得出來的改自動、純說明不打 ⚠️
def _st(cust, plan, kw):
    r = [x for x in m.evaluate_case(dict(_b, **cust))["results"] if x["company"] == plan][0]
    return [c for c in r["checks"] if kw in c["label"]][0]["status"]
_car = lambda pa: json.dumps([{"co": "和潤汽車", "dy": "動保", "sp": "有", "pa": pa}])
check("第一汽車：車貸已繳 20 期 → 滿 1 年 ✅", _st({"debt_list": _car("20")}, "第一汽車", "繳滿 1 年") == "pass")
check("第一汽車：車貸已繳 8 期 → ❌", _st({"debt_list": _car("8")}, "第一汽車", "繳滿 1 年") == "fail")
check("第一汽車：遲繳 20 天 → ❌", _st({"eval_late": "有", "eval_late_days": "20"}, "第一汽車", "遲繳") == "fail")
check("第一汽車：沒遲繳 → ✅", _st({"eval_late": "無"}, "第一汽車", "遲繳") == "pass")
check("負債表沒填 → ⚠️ 不可當成 ❌", _st({}, "第一汽車", "繳滿 1 年") == "unknown")
check("興達勞保貸：有一筆只繳 3 期 → ❌", _st({"debt_list": json.dumps([{"co": "裕融機車", "pa": "10"}, {"co": "21商品", "pa": "3"}])},
      "興達勞保貸", "每一筆") == "fail")
check("和裕：40 歲 → 45~55 那條直接 ✅", _st({"birth_date": f"{_ny-40:03d}/01/01"}, "和裕機車", "45~55") == "pass")
check("和裕：50 歲 → 45~55 那條 ⚠️", _st({"birth_date": f"{_ny-50:03d}/01/01"}, "和裕機車", "45~55") == "manual")
check("融易：35 歲年齡自動 ✅", _st({"birth_date": f"{_ny-35:03d}/01/01"}, "融易機車專案", "年齡") == "pass")
check("代書「不看條件」是說明、不是 ⚠️", _st({}, "代書", "不看條件") == "info")
# 使用者：夠不夠貸業務已經標在空間欄，1.4 倍只是說明
check("中租 1.4 倍是說明、不打 ⚠️", _st({}, "中租汽車", "1.4 倍") == "info")
check("日報歸東豐／願望",m.normalize_section("東豐小額信貸") == "東豐" and m.normalize_section("願望小車25萬") == "願望")

# ========== 65. 業務實際打的寫法要認得（2026-10-13 使用者：願望25萬沒用進去）==========
# 規則表用全名（願望小車25萬），但業務打「願望25萬」——不認得就不會歸到日報「願望」那一格。
# 順便把這次規則表新增的方案名都走一遍，避免同一個洞。
print("\n=== 65. 新方案的各種寫法 ===")
for _n, (_co, _sec) in enumerate([("願望25萬", "願望"), ("願望15萬", "願望"), ("願望工會", "願望"),
                                   ("願望小車25萬", "願望"), ("願望工會15萬", "願望"),
                                   ("亞太汽車25萬", "亞太"), ("亞太老車15萬", "亞太"),
                                   ("東豐小額信貸", "東豐"), ("中租汽車", "中租")]):
    _nm = "寫" + "一二三四五六七八九"[_n]
    check(f"normalize_section({_co})={_sec}", m.normalize_section(_co) == _sec, m.normalize_section(_co))
    _i1, _i2 = f"D3000{_n:02d}001", f"D3000{_n:02d}002"
    bc(f"10/13-{_nm}甲 {_i1}", gid="TEST_B")
    bc(f"10/13-{_nm}甲-{_co}/21商品", gid="TEST_B")
    _d = get_cust(_i1)
    check(f"[{_co}] 送件順序第一家歸{_sec}", _d and m.normalize_section(_d["current_company"]) == _sec,
          _d and _d.get("current_company"))
    bc(f"@AI {_nm}甲 送{_co}", gid="TEST_B")
    _lines = [l.strip() for l in "\n".join(m.generate_report_lines("TEST_B")).splitlines()]
    check(f"[{_co}] 日報有「{_sec}」那一格", _sec in _lines)
    check(f"[{_co}] 日報沒有多長一格「{_co}」", _co == _sec or _co not in _lines)
    a(f"{_nm}甲 {_co} 核准10萬")
    _d = get_cust(_i1)
    check(f"[{_co}] 核准後進待撥款", _d.get("report_section") == "待撥款", _d.get("report_section"))
    bc(f"10/13-{_nm}乙 {_i2}", gid="TEST_B")
    bc(f"10/13-{_nm}乙-{_co}/21商品", gid="TEST_B")
    a(f"{_nm}乙 {_co} 婉拒 信用評分不足")
    _d2 = get_cust(_i2)
    check(f"[{_co}] 婉拒後推進下一家", _d2.get("current_company") == "21商品", _d2.get("current_company"))

# ========== 66. 南亞（民間，2026-10-13 使用者）==========
# 「一律排到代書跟當鋪前面、不看條件有汽機車就可以、警示戶可以送、填他們表格就可以」
print("\n=== 66. 南亞規則 ===")
_y = m.now_tw().year - 1911
def _ny(**kw):
    c = {"birth_date": f"{_y-35:03d}/01/01", "id_no": "A123456789", "eval_labor_ins": "公司保",
         "eval_alert_warning": "否", "eval_late": "無", "eval_property": "", "debt_list": "[]", "unloan_vehicles": "[]"}
    c.update(kw)
    res = m.evaluate_case(c)["results"]
    names = [r["company"] for r in res]
    return {r["company"]: r for r in res}, names
for _tag, _kw in [("評估表 1機", {"eval_property": "1機"}),
                  ("評估表 1汽", {"eval_property": "1汽"}),
                  ("只有無貸款車輛清單", {"unloan_vehicles": '[{"type": "機車", "year": "2010"}]'}),
                  ("只有負債表汽車貸款（滿貸也算）", {"debt_list": '[{"co": "和潤汽車", "dy": "動保", "sp": "無", "pa": "3"}]'}),
                  ("警示戶＋有機車", {"eval_property": "1機", "eval_alert_warning": "是"})]:
    _r, _names = _ny(**_kw)
    check(f"南亞［{_tag}］→ 可送", _r["南亞"]["eligible"] is True, _r["南亞"]["summary"])
    check(f"南亞［{_tag}］排在代書前面", _names.index("南亞") < _names.index("代書"), _names)
    check(f"南亞［{_tag}］排在當鋪前面", _names.index("南亞") < _names.index("當舖"), _names)
_r, _names = _ny(eval_property="無")
check("南亞［沒有汽機車］→ 不送", _r["南亞"]["eligible"] is False, _r["南亞"]["summary"])
check("南亞［沒有汽機車］代書照樣排", _r["代書"]["eligible"] is True)
_r, _ = _ny(eval_property="1機")
check("南亞金額看審核（畫面不顯示 10 萬）", _r["南亞"].get("amount_by_review") is True)
check("南亞文件＝填他們的表格", _r["南亞"]["required_docs"] == ["南亞填寫表"], _r["南亞"]["required_docs"])
check("南亞私設可以送", "南亞" in m.PRIVATE_OK_PLANS)
_r, _names = _ny(eval_property="1機")
check("南亞不會插到同樣 10 萬的融資（貸救補）前面", _names.index("貸救補") < _names.index("南亞"), _names)

# ========== 67. 建議送件順序（2026-10-13 使用者：寫建議送但沒排順序；21可以送麻吉就不會排）==========
print("\n=== 67. 建議送件順序 ===")
def _so(**kw):
    c = {"birth_date": f"{_y-35:03d}/01/01", "id_no": "A123456789", "contact1_relation": "母", "contact2_relation": "父",
         "eval_labor_ins": "公司保", "eval_alert_warning": "否", "eval_late": "無", "eval_property": "1機",
         "debt_list": "[]", "unloan_vehicles": '[{"type": "機車", "year": "2019"}]',
         # 2026-10-14 起資料表沒填齊不排順序 → 測試客戶要填齊
         "company_salary": "3.5", "company_years": "3", "eval_salary_transfer": "有薪轉", "eval_alert": "無"}
    c.update(kw)
    r = m.evaluate_case(c)
    return r, [o["company"] for o in r["suggested_order"]], [o["company"] for o in r["backup_plans"]]
_r, _o, _b = _so()
_res = {x["company"]: x for x in _r["results"]}
check("21機車25萬 是 ⚠️（要確認）時，麻吉也要降成不排",
      _res["21機車25萬"]["eligible"] == "manual" and all(_res[k]["eligible"] in ("demoted", False) for k in _res if k.startswith("麻吉")),
      {k: v["eligible"] for k, v in _res.items() if k.startswith(("21", "麻吉"))})
check("建議順序沒有麻吉", not any(x.startswith("麻吉") for x in _o), _o)
check("同家族只排最大：有 21機車25萬 就不排 21機車12萬／21商品", "21機車25萬" in _o and "21機車12萬" not in _o and "21商品" not in _o, _o)
check("同家族只排最大：有 亞太機車25萬 就不排 亞太機車15萬／亞太商品", "亞太機車25萬" in _o and "亞太機車15萬" not in _o and "亞太商品" not in _o, _o)
check("金額大先送：21機車25萬 在 和裕機車 前面", _o.index("21機車25萬") < _o.index("和裕機車"), _o)
# 資料表沒填齊不排順序（使用者 2026-10-14：客戶有給機車行照，資料表沒寫車就直接判亞太商品）
_r, _o, _b = _so(company_salary="", eval_alert="")
check("資料表沒填齊 → 不排順序，列出還缺什麼", not _o and not _b and "月薪" in _r["missing_info"] and "當鋪私設" in _r["missing_info"], _r["missing_info"])
_r, _o, _b = _so(unloan_vehicles="[]", eval_property="")
check("負債／車輛都沒填、動產也沒寫 → 不排", not _o and any("負債明細／車輛" in x for x in _r["missing_info"]), _r["missing_info"])
_r, _o, _b = _so(unloan_vehicles="[]", eval_property="無")
check("沒有車、動產寫「無」→ 算填了，照排", not _r["missing_info"] and _o, _r["missing_info"])
# 21機車25萬：機車貸款有設動保就不做（只有亞太機車做）；沒設動保可以排（使用者 2026-10-14）
_r, _o, _b = _so(unloan_vehicles="[]", debt_list=json.dumps([{"co": "和裕機車", "dy": "動保", "sp": "有", "yr": "2020"}], ensure_ascii=False))
_res = {x["company"]: x for x in _r["results"]}
check("機車貸款有設動保 → 21機車25萬 不排", _res["21機車25萬"]["eligible"] is False and "21機車25萬" not in _o,
      (_res["21機車25萬"]["eligible"], _o))
_r, _o, _b = _so(unloan_vehicles="[]", debt_list=json.dumps([{"co": "和裕機車", "dy": "無", "sp": "有", "yr": "2020"}], ensure_ascii=False))
_res = {x["company"]: x for x in _r["results"]}
check("機車貸款沒設動保 → 21機車25萬 可以排", _res["21機車25萬"]["eligible"] is not False, _res["21機車25萬"])
check("有勞保 → 排代書、不排當鋪", "代書" in _o and "當舖" not in _o, _o)
check("南亞在代書前面", _o.index("南亞") < _o.index("代書"), _o)
check("融易是少送方案 → 放備選、不進順序", not any(x.startswith("融易") for x in _o) and any(x.startswith("融易") for x in _b), (_o, _b))
_r, _o, _b = _so(eval_labor_ins="無勞保", eval_property="無", unloan_vehicles="[]")
check("沒勞保 → 排當鋪、不排代書", "當舖" in _o and "代書" not in _o, _o)
check("沒車 → 不排南亞", "南亞" not in _o, _o)
_r, _o, _b = _so(eval_property="1機1汽", unloan_vehicles='[{"type": "機車", "year": "2019"}, {"type": "汽車", "year": "2018"}]')
check("有汽車＋機車 → 汽車家族排在機車前面", _o.index("和潤汽車") < _o.index("21機車25萬"), _o)
check("21汽車A／B／C 只排一個", sum(1 for x in _o if x.startswith("21汽車")) == 1, _o)

# ========== 68. 建議順序照行政實際順序（2026-10-13 正式站 7 月後 1447 件對帳＋使用者裁示）==========
# 期望值來源：真實送件順序統計（括號是「前者在後者前面」的件數），不是從程式反推
print("\n=== 68. 建議順序對帳真實順序 ===")
def _before(o, a, b):
    ia = next((i for i, x in enumerate(o) if x.startswith(a)), None)
    ib = next((i for i, x in enumerate(o) if x.startswith(b)), None)
    return ia is not None and ib is not None and ia < ib
_r, _o, _b = _so(eval_property="1機1汽", unloan_vehicles='[{"type": "機車", "year": "2019"}, {"type": "汽車", "year": "2018"}]')
for _a, _bb, _why in [("中租", "裕融", "19/19"), ("裕融", "21汽車", "17/17"), ("和潤", "21汽車", "5/5 ＋使用者：中租/和潤/21"),
                      ("21汽車", "亞太", "11/11"), ("亞太", "21機車25萬", "64/90 ＋使用者：亞太扣比較少"),
                      ("21機車25萬", "和裕", "7/7"), ("和裕", "喬美", "688/727"), ("喬美", "貸救補", ""),
                      ("貸救補", "分貝", "201/201 ＋使用者：分貝新專案排貸救補後面"),
                      ("分貝", "鄉民", "123/123"), ("鄉民", "銀行", "570/570"), ("銀行", "零卡", "696/696"),
                      ("零卡", "商品貸", "1479/1479"), ("商品貸", "南亞", ""), ("南亞", "代書", "31/31")]:
    check(f"建議順序：{_a} 在 {_bb} 前面（{_why}）", _before(_o, _a, _bb), _o)
_r, _o, _b = _so(eval_property="無", unloan_vehicles="[]")
check("沒車：亞太商品 → 和裕商品 → 喬美 → 21商品（414/416、626/630）",
      _before(_o, "亞太商品", "和裕商品") and _before(_o, "和裕商品", "喬美") and _before(_o, "喬美", "21商品"), _o)
check("21商品 提醒「通常跟喬美一起送」", any(o["company"] == "21商品" and "喬美" in o["note"] for o in _r["suggested_order"]))
for _lab, _want in [("公司保", "代書"), ("軍保", "代書"), ("工會保", "當舖"), ("無勞保", "當舖"), ("漁保", "當舖")]:
    _r, _o, _b = _so(eval_labor_ins=_lab, eval_property="無", unloan_vehicles="[]")
    _other = "當舖" if _want == "代書" else "代書"
    check(f"勞保欄「{_lab or '空白'}」→ 排{_want}、不排{_other}", _want in _o and _other not in _o, _o)
# 2026-10-14 起：勞保空白＝還沒填（不是無勞保）→ 不排順序（原本當成無勞保排當舖）
_r, _o, _b = _so(eval_labor_ins="", eval_property="無", unloan_vehicles="[]")
check("勞保欄空白 → 不排順序、寫缺勞保（要選「無勞保」才算填了）", not _o and "勞保" in _r["missing_info"], (_o, _r["missing_info"]))

# ========== 69. 鄉民不是固定排（2026-10-13 使用者：要信用報告、440 分以上，呆帳或協商就不用排）==========
print("\n=== 69. 鄉民條件 ===")
_r, _o, _b = _so()
_x = {x["company"]: x for x in _r["results"]}["鄉民"]
check("鄉民一般客戶 → ⚠️ 要確認（先問信用報告），不是 ✅ 固定排", _x["eligible"] == "manual", _x["summary"])
for _k, _v in [("eval_credit_card", "銀行協商"), ("eval_note", "有銀行呆帳"), ("eval_law", "呆帳 1 筆")]:
    _r, _o, _b = _so(**{_k: _v})
    _x = {x["company"]: x for x in _r["results"]}["鄉民"]
    check(f"鄉民：{_k}＝「{_v}」→ 不排", _x["eligible"] is False and "鄉民" not in _o, _x["summary"])
for _v in ("無呆帳", "沒有協商"):
    _r, _o, _b = _so(eval_note=_v)
    _x = {x["company"]: x for x in _r["results"]}["鄉民"]
    check(f"鄉民：備註寫「{_v}」不算", _x["eligible"] != False, _x["summary"])

# ========== 70. 「已從舊案帶入」不可以掛到別的客人身上（2026-10-13 吳鴻志）==========
# 有的建檔路徑帶了舊案資料卻沒把筆數取走 → 下一位新客人建檔時跳「已從舊案帶入 57 項」
print("\n=== 70. 舊案帶入筆數不可殘留 ===")
_c = sqlite3.connect(TEST_DB)
_c.execute("INSERT OR REPLACE INTO groups (group_id, group_name, group_type, is_active, created_at) VALUES (?,?,?,?,?)",
           ("TEST_C_GRP", "C群", "SALES_GROUP", 1, now))
_c.commit(); _c.close()
bc("10/13-舊客甲 E200000001", gid="TEST_B")
bc("10/13-舊客己 E200000006", gid="TEST_B")
_c = sqlite3.connect(TEST_DB)
_c.execute("UPDATE customers SET phone='0912000001', company_name_detail='測試公司', status='CLOSED' WHERE id_no IN ('E200000001','E200000006')")
_c.commit(); _c.close()
def _stale():   # 模擬「帶了舊案資料、但沒用 _copied_hint() 取走筆數」的建檔路徑（轉移／搬群組等）
    m.create_customer_record("舊客甲", "E200000001", "", "TEST_C_GRP", "直接建檔")
_stale()
_ret = bc("10/13-新客乙 E200000002", gid="TEST_B")
check("新客人建檔有回「已建立客戶」", "已建立客戶" in (_ret or ""), _ret)
check("新客人建檔不會跳「已從舊案帶入」", "舊案帶入" not in (_ret or ""), _ret)
_c = sqlite3.connect(TEST_DB)
_c.execute("INSERT INTO customers (case_id,customer_name,id_no,source_group_id,company,status,phone,created_at,updated_at) "
           "VALUES ('pend0070','待確認丙','E200000003','','', 'PENDING','0912000003',?,?)", (now, now))
_c.commit(); _c.close()
_stale()
_ret = bc("10/13-待確認丙 E200000003", gid="TEST_B")
check("待確認資料接上建檔不會跳「已從舊案帶入」（吳鴻志案）", "舊案帶入" not in (_ret or ""), _ret)
_ret = bc("10/13-舊客己 E200000006", gid="TEST_C_GRP")
check("真的舊客（別群組結案過）還是會跳「已從舊案帶入」", "舊案帶入" in (_ret or ""), _ret)

# ========== 71. 生日打成數字連在一起（2026-10-13 張韶娟：喬美申請書生日空白）＋喬美公司名稱塞不下 ==========
print("\n=== 71. 日期格式＋喬美版面 ===")
for _raw, _want in [("0750312", "075/03/12"), ("750312", "075/03/12"), ("19860312", "075/03/12"),
                    ("075/03/12", "075/03/12"), ("75/3/12", "075/03/12"), ("民國 75 年 3 月 12 日", "075/03/12"),
                    ("0751332", "0751332"), ("1234", "1234"), ("", "")]:
    check(f"日期「{_raw}」→「{_want}」", m._normalize_roc_date(_raw) == _want, m._normalize_roc_date(_raw))
check("生日 0750312 算得出年齡（規則表才不會變「不知道」）", m._calc_age_from_birth("0750312") == m.now_tw().year - 1986)
import fitz as _fitz
def _qm_words(**kw):
    r = {"customer_name": "測試客", "id_no": "A123456789", "birth_date": "0750312",
         "company_phone_area": "mobile", "company_phone_num": "0975702539"}
    r.update(kw)
    b = m._fill_qiaomei_pdf(r)
    return [(w[0], w[1], w[4]) for w in _fitz.open(stream=b, filetype="pdf")[0].get_text("words")]
_w = _qm_words(company_name_detail="大明工程行")
_bd = [t for x, y, t in _w if 70 < y < 95 and 200 < x < 300]
check("喬美：生日 0750312 → 印出 75／3／12", "75" in _bd and "3" in _bd and "12" in _bd, _bd)
check("喬美：短的公司名稱放在原格子（公司電話標籤前）",
      any(t == "大明工程行" and 95 < x < 146 for x, y, t in _w), [w for w in _w if "大明" in w[2]])
# 使用者 2026-10-13 選的位置：塞不下 → 「職業資料」標題右邊（表格上方）、原格子寫「見上方」（不可留空白，會以為不見了）
for _co in ("睿群環衛科技有限公司", "財團法人台灣敦睦聯誼會所屬作業組織圓山大飯店"):
    _w = _qm_words(company_name_detail=_co)
    _in = "".join(t for x, y, t in _w if 95 < x < 146 and 400 < y < 425)
    _top = "".join(t for x, y, t in _w if 110 < x < 300 and 384 < y < 404)
    check(f"喬美：「{_co[:6]}…」塞不下 → 標題右邊寫全名、原格子寫「見上方」",
          "見上方" in _in and _co in _top.replace("公司名稱：", "") and "公司名稱" in _top, (_in, _top))
_w = _qm_words(company_name_detail="睿群環衛科技有限公司")
_ph = [(x, t) for x, y, t in _w if "0975702539" in t]
check("喬美：公司電話不壓到「分機」標籤", _ph and _ph[0][0] > 186, _ph)
# 舊資料一次性統一（init_db 會跑）
bc("10/13-日期丁 E200000010", gid="TEST_B")
_c = sqlite3.connect(TEST_DB)
_c.execute("UPDATE customers SET birth_date='0750312', id_issue_date='1061115' WHERE id_no='E200000010'")
_c.commit(); _c.close()
m.init_db()
_d = get_cust("E200000010")
check("啟動時把舊資料 0750312 統一成 075/03/12", _d["birth_date"] == "075/03/12" and _d["id_issue_date"] == "106/11/15",
      (_d["birth_date"], _d["id_issue_date"]))
_c = sqlite3.connect(TEST_DB)
_lg = [r[0] for r in _c.execute("SELECT message_text FROM case_logs WHERE id_no='E200000010'")]
_c.close()
check("統一日期有寫案件歷程（看得到原本打什麼）", any("0750312" in t and "075/03/12" in t for t in _lg), _lg)

# ========== 72. 業務 AI 連線：密碼改放 header、身分證不放網址（2026-10-13）；舊寫法先照收 ==========
print("\n=== 72. 連線密碼放 header ===")
m.set_setting("vba_secret", m.hash_pw("t72secret"))
bc("10/13-連線甲 V100000072", gid="TEST_B")
_tc72 = TestClient(m.app)
_H = {"X-Loan-Secret": "t72secret"}
_r = _tc72.post("/api/customer-status", json={"id_no": "V100000072"}, headers=_H)
check("新寫法：POST＋header 查得到", _r.status_code == 200 and _r.json().get("exists") is True, _r.text[:200])
_r = _tc72.post("/api/customer-status", json={"id_no": "V100000072"}, headers={"X-Loan-Secret": "錯的".encode().hex()})
check("新寫法：header 密碼錯 → 擋下", _r.status_code == 403)
_r = _tc72.post("/api/customer-status", json={"id_no": "V100000072"})
check("新寫法：沒帶密碼 → 擋下", _r.status_code == 403)
_r = _tc72.get("/api/customer-status", params={"id_no": "V100000072"}, headers=_H)
check("GET＋header 也可以", _r.status_code == 200 and _r.json().get("exists") is True)
_r = _tc72.get("/api/customer-status", params={"id_no": "V100000072", "secret": "t72secret"})
check("舊寫法（密碼放網址）先照收，業務 AI 不會斷線", _r.status_code == 200 and _r.json().get("exists") is True)
_r = _tc72.post("/api/customer-upsert", json={"id_no": "V100000072", "customer_name": "連線甲", "fields": {"phone": "0912000072"}}, headers=_H)
check("寫入窗口：header 密碼可以", _r.json().get("ok") is True, _r.text[:200])
_r = _tc72.post("/api/customer-upsert", json={"id_no": "V100000072", "fields": {}}, headers={"X-Loan-Secret": "wrong"})
check("寫入窗口：header 密碼錯 → 擋下", _r.status_code == 403)
_r = _tc72.post("/api/customer-upsert", json={"secret": "t72secret", "id_no": "V100000072", "fields": {"email": "a@b.cc"}})
check("寫入窗口：舊寫法（密碼放 body）先照收", _r.json().get("ok") is True, _r.text[:200])

# ========== 73. 證件存檔：建檔後去業務 AI 拉證件、群組貼一則收件連結（2026-10-13）==========
print("\n=== 73. 證件存檔 ===")
import shutil as _sh73, io as _io73
from PIL import Image as _Img73
_sh73.rmtree(m._docs_dir(), ignore_errors=True)
os.environ["BUSINESS_AI_URL"] = "https://fake-bizai.test"
os.environ["BUSINESS_AI_SECRET"] = "fakesecret"
m._DOCS_SYNC = True
m._DOCS_POST_TO_GROUP = True   # 正式站試用期是關的；這裡測「打開之後」的行為
def _png(color):
    b = _io73.BytesIO(); _Img73.new("RGB", (60, 40), color).save(b, "PNG"); return b.getvalue()
_FAKE = {"E300000001": [
    {"doc_id": 11, "created_at": "2026-10-13T10:01:00", "content_type": "image/png", "source": "LINE聊天", "label": "身分證反面"},
    {"doc_id": 12, "created_at": "2026-10-13T10:02:00", "content_type": "image/png", "source": "LINE聊天", "label": "勞保明細"},
    {"doc_id": 13, "created_at": "2026-10-13T10:03:00", "content_type": "image/png", "source": "線上申請書", "label": "身分證正面"}]}
_fetch_calls = []
_orig_list, _orig_fetch = m._bizai_list, m._bizai_fetch
m._bizai_list = lambda idv: (_FAKE.get(idv, []), "")
def _fake_fetch(doc_id, idv):
    _fetch_calls.append((str(doc_id), idv))
    return _png((int(doc_id) * 10 % 255, 80, 120)), "image/png", ""
m._bizai_fetch = _fake_fetch

_cards = []
_orig_flex = m.push_flex
def _fake_flex(gid, alt, contents):
    _cards.append((gid, alt, contents)); pushes.append((gid, alt)); return True, ""
m.push_flex = _fake_flex
bc("10/13-證件甲 E300000001", gid="TEST_B")
check("建檔後群組貼一張「📎 收件」卡片（只算 1 則）", len(_cards) == 1 and _cards[0][0] == "TEST_B", _cards)
_cj = json.dumps(_cards[0][2], ensure_ascii=False) if _cards else ""
check("卡片：通知文字有姓名、身分證、張數", _cards and "證件甲" in _cards[0][1] and "E300000001" in _cards[0][1]
      and "收件（3 張）" in _cards[0][1], _cards and _cards[0][1])
check("卡片：有「看證件」按鈕連到相簿頁", '"uri"' in _cj and "/album/E300000001" in _cj and "看證件" in _cj)
check("卡片：還沒判讀完的會寫「AI 判讀中」", "3 張 AI 判讀中" in _cj, _cj[:300])
check("下載時一定帶身分證（業務 AI 會比對，不帶拿不到）", _fetch_calls and all(i == "E300000001" for _d, i in _fetch_calls), _fetch_calls)
_rows = m._docs_rows("E300000001")
check("照片存進自己的磁碟（一位客人一個資料夾）", len(_rows) == 3 and all(os.path.isfile(r["file_path"]) and
      os.sep + "E300000001" + os.sep in r["file_path"] for r in _rows), _rows)
check("照行政存檔順序排：身分證正面 → 反面 → 勞保", [r["label"] for r in _rows] == ["身分證正面", "身分證反面", "勞保明細"],
      [r["label"] for r in _rows])
_fetch_calls.clear()
_res = m.pull_customer_docs("E300000001")
check("再拉一次不會重複下載", _res["new"] == 0 and _res["total"] == 3 and not _fetch_calls, (_res, _fetch_calls))
_FAKE["E300000001"].append({"doc_id": 14, "created_at": "2026-10-14T09:00:00", "content_type": "image/png", "source": "LINE聊天", "label": "行照"})
_res = m.pull_customer_docs("E300000001")
check("客人補傳 → 只下載新的那張", _res["new"] == 1 and _res["total"] == 4, _res)

bc("10/13-證件乙 E300000002", gid="TEST_B")
check("業務 AI 還沒有證件 → 群組不貼（不吵）", not any("📎" in t or "證件" in t for g, t in pushes), pushes)
m._bizai_list = lambda idv: (None, "業務 AI 限流中，等一分鐘再拉")
bc("10/13-證件丙 E300000003", gid="TEST_B")
check("拉失敗 → 群組有提醒（不可以安靜失敗）", any("證件拉取失敗" in t and "限流" in t for g, t in pushes), pushes)
m._bizai_list = lambda idv: (_FAKE.get(idv, []), "")

_old_auth = m.check_auth
m.check_auth = lambda req: "admin"
_tc73 = TestClient(m.app)
_r = _tc73.get("/archive")
check("存檔清單頁：一位客人一列", _r.status_code == 200 and "證件甲" in _r.text and "E300000001" in _r.text, _r.status_code)
_r = _tc73.get("/archive", params={"q": "E300000001"})
check("存檔清單可以用身分證搜尋", "證件甲" in _r.text)
_r = _tc73.get("/archive/E300000001")
_d73 = _tc73.get("/archive/E300000001/data").json()["documents"]
check("客人證件頁（新介面）打得開、資料有 4 張都有縮圖", _r.status_code == 200 and "DocumentGallery" in _r.text and
      len(_d73) == 4 and all(d["previewUrl"].startswith("/archive/file/") for d in _d73), _d73)
import re as _re73
check("新介面：設計稿的示範證件縮圖沒有帶上正式站", not _re73.search(r"base64,[A-Za-z0-9+/]{200,}", _r.text)
      and '"d01"' not in _r.text)
check("證件頁上方直接有客戶資料（開卡片連結就看得到）", "📋 客戶資料" in _r.text and "基本資料" in _r.text and "證件甲" in _r.text)
_sh_css, _sh_html = m._customer_sheet_html(m._docs_latest_customer("E300000001"), ".csheet ")
check("證件頁上方客戶資料跟「客戶資料表」頁同一份（PDF 兩頁的樣子）", _sh_html in _r.text and 'class="csheet"' in _r.text)
check("證件頁拿掉「合約／簽名不會放進去」那句、拿掉合約分頁", "合約／簽名不會放進去" not in _r.text and "['contract','合約']" not in _r.text)
check("新介面：分類下拉是我們的 24 種", "人車合照" in _r.text and "強制險" in _r.text)
_ids = ",".join(d["id"] for d in _d73[:2])
_rz = _tc73.get("/archive/E300000001/zip", params={"ids": _ids})
import zipfile as _zf73
check("只下載勾選的照片（勾 2 張 → 壓縮檔 2 張）", len(_zf73.ZipFile(_io73.BytesIO(_rz.content)).namelist()) == 2
      if _rz.status_code == 200 else False, _rz.status_code)
_pk = m._docs_rows("E300000001")[0]["id"]
_r = _tc73.get(f"/archive/file/{_pk}")
check("單張照片打得開", _r.status_code == 200 and _r.headers.get("content-type", "").startswith("image/"))
_r = _tc73.get("/archive/E300000001/pdf")
import pypdf as _pp73
_sheet_n = len(_pp73.PdfReader(_io73.BytesIO(m._customer_sheet_pdf(m._docs_latest_customer("E300000001")))).pages)
check("客戶資料表產生得出來（存檔 PDF 第一頁）", _sheet_n >= 1, _sheet_n)
check("下載整理好的 PDF：客戶資料表＋4 張證件", _r.status_code == 200 and
      len(_pp73.PdfReader(_io73.BytesIO(_r.content)).pages) == _sheet_n + 4, _r.status_code)
_r = _tc73.get("/archive/E300000001/zip")
import zipfile as _zf73
_names = _zf73.ZipFile(_io73.BytesIO(_r.content)).namelist() if _r.status_code == 200 else []
_d73 = get_cust("E300000001")["created_at"]   # 收件日期＝業務建檔那天（2026-10-14 業務 AI 交接第 8 段），不是照片日期
_d73 = f"{int(_d73[:4]) - 1911}-{_d73[5:7]}-{_d73[8:10]}"
check("下載全部照片：資料夾名跟相簿一樣（建檔日期-姓名-身分證）、照順序編號",
      _names and _names[0].startswith(f"{_d73}-證件甲-E300000001/01 身分證正面"), _names)
_cid73 = get_cust("E300000001")["case_id"]
check("行政B 客戶頁有「📎 看證件」按鈕（總管理）", "/archive/E300000001" in _tc73.get("/adminb", params={"case_id": _cid73}).text)
m.check_auth = lambda req: "group_TEST_B"
check("業務群組帳號看不到證件頁", TestClient(m.app).get("/archive/file/%d" % _pk).status_code == 403)
m.check_auth = lambda req: "adminB"
check("試用期：行政B 帳號還看不到證件（只開總管理）", TestClient(m.app).get("/archive/file/%d" % _pk).status_code == 403)
check("試用期：行政B 客戶頁沒有「看證件」按鈕", "/archive/E300000001" not in TestClient(m.app).get("/adminb", params={"case_id": _cid73}).text)
m._DOCS_POST_TO_GROUP = False
pushes.clear()
bc("10/13-證件丁 E300000001", gid="TEST_C_GRP")
check("試用期：建檔照樣拉照片，但不在群組貼收件訊息", not any("📎" in t for g, t in pushes), pushes)
check("沒上鎖的 API 說明頁已關（/docs、/openapi.json）",
      TestClient(m.app).get("/docs").status_code == 404 and TestClient(m.app).get("/openapi.json").status_code == 404)
_c = sqlite3.connect(TEST_DB)
_c.execute("UPDATE customer_docs SET ai_label=label WHERE id_no='E300000001'")
_c.commit(); _c.close()
_alt, _card = m._docs_flex("E300000001", "證件甲")
# 相簿頁（使用者 2026-10-14 給 LINE 相簿截圖：標題＝收件日期姓名送件順序、N張照片、第一張客戶資料表）
m.check_auth = lambda req: "admin"
_c = sqlite3.connect(TEST_DB)
_c.execute("""UPDATE customers SET route_plan='{"order": ["亞太商品", "和裕商品", "喬美"], "current_index": 0}'
              WHERE id_no='E300000001' AND status!='DELETED'""")
_c.commit(); _c.close()
_ra = TestClient(m.app).get("/album/E300000001")
_n_sheet = len(_pp73.PdfReader(_io73.BytesIO(m._customer_sheet_pdf(m._docs_latest_customer("E300000001")))).pages)
check("相簿頁：標題＝收件日期＋姓名＋送件順序（跟 LINE 相簿一樣）", _ra.status_code == 200 and "證件甲亞太商品/和裕商品/喬美" in _ra.text,
      _ra.status_code)
check("相簿頁：第一張是客戶資料表，接著證件，寫幾張照片",
      _ra.text.index("客戶資料表") < _ra.text.index("身分證正面") and f"{_n_sheet + 4}張照片" in _ra.text)
_rs = TestClient(m.app).get("/archive/E300000001/sheet.jpg?page=1")
check("客戶資料表轉成圖片", _rs.status_code == 200 and _rs.headers["content-type"] == "image/jpeg", _rs.status_code)
check("相簿頁：每張都可以單張下載", _ra.text.count("?dl=1") == 4, _ra.text.count("?dl=1"))
m.check_auth = lambda req: "group_TEST_B"
check("相簿頁：沒權限的帳號打不開", TestClient(m.app).get("/album/E300000001", follow_redirects=False).status_code in (302, 303, 307))
_cj = json.dumps(_card, ensure_ascii=False)
check("判讀過的證件在卡片上打勾列出、身分證正反面合成一行", "check.png" in _cj and "身分證正反面" in _cj
      and "身分證正面" not in _cj and "身分證反面" not in _cj and "勞保明細" in _cj, _cj[:300])
m.check_auth = lambda req: "admin"
_cards.clear()
m.get_notify_group_ids = (lambda _o: (lambda: ["TEST_NOTIFY"]))(m.get_notify_group_ids)
_rp = TestClient(m.app).post("/archive/E300000001/card-preview").json()
check("「收件卡片預覽」傳到系統通知群（業務群收不到）", _rp.get("ok") and _cards and _cards[0][0] == "TEST_NOTIFY"
      and _cards[0][1].startswith("【預覽】"), (_rp, _cards[:1]))
# 補件：業務打「已補」→ 拉新照片 → 群組貼一張小的補件卡片（使用者 2026-10-14）
m._DOCS_POST_TO_GROUP = True
_cards.clear()
bc("@AI 證件甲 已補 勞保", gid="TEST_B")
check("已補但業務 AI 沒有新照片 → 不貼卡片（不吵）", not _cards, _cards)
_FAKE["E300000001"].append({"doc_id": 15, "created_at": "2026-10-20T09:00:00", "content_type": "image/png",
                            "source": "LINE聊天", "label": "強制險"})
bc("@AI 證件甲 已補 強制險", gid="TEST_B")
_cj = json.dumps(_cards[0][2], ensure_ascii=False) if _cards else ""
check("已補＋有新照片 → 群組貼補件卡片（+1 張、補了什麼、看證件）", len(_cards) == 1 and _cards[0][0] == "TEST_B"
      and "補件" in _cards[0][1] and "+1 張" in _cj and "強制險" in _cj and "/album/E300000001" in _cj, _cards)
check("補件卡片是小張的（kilo）", _cards and _cards[0][2]["size"] == "kilo")
m._DOCS_POST_TO_GROUP = False
m.check_auth = lambda req: "admin"
_cards.clear()
_rp = TestClient(m.app).post("/archive/E300000001/supp-preview").json()
check("「補件卡片預覽」傳到系統通知群（小張、最後補的那批）", _rp.get("ok") and _cards and _cards[0][0] == "TEST_NOTIFY"
      and _cards[0][2]["size"] == "kilo" and "強制險" in json.dumps(_cards[0][2], ensure_ascii=False), (_rp, _cards[:1]))
check("證件頁有「補件卡片預覽」按鈕", "補件卡片預覽" in TestClient(m.app).get("/archive/E300000001").text)
# 卡片裝飾圖：LINE 伺服器要抓得到 → 不用登入；但只能拿白名單那幾張
m.check_auth = lambda req: ""
_ra = TestClient(m.app).get("/static/card/header.png")
check("卡片裝飾圖不用登入就打得開（LINE 要抓）", _ra.status_code == 200 and _ra.headers["content-type"] == "image/png", _ra.status_code)
check("裝飾圖網址不能拿來讀別的檔案", TestClient(m.app).get("/static/card/..%2Fmain.py").status_code == 404
      and TestClient(m.app).get("/static/card/secret.png").status_code == 404)
_cjc = json.dumps(m._docs_flex('E300000001', '證件甲')[1], ensure_ascii=False)
check('收件卡片有用到設計稿的圖示（底圖、打勾、眼睛）', all(x in _cjc for x in ('header.png', 'check.png', 'eye.png')))
for _u in _re73.findall(r'https?://[^"]+/static/card/([a-z]+\.png)', _cjc):
    check(f"卡片用到的圖 {_u} 都在白名單", _u in m._CARD_ASSETS)
# 試用期：只有名稱含「測試」的群組會發卡片（使用者 2026-10-14 拿測試B群的客人試）
_c = sqlite3.connect(TEST_DB)
_c.execute("INSERT OR REPLACE INTO groups (group_id, group_name, group_type, is_active, created_at) VALUES (?,?,?,?,?)",
           ("TEST_TB", "測試B群", "SALES_GROUP", 1, now))
_c.commit(); _c.close()
m._DOCS_POST_TO_GROUP = False
check("試用期：測試B群可以發卡片、一般業務群不行", m._docs_can_post("TEST_TB") and not m._docs_can_post("TEST_B"))
_FAKE["E300000020"] = [{"doc_id": 31, "created_at": "2026-10-14T09:00:00", "content_type": "image/png", "source": "LINE聊天", "label": "身分證正面"}]
_cards.clear()
bc("10/14-謝測試 E300000020", gid="TEST_TB")
check("試用期：測試B群建檔 → 自動發收件卡片", len(_cards) == 1 and _cards[0][0] == "TEST_TB", _cards)
m.check_auth = lambda req: "admin"
_cards.clear()
_rs = TestClient(m.app).post("/archive/E300000020/card-send").json()
check("「傳卡片到客人的群組」補發到測試B群", _rs.get("ok") and _cards and _cards[0][0] == "TEST_TB", (_rs, _cards[:1]))
_rs = TestClient(m.app).post("/archive/E300000001/card-send").json()
check("試用期：一般業務群的客人不能補發（擋下並說明）", _rs.get("ok") is False and "測試" in _rs.get("error", ""), _rs)
# 「已補」回覆後面加看證件連結（回覆不算額度）：測試群有、一般群沒有
_c = sqlite3.connect(TEST_DB)
_c.execute("UPDATE customers SET current_company='21商品', pending_docs='勞保' WHERE id_no='E300000020' AND source_group_id='TEST_TB'")
_c.commit(); _c.close()
bc("@AI 謝測試 已補 勞保", gid="TEST_TB")
check("測試群打「已補」→ 回覆後面有「📎 看證件」連結", any("看證件" in t and "/album/E300000020" in t for t in replies), replies)
bc("@AI 證件甲 已補 勞保", gid="TEST_B")
check("一般業務群打「已補」→ 試用期不加連結", replies and not any("看證件" in t for t in replies), replies)
# 之前就拉進來、還沒判讀的照片：建檔發卡片前要先判完，卡片才列得出清單（A100000001 那種）
_c = sqlite3.connect(TEST_DB)
_c.execute("UPDATE customer_docs SET ai_label=NULL, manual_label=NULL, ai_note=NULL WHERE id_no='E300000020'")
_c.commit(); _c.close()
os.environ["ANTHROPIC_API_KEY"] = "fake-key-for-test"
_orig_ai2 = m._ai_doc_request
m._ai_doc_request = lambda block, prompt: '{"type": "身分證正面", "note": "x"}'
_cards.clear()
m.check_auth = lambda req: "admin"
TestClient(m.app).post("/archive/E300000020/card-send")
_cj20 = json.dumps(_cards[0][2], ensure_ascii=False) if _cards else ""
check("舊照片沒判讀過 → 發卡片前先判完，清單列得出來", _cards and "身分證正面" in _cj20 and "判讀中" not in _cj20, _cj20[:200])
m._ai_doc_request = _orig_ai2
os.environ.pop("ANTHROPIC_API_KEY", None)
m.push_flex = _orig_flex
m.check_auth = _old_auth
m._bizai_list, m._bizai_fetch = _orig_list, _orig_fetch
m._DOCS_SYNC = False
os.environ.pop("BUSINESS_AI_URL", None); os.environ.pop("BUSINESS_AI_SECRET", None)

# ========== 74. 證件 AI 判讀＋人工改名稱＋看檔案內容判斷 PDF（2026-10-13 測試：業務 AI 的標籤不可信）==========
print("\n=== 74. 證件 AI 判讀 ===")
from reportlab.pdfgen import canvas as _cv74
def _pdf74(pages):
    b = _io73.BytesIO(); c = _cv74.Canvas(b)
    for i in range(pages):
        c.drawString(100, 700, f"page {i+1}"); c.showPage()
    c.save(); return b.getvalue()
_FAKE["E300000005"] = [
    {"doc_id": 21, "created_at": "2026-10-13T11:00:00", "content_type": "image/png", "source": "LINE聊天", "label": "AI自己判斷"},
    {"doc_id": 24, "created_at": "2026-10-13T11:03:00", "content_type": "application/pdf", "source": "線上申請書", "label": "委任合約"},
    {"doc_id": 25, "created_at": "2026-10-13T11:04:00", "content_type": "image/png", "source": "LINE聊天", "label": "電子簽名"},
    {"doc_id": 22, "created_at": "2026-10-13T11:01:00", "content_type": "image/png", "source": "LINE聊天", "label": "AI自己判斷"},
    {"doc_id": 23, "created_at": "2026-10-13T11:02:00", "content_type": "image/png", "source": "線上申請書", "label": "第二證件"}]
os.environ["BUSINESS_AI_URL"] = "https://fake-bizai.test"; os.environ["BUSINESS_AI_SECRET"] = "fakesecret"
m._bizai_list = lambda idv: (_FAKE.get(idv, []), "")
m._bizai_fetch = lambda doc_id, idv: ((_pdf74(5), "image/png", "") if str(doc_id) == "21"
                                      else (_png((255, 0, 0) if str(doc_id) == "22" else (0, 0, 255)), "image/png", ""))
m._DOCS_SYNC = True
m.pull_customer_docs("E300000005")
_rows = {r["remote_doc_id"]: r for r in m._docs_rows("E300000005")}
check("業務 AI 標成「委任合約」「電子簽名」的不拉（合約存在業務 AI，使用者 2026-10-14）",
      "24" not in _rows and "25" not in _rows, list(_rows))
check("業務 AI 標成圖片的 PDF，看檔案內容認出是 PDF", _rows["21"]["real_type"] == "application/pdf", _rows["21"]["real_type"])
check("存檔時就照真正類型存（.pdf）", _rows["21"]["file_path"].endswith(".pdf"), _rows["21"]["file_path"])
m.check_auth = lambda req: "admin"
_tc74 = TestClient(m.app)
_d21 = [d for d in _tc74.get("/archive/E300000005/data").json()["documents"] if d["id"] == str(_rows["21"]["id"])][0]
# 使用者 2026-10-14「PDF 沒辦法打開來看」→ PDF 轉成圖片給人看（LINE 內建瀏覽器常常不顯示 PDF）
check("證件頁：PDF 轉成圖片看（縮圖第一頁、點開 5 頁全部）", _d21["kind"] == "image" and _d21["pages"] == 5
      and _d21["previewUrl"].endswith("/preview?page=1") and _d21["viewUrl"].endswith("/preview"), _d21)
_pv = _tc74.get(_d21["viewUrl"])
_pv1 = _tc74.get(_d21["previewUrl"])
_im_all = _Img73.open(_io73.BytesIO(_pv.content)) if _pv.status_code == 200 else None
_im_one = _Img73.open(_io73.BytesIO(_pv1.content)) if _pv1.status_code == 200 else None
check("PDF 全部頁面接成一張長圖、第一頁縮圖比較短", _im_all and _im_one and _pv.headers["content-type"] == "image/jpeg"
      and _im_all.height > _im_one.height * 4, (_pv.status_code, _pv1.status_code))
_dl = _tc74.get(f"/archive/file/{_rows['22']['id']}", params={"dl": "1"})
check("點開照片可以單張下載（檔名＝姓名或身分證_證件種類）", _dl.status_code == 200 and "attachment" in _dl.headers.get("content-disposition", "")
      and "E300000005" in _dl.headers.get("content-disposition", "").encode("latin-1").decode("utf-8", "ignore") + _dl.headers.get("content-disposition", ""),
      _dl.headers.get("content-disposition"))
_pdf = _tc74.get("/archive/E300000005/pdf")
check("整理好的 PDF：合約 5 頁完整併進去（5＋2 張照片＝7 頁）",
      len(_pp73.PdfReader(_io73.BytesIO(_pdf.content)).pages) == 7, _pdf.status_code)

# AI 判讀（換成假的 AI 回答）
_answers = {"22": '{"type": "行照", "note": "機車行照"}', "23": '```json\n{"type": "身分證正面", "note": "有大頭照"}\n```',
            "21": '{"type": "委任合約", "note": "合約文件"}'}
_seen_blocks = []
def _fake_ai(block, prompt):
    """照圖片顏色回答（同時判 4 張，順序不固定，所以不能照呼叫順序）：紅→22 行照、藍→23 身分證正面"""
    _seen_blocks.append(block["type"])
    if block["type"] == "document":
        return _answers["21"]
    import base64 as _b64
    px = _Img73.open(_io73.BytesIO(_b64.b64decode(block["source"]["data"]))).convert("RGB").getpixel((5, 5))
    return _answers["22"] if px[0] > px[2] else _answers["23"]
_orig_ai = m._ai_doc_request
m._ai_doc_request = _fake_ai
os.environ["ANTHROPIC_API_KEY"] = "fake-key-for-test"
_r = _tc74.post("/archive/E300000005/classify").json()
check("AI 判讀全部：3 張都判了", _r.get("ok") and _r.get("done") == 3, _r)
check("PDF 用文件方式給 AI 看、照片用圖片方式", "document" in _seen_blocks and "image" in _seen_blocks, _seen_blocks)
_lbl = [r["show_label"] for r in m._docs_rows("E300000005")]
check("判讀後照存檔順序排：身分證正面 → 行照 → 委任合約", _lbl == ["身分證正面", "行照", "委任合約"], _lbl)
check("證件頁標「🤖 AI 判讀」", "AI 判讀" in _tc74.get("/archive/E300000005").text)
# 人工改
_pk22 = [r for r in m._docs_rows("E300000005") if r["remote_doc_id"] == "22"][0]["id"]
check("人工改成清單外的名稱 → 擋下", _tc74.post(f"/archive/doc/{_pk22}/label", json={"label": "亂寫"}).json().get("ok") is False)
check("人工改成「強制險」", _tc74.post(f"/archive/doc/{_pk22}/label", json={"label": "強制險"}).json().get("ok") is True)
_tc74.post("/archive/E300000005/classify")
_pdf2 = _tc74.get("/archive/E300000005/pdf")
check("AI 判讀成委任合約的 → 不放進整理好的 PDF（剩 2 張照片）",
      len(_pp73.PdfReader(_io73.BytesIO(_pdf2.content)).pages) == 2, len(_pp73.PdfReader(_io73.BytesIO(_pdf2.content)).pages))
check("判讀成合約的歸在「合約」分頁", any(d["category"] == "委任合約" for d in _tc74.get("/archive/E300000005/data").json()["documents"]))
_r22 = [r for r in m._docs_rows("E300000005") if r["remote_doc_id"] == "22"][0]
check("AI 重判不會蓋掉人工改的（那張還是強制險）", _r22["show_label"] == "強制險" and _r22["manual_label"] == "強制險",
      (_r22["show_label"], _r22["ai_label"], _r22["manual_label"]))
# AI 亂回
m._ai_doc_request = lambda block, prompt: "我覺得這是一張身分證吧"
_typ, _note = m._ai_classify_doc([r for r in m._docs_rows("E300000005") if r["remote_doc_id"] == "23"][0]["file_path"], "image/png")
check("AI 回的不是清單裡的 → 當「其他」，不亂猜", _typ == "其他", _typ)
m._ai_doc_request = lambda block, prompt: '{"type": "護照", "note": "x"}'
_typ, _note = m._ai_classify_doc([r for r in m._docs_rows("E300000005") if r["remote_doc_id"] == "23"][0]["file_path"], "image/png")
check("AI 回清單外的種類 → 其他", _typ == "其他", _typ)
os.environ.pop("ANTHROPIC_API_KEY", None)
check("正式站沒設 AI 金鑰 → 按判讀會講清楚原因", "ANTHROPIC_API_KEY" in _tc74.post("/archive/E300000005/classify").json().get("error", ""))
_bad = lambda block, prompt: (_ for _ in ()).throw(RuntimeError("AI 回 HTTP 529：overloaded"))
os.environ["ANTHROPIC_API_KEY"] = "fake-key-for-test"
m._ai_doc_request = _bad
_r = _tc74.post("/archive/E300000005/classify").json()
check("判讀失敗會回報有幾張失敗", len(_r.get("errors", [])) == 3, _r)
_c = sqlite3.connect(TEST_DB); _c.execute("UPDATE customer_docs SET ai_label=NULL WHERE id_no='E300000005' AND remote_doc_id='23'"); _c.commit(); _c.close()
_tc74.post("/archive/E300000005/classify")
_d23 = [d for d in _tc74.get("/archive/E300000005/data").json()["documents"] if d["id"] == str(_rows["23"]["id"])][0]
check("判讀失敗的那張，卡片標判讀失敗＋原因（不可以默默空著）", "判讀失敗" in _d23["error"] and "529" in _d23["error"], _d23)
m._ai_doc_request = lambda block, prompt: '{"type": "存摺封面", "note": "x"}'
_r1 = _tc74.post(f"/archive/doc/{_rows['23']['id']}/classify").json()
check("單張重新判讀（介面「重新判讀」）", _r1.get("ok") and _r1["document"]["category"] == "存摺封面" and not _r1["document"]["error"], _r1)
check("人工分類過的那張不重新判讀", _tc74.post(f"/archive/doc/{_pk22}/classify").json().get("ok") is False)
# 使用者 2026-10-14「拉進去照片之後應該是要直接自動判讀」
os.environ["ANTHROPIC_API_KEY"] = "fake-key-for-test"
m._ai_doc_request = lambda block, prompt: '{"type": "健保卡", "note": "x"}'
_FAKE["E300000005"].append({"doc_id": 26, "created_at": "2026-10-14T09:00:00", "content_type": "image/png",
                            "source": "LINE聊天", "label": "AI自己判斷"})
m.pull_customer_docs("E300000005")
_r26 = [r for r in m._docs_rows("E300000005") if r["remote_doc_id"] == "26"][0]
check("新照片拉進來自動判讀（不用按按鈕）", _r26["ai_label"] == "健保卡", (_r26["ai_label"], _r26["ai_note"]))
_r22b = [r for r in m._docs_rows("E300000005") if r["remote_doc_id"] == "22"][0]
check("自動判讀只判新的、人工改過的不動", _r22b["show_label"] == "強制險", _r22b["show_label"])
check("證件頁已經沒有「AI 判讀全部」按鈕", 'id="classifyAll" hidden' in _tc74.get("/archive/E300000005").text)
os.environ.pop("ANTHROPIC_API_KEY", None)
m._DOCS_SYNC = False
m._ai_doc_request = _orig_ai
m.check_auth = _old_auth
m._bizai_list, m._bizai_fetch = _orig_list, _orig_fetch
os.environ.pop("BUSINESS_AI_URL", None); os.environ.pop("BUSINESS_AI_SECRET", None)

# ========== 75. 業務顧兩個群組：登入後「加群組」，所有畫面都看得到（2026-10-14 使用者）==========
print("\n=== 75. 業務顧多個群組 ===")
# 群組 ID 用真的 LINE 格式（只有英數），加群組那支會擋其他字元
_c = sqlite3.connect(TEST_DB)
for _g, _n, _p in (("Cmg75aaa", "多群B群", "pwB75"), ("Cmg75bbb", "多群C群", "pwC75"), ("Cmg75ccc", "多群D群", "pwD75")):
    _c.execute("INSERT OR REPLACE INTO groups (group_id, group_name, group_type, is_active, created_at, password_hash) VALUES (?,?,?,?,?,?)",
               (_g, _n, "SALES_GROUP", 1, now, m.hash_pw(_p)))
_c.commit(); _c.close()
bc("10/14-多群甲 F300000001", gid="Cmg75aaa")
bc("10/14-多群乙 F300000002", gid="Cmg75bbb")
_old_auth75 = m.check_auth
m.check_auth = _REAL_CHECK_AUTH   # 這組要測真的登入
_tok = m._create_session("group", "Cmg75aaa", "dev75")
_tc75 = TestClient(m.app, cookies={"auth_token": _tok})
_r = _tc75.get("/report2")
check("只登入 B 群：日報看不到 C 群的客人", "多群甲" in _r.text and "多群乙" not in _r.text)
_r = _tc75.post("/my-groups/add", data={"group_id": "Cmg75bbb", "password": "錯的"}, follow_redirects=False)
check("加群組：密碼錯 → 擋下", "msg=bad" in _r.headers.get("location", ""), _r.headers.get("location"))
_r = _tc75.post("/my-groups/add", data={"group_id": "Cmg75bbb", "password": "pwC75"}, follow_redirects=False)
check("加群組：密碼對 → 加入", "msg=added" in _r.headers.get("location", ""), _r.headers.get("location"))
check("角色變成兩個群組", m._role_group_ids(m.check_auth(type("R", (), {"cookies": {"auth_token": _tok}})())) == ["Cmg75aaa", "Cmg75bbb"])
_r = _tc75.get("/report2")
check("加群組後：日報同時看到兩個群組的客人", "多群甲" in _r.text and "多群乙" in _r.text)
_csv = _tc75.get("/report/export").content.decode("utf-8-sig")
check("匯出日報：兩個群組都有", "多群甲" in _csv and "多群乙" in _csv)
check("可以改兩個群組的客人", m.can_edit_customer_data("group_Cmg75aaa,Cmg75bbb", "", "Cmg75bbb")
      and not m.can_edit_customer_data("group_Cmg75aaa,Cmg75bbb", "", "Cmg75ccc"))
_cg = {g["group_id"] for g in m._console_allowed_groups("group_Cmg75aaa,Cmg75bbb")}
check("指令台：兩個群組都能操作", {"Cmg75aaa", "Cmg75bbb"} <= _cg, _cg)
check("導覽列有「我的群組（2）」", "我的群組（2）" in _tc75.get("/my-groups").text)
check("日報側欄也有「我的群組（2）」", "我的群組（2）" in _tc75.get("/report2").text)
_tc75.post("/my-groups/remove", data={"group_id": "Cmg75aaa"})
check("主要群組不能移除", m._role_group_ids(m.check_auth(type("R", (), {"cookies": {"auth_token": _tok}})()))[0] == "Cmg75aaa")
_tc75.post("/my-groups/remove", data={"group_id": "Cmg75bbb"})
_r = _tc75.get("/report2")
check("移除 C 群後：日報又只看 B 群", "多群甲" in _r.text and "多群乙" not in _r.text)
_tok2 = m._create_session("group", "Cmg75ccc", "dev75b")
_r = TestClient(m.app, cookies={"auth_token": _tok2}).post("/my-groups/add", data={"group_id": "Cmg75bbb", "password": "pwC75"},
                                                          follow_redirects=False)
check("別的業務帳號登入不影響（各自的群組清單）", m._role_group_ids(m.check_auth(type("R", (), {"cookies": {"auth_token": _tok}})())) == ["Cmg75aaa"])
m.check_auth = _old_auth75

# ========== 76. 融易對帳官方一覽表（114.12.01）＋新證件分類（2026-10-14 使用者）==========
print("\n=== 76. 融易規則＋證件分類 ===")
_ry = {p["company"]: p for p in m.PLAN_ELIGIBILITY_RULES if p["company"].startswith("融易")}
check("融易 4 個方案都寫清楚保人要 55 歲以下、保人也 18~65",
      all(any("55 歲以下的保人" in (r.get("label") or "") for r in p["rules"]) for p in _ry.values()), list(_ry))
check("融易資優專案要附勞保明細（官方附件二）", any("勞保明細" in d for d in _ry["融易機車資優專案"]["required_docs"]))
for _cat in ("信用報告", "貸款證明", "店面／工作照", "所得／財產清單", "車輛照片", "證照", "攤商合約書"):
    check(f"證件分類有「{_cat}」", _cat in m._DOC_CATS)
check("每個證件分類都有對應的分頁（新增分類忘了設會測出來）", all(c in m._DOC_GROUP for c in m._DOC_CATS),
      [c for c in m._DOC_CATS if c not in m._DOC_GROUP])

# ========== 77. 每一家缺什麼（業務 AI 的勞保判讀／時間浮水印）2026-10-14 ==========
print("\n=== 77. 每一家缺什麼 ===")
import io as _io77
from PIL import Image as _Img77
def _png77(c):
    b = _io77.BytesIO(); _Img77.new("RGB", (40, 30), c).save(b, "PNG"); return b.getvalue()
_LB = {"41": "身分證正面", "42": "身分證反面", "43": "健保卡", "44": "存摺封面", "45": "勞保明細", "46": "行照", "47": "人車合照"}
_o_list, _o_fetch, _o_judge = m._bizai_list, m._bizai_fetch, m._bizai_judge
m._bizai_list = lambda idv: ([{"doc_id": int(k), "created_at": "2026-10-14T09:00:00", "content_type": "image/png",
                               "source": "LINE聊天", "label": v} for k, v in _LB.items()], "") if idv == "G300000001" else ([], "")
m._bizai_fetch = lambda d, idv: (_png77((int(d) * 5, 9, 9)), "image/png", "")
_JUDGE = {"labor": {"ok": True, "is_labor": True, "查詢日": "115/10/11", "明細幾天前印的": 3,
                    "目前投保": [{"單位": "大明工程行", "類型": "公司保", "加保日期": "114/09/01", "加保滿幾個月": 13}]},
          "stamp": {"ok": True, "has_stamp": True, "datetime": "2026-10-13 15:20", "address": "台中市西屯區台灣大道三段1號"}}
_judge_calls = []
def _fake_judge(kind, doc_id, idv, **kw):
    _judge_calls.append((kind, str(doc_id), idv))
    return dict(_JUDGE[kind]) if kind in _JUDGE else {"ok": True, "欄位": {}}
m._bizai_judge = _fake_judge
os.environ["BUSINESS_AI_URL"] = "https://fake-bizai.test"; os.environ["BUSINESS_AI_SECRET"] = "x"
bc("10/14-缺件甲 G300000001", gid="TEST_B")
_c = sqlite3.connect(TEST_DB)
_c.execute("""UPDATE customers SET route_plan='{"order": ["21機車25萬", "亞太機車25萬"], "current_index": 0}' WHERE id_no='G300000001'""")
_c.execute("UPDATE customer_docs SET ai_label=label WHERE id_no='G300000001'")
_c.commit(); _c.close()
m.pull_customer_docs("G300000001")
_c = sqlite3.connect(TEST_DB); _c.execute("UPDATE customer_docs SET ai_label=label WHERE id_no='G300000001'"); _c.commit(); _c.close()
m.judge_customer_docs("G300000001")
check("勞保明細拿勞保判讀、人車合照拿浮水印（各一次、帶身分證）",
      sorted(k for k, d, i in _judge_calls if not k.startswith("fields:")) == ["labor", "stamp"] and all(i == "G300000001" for k, d, i in _judge_calls), _judge_calls)
_judge_calls.clear(); m.judge_customer_docs("G300000001")
check("已經拿過的不再拿（業務 AI 判一次就好）", not _judge_calls, _judge_calls)
_rows = m._docs_rows("G300000001")
_p21 = m._plan_for_company("21機車25萬")
_res = {r: (st, n) for r, st, n in m.docs_missing_for_plan(_rows, _p21["required_docs"])}
check("21機車25萬：證件齊、勞保 3 天前、合照有浮水印 → 全部 ✅",
      all(st in ("ok", "skip") for st, n in _res.values()), _res)
_rp = m._plan_for_company("亞太機車25萬")
_res2 = {r: (st, n) for r, st, n in m.docs_missing_for_plan(_rows, _rp["required_docs"])}
check("亞太機車25萬：缺強制險", any(st == "missing" and "強制險" in r for r, (st, n) in _res2.items()), _res2)
# 勞保超過一週
_c = sqlite3.connect(TEST_DB)
_c.execute("""UPDATE customer_docs SET judge_json=? WHERE id_no='G300000001' AND remote_doc_id='45'""",
           (json.dumps({"_kind": "labor", "ok": True, "is_labor": True, "明細幾天前印的": 10, "目前投保": []}),))
_c.execute("""UPDATE customer_docs SET judge_json=? WHERE id_no='G300000001' AND remote_doc_id='47'""",
           (json.dumps({"_kind": "stamp", "ok": True, "has_stamp": False}),))
_c.commit(); _c.close()
_rows = m._docs_rows("G300000001")
_res = {r: (st, n) for r, st, n in m.docs_missing_for_plan(_rows, _p21["required_docs"])}
check("勞保 10 天前印的 → ❌ 要重拉", any(st == "missing" and "重拉" in n for st, n in _res.values()), _res)
check("要時間相機但合照沒浮水印 → ❌ 要重拍", any(st == "missing" and "時間相機" in n for st, n in _res.values()), _res)
_c = sqlite3.connect(TEST_DB)
_c.execute("""UPDATE customer_docs SET judge_json=? WHERE id_no='G300000001' AND remote_doc_id='47'""",
           (json.dumps({"_kind": "stamp", "ok": True, "error": "判讀失敗"}),))
_c.commit(); _c.close()
_res = {r: (st, n) for r, st, n in m.docs_missing_for_plan(m._docs_rows("G300000001"), _p21["required_docs"])}
check("浮水印判讀失敗 → ⏳ 待確認，⛔ 不當成缺（業務 AI：不然業務白跑一趟）",
      any(st == "check" and "浮水印" in n for st, n in _res.values()) and not any("要重拍" in n for st, n in _res.values()), _res)
check("不檢查的不會算缺（例：勞保有就給、身分證查詢截圖是行政自己查的）",
      all(st == "skip" for r, st, n in m.docs_missing_for_plan(_rows, ["勞保（有就給）", "記事本申請表", "汽車繳息（汽車有貸款才要）", "身分證查詢截圖"])))
m.check_auth = lambda req: "admin"
_html = TestClient(m.app).get("/archive/G300000001").text
# 使用者 2026-10-14：只要說明目前送的那家缺什麼（後面可能不會送到），不要拉太長
check("證件頁只寫目前送的那家缺什麼（21機車25萬），不列後面幾家",
      "目前送 <b>21機車25萬</b>" in _html and "亞太機車25萬" not in _html.split("目前送")[1][:300] and "每一家缺什麼" not in _html)
check("缺件一行寫完（勞保要重拉、時間相機）", "要重拉" in _html.split("目前送")[1][:400], _html.split("目前送")[1][:400])
_dd = {d["id"]: d for d in TestClient(m.app).get("/archive/G300000001/data").json()["documents"]}
_lab = [d for d in _dd.values() if d["category"] == "勞保明細"][0]
check("勞保卡片上有判讀摘要（超過一週要重拉）", "重拉" in _lab["judge"], _lab["judge"])
# 刪除（2026-10-14：陳曉琳的真勞保掛在測試客底下）
_del = [r for r in m._docs_rows("G300000001") if r["remote_doc_id"] == "45"][0]
m.check_auth = lambda req: "adminB"
check("非總管理不能刪", TestClient(m.app).post(f"/archive/doc/{_del['id']}/delete").status_code == 403)
m.check_auth = lambda req: "admin"
_rd = TestClient(m.app).post(f"/archive/doc/{_del['id']}/delete").json()
check("總管理刪除：檔案從硬碟刪掉、頁面上不見", _rd.get("ok") and not os.path.exists(_del["file_path"])
      and all(r["id"] != _del["id"] for r in m._docs_rows("G300000001")), _rd)
m.pull_customer_docs("G300000001")
check("刪掉的不會被重拉回來（業務 AI 那邊還在）", all(r["remote_doc_id"] != "45" for r in m._docs_rows("G300000001")))
check("刪掉的照片網址也打不開", TestClient(m.app).get(f"/archive/file/{_del['id']}").status_code == 404)
# 行政上傳（身分證查詢截圖、天書截圖…客人不會傳的；使用者 2026-10-14）
m.check_auth = lambda req: "admin"
_tcu = TestClient(m.app)
_r = _tcu.post("/archive/G300000001/upload", data={"category": "身分證查詢截圖"},
               files=[("files", ("查詢.png", _png77((1, 2, 3)), "image/png")), ("files", ("天書.pdf", _pdf74(2), "application/pdf"))]).json()
check("行政上傳：圖片＋PDF 都收", _r.get("ok") and _r.get("saved") == 2, _r)
_r = _tcu.post("/archive/G300000001/upload", data={"category": "身分證查詢截圖"},
               files=[("files", ("x.txt", b"hello", "text/plain"))]).json()
check("行政上傳：不是圖片或 PDF → 擋下", _r.get("ok") is False and "不是圖片或 PDF" in _r.get("error", ""), _r)
_r = _tcu.post("/archive/G300000001/upload", data={"category": ""}, files=[("files", ("q.png", _png77((1, 2, 3)), "image/png"))]).json()
check("行政上傳：沒選分類 → 擋下", _r.get("ok") is False, _r)
_up = [d for d in _tcu.get("/archive/G300000001/data").json()["documents"] if d["source"] == "admin"]
check("上傳的出現在證件頁、來源寫行政上傳、算人工分類（AI 不蓋）",
      len(_up) == 2 and all(d["category"] == "身分證查詢截圖" and d["isManual"] for d in _up), _up)
_zn = _zf73.ZipFile(_io73.BytesIO(_tcu.get("/archive/G300000001/zip").content)).namelist()
check("上傳的會一起下載", sum("身分證查詢截圖" in n for n in _zn) == 2, _zn)
m.pull_customer_docs("G300000001")
check("重拉業務 AI 不會動到行政上傳的", len([r for r in m._docs_rows("G300000001") if r["source"] == "行政上傳"]) == 2)
check("新分類：申請書／填寫表、天書／車價截圖、法學／罰單／動保截圖",
      all(c in m._DOC_CATS for c in ("申請書／填寫表", "天書／車價截圖", "法學／罰單／動保截圖")))
# 部分工時／重複投保要顯示（2026-10-14 使用者：陳曉琳有部分工時沒判讀到）
_jt = m._doc_judge_text({"show_label": "勞保明細", "judge_json": json.dumps({"_kind": "labor", "ok": True, "is_labor": True,
      "明細幾天前印的": 1, "提示": ["⚠️ 目前勞保投保薪資 11,100，低於基本工資（部分工時），**勞保可以去改**"],
      "目前投保": [{"單位": "悅盛人力", "類型": "公司保", "加保日期": "115/09/29", "加保滿幾個月": 0, "投保薪資": 11100, "備註部分工時": True}]})})
check("勞保卡片寫部分工時（投保薪資）", "部分工時" in _jt and "11,100" in _jt, _jt)
_jd = m._doc_judge_detail({"show_label": "勞保明細", "judge_json": json.dumps({"_kind": "labor", "ok": True, "提示": ["⚠️ 部分工時，**勞保可以去改**"]})})
check("看大圖右邊列業務 AI 的完整提醒（去掉 ** 符號）", "勞保可以去改" in _jd and "**" not in _jd, _jd)
# 更新前就分類好的勞保（沒拿過判讀）→ 打開證件頁要補拿（使用者 2026-10-14：一直寫「勞保日期還沒判讀」）
_c = sqlite3.connect(TEST_DB); _c.execute("UPDATE customer_docs SET judge_json=NULL WHERE id_no='G300000001'"); _c.commit(); _c.close()
m._DOCS_SYNC = True
_judge_calls.clear()
_tcu.get("/archive/G300000001")
check("打開證件頁 → 沒拿過判讀的勞保／合照會去拿（勞保那張前面測刪除時刪掉了，看合照）", any(k == "stamp" for k, d, i in _judge_calls), _judge_calls)
_judge_calls.clear(); _tcu.get("/archive/G300000001")
check("拿過了就不再拿", not _judge_calls, _judge_calls)
m._bizai_judge = lambda kind, d, idv, **kw: {"ok": False, "error": "業務 AI 限流中"}
_c = sqlite3.connect(TEST_DB); _c.execute("UPDATE customer_docs SET judge_json=NULL WHERE id_no='G300000001'"); _c.commit(); _c.close()
_tcu.get("/archive/G300000001")
_lab2 = [d for d in _tcu.get("/archive/G300000001/data").json()["documents"] if d["category"] == "人車合照"]
check("拿不到判讀 → 卡片寫原因（不默默空著）、下次再試", _lab2 and "限流" in _lab2[0]["judge"] and "再試" in _lab2[0]["judge"], _lab2)
m._DOCS_SYNC = False
m._bizai_list, m._bizai_fetch, m._bizai_judge = _o_list, _o_fetch, _o_judge
m.check_auth = _old_auth75
os.environ.pop("BUSINESS_AI_URL", None); os.environ.pop("BUSINESS_AI_SECRET", None)

# ========== 78. 下載照片時不可以鎖住資料庫（2026-10-14 自查）==========
# 原本整段下載包在寫入交易裡 → LINE 的建檔／已補／核准在那段時間全部排隊，超過 30 秒就失敗
print("\n=== 78. 下載照片不鎖資料庫 ===")
_lock_err = []
def _fetch_and_write(doc_id, idv):
    try:
        _cc = sqlite3.connect(TEST_DB, timeout=0.5)
        _cc.execute("UPDATE groups SET group_name=group_name WHERE group_id='TEST_B'"); _cc.commit(); _cc.close()
    except Exception as e:
        _lock_err.append(str(e))
    return _png77((9, 9, 9)), "image/png", ""
_o2l, _o2f = m._bizai_list, m._bizai_fetch
m._bizai_list = lambda idv: ([{"doc_id": 91, "created_at": "2026-10-14T10:00:00", "content_type": "image/png", "source": "LINE聊天", "label": "行照"},
                              {"doc_id": 92, "created_at": "2026-10-14T10:01:00", "content_type": "image/png", "source": "LINE聊天", "label": "行照"}], "")
m._bizai_fetch = _fetch_and_write
os.environ["BUSINESS_AI_URL"] = "https://fake-bizai.test"; os.environ["BUSINESS_AI_SECRET"] = "x"
_rr = m.pull_customer_docs("H300000001")
check("下載照片的同時，其他人（LINE 指令）還寫得進資料庫", _rr["new"] == 2 and not _lock_err, (_rr, _lock_err))
# 業務 AI 那邊刪掉的照片 → 這邊也藏起來（2026-10-14 使用者：放錯別的客人的照片，業務 AI 刪了，這邊提醒還在）
m._bizai_list = lambda idv: ([{"doc_id": 91, "created_at": "2026-10-14T10:00:00", "content_type": "image/png", "source": "LINE聊天", "label": "行照"}], "")
m.pull_customer_docs("H300000001")
_ids78 = [r["remote_doc_id"] for r in m._docs_rows("H300000001")]
check("業務 AI 刪掉的（92）這邊也不見，還在的（91）留著", _ids78 == ["91"], _ids78)
m._bizai_list = lambda idv: ([], "")
m.pull_customer_docs("H300000001")
check("業務 AI 回「一張都沒有」→ 不跟著藏（可能是那邊出狀況）", [r["remote_doc_id"] for r in m._docs_rows("H300000001")] == ["91"])
m._bizai_list = lambda idv: (None, "業務 AI 限流中")
m.pull_customer_docs("H300000001")
check("連不到業務 AI → 不跟著藏", [r["remote_doc_id"] for r in m._docs_rows("H300000001")] == ["91"])
m._bizai_list, m._bizai_fetch = _o2l, _o2f
os.environ.pop("BUSINESS_AI_URL", None); os.environ.pop("BUSINESS_AI_SECRET", None)
import inspect as _insp78
check("會下載／轉 PDF 的網頁不是 async（不卡住整個網站）",
      not any(_insp78.iscoroutinefunction(getattr(m, n)) for n in ("docs_customer_page", "docs_album_page", "docs_pdf", "docs_zip", "docs_file_preview", "docs_data")))

# ========== 79. 業務 AI 第 8 段：身分證／行照／存摺讀到的欄位 → 這邊比是不是本人、幾歲、持有多久（2026-10-14）==========
print("\n=== 79. 證件欄位比對 ===")
bc("10/14-欄位乙 J300000001", gid="TEST_B")
_c = sqlite3.connect(TEST_DB)
_c.execute("UPDATE customers SET birth_date='075/03/12', id_issue_date='105/07/01' WHERE id_no='J300000001'")
_c.commit(); _c.close()
_LB79 = {"81": "身分證正面", "82": "身分證反面", "83": "行照", "84": "存摺封面"}
# 回傳格式照交接文件第 8 段（業務 AI 2026-10-14 正式站的範例）
_F79 = {"身分證正面": {"姓名": "欄位乙", "身分證號": "J300000001", "出生日期": "民國75年3月12日", "出生日期_民國": "075/03/12"},
        "身分證背面": {"發證日期": "民國105年7月1日", "發證日期_民國": "105/07/01", "發證地": "北市", "換補發": "換發", "配偶": ""},
        "行照": {"車主姓名": "王大明", "車牌": "EQD-0097", "發照日期": "110.10.25", "發照日期_民國": "110/10/25",
                 "出廠年月": "2021.10", "廠牌": "睿能", "引擎號碼": ""},
        "存摺封面": {"戶名": "欄 位乙", "銀行": "", "分行": "台中分行", "帳號": "1234"}}
_calls79 = []
def _fj79(kind, d, idv, **kw):
    _calls79.append((kind, str(d)) + (("refresh",) if kw.get("refresh") else ()))
    return {"ok": True, "AI判讀待確認": True, "種類": kind, "欄位": dict(_F79.get(kind.split(":", 1)[-1], {}))}
_o79 = (m._bizai_list, m._bizai_fetch, m._bizai_judge)
m._bizai_list = lambda idv: ([{"doc_id": int(k), "created_at": "2026-10-14T09:00:00", "content_type": "image/png",
                               "source": "LINE聊天", "label": v} for k, v in _LB79.items()], "") if idv == "J300000001" else ([], "")
m._bizai_fetch = lambda d, idv: (_png77((int(d), 7, 7)), "image/png", "")
m._bizai_judge = _fj79
os.environ["BUSINESS_AI_URL"] = "https://fake-bizai.test"; os.environ["BUSINESS_AI_SECRET"] = "x"
m.pull_customer_docs("J300000001")
_c = sqlite3.connect(TEST_DB); _c.execute("UPDATE customer_docs SET ai_label=label WHERE id_no='J300000001'"); _c.commit(); _c.close()
m.judge_customer_docs("J300000001")
check("身分證反面問業務 AI 時用它認得的寫法「身分證背面」，四張都有帶分類",
      sorted(k for k, d in _calls79) == ["fields:存摺封面", "fields:行照", "fields:身分證正面", "fields:身分證背面"], _calls79)
_d79 = {d["category"]: d for d in TestClient(m.app).get("/archive/J300000001/data").json()["documents"]} if (setattr(m, "check_auth", lambda req: "admin") or True) else {}
check("身分證：算出幾歲（2026-10-14 時 40 歲）、生日跟系統一樣不提醒",
      "40 歲" in _d79["身分證正面"]["judge"] and "⚠️" not in _d79["身分證正面"]["judge"], _d79["身分證正面"]["judge"])
check("行照：車主不是本人 → 提醒", "車主不是本人（車主：王大明）" in _d79["行照"]["judge"], _d79["行照"]["judge"])
check("行照：持有幾個月（110/10/25 發照 → 59 個月）、車齡用西元出廠年算（⛔ 不可以當民國）",
      "持有 59 個月" in _d79["行照"]["judge"] and "車齡 5 年" in _d79["行照"]["judge"], _d79["行照"]["judge"])
check("存摺：戶名中間有空白也算本人，不提醒", "⚠️" not in _d79["存摺封面"]["judge"] and "末4碼 1234" in _d79["存摺封面"]["judge"],
      _d79["存摺封面"]["judge"])
check("身分證反面：發證日期跟系統一樣不提醒", "⚠️" not in _d79["身分證反面"]["judge"] and "105/07/01" in _d79["身分證反面"]["judge"],
      _d79["身分證反面"]["judge"])
check("看大圖右邊：列出讀到的原樣欄位（空的不列、不重複列 _民國）",
      "發照日期：110.10.25" in _d79["行照"]["judgeDetail"] and "_民國" not in _d79["行照"]["judgeDetail"]
      and "引擎號碼" not in _d79["行照"]["judgeDetail"], _d79["行照"]["judgeDetail"])
_h79 = TestClient(m.app).get("/archive/J300000001").text
_del79 = _h79.split("deleteOne:")[1].split("saveCategory:")[0] if "deleteOne:" in _h79 else ""
check("刪照片後整頁重新整理（上方提醒是伺服器排的；使用者 2026-10-14：刪了字沒改掉）", "location.reload()" in _del79, _del79[:200])
check("證件頁上方彙總：行照車主不是本人", "要跟客戶確認" in _h79 and "行照：車主不是本人" in _h79)
# 持有時間看換補照日期（使用者 2026-10-14）
_ix, _wx = m._doc_fields_check({"欄位": {"發照日期_民國": "110/10/25", "換補照日期_民國": "114/10/01"}}, "行照", {"customer_name": "欄位乙"})
check("行照有換補照日期 → 持有時間從換補照算（114/10/01 → 12 個月）",
      any("持有 12 個月（換補照 114/10/01）" in x for x in _ix), _ix)
# 持有未滿目前送的那家要求 → 提醒問客戶（使用者 2026-10-14：亞太要持有 1 年；只是遺失補發就沒問題）
_cu79 = {"customer_name": "欄位乙", "current_company": "亞太機車25萬"}
_ix, _wx = m._doc_fields_check({"種類": "機車行照", "欄位": {"發照日期_民國": "110/10/25", "換補照日期_民國": "115/08/01"}}, "行照", _cu79)
check("亞太機車25萬、換補照 2 個月 → ⚠️ 請客戶補證明換補照原因",
      any("亞太機車25萬 要滿 1 年" in w and "請客戶補證明換補照原因" in w for w in _wx), _wx)
_ix, _wx = m._doc_fields_check({"種類": "機車行照", "欄位": {"發照日期_民國": "115/08/01"}}, "行照", _cu79)
check("沒換補照、發照很近 → ⚠️ 請客戶補證明（剛買／剛過戶）", any("請客戶補證明（剛買／剛過戶）" in w for w in _wx), _wx)
_ix, _wx = m._doc_fields_check({"種類": "機車行照", "欄位": {"發照日期_民國": "110/10/25", "換補照日期_民國": "114/08/01"}}, "行照", _cu79)
check("持有滿 1 年 → 不提醒", not [w for w in _wx if "重拍" not in w], _wx)
_ix, _wx = m._doc_fields_check({"種類": "汽車行照", "欄位": {"發照日期_民國": "115/08/01"}}, "行照", _cu79)
check("亞太機車要的是機車持有 → 汽車行照不套用", not [w for w in _wx if "重拍" not in w], _wx)
_ix, _wx = m._doc_fields_check({"種類": "機車行照", "欄位": {"發照日期_民國": "115/08/01"}}, "行照",
                                {"customer_name": "欄位乙", "current_company": "願望小車25萬"})
check("願望（持有時間不限）→ 不提醒", not [w for w in _wx if "重拍" not in w], _wx)
_ix, _wx = m._doc_fields_check({"種類": "汽車行照", "欄位": {"發照日期_民國": "115/08/01"}}, "行照",
                                {"customer_name": "欄位乙", "current_company": "21汽車A"})
check("21汽車：要滿半年，新車一手／親屬過戶可送也寫出來", any("滿半年（新車一手車主、親屬過戶可送）" in w for w in _wx), _wx)
# 生日打錯
_c = sqlite3.connect(TEST_DB); _c.execute("UPDATE customers SET birth_date='0750313' WHERE id_no='J300000001'"); _c.commit(); _c.close()
_d79 = {d["category"]: d for d in TestClient(m.app).get("/archive/J300000001/data").json()["documents"]}
check("系統生日跟身分證不一樣 → 提醒（系統的寫法 0750313 也認得）",
      "生日跟系統不一樣" in _d79["身分證正面"]["judge"], _d79["身分證正面"]["judge"])
# 行政把行照改成身分證正面 → 要用新分類重拿（⛔ 不可以拿行照的答案當身分證）
_c = sqlite3.connect(TEST_DB); _c.execute("UPDATE customer_docs SET manual_label='身分證正面' WHERE id_no='J300000001' AND remote_doc_id='83'"); _c.commit(); _c.close()
_calls79.clear(); m.judge_customer_docs("J300000001")
check("改分類 → 用新分類重拿，其他張不重拿", _calls79 == [("fields:身分證正面", "83")], _calls79)
# 業務 AI 讀的欄位改版（發證三欄改從正面讀）→ 舊答案要自動重拿一次，而且叫業務 AI 重讀（它也存了舊的）
_c = sqlite3.connect(TEST_DB)
_c.execute("""UPDATE customer_docs SET judge_json=? WHERE id_no='J300000001' AND remote_doc_id='81'""",
           (json.dumps({"_kind": "fields:身分證正面", "ok": True, "欄位": {"姓名": "欄位乙"}}, ensure_ascii=False),))
_c.commit(); _c.close()
_calls79.clear(); m.judge_customer_docs("J300000001")
check("舊版欄位答案 → 自動重拿、帶 refresh；新版的不重拿", _calls79 == [("fields:身分證正面", "81", "refresh")], _calls79)
_calls79.clear(); m.judge_customer_docs("J300000001")
check("重拿過就不再拿", not _calls79, _calls79)
# 窗口網址
_got = {}
class _R79:
    status_code = 200
    def json(self): return {"ok": True, "欄位": {}}
_oget = m.req_lib.get
m.req_lib.get = lambda url, params=None, headers=None, timeout=None: (_got.update(url=url, params=params, headers=headers) or _R79())
_o79j = m._bizai_judge; m._bizai_judge = _o79[2]
m._bizai_judge("fields:身分證背面", 82, "J300000001")
m.req_lib.get = _oget
check("打業務 AI：/fields?kind=身分證背面、身分證放參數、密碼放標頭",
      _got["url"].endswith("/api/loan/docs/82/fields") and _got["params"] == {"id_no": "J300000001", "kind": "身分證背面"}
      and _got["headers"] == {"X-Loan-Secret": "x"}, _got)
# 行政B「從行照帶入」（使用者 2026-10-14：補充資料的顏色、車身號碼要自動填；選按鈕帶入）
_c = sqlite3.connect(TEST_DB); _c.execute("UPDATE customer_docs SET manual_label=NULL WHERE id_no='J300000001' AND remote_doc_id='83'"); _c.commit(); _c.close()
m._bizai_judge = _fj79; m.judge_customer_docs("J300000001")   # 前面改成身分證正面了，改回行照重拿
_cid79 = sqlite3.connect(TEST_DB).execute("SELECT case_id FROM customers WHERE id_no='J300000001'").fetchone()[0]
m.check_auth = lambda req: "admin"
_ab = TestClient(m.app).get(f"/adminb?case_id={_cid79}").text
check("行政B有「從行照帶入」，帶的是行照讀到的車牌、出廠年月改成 2021/10 的寫法",
      "從行照帶入" in _ab and '"at_plate": "EQD-0097"' in _ab and '"at_mfg": "2021/10"' in _ab and '"at_brand": "睿能"' in _ab, _ab[:0])
m.check_auth = lambda req: "adminB"
check("證件存檔還沒開放的角色看不到這顆", "從行照帶入" not in TestClient(m.app).get(f"/adminb?case_id={_cid79}").text)
# 勞保 × 客戶填的年資（使用者 2026-10-14 拿一份真明細：前一家跟現在這家名字很像、剛好接著，客戶填 3 年；另外同時保兩個）
#   ⛔ 用假公司名，數字照那份明細的形狀：前一家 112/09 加保、115/04/30 退保；現在這家 115/05/01 加保（5 個月）；另一家部分工時
_lj = {"_kind": "labor", "ok": True, "is_labor": True, "明細幾天前印的": 1,
       "目前投保": [{"單位": "大米食品有限公司", "類型": "公司保", "加保日期": "115/05/01", "加保滿幾個月": 5},
                  {"單位": "小米小吃店", "類型": "公司保", "加保日期": "115/09/17", "加保滿幾個月": 0, "投保薪資": 11100, "備註部分工時": True}],
       "上一份單位": "大米食品行", "上一份退保日期": "115/04/30", "接續": True}
_w = m._doc_labor_check(_lj, {"company_name": "大米食品", "company_years": "3", "company_months": ""})
check("同時保兩個 → 問是不是同一間公司／同一個負責人", any("同時保 2 個" in w and "同一個負責人" in w for w in _w), _w)
check("客戶填 3 年、勞保這家只保 5 個月 → 問是不是同一個老闆，並寫出上一份跟這份接著",
      any("客戶填年資 3 年" in w and "大米食品有限公司 只保 5 個月" in w and "大米食品行" in w and "接著" in w and "同一個老闆" in w and "年資只算 5 個月" in w for w in _w), _w)
_w = m._doc_labor_check({**_lj, "目前投保": _lj["目前投保"][:1]}, {"company_name": "大米食品", "company_years": "0", "company_months": "6"})
check("年資跟勞保差 3 個月內 → 不提醒", not _w, _w)
_jt = m._doc_judge_text({"show_label": "勞保明細", "judge_json": json.dumps(_lj)}, {"company_name": "大米食品", "company_years": "3"})
check("勞保卡片上也寫要問的事", "同一個老闆" in _jt and "同一個負責人" in _jt, _jt)
m._bizai_list, m._bizai_fetch, m._bizai_judge = _o79
m.check_auth = _old_auth75
os.environ.pop("BUSINESS_AI_URL", None); os.environ.pop("BUSINESS_AI_SECRET", None)

# ========== 80. 線上申請書送來 → 背景去業務 AI 拉照片（2026-10-14 真客戶：「證件那邊沒有照片」）==========
print("\n=== 80. 申請書送來就拉照片 ===")
m.set_setting("vba_secret", m.hash_pw("t80secret"))
_started = []
class _FakeThread80:
    def __init__(self, target=None, args=(), daemon=None): self.t = target
    def start(self): _started.append(self.t)
_oT80, _oP80 = m._threading.Thread, m.pull_customer_docs
m._threading.Thread = _FakeThread80
os.environ["BUSINESS_AI_URL"] = "https://fake-bizai.test"; os.environ["BUSINESS_AI_SECRET"] = "x"
_r80 = TestClient(m.app).post("/api/customer-upsert", headers={"X-Loan-Secret": "t80secret"},
                              json={"id_no": "K300000001", "customer_name": "申請乙", "fields": {"phone": "0912345678"}}).json()
_pulled = []
m.pull_customer_docs = lambda idv: _pulled.append(idv)
m._threading.Thread = _oT80
import time as _t80; _o_sleep = _t80.sleep; _t80.sleep = lambda x: None
for _f in _started: _f()
_t80.sleep = _o_sleep
check("線上申請書建成待確認 → 背景去拉這位客人的照片", _r80.get("action") == "created" and _pulled == ["K300000001"], (_r80, _pulled))
_started.clear(); _pulled.clear(); m._threading.Thread = _FakeThread80
_r80 = TestClient(m.app).post("/api/customer-upsert", headers={"X-Loan-Secret": "t80secret"},
                              json={"id_no": "K300000001", "customer_name": "申請乙", "fields": {"email": "a@b.com"}}).json()
m._threading.Thread = _oT80
check("再送一次（更新）也會去拉（客人後來補傳的）", _r80.get("action") == "updated" and len(_started) == 1, _r80)
m.pull_customer_docs = _oP80
# 客戶資料表：沒填的部分不印（2026-10-14 真客戶客戶資料表印成「5年月」「年3月」「天」）
_c = sqlite3.connect(TEST_DB); _c.execute("UPDATE customers SET created_at='2026-10-10T15:00:00' WHERE id_no='J300000001'"); _c.commit(); _c.close()
_fn80, _ = m._docs_folder_name("J300000001", m._docs_rows("J300000001"))
check("相簿名稱日期用建檔那天（115-10-10），不是照片日期（10-14）", _fn80.startswith("115-10-10-"), _fn80)
_ix, _wx = m._doc_fields_check({"欄位": {"發照日期_民國": "110/10/25", "換補照日期_民國": "110/10/25"}}, "行照", {})
check("發照＝換補照是正常的 → 不提醒（業務 AI 10 張有 5 張沒換過照）", not any("同一天" in x for x in _ix + _wx), (_ix, _wx))
_ix, _wx = m._doc_fields_check({"欄位": {"車牌": "", "車主姓名": "欄位乙", "發照日期_民國": "110/10/25", "廠牌": "三陽"}}, "行照", {"customer_name": "欄位乙"})
check("行照關鍵欄（車牌）讀不到 → 請客戶重拍", any("讀不到車牌" in w and "重拍" in w for w in _wx), _wx)
_ix, _wx = m._doc_fields_check({"欄位": {"姓名": "欄位乙", "身分證號": "J300000001", "出生日期_民國": "075/03/12",
                                 "發證日期_民國": "111/06/10", "發證地": "竹縣", "換補發": "換發"}}, "身分證正面", {"customer_name": "欄位乙", "birth_date": "075/03/12"})
check("發證資料印在身分證正面 → 正面讀到也顯示（系統沒填就提示可以補）",
      any("發證 111/06/10 竹縣換發" in x for x in _ix) and any("可以照這個補" in x for x in _ix), _ix)
_ix, _wx = m._doc_fields_check({"欄位": {"戶名": "", "銀行": "", "分行": "台中", "帳號": "1234"}}, "存摺封面", {"customer_name": "欄位乙"})
check("存摺讀不到戶名 → 請客戶重拍；銀行讀不到不算（有的封面只有 logo）", any("讀不到戶名" in w and "銀行" not in w for w in _wx), _wx)
_mi = m._order_missing_info({"id_no": "J300000001", "eval_labor_ins": "公司保", "company_salary": "3", "company_years": "2",
                             "eval_salary_transfer": "有薪轉", "eval_alert_warning": "否", "eval_alert": "無", "eval_late": "無",
                             "birth_date": "075/03/12", "eval_property": "無", "debt_list": "[]", "unloan_vehicles": "[]"})
check("客戶有傳行照、資料表卻沒有車 → 不排，寫「客戶有傳行照，資料表沒填車」", any("有傳行照" in x for x in _mi), _mi)
check("年資／居住時間：只填年、只填月、都填",
      (m._ym_txt("5", ""), m._ym_txt("", "3"), m._ym_txt("2", "6"), m._ym_txt("0", "3"), m._ym_txt("", "")) == ("5年", "3個月", "2年6個月", "3個月", ""))
_c = sqlite3.connect(TEST_DB)
_c.execute("UPDATE customers SET live_years='5', live_months='', company_years='', company_months='3', eval_late='', eval_late_days='' WHERE id_no='K300000001'")
_c.commit(); _cid80 = _c.execute("SELECT case_id FROM customers WHERE id_no='K300000001'").fetchone()[0]; _c.close()
m.check_auth = lambda req: "admin"
_hp = TestClient(m.app).get(f"/customer-pdf?case_id={_cid80}").text
check("客戶資料表不會印出「5年月」「年3月」「 天」", "5年月" not in _hp and "年3月" not in _hp and "<td> 天</td>" not in _hp
      and "5年" in _hp and "3個月" in _hp)
m.check_auth = _old_auth75
os.environ.pop("BUSINESS_AI_URL", None); os.environ.pop("BUSINESS_AI_SECRET", None)

# ========== 81. 業務 AI 寫入負債明細／無貸款車輛（2026-10-14 開放，交接第 10 段）==========
print("\n=== 81. 負債明細寫入 ===")
m.set_setting("vba_secret", m.hash_pw("t81secret"))
_body81 = {"id_no": "L300000001", "customer_name": "負債丙", "fields": {
    "debt_list": [{"co": "和潤", "kind": "車貸", "vehicle": "機車", "lo": "70000", "pe": "24", "pa": "19", "mo": "2808", "sp": "有"},
                  {"co": "裕融", "kind": "信貸", "lo": "7", "pe": "36", "pa": "10", "mo": "3000"}],
    "unloan_vehicles": [{"type": "汽車", "year": "2018"}, {"type": "腳踏車"}]}}
_r81 = TestClient(m.app).post("/api/customer-upsert", headers={"X-Loan-Secret": "t81secret"}, json=_body81).json()
_c81 = get_cust("L300000001")
_d81 = json.loads(_c81["debt_list"] or "[]")
check("負債明細寫進去、格式跟行政表單一樣（和潤 → 和潤機車；剩餘自己算）",
      _r81.get("ok") and _d81[0]["co"] == "和潤機車" and _d81[0]["re"] == "$14,040" and _d81[0]["sp"] == "有", (_r81, _d81))
check("車貸動保 AI 問不到 → 留空（⛔ 不可以預設「無」）", _d81[0]["dy"] == "", _d81[0])
check("金額 7（萬）不猜 → 留空；信貸那筆的動保／空間跟表單一樣寫「-」", _d81[1]["lo"] == "" and _d81[1]["dy"] == "-" and _d81[1]["sp"] == "-", _d81[1])
check("無貸款車輛只收機車／汽車", json.loads(_c81["unloan_vehicles"]) == [{"type": "汽車", "plate": "", "year": "2018"}], _c81["unloan_vehicles"])
_mi81 = m._order_missing_info(_c81)
check("業務 AI 帶進來的車貸沒有動保／公路 → 不排順序，寫行政要查", any("和潤機車" in x and "動保" in x for x in _mi81), _mi81)
# 行政已經有填 → 不動
_c = sqlite3.connect(TEST_DB); _c.execute("""UPDATE customers SET debt_list='[{"co": "中租機車", "dy": "動保"}]' WHERE id_no='L300000001'"""); _c.commit(); _c.close()
_r81 = TestClient(m.app).post("/api/customer-upsert", headers={"X-Loan-Secret": "t81secret"}, json=_body81).json()
check("行政的負債表已經有填 → 不覆蓋、不跳提醒", json.loads(get_cust("L300000001")["debt_list"])[0]["co"] == "中租機車"
      and not any(c["field"] == "debt_list" for c in _r81.get("conflicts", [])), _r81)

# 已經帶進來的那筆沒車牌 → 之後業務 AI 讀到車牌再送，要補得進去（2026-10-14 陳秋蘭：車牌／出廠沒帶入）
_c = sqlite3.connect(TEST_DB)
_c.execute("""UPDATE customers SET debt_list=?, unloan_vehicles=? WHERE id_no='L300000001'""",
           (json.dumps([{"co": "和潤機車", "lo": "70000", "dy": "", "sp": "有", "pl": "", "yr": "", "src": "業務AI"},
                        {"co": "裕融", "dy": "-", "sp": "-", "pl": "", "yr": ""}], ensure_ascii=False),
            json.dumps([{"type": "汽車", "plate": "", "year": ""}], ensure_ascii=False)))
_c.commit(); _c.close()
_body81b = {"id_no": "L300000001", "fields": {
    "debt_list": [{"co": "和潤", "kind": "車貸", "vehicle": "機車", "lo": "90000", "pl": "NTV-9226", "yr": "2023.10", "sp": "無"},
                  {"co": "裕融", "kind": "信貸", "lo": "50000"}],
    "unloan_vehicles": [{"type": "汽車", "plate": "6555-TW", "year": "2008.02"}]}}
TestClient(m.app).post("/api/customer-upsert", headers={"X-Loan-Secret": "t81secret"}, json=_body81b)
_c81 = get_cust("L300000001"); _d81 = json.loads(_c81["debt_list"])
check("後送來的車牌／出廠補進那筆車貸的空格", _d81[0]["pl"] == "NTV-9226" and _d81[0]["yr"] == "2023.10", _d81[0])
check("有值的格子不動（金額、空間不被蓋掉）", _d81[0]["lo"] == "70000" and _d81[0]["sp"] == "有", _d81[0])
check("信貸那列的「-」不被補", _d81[1]["dy"] == "-" and _d81[1]["sp"] == "-" and len(_d81) == 2, _d81)
check("無貸款車輛的車牌／出廠也補得進去", json.loads(_c81["unloan_vehicles"]) == [{"type": "汽車", "plate": "6555-TW", "year": "2008.02"}],
      _c81["unloan_vehicles"])
# 兩筆同名車貸 → 分不出配哪筆 → 不補
check("同名兩筆 → 不補", m._apply_merge_rows("debt_list", json.dumps([{"co": "和潤機車"}, {"co": "和潤機車"}]),
                                         json.dumps([{"co": "和潤機車", "pl": "A-1"}])) == "")

# 「無可知情」＝保密（使用者 2026-10-14）—— 原本字裡有「知情」被當成知情
check("聯絡人「無可知情」不算知情", not m._customer_has_known_contact({"contact1_known": "無可知情", "contact2_known": "保密"}))
check("聯絡人「可知情」算知情", m._customer_has_known_contact({"contact1_known": "可知情"}))

# ========== 82. 證件存檔：群組篩選＋送件順序跟群組一樣的寫法（2026-10-14 使用者）==========
print("\n=== 82. 存檔頁群組篩選／送件順序 ===")
_line82 = "10/14-證件甲-和裕/喬美/21/貸10/分貝商/C/商/南亞/當"
_rp82 = m.make_route_json(m.parse_route_order_line(_line82)["companies"])
_c = sqlite3.connect(TEST_DB)
_cid82 = _c.execute("SELECT case_id FROM customers WHERE id_no='E300000001' AND status!='DELETED' ORDER BY created_at DESC").fetchone()[0]
_c.execute("UPDATE customers SET route_plan=? WHERE case_id=?", (_rp82, _cid82))
_c.execute("INSERT INTO case_logs (case_id,customer_name,id_no,company,message_text,from_group_id,created_at) VALUES (?,?,?,?,?,?,?)",
           (_cid82, "證件甲", "E300000001", "", _line82 + "\n@AI", "TEST_B", datetime.now().isoformat()))
_c.commit(); _c.close()
m.check_auth = lambda req: "admin"
_tc82 = TestClient(m.app)
_r = _tc82.get("/archive")
check("存檔清單：送件順序用業務在群組打的原文（簡稱照業務寫的）", _line82 in _r.text, _r.text[-1500:])
_r = _tc82.get("/archive/E300000001")
check("客人證件頁：最上面一行送件順序，缺件那行還在", "📋 送件順序：" in _r.text and _line82 in _r.text and "目前送" in _r.text)
check("群組篩選：選 B群 看得到", "證件甲" in _tc82.get("/archive", params={"g": "TEST_B"}).text)
check("群組篩選：選「還沒有群組」看不到 B群 的人", "證件甲" not in _tc82.get("/archive", params={"g": "_none"}).text)
# 順序改過 → 原文是舊的，不可以再用
_c = sqlite3.connect(TEST_DB)
_c.execute("UPDATE customers SET route_plan=? WHERE case_id=?", (m.make_route_json(["喬美", "和裕"]), _cid82)); _c.commit(); _c.close()
_ro82 = m._route_order_text(get_cust("E300000001"))
check("順序改過 → 改用現在的順序組一行（不顯示舊的原文）", _ro82.endswith("-證件甲-喬美/和裕"), _ro82)
m.check_auth = _old_auth

# ========== 總結 ==========
print(f"\n{'='*50}")
print(f"結果：{PASS} 通過、{FAIL} 失敗")
print(f"{'='*50}")
sys.exit(0 if FAIL == 0 else 1)
