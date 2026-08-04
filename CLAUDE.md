# CLAUDE.md — 貸款案件管理系統

> 這份是**每次開工必讀**的薄手冊。細節拆到 `docs/`，需要時才翻。
> 內容一個字都沒刪，只是重新歸位（2026-08-05 拆分，原檔 1494 行）。

## 這是什麼

LINE Bot + Web 管理介面，追蹤貸款客戶從進件、送件、核准到撥款。
FastAPI + SQLite，部署在 Render。

**主檔：`main.py`（25630 行／2026-08-02 實測）。**
⚠️ 這個數字會過時，**不要照抄，用 `wc -l main.py` 實際數**。曾經寫成「約 8500 行」
放了好幾個月，實際已是三倍大 —— 照著它判斷「檔案不大、整份讀進來」就會被截斷、
在資訊不全的情況下改程式。
`main (2).py` 是舊版備份（6682 行），不會被部署，改錯檔案等於沒改。

---

## 📁 要動什麼，先讀哪一份

| 我要做的事 | **動手前先讀** |
|---|---|
| 改申請書 Excel／欄位對應／下拉選單 | `docs/excel-範本規則.md` ⛔ 不要憑印象改，破壞下拉選單行政就做不出申請書 |
| 找某個功能的程式在哪一段 | `docs/模組地圖.md`（行號會位移，用 grep 找函式名比較準） |
| 動規則表 / 判斷可送哪幾家 / 車貸名詞 | `docs/業務規則.md` |
| 查「這個行為為什麼長這樣」的根因 | `docs/開發紀錄.md`（依時間，平常不用讀） |
| 修 bug | **本檔下面的「6 個必對清單」和「LINE vs 網頁」兩節，一定要先看完** |

---

## 🔒 已定案的決策 —— 不要重新調查、不要再提

這些討論過、拍板了。**再提一次等於浪費使用者的時間**（2026-08-05 就犯過：
把「Google Drive 備份沒開」列成最高優先待辦，而那是明確決定不做的）。

| 議題 | 結論 | 完整理由 |
|---|---|---|
| **資料庫備份** | 只做「每週 LINE 提醒 + 手動下載」。**Google Drive／Email／本機定時抓都明確不做** —— Render 磁碟本來就有每日快照保留 7 天，剩下的洞一週抓一次就夠；B/C 都是把含個資的資料庫往外送，多一個要保護的地方 | `docs/開發紀錄.md` → 2026/08/02 |
| **跨群轉移已撥款的案子** | LINE 一律擋（分不出誰是管理員），網頁管理員仍可處理 | 本檔「LINE vs 網頁」節 |
| **指令打冒號會失效**（`改順序：` 等 6 個） | 使用者決定不修，業務會自己改成空格 | 2026-08-03 |

⚠️ 要推翻其中任何一條，先講清楚「情況變了什麼」，不要當成新發現重講一遍。

---

## ⛔ 三條最貴的教訓（2026-08 累積）

1. **不要只讀程式碼推論，要實際跑一次。** 2026-08-03 為了一個 bug 推論兩次都錯
   （「21汽 和 21機車25萬 被當成同一家」、「婉拒沒防護會多推一格」），
   實測才發現兩個都不成立，白繞一大圈。
2. **查案件資料異常，第一件事看 `case_logs.from_group_id`。**
   `WEB` / `WEB_ADMIN` = 網頁改的，別去查 LINE；`message_text` 含 `

` 也是網頁貼上的。
3. **測試寫完要還原程式驗一次會不會紅。** 不驗就可能是假通過
   （TestClient 下 session cookie 不生效，POST 被導去 /login，要
   `m.check_auth = lambda req: "admin"` 繞過）。

---

## 工作規則

- **Agent 自動分配：** Claude 在執行任務時可自由啟動 Agent（Explore、Plan、general-purpose 等）來並行處理子任務，不需要逐一手動批准。可根據任務複雜度自行決定 Agent 數量與類型。
- **修改 Excel 範本邏輯後務必本地測試後才能交付。**
- **絕對保留所有申請書範本的格式、公式、下拉選單**，不可破壞。
- **欄位無資料時清空（不留範本示範值）**，但**不能覆蓋標籤儲存格**。

---

## 專案概覽

這是一個 **貸款案件管理系統**，整合 LINE Bot + Web 管理介面，用於追蹤貸款客戶從進件、送件、核准到撥款的完整流程。系統以 FastAPI 為後端，SQLite 為資料庫，部署在 Render 平台上。

**主要檔案：** `main.py`（Render 部署檔，Procfile 指向 `uvicorn main:app`，單檔架構，**25630 行**／2026-08-02 實測）。
⚠️ 這個數字會過時，**不要照抄，要用 `wc -l main.py` 實際數**。曾經寫成「約 8500 行」放了好幾個月，實際已經是三倍大 —— 照著它判斷「檔案不大、整份讀進來」就會被截斷、在資訊不全的情況下改程式。
注意：`main (2).py` 是舊版備份（6682 行），不會被部署，修改請對準 `main.py`。

---

## 環境變數

| 變數 | 用途 | 預設值 |
|------|------|--------|
| `CHANNEL_ACCESS_TOKEN` | LINE Bot Token | （必填） |
| `A_GROUP_ID` | A群 LINE Group ID | `Cb3579e...` |
| `DB_PATH` | SQLite 路徑 | `/var/data/loan_system.db` |
| `ANTHROPIC_API_KEY` | Claude API Key（AI金額辨識） | （選填） |
| `REPORT_PASSWORD` | 行政A密碼 | `admin123` |
| `ADMIN_PASSWORD` | 管理員密碼 | `admin_secret` |
| `VBA_SECRET` | VBA API 密鑰 | `vba_secret_2026` |
| `PORT` | 伺服器端口 | `10000` |

---

## 開發注意事項

- **單檔架構：** 所有邏輯集中在一個 Python 檔案中，修改時注意函式間的依賴關係
- **重複定義：** `COMPANY_SECTION_MAP`、`normalize_section()`、`parse_single_approval_line()`、`is_single_approval_line()` 各有兩份定義（後者覆蓋前者）
- **HTML 內嵌：** 所有 Web 頁面以 f-string 直接輸出 HTML，無模板引擎
- **資料庫遷移：** `init_db()` + `migrate_db()` + `ensure_column()` 組合處理 schema 演進
- **啟動順序：** `startup` → `init_db()` → `migrate_db()` → `seed_groups()`
- **LINE 訊息長度限制：** 所有回覆截斷到 4900 字元
- **Quick Reply 按鈕上限：** 13 個按鈕
- **AI 金額辨識：** 背景執行緒非同步呼叫 Claude Haiku，結果推送回 A 群

---

## 2026/04/30 補充規則（重要、修 bug 必看）

### 「送件區塊」vs「公司區塊」明確規則 ⭐

| 客戶狀態 | 應該在哪 |
|---------|---------|
| 設了送件順序、業務還沒打過任何指令 | **送件區塊**（report_section="送件"）|
| 業務打過「送/轉/照會」其中一個 | **公司區塊**（current_company section、清掉送件標記）|
| A 群核准 + 有金額 | **待撥款區塊** |
| 結案 | 不顯示 |

**實作對齊**（每個 handler 都要做）：
- `handle_route_order_block`（設順序）：force `report_section="送件"`（commit cd877ea）
- `add_concurrent`（送）：若 `report_section=="送件"` → 清空（commit bb68495）
- `advance`（轉、單公司+多公司+下一家三條路徑）：清空送件標記
- `notification`（照會）：清空送件標記
- `mark_doc_completed` / `clear_missing_docs`（已補）：補完後若 company_status 空 → **保持送件**（commit b08e0d8）

### concurrent_companies 永遠散到對應區塊（commit 324edc8）

`build_section_map` 中、`concurrent_str` 的散開邏輯**不要**用 `_is_pre_send` 擋。
理由：`concurrent_companies` 是業務明確打 `@AI 姓名 送 X` 寫進去的、表達意圖、必顯示。

```python
# ✅ 對：
if concurrent_str:
    for co in concurrent_str.split(","):
        # 散到 co 對應區塊

# ❌ 錯（被 _is_pre_send 擋住、加送的家不顯示）：
if concurrent_str and not _is_pre_send:
```

### 單一公司照會 = 跳過原 current（commit 82aa3a1）

業務打 `@AI 王小明 21 照會`（單一公司、不是 +）：
- 若 21 == current → 只給話術、不動 case
- 若 21 在 concurrent → 只給話術、不動 case
- 若 21 是新的 → **跳過原 current、改送 21**：
  1. `current_company = 21`
  2. `route_plan` 用 `advance_route_to(..., status="跳過")` 推進
  3. 從 concurrent 移除舊 current
  4. 清舊 current 的 `company_status` entry
  5. 清 `pending_docs`（舊 current 缺的件跟 21 沒關）

業務心智：「21 照會」= 「決定送 21、原 current 不送了、跳過」、不是「對 21 發照會話術」。

### 「送/轉/照會」指令統一原則

三個指令在「業務語意」上等價：「我把案子送出去了」。差別：
- **送 X**：原 current 保留、加 X 進 concurrent（多家同送）
- **轉 X**：原 current 換成 X、原 X 進 history
- **X 照會**：等同「轉 X」（原 current 跳過、不留 history 婉拒紀錄）

三個都會清掉「送件」標記、跳到公司區塊。

### 跨群組同送 / 加送防呆

`update_customer` 的 `concurrent_companies` 設置會自動：
- 移除跟 `current_company` 相同的項目（normalize_section 比對）
- 移除重複公司（同公司不同產品如「21機車25萬」「21商品」算同公司）

### 6 個必對清單（修 bug 前先看）

每次修一個 handler、要對照下表 confirm 不會踩到別的：

| Handler | report_section | current_company | concurrent_companies | route_plan | company_status | pending_docs |
|---------|----------------|-----------------|---------------------|-----------|----------------|--------------|
| 設順序 | force "送件" | 第一家 | 不動 | 寫 order | 不動 | 不動 |
| 送 X | 若"送件"→清 | 不動（除非 alias 替換）| 加 X | 不動 | 不動 | 不動 |
| 轉 X | 若"送件"→清 | 改 X | 不動 | advance_route_to(X) | 不動 | 不動 |
| X 照會 | 若"送件"→清 | 改 X | 移除舊 current | advance_route_to(X) | 移除舊 current | 清空 |
| X 婉拒 | 若"送件"→清 | 推下一家 or 清 | 移除 X | advance + 標婉拒 | 不動 | 不動 |
| 已補 | 若"送件"+空 cs+補完 → 保持"送件" | 不動 | 不動 | 不動 | 不動 | 移除該項 |

### 修 bug 之前必須做的事

1. **讀這份 CLAUDE.md 2026/04/30 補充規則**（這份）
2. 在 `main.py` 找對應 handler、看現在做了什麼
3. **不要只看眼前的 bug 改**：用「6 個必對清單」逐欄位 review、確認新邏輯不破壞別的指令
4. 改完跑測試（建客戶 → 設順序 → 各種指令 → 看日報）

---

## 2026/08/03 LINE vs 網頁邏輯不一致（⭐ 修 bug 前必看）

### 病根

**同一件事有兩套實作，早晚走岔。** 這天連續兩個災情都是這個病：

| 同一件事 | LINE 的做法 | 網頁的做法 |
|---|---|---|
| 改「目前送哪家」 | `advance_route_to` 移指標、保留後面的家 | `/case-edit` 自己重建 order → **砍掉後面的家** |
| 改進度 | `update_customer`（寫 log + 快照） | 直接下 SQL → **沒紀錄、不能還原** |

### ⛔ 網頁編輯不可重建 route_plan（L19547 附近）

`/case-edit` 儲存時**只能移動 `current_index`，或把新公司插進 order**。
絕不可以 `_rp["order"] = [current] + concurrent` —— 那等於把還沒送的家全丟掉。

**災情**：一次儲存砍掉 6 位 ACTIVE 客戶的送件順序（潘藝中 12→1、曾子昇 12→2、
林珊妤 11→1、林宛稜 10→1、張永德 9→2、陳文汪 4→1）。

**為什麼特別容易漏**：網頁編輯正是行政拿來「**修日報顯示錯誤**」的工具 —— 去修 A、
製造 B，而且完全沒有提示。潘藝中就是去修顯示，把順序砍成 1 家，再打「改順序：」
（冒號，靜默失效）想救，越弄越亂。

### 🔍 查案件資料異常：第一件事看 `case_logs.from_group_id`

上面那個 bug 繞了很久，因為一開始沒看這個欄位，一路在查 LINE 的婉拒邏輯
（四條路徑全部查完都是對的）。

- `from_group_id` = `WEB` / `WEB_ADMIN` → **網頁改的**，別去查 LINE
- `message_text` 含 `\r\n` → 從網頁貼上的（LINE 訊息不會有）

### ⛔ 已核准/已撥款的案子不可跨群轉移

轉移＝整筆案件搬走，**建案日期、核准金額、撥款紀錄全部跟著走**＝業績搬家。
使用者說「他們不會在別人群組」，這功能實務上用不到卻能一鍵搬走業績。

| 管道 | 已撥款能不能搬 |
|---|---|
| LINE 按鈕 | ⛔ 一律擋（**LINE 分不出誰是管理員**，群裡誰都能按） |
| 網頁 `/admin/move-customer` | ✅ 管理員可以 |

實作 `_transfer_lock_reason()`，**按鈕層 + callback 層都要擋** ——
舊按鈕會留在 LINE 對話裡，隔天點還是會生效，只擋按鈕層沒用。

### 前後對照（diff）不要印欄位原值

`update_with_verify` 的 diff 用 `_fmt_diff_value()`：`route_plan` 是 JSON，
直接印會在群裡噴一大串英文，業務看不懂。改成「第2家 21機車25萬（共11家）」，
順序被砍時「共11家 → 共1家」一眼可見（順便當預警）。

### 盤點結果：還沒統一的

| 操作 | 狀況 |
|---|---|
| 還原 | ✅ 兩邊共用 `restore_prev_state()` ——**唯一不會再分岔的** |
| 改順序 / 批次結案 | ⚠️ 行為一致，但**程式碼複製兩份**，改一邊另一邊不會跟著改 |
| 婉拒 | ✅ 沒問題（2026-08-05 實測：A 群重複打同一家婉拒，current 不會多推） |

⛔ **婉拒那項風險最高**（每天用最多次、分支多：同送清單 / 房地銀行C 二次確認 /
找下家四層 fallback），要改請在使用者能即時驗證時做，別自己摸。

### 這批的測試

`test_flows.py` 第 37（網頁編輯不砍順序）、38（網頁改進度留紀錄）、39（已撥款不給轉移）。
三組都**驗證過還原程式會變紅**——寫完測試一定要這樣驗一次，否則可能是假通過
（第一次寫 37 時就假通過過：TestClient 下 session cookie 不生效，POST 被導去 /login，
要 `m.check_auth = lambda req: "admin"` 繞過）。
