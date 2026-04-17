# CLAUDE.md - 貸款案件管理系統

## 工作規則

- **Agent 自動分配：** Claude 在執行任務時可自由啟動 Agent（Explore、Plan、general-purpose 等）來並行處理子任務，不需要逐一手動批准。可根據任務複雜度自行決定 Agent 數量與類型。
- **修改 Excel 範本邏輯後務必本地測試後才能交付。**
- **絕對保留所有申請書範本的格式、公式、下拉選單**，不可破壞。
- **欄位無資料時清空（不留範本示範值）**，但**不能覆蓋標籤儲存格**。

---

## Excel 範本填入核心規則

### 通用原則

1. **只修改 sharedStrings.xml**：用「新增 shared string + 修改 sheet XML 引用索引」法，不修改現有 ss（避免破壞下拉選單共用的 ss）。
2. **下拉選單值嚴格匹配**：填入的值必須在範本下拉選項清單內，不匹配則留空。
3. **空值處理**：cell_map 中 `""` 表示清空儲存格，`None` 表示不動原值，`__FORMULA_RECALC__` 表示清除公式快取值（強制 Excel 重算）。
4. **標籤 vs 值儲存格**：必須區分清楚，例如亞太 D17 是「行業類別」標籤，E17 才是值的位置。
5. **adminB 自動調整規則必須保留**（年資<1→1、月薪<3.5→3.5、居住<5→5、租屋→親屬、國中→高中/職）。

### 亞太系列（商品/機車15萬/工會機車/機車25萬）

- **主工作表：** 工作表3
- **車輛欄位**（車輛型式/引擎號碼/排氣量/顏色/廠牌/牌照號碼）：**只有機車變體填入**，亞太商品不填寫
- **資金用途 B5**：來自 adminB 補充資料 (`adminb_fund_use`)，下拉清單 13 個選項（I-1教育費 ~ V-2整合負債）
- **出生日期/發證日**：民國年自動轉西元年（086/12/15→1997/12/15）
- **教育程度 D10**：必須匹配 `小學/國中, 高中/職, 專科/大學, 研究所以上`，含轉換映射
- **戶籍/住家/公司縣市**：用**完整名稱**（桃園市非桃市），對應 INDIRECT 下拉
- **發證地 D11**：用**短碼**（苗縣非苗栗縣）
- **居住狀況 B14**：智能判別，含父母/租屋/宿舍/借住 → 親屬
- **行業類別 E17**（注意是 E17 不是 D17）：來自 `adminb_industry`，無值清空
- **職務 G17**：來自 `adminb_role`（亞太用 at_role，不可 fallback 到和裕 hr_role），無值清空
- **公司電話區碼 B18**：手機（co_phone_area=mobile 或號碼以 09 開頭）→ 自動選 `0`
- **分機 E18**：來自 `company_phone_ext`
- **年資 G18**：5年6個月→`5.6`（小數點分隔）
- **月薪 H18**：58000→`5.8`（萬元）
- **聯絡人關係 D21**：智能判別函式 `map_relation()`，含夫妻→配偶、媽媽→父母、友→朋友等
- **車輛資料寫入位置**（機車變體）：B7=車輛型式、D7=引擎號碼、F7=排氣量、H7=顏色、K2=廠牌（adminb_brand）、K3=牌照號碼

### 和裕系列（機車/商品）

- **主工作表：** 和裕維力貸
- **資金用途 F17**：固定填「家用」
- **發證地 F13**：用短碼，必須匹配下拉
- **婚姻 C13**：必須匹配 `已婚, 未婚`
- **學歷 C14**：必須匹配和裕下拉 `高中職, 專科、大學, 研究所以上, 其他`（注意是「專科、大學」用頓號）
- **公司電話 H17**：格式 `02-27189090` 或 `0989-615422`（區碼-號碼）
- **年資 I18**：格式 `N年M月`，**年/月字一律保留**，缺數字以空白代替
  - 1年8月 → `1年8月`
  - 2年無月 → `2年 月`
  - 5年無月 → `5年 月`
- **月薪 I19**：格式 `N.N萬`（45000→`4.5萬`）
- **行業類別 G19**：匹配和裕下拉 21 個選項，含亞太→和裕轉換映射，無值清空
- **手機電信 C20**：匹配 `中華電信, 遠傳電信, 台灣大哥大, 其他`，不在清單時自動填「其他」
- **聯絡人知情 D22/G22**：智能判別 `知情, 保密`（含「可知情」「無可知情」等）
- **聯絡人電話 C25/F25**：格式 `0955-389338`（4碼-6碼）
- **戶名 C39**：是公式 `=C11`，使用 `__FORMULA_RECALC__` 標記清除快取值
- **撥款資訊** C37 銀行 / C38 分行：來自 `adminb_bank/adminb_branch`，無填寫清空
- **商品資訊** C42 廠牌+商品 / F42 型號或車號：來自 `adminb_product/adminb_model`，無填寫清空
- **不自動填入**：分期項目(F8)、申貸金額/期數/期付款/分期總價/回購金額

### 21 系列（機車12萬/機車25萬/商品）

- **主工作表：** 工作表1
- **日期格式：** 民國年有斜線 `077/10/03`
- **城市：** 全名但「台」改「臺」（臺中市、臺北市）
- **發證地短碼：** 與亞太不同（新北市→北縣、桃園市→桃縣）
- **關係下拉：** 包含「兄弟姐妹」「外祖父母」「孫子女」「外孫子女」
- **公司電話：** 區碼+號碼無橫線（037612890）
- **不自動填入：** 貸款金額、期數、商品別、保證人、簡訊帳單

### 第一國際資融

- **主工作表：** 申請書
- **日期格式：** 民國年無斜線 `1141126`、`0660602`
- **含 x14 extension data validation**，使用 ZIP-level 處理保留
- **不自動填入：** 貸款金額(B13)、期數(G13)

### 貸就補

- **主工作表：** 進件申請書
- **日期格式：** 民國年無斜線 `0741029`、`1080408`
- **發證地：** 短碼下拉
- **不自動填入：** 貸款金額(C6)、期數(E6)、商品名稱(J7)、型號(J8)、保證人區域

### 範本對應表

| 方案 | 範本檔案 |
|------|---------|
| 亞太商品 | 亞太商品範本.xlsx |
| 亞太機車15萬 | 亞太15萬機車範本.xlsx |
| 亞太機車25萬 | 亞太15萬機車範本.xlsx (共用) |
| 亞太工會機車 | 亞太工會範本.xlsx |
| 和裕機車 | 和裕維力貸機車範本).xlsx |
| 和裕商品 | 和裕維力貸商品範本.xlsx |
| 第一 | 第一申請書範本.xlsx |
| 貸就補 | 貸就補範本.xlsx |
| 21機車12萬 | 21機車申請書範本xlsx.xlsx |
| 21機車25萬 | 21機25萬範本.xlsx |
| 21商品 | 21商品範本.xlsx |

### adminB 自動調整規則（apply_adminb_rules）

儲存 adminB 資料時自動套用：

| 欄位 | 條件 | 調整為 |
|------|------|--------|
| company_years | < 1 年 | 1 |
| company_salary | < 3.5 萬 | 3.5 |
| live_years | < 5 年 | 5 |
| live_status | 含「租」或「宿舍」 | 親屬 |
| education | 含「國中」「國小」 | 高中/職 |

調整後的值會回寫 customers 表，Excel 下載時讀取的就是調整後的值。

### DB 欄位

- **adminb_industry**：行業（亞太/和裕共用）
- **adminb_brand**：廠牌（亞太機車）
- **adminb_role**：職務（亞太 at_role 專用，避免和裕 hr_role 污染）
- **adminb_hr_role**：和裕專用職務
- **carrier**：電信業者
- **company_months**：年資月份（與 company_years 配對）

### 歷史頁面狀態過濾

`/history` 和 `/report` 「本月結案」必須使用：
```python
status IN ('CLOSED','PENALTY','ABANDONED','REJECTED')
```
**不可用** `status != 'ACTIVE'`，否則 PENDING 客戶會誤被視為結案。

---

## 專案概覽

這是一個 **貸款案件管理系統**，整合 LINE Bot + Web 管理介面，用於追蹤貸款客戶從進件、送件、核准到撥款的完整流程。系統以 FastAPI 為後端，SQLite 為資料庫，部署在 Render 平台上。

**主要檔案：** `main.py`（Render 部署檔，Procfile 指向 `uvicorn main:app`，單檔架構，約 8500 行）。注意：`main (2).py` 是舊版備份，不會被部署，修改請對準 `main.py`。

---

## 系統架構

### 技術棧
- **後端框架：** FastAPI + Uvicorn
- **資料庫：** SQLite（WAL 模式），路徑 `/var/data/loan_system.db`
- **外部 API：** LINE Messaging API（push/reply）、Anthropic Claude API（AI 金額辨識）
- **前端：** Server-side rendered HTML（無前端框架），內嵌 CSS + JavaScript
- **認證：** Cookie-based session，SHA-256 salted hash 密碼

### 群組架構（LINE 群組）
| 群組類型 | 用途 |
|---------|------|
| `A_GROUP` | 行政A群 - 接收進度回報、撥款名單，回貼結果到業務群 |
| `SALES_GROUP` | 業務群（B群、C群等） - 業務員建立客戶、送件順序、狀態更新 |
| `ADMIN_GROUP` | 行政群 - 接收排撥名單推送 |

### 角色權限
| 角色 | Web 權限 |
|------|---------|
| `admin` | 全部功能（群組管理、密碼管理、清除資料） |
| `adminB` | 行政B作業（方案判別、申請書下載） |
| `normal` | 日報、查詢、新增客戶 |
| `group_xxx` | 業務角色，只能看自己群組的日報 |

---

## 核心模組分區

### 1. 常數與正則（L1-140）
- `REPORT_SECTION_1/2/3`：日報三段分類（貸款方案 / 民間方案 / 房地）
- `COMPANY_LIST`：公司辨識清單（按長度優先匹配）
- `COMPANY_ALIAS`：別名對照（如 `熊速貸 → 亞太商品`）
- `STATUS_WORDS` / `ACTION_KEYWORDS` / `DELETE_KEYWORDS`：狀態/動作/結案關鍵字
- 多組正則：`DATE_NAME_ID_INLINE_RE`、`ROUTE_ORDER_RE`、`SINGLE_APPROVAL_RE` 等

### 2. 工具函式（L143-255）
- `normalize_ai_text()`：全形轉半形
- `has_ai_trigger()` / `strip_ai_trigger()`：偵測/移除 `@AI` 觸發詞
- `extract_company()` / `extract_name()` / `extract_id_no()`：從訊息解析公司、姓名、身分證
- `get_group_name()` / `get_sales_group_ids()` / `get_admin_group_ids()`：群組查詢

### 3. 送件順序引擎（L258-400）
- **格式：** `4/1-高郡惠-喬美/亞太/和裕`
- `parse_route_order_line()`：解析送件順序
- `make_route_json()` / `parse_route_json()`：JSON 格式的路由計畫
- `advance_route()` / `advance_route_to()`：推進到下一家/指定公司
- `get_all_approved()` / `get_total_approved_amount()`：多家核准金額追蹤
- 路由資料結構：`{"order": [...], "current_index": 0, "history": [...]}`

### 4. 訊息解析（L402-503）
- `parse_header_fields()`：從訊息首行解析日期、姓名、身分證
- `looks_like_new_case_block()`：判斷是否為新案件格式
- `split_multi_cases()`：分割多筆案件（空行或 `/` 分隔）
- `looks_like_case_start()`：判斷一行是否為案件起始

### 5. LINE API 層（L508-553）
- `push_text()`：主動推送訊息
- `reply_text()`：回覆訊息
- `reply_quick_reply()`：帶快速回覆按鈕的回覆

### 6. 資料庫層（L556-841）

#### 資料表
| 表名 | 用途 |
|------|------|
| `groups` | 群組設定（ID、名稱、類型、對應業務群、密碼） |
| `customers` | 客戶資料（60+ 欄位，含個資、職業、負債、方案等） |
| `case_logs` | 案件操作日誌 |
| `pending_actions` | 待確認操作（Quick Reply 按鈕的暫存） |
| `settings` | 系統設定（密碼 hash 等） |
| `login_attempts` | 登入失敗記錄（5次鎖定15分鐘） |

#### 客戶狀態流轉
```
PENDING → ACTIVE → CLOSED
                 → PENALTY（違約金結案）
                 → ABANDONED（放棄）
                 → REJECTED（全數婉拒）
```

#### CRUD 函式
- `create_customer_record()`：建立客戶（會先查 PENDING 客戶合併）
- `update_customer()`：更新客戶 + 自動寫 case_logs
- `find_active_by_id_no()` / `find_active_by_name()`：查找客戶
- `save_pending_action()` / `get_pending_action()`：Quick Reply 暫存

### 7. 核准金額解析（L846-926）
- `extract_approved_amount()`：正則解析金額（支援 20萬 / 6W / 120,000 / (5).(8)萬）
- `extract_approved_amount_with_ai()`：呼叫 Claude Haiku API 做 AI 金額辨識（fallback）

### 8. 撥款名單處理（L928-1013）
- `parse_disbursement_list()`：解析 A 群撥款名單格式
- `handle_disbursement_list()`：批次更新客戶撥款日期，推送排撥名單到行政群

### 9. 日報產生（L1048-1242）
- `COMPANY_SECTION_MAP`：子分類歸到日報主欄位（如 `21機車 → 21`）
- `build_section_map()`：客戶分組
- `generate_report_lines()`：產生三段日報（貸款/民間/房地），超過 4500 字自動切割
- `search_customer_info()`：查詢客戶詳細資訊

### 10. Quick Reply 按鈕系統（L1248-1298）
- `send_reopen_case_buttons()`：結案客戶重啟/新建選擇
- `send_ambiguous_case_buttons()`：同名客戶選擇
- `send_transfer_case_buttons()`：跨群組轉移確認
- `send_confirm_new_case_buttons()`：身分證重複確認

### 11. 特殊 @AI 指令（L1300-1488）
| 指令格式 | 功能 |
|----------|------|
| `@AI 群組ID` | 查詢群組資訊 |
| `@AI 日報` | 產生日報 |
| `@AI 查 姓名` | 查詢客戶 |
| `@AI 姓名 轉下一家` | 推進送件順序 |
| `@AI 姓名 轉XXX` | 轉到指定公司 |
| `@AI 姓名 結案` | 結案 |
| `@AI 姓名 婉拒` | 婉拒並推下一家 |
| `@AI 姓名 婉拒轉XXX` | 婉拒並跳到指定公司 |
| `@AI 姓名 違約金已支付XXXX` | 違約金結案 |
| `@AI 姓名 公司 核准 金額` | 修改核准金額（→ 移到待撥款） |
| `@AI 姓名 公司[+公司2] 金額/期數` | 設送件金額（notify_amount）+ 同送 |

**送件金額 vs 核准金額**：
- `notify_amount` / `notify_period` = **送件金額/期數**（業務希望送多少），由上表最後一個指令、送件順序尾巴 `N/M @AI`、或 A 群照會注意事項寫入
- `approved_amount` = **核准金額**（公司實際核准多少），由 A 群核准訊息或「@AI 姓名 公司 核准 金額」寫入
- 兩欄獨立，核准訊息不會覆蓋 notify_amount
- `generate_notification_text` 優先用 notify_amount → PLAN_INFO → eval_fund_need，**不** fallback 到 approved_amount

**送件順序/轉送尾巴帶金額**（`extract_notify_amount_period`）：
- `4/17-姓名-A/B/C 100/24 @AI` → 建案 + notify_amount=100 / notify_period=24
- `4/17-姓名-轉A+B 30萬/24期 @AI` → 轉送（+）= 同送，寫入 concurrent_companies
- 只在訊息末尾是 `@AI` 且前有 `N[萬]/M[期]` 才抓，避免誤判日期

**補件/補申覆 待補 vs 已補**（`extract_status_summary`）：
- A 群貼「補XX / 補申覆」→ 「待補資料 / 待補申覆」（要求補）
- 業務回「已補XX / 補好 / 補完 / 申覆通過」→ 「已補資料 / 已補申覆」（真的補了）

### 12. 主要業務邏輯（L1490-2019）
- `handle_new_case_block()`：處理新案件建立
- `handle_route_order_block()`：處理送件順序設定
- `handle_a_case_block()`：A 群訊息處理（回貼到業務群、核准金額、婉拒推進）
- `handle_bc_case_block()`：業務群訊息處理（含 @AI 補件回貼 A 群功能）

### 13. 按鈕指令處理（L1664-1799）
處理 Quick Reply 回調：`FORCE_CREATE_NEW|`、`USE_EXISTING_CASE|`、`TRANSFER_FROM_CONFIRM|`、`CONFIRM_TRANSFER|`、`KEEP_OLD_CASE|`、`CREATE_NEW_FROM_TRANSFER|`、`SELECT_CASE|`、`REOPEN_CASE|`、`CREATE_NEW_CASE|` 等

**跨群組同身分證三按鈕語意**（`send_confirm_new_case_buttons` / `send_transfer_case_buttons`）：
- **沿用(兩邊都有)** → 各群組獨立案件（呼叫 `create_customer_record` 建新 case_id）
- **轉移到{新群組}** → 改 `source_group_id` 把原案搬到新群組
- **取消** → 不動作

業務群訊息查找策略：`_handle_bc_case_block_locked` 先用 `find_active_by_id_no_in_group(id_no, group_id)` 找本群組 ACTIVE；本群組沒有才用 `find_active_by_id_no` 跨群組找 → 跳按鈕。

### 14. Webhook 入口（L2031-2127）
- `process_event()`：LINE Webhook 事件分發
  - 按鈕指令 → `handle_command_text()`
  - 業務群/行政群 → 判斷是否需處理 → `handle_bc_case_block()`
  - A 群 → 撥款名單 or @AI 指令 or 回貼 → `handle_a_case_block()`
- `POST /callback`：FastAPI endpoint，背景執行事件處理

### 15. 管理 API（L2130-2210）
- `POST /admin/reset_data`：清除所有案件資料
- `POST /admin/add_group`：新增/更新群組
- `POST /admin/update_group`：更新群組名稱/狀態

### 16. 密碼與認證（L2214-2280）
- `hash_pw()` / `verify_pw()`：salted SHA-256
- `is_login_locked()` / `record_login_fail()` / `clear_login_fail()`：登入鎖定機制
- `get_setting()` / `set_setting()`：系統設定 CRUD

### 17. Web 頁面（L2282-4337）

| 路由 | 頁面 | 權限 |
|------|------|------|
| `GET /login` | 登入頁 | 公開 |
| `GET /report` | 日報儀表板（分群組、分狀態） | 已登入 |
| `GET /search` | 客戶搜尋（姓名/身分證/群組/日期） | 已登入 |
| `GET /history` | 結案歷史（支援篩選） | 已登入 |
| `GET /adminb` | 行政B作業（方案判別、規則校驗） | admin/adminB |
| `GET /new-customer` | 新增客戶表單 | admin/adminB/normal |
| `GET /pending-customers` | 客戶資料庫 | 已登入 |
| `GET /edit-pending` | 編輯客戶資料 | 已登入 |
| `GET /admin/groups` | 群組管理 | admin |
| `GET /admin/passwords` | 密碼管理 | admin |

### 18. 行政B規則引擎（L2883-2964）
`apply_adminb_rules()` 自動調整：
- 年資 < 1年 → 填 1年
- 月薪 < 35000 → 填 3.5萬
- 居住時間 < 5年 → 填 5年
- 居住狀況為租屋 → 改為親屬
- 學歷為國中/國小 → 改為高中/職

### 19. VBA API（L3827-3930）
- `GET /api/customer-lookup`：供 Excel VBA 查詢客戶資料，需 VBA 密鑰驗證

### 20. PDF 匯出（L4339-4433）
- 客戶端 JavaScript，產生客戶資料表 PDF（列印用）

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
- **密碼初始化：** `init_settings()` 只在首次運行時設定預設密碼 hash
