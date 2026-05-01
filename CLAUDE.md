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

### 11. 特殊 @AI 指令（L2914~）

**進入前必經**：`parse_special_command` 進入前先跑 `normalize_command_text`（全形→半形、異體字統一「核準→核准」、多空白合併）。regex 只需寫單一標準寫法。

#### 指令表

| 指令格式 | 功能 |
|----------|------|
| `@AI 群組ID` | 查詢群組資訊 |
| `@AI 日報` | 產生日報 |
| `@AI 查 姓名` | 查詢客戶 |
| `@AI 統計` | 今日/本月進件、核准、結案數 |
| `@AI 待撥款` | 待撥款名單 |
| `@AI 格式` / `@AI 說明` | 指令速查卡（按鈕分類顯示）|
| `@AI 姓名 轉下一家` | 推進送件順序 |
| `@AI 姓名 轉XXX` | 轉到指定公司（current 改，concurrent 不動）|
| `@AI 姓名 轉A+B` | 轉到多公司同送（清原 route，重建 [A, B]） |
| `@AI 姓名 轉A+B 100萬/24期` | 轉送 + 設送件金額給最後一家 |
| `@AI 姓名 送XX` | 加送一家（原 route 保留）|
| `@AI 姓名 送A+B` | 加送多家 |
| `@AI 姓名 結案` / `結案 原因` | 整筆結案 |
| `@AI 姓名 公司 結案` | **A 方案**：已核准時只從同送清單移除該家（保留核准）；未核准時退回整筆結案+提示 |
| `@AI 姓名 公司1/公司2 結案` | 多家一起從同送移除（支援 `/`、`+`、`、`、`,`）|
| `@AI 姓名 婉拒` | 婉拒並推下一家 |
| `@AI 姓名 公司 婉拒` | 指定某家婉拒（不跳轉）；是 current 走原婉拒邏輯；在 concurrent 從同送移除 |
| `@AI 姓名 婉拒轉XX` | 婉拒當前 + 跳 XX |
| `@AI 姓名 公司 婉拒 轉XX` | 明確指定哪家婉拒（可同送的某家）+ 跳 XX |
| `@AI 姓名 公司A婉拒 送公司B` | **複合指令**：婉拒 A + 加送 B |
| `@AI 姓名 違約金已支付XXXX` | 違約金結案 |
| `@AI 姓名 公司 核准 金額` | 修改核准金額（→ 自動移到待撥款） |
| `@AI 姓名 核准 金額` | 同上，未指定公司時用 current_company |
| `@AI 姓名 公司 取消核准` | 取消核准（company 必填、空會阻擋）|
| `@AI 姓名 撥款 M/D` | 撥款（1 家核准可省公司、多家核准會要求指定）|
| `@AI 姓名 公司 撥款 M/D` | 指定哪家撥款（日期可在前或後：`姓名 公司 M/D 撥款` 也接受）|
| `@AI 姓名 重啟` | 結案客戶回到 ACTIVE |
| `@AI 姓名 改名 新名` | 改名（允許結案客戶）|
| `@AI 姓名 改身分證 新ID` | 改身分證（允許結案客戶）|
| `@AI 姓名 歷史` | 列最近 10 筆操作（附編號、時間）|
| `@AI 姓名 還原 N` | 跳回第 N 筆之前的狀態（snapshot rollback，一次只能退一步）|
| `@AI 批次結案` / `批次婉拒` | 第一行指令，後面每行一個姓名 |

#### 對保派件/時間地點（A/B 兩段訊息，無 @AI 前綴）

**A：派對保訊息**（行政 A/B 發）
```
辦理方案：裕融
核准金額：50萬
客戶姓名：王陽明
對保地區：台北
```
→ 系統抓到「客戶姓名」「對保地區」兩關鍵字 → type=`signing_request` → 寫入 `signing_area` + 標 `report_section="派對保"`

**B：對保員回覆（接單 + 排時間地點）**
```
對保 張三 新光對保
時間：8/20 下午2點
地點：台北 XX 路
```
→ 第一行匹配 `^對保 業務員 XX對保$` → type=`signing_return_time` → 寫入 `signing_salesperson` / `signing_company` / `signing_time` / `signing_location`

**對保子步驟偵測**（`detect_pairing_substep`）：訊息含特定關鍵字會自動辨識
- `派對保` / `辦理方案` + `對保地區` → 「派對保」
- `委對收` → 「委對收」
- `對保時間` / `對保地點` → 「約時間」
- `對好` / `對保完成` / `不收不簽` → 「對好」

#### 送件金額 vs 核准金額

- `notify_amount` / `notify_period` = **送件金額/期數**（業務希望送多少）；由「轉 A+B N/M」尾巴、送件順序尾巴 `N/M @AI`、A 群照會注意事項寫入
- `approved_amount` = **核准金額**（公司實際核准多少）；由 A 群核准訊息或「姓名 公司 核准 金額」寫入
- **兩欄獨立**，核准訊息不會覆蓋 notify_amount
- `generate_notification_text` 優先用 notify_amount → PLAN_INFO → eval_fund_need，**不** fallback 到 approved_amount

#### 送件順序/轉送尾巴帶金額（`extract_notify_amount_period`）

- `4/17-姓名-A/B/C 100/24 @AI` → 建案 + notify_amount=100 / notify_period=24
- `4/17-姓名-轉A+B 30萬/24期 @AI` → 轉送（+）= 同送，寫入 concurrent_companies
- 只在訊息末尾是 `@AI` 且前有 `N[萬]/M[期]` 才抓，避免誤判日期

#### 補件/補申覆 待補 vs 已補（`extract_status_summary`）

四層判斷（由明確到含糊）：

1. **明確「已補」**：含「已補 / 申覆完 / 申覆通過 / 申覆好 / 補完申覆 / 補好申覆」→ 已補
   - 注意：`"已補申覆" in "已補薪轉申覆"` 會失敗（中間夾字）；改用 `"已補" in text AND "申覆" in text` 鬆散比對
2. **明確「待補」**：含「待補 / 請補 / 要補 / 缺 / 未補 / 還沒補 / 尚缺 / 請提供 / 麻煩補 / 需補」→ 待補
3. **業務主動語氣**（C 方案）：含「業務說 / 我補 / 我已補 / 幫補 / 主動補 / 補好了 / 補完了 / 業務補 / 補完成」→ 已補
4. **含糊「補 XX」**（無任何明確線索）→ 預設待補（保守、對應 A 群要求補的常見用法）

**內部動作文字不污染日報** (`_INTERNAL_ACTION_KEYWORDS`)：
- 「設送件」「核准金額修改為」「轉送」「加送/備註」「加送」
- 「改名為」「改身分證」「重啟案件」「還原到」
- 「不送：」（A 方案移除同送）、「婉拒（從同送」（reject_company 移除同送）

這些動作文字出現在 `last_update` 時不當作業務狀態顯示。

#### 對應詞（COMPANY_ALIAS 摘錄）

| 簡稱 | 對應 |
|------|------|
| 貸10 / 貸救 / 貸就補（錯字）| 貸救補 |
| 21 → 21商品；21機 → 21機車12萬；21機25 → 21機車25萬 | - |
| 喬 / 鼎多 → 喬美（PLAN_INFO 預設 14萬/30期）；麻 → 麻吉機車；分 → 分貝機車 | - |
| 鄉→鄉民、銀→銀行、C→零卡、商→商品貸、代→代書、當→當舖、研→商品貸 | - |
| 維力 / 新新 / 新新專 → 和裕；熊速貸 → 亞太商品 | - |
| TAC / 一路發 → 裕融；EGO → 第一 | - |
| 亞太系列 `亞太機 / 亞太機車` PLAN_INFO 對應 15萬/36期；亞太機車25萬 → 25萬/48期 | - |

#### 公司名比對：一律用 normalize_section 做模糊匹配

系統裡公司名是**三層結構**：
- **公司（= 日報區塊）**：21、亞太、和裕、喬美、第一、裕融、麻吉、分貝、貸救補、鄉民、銀行、零卡、商品貸、代書、當舖、房地...
- **方案（= 同公司不同產品）**：機、機車、商品、工會、汽車、手機
- **金額（= 送貸金額）**：15、25、50（萬）

合體例：`21機車25萬` = 21（公司）+ 機車（方案）+ 25萬（金額）

所有比對（結案、撥款、婉拒、加送、取消）都要用 `normalize_section` 處理：
- 業務打「銀行」→ 匹配 concurrent 裡「元大/渣打」
- 業務打「亞太」→ 匹配「亞太機車15萬/25萬/商品」
- 業務打「21」→ 匹配「21機車12萬/25萬/21商品/21汽車」

規則：不要用 `co == current_co` 這種字串等比對；改用 `normalize_section(co) == normalize_section(current_co)`。

#### 婉拒後 current 補位邏輯（`reject` / `reject_company` 情況 1）

婉拒 current 後，決定新 current 的順序：
1. route 有下一家 → 用 route 下一家
2. route 沒下一家、**concurrent 有家 → 從 concurrent 第一家升上來當新 current**（避免被婉拒那家殘留）
3. 都沒有 → current 清空 + 提示「已全數婉拒，請手動結案或送新家」

例：route=[第一]、concurrent=[亞太]，打「第一婉拒」→ 亞太從同送升上來當 current、第一從日報消失。

#### 照會金額不沿用舊值（`notification` handler）

每次新照會（`@AI 姓名 XX+YY 照會`）沒帶金額時，**清掉舊 notify_amount / notify_period**，避免上次送件的金額污染這次照會（例如先送房地 1000萬、後照會第一+亞太會誤顯示 1000萬）。清空後 fallback 到 PLAN_INFO 預設。

### 12. 主要業務邏輯（L1490-2019）
- `handle_new_case_block()`：處理新案件建立
- `handle_route_order_block()`：處理送件順序設定
- `handle_a_case_block()`：A 群訊息處理（回貼到業務群、核准金額、婉拒推進）
- `handle_bc_case_block()`：業務群訊息處理（含 @AI 補件回貼 A 群功能）

### 13. 按鈕指令處理（L4200~）

**Quick Reply 回調**：`FORCE_CREATE_NEW|`、`USE_EXISTING_CASE|`、`TRANSFER_FROM_CONFIRM|`、`CONFIRM_TRANSFER|`、`KEEP_OLD_CASE|`、`CREATE_NEW_FROM_TRANSFER|`、`SELECT_CASE|`、`REOPEN_CASE|`、`CREATE_NEW_CASE|`、`EXEC_CMD|`、`SELECT_SUPPLEMENT|`、`SAME_PERSON|`、`NEW_PERSON|` 等

#### 跨群組同身分證三按鈕（`send_confirm_new_case_buttons` / `send_transfer_case_buttons`）

- **沿用(兩邊都有)** → 各群組獨立案件（呼叫 `create_customer_record` 建新 case_id）
- **轉移到{新群組}** → 改 `source_group_id` 把原案搬到新群組
- **取消** → 不動作

#### 同姓名不同身分證（`send_same_name_diff_id_buttons`）

新案件姓名與本群組既有客戶相同但身分證不同 → 跳按鈕：
- **同一人-XX末4** → 更新既有客戶的身分證（糾正打錯）
- **不同人(建新)** → 建立新客戶
- **取消** → 不處理

#### 破壞性指令多筆同名（`_resolve_target_strict` + `EXEC_CMD` callback）

本群組多筆同名時，結案/婉拒/轉/送/核准金額/取消核准/撥款/改身分證/重啟 等指令 → 跳按鈕列每筆「公司-末4:XXXX」讓使用者選。callback 用 `_forced_case_id` 重新執行指令。

#### 補件多筆同名（`send_same_name_supplement_buttons`）

業務群打「XX 補件/補照會/核准」遇多筆同名 → 跳按鈕選，`SELECT_SUPPLEMENT` callback 用 `update_with_verify` 執行並實測回報變化。

#### 查找策略

`_handle_bc_case_block_locked` 先用 `find_active_by_id_no_in_group(id_no, group_id)` 找本群組 ACTIVE；本群組沒有才用 `find_active_by_id_no` 跨群組找 → 跳按鈕。

---

### 13.5 系統性防錯（L3200~）

為避免誤操作產生錯誤資料，所有破壞性指令套用：

1. **已結案/非 ACTIVE 客戶再操作** (`_check_active_or_warn`)：阻擋 + 提示「請先重啟」
2. **金額異常** (`_validate_amount_or_warn`)：核准金額 = 0/負數 → 阻擋；> 1000 萬 → 警告要求加「確認」重打
3. **送件順序重複公司**：`parse_route_order_line` 自動去重 + 回覆「已去除 N 家」
4. **身分證校驗位** (`validate_tw_id_checksum`)：台灣身分證校驗碼不符 → 警告（不擋）
5. **建客戶欄位檢查** (`_validate_new_case_fields`)：日期民國年異常、身分證格式、姓名過長 → 警告
6. **重複訊息偵測** (`is_duplicate_message`, 5 秒窗)：同群組同內容 5 秒內 → 自動忽略（防手滑）
7. **撥款日期邏輯**：撥款日 > 30 天後 / 早於建案 → 警告但仍執行
8. **push_text 長訊息分段**：> 4900 字優先在換行處切，每段加「(N/M)」

**update_with_verify**：破壞性 callback 執行 update 後讀回 DB 比對前後，回報具體變化（「公司: 亞太 → 裕融」），避免「顯示已更新但 DB 沒改」誤導。

---

### 13.6 誤操作救急（L3500~）

- **`@AI 姓名 歷史`** → 列最近 10 筆 case_logs（編號 + 時間 + 訊息摘要）
- **`@AI 姓名 還原 N`** → 跳回第 N 筆之前的狀態
  - `update_customer` 每次改前自動存 before snapshot 到 `case_logs.snapshot_json`
  - 還原時讀 snapshot 套回所有關鍵欄位（current/concurrent/status/金額/報表區塊等）
  - `update_with_verify` 回報實際變化給使用者確認

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

---

## 近期新增功能（2026/04）

### 新 DB 欄位
- `customers.approved_at`：核准時間（「待撥款超過 N 天」從核准日算，不是建案日）
- `customers.pending_docs`：缺件清單（逗號分隔：身分證,薪轉,帳單）
- `customers.adminb_mj_6w18p`：麻吉「6萬/18期/月付3771」勾選（勾了下載申請書 10萬→6萬、24→18）
- `groups.linked_a_group_id`：業務群對應的 A 群 ID（支援正式/測試雙軌 A 群）

### 新 LINE 指令
- `@AI 姓名 改順序 和裕/貸救補/21` — 覆寫 current 之後的 route，保留 history/current
- `@AI 姓名 缺 身分證+薪轉+帳單` — 記缺件清單、日報顯示「⚠️缺:身分證/薪轉/帳單」
- `@AI 姓名 已補 身分證` — 從清單移除某項；`@AI 姓名 已補 全部` 清空
- `@AI 姓名 公司 N萬 撥款 [M/D]` — 一步到位（自動核准+撥款，零卡/慢點付/商品貸常用）

### 照會話術規則（generate_notification_text）
- **套 adminB 規則**：月薪<3.5→3.5萬、年資<1→1年、居住時間<5→5年、學歷國中→高中/職
- **不套規則（照 DB 原值）**：居住狀況 — 宿舍就說宿舍、父母就說父母、租屋就說租屋
- （申請書邏輯仍保留 adminB 全部規則，包括租屋/宿舍→親屬）

### 日報顯示
- **「已補/待補」區分申覆/資料/照會/時段**：`_compress_status` 保留完整 prefix（已補申覆/已補時段/待補資料 等）
- **已補時段 vs 補時段**：`extract_status_summary` 優先識別「已補時段」返回「已補時段 時間」（業務已提供、不再壓成「補時段」= 待補）
- **「待核准20萬」完整顯示**：extract_status_summary 遇「待核准」優先返回「待核准 N萬」，不誤判成「已補資料」
- **「4/27續審」日期保留**：fallback 日期剝除要求後接 dash/空白才剝，純文字「4/27 續審」保留完整
- **內部動作關鍵字不污染日報**：_INTERNAL_ACTION_KEYWORDS 加「匯入」「搬到【」「改順序：」
- **申覆「已更換/已提供/補足」視為已補**（非待補）
- **送件順序格式（含 /）** 不剝公司名，直接保留（曾月英「分貝機/銀/C/商/當」完整顯示）
- **女友/男友不誤歸子女**：map_relation 三處（亞太/第一/21）在子女前攔截「男友/女友/朋友」
- **審核未通過不誤判核准**：is_approved 先剔除「未通過/不通過/沒通過/審核未通過」再比對「通過」

### 網頁功能
- `/admin/import-loan` — 從 apr_loan_import.json 一鍵匯入 40+ 筆勞工客戶（含搬群組/新建/清空）
- `/admin/clear-group` — 清空指定群組所有客戶（admin 限定）
- `/admin/delete-customers` — /history 批次勾選刪除（admin 限定）
- `/history` + `/search` — 每頁 50 筆分頁
- `/report` — 頂部搜尋姓名即時過濾、批次結案/刪除；點客戶整列展開（外層用 `.cust-wrap` class、不踩舊 `.cust-row` flex）
- **送件順序編輯**：`/report` 客戶展開區（admin/adminB 限定）+ POST `/report/reorder-route`，跟 LINE 「改順序」指令同邏輯
- **客戶卡 PDF**：`/customer-pdf?case_id=xxx`（/search 每筆有「📄 匯出 PDF」按鈕）

### 月薪顯示統一 `fmt_salary()`
- 輸入 `3.5` / `3.5萬` / `35000` / `35000元` 統一輸出 `3.5萬`
- PDF、客戶卡、adminB 顯示、分貝/麻吉 TXT 都用同一個 helper

### 範本修正
- **21機25萬年資欄位**：F10=年資標籤/G10=年數字/**H10=「年」標籤不動**/I10=月數字/J10=「月」標籤不動（12萬/21商品仍 G10=「N年」H10=「N月」）
- **21 發證地**：新北市→新北市（全名，非「北縣」）；下拉實測有朋友/同事/股東，聯絡人關係保留不再歸其他親屬
- **和裕電話**：G15/G16 是標籤不動、H15/H16 才是值欄位（原寫到標籤格覆蓋「戶籍電話」/「住家電話」字）
- **喬美 PDF 居住狀況**：父母勾「親友」格（不再誤勾配偶）
- **亞太 adminB**：at_industry / at_role 下拉加 `selected` 屬性、避免重開頁變「請選擇」
- **第一 Y13/Y14**：保密/無可知情→填「是」；可知情/空→清空（下拉只接受「是」或空白）
- **公司電話 mobile 前綴剝除**：客戶資料卡 area='mobile' 時只顯示號碼（不是「mobile-0966937779」）
- **發證地下拉補舊制縣**：加「北縣/桃縣/中縣/南縣/高縣」（身分證民國早期還會用）
- **adminB 年資 <1 強制 1 時月份同步歸 0**：避免 0年10月 → 1年10月 超過 1 年

### 匯入既有客戶
- `_import_parse.py` 本地跑、從 LINE 聊天記錄（`[LINE]勞工紓困貸-鉅烽.txt`）抓 4 月活案 53 筆，輸出 `apr_loan_import.json`
- 含身分證/送件順序/核准金額/撥款日/每家獨立備註（寫入 customers.company_status）
- 日報 build_date 覆寫 `created_at`（原建案日、非匯入日）
- **密碼初始化：** `init_settings()` 只在首次運行時設定預設密碼 hash

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

## 2026/05/01 案件判別功能（規則查表助手）

### 在解什麼問題
- 行政判太慢（各家規則太多記不住）
- 只有 1 個老行政會、新人想學
- 用「規則表」自動比對客戶條件 vs 各家公司規則、列出可送/不可送 + 理由

### DB 新增欄位（customers）
- `eval_labor_amount` — 勞保投保金額（萬）
- `vehicle_model` — 車款（如 YAMAHA SMAX）
- `vehicle_year` — 出廠年份（西元 2019 或民國 108 都可、自動轉）

### /new-customer + /edit-pending 新欄位
「貸款諮詢事項」區塊加：
- 勞保投保金額（萬）：`elabor_amt`
- 車款：`vmodel`
- 出廠年份：`vyear`

### 規則表結構（PLAN_ELIGIBILITY_RULES）

每家公司一筆 dict、按 priority 排序（金額大、容易過件 → 高 priority）

```python
{
  "company": "亞太機車25萬",
  "max_amount": 25,
  "priority": 75,
  "rules": [...],
  "required_docs": [...],
  "bonus_items": [...],  # 沒給就用 COMMON_BONUS_ITEMS
}
```

### Rule type 三類

| type | 用途 | 行為 |
|------|------|------|
| `simple` | 系統可自動比對的條件 | 自動 ✅/❌ |
| `oneof` | 擇一達成（任一 ✅ 整個 ✅）| 內部選項任一 pass 就 pass |
| `manual` | 系統沒此欄位、靠人 | 一律 ⚠️ |

### simple op 列表

| op | 用法 |
|----|------|
| `between` | `[lo, hi]` 範圍（年齡）|
| `tw_id` | 中華民國身分證格式（居留證自動 fail）|
| `year_age_le` | 從出廠年份算車齡 ≤ N |
| `in` | actual 在 value list 內 |
| `contains` | actual 含 value 字串、可加 exclude_field/exclude_value |
| `not_contains` | actual 不含 value（字串或字串列表）|
| `=` `>=` `<=` | 一般比較、空值視為 0（>= / <=）|
| `has_dynbao` / `not_has_dynbao` | 從 debt_list 看有無動保（區分二車貸款）|
| `creditcard_good` | eval_credit_card 判：有銀行名 ✅、卡循/協商 ❌、空 unknown |
| `creditcard_no_bad` | 只要無紅線（卡循/協商）就 pass |

### manual_check 修飾

simple op pass 時、如有 `manual_check` 欄位 → 改回「⚠️ 自動✓ + 需人工確認 XXX」。
用於「自動可知道但仍需人看」的情境（如：勞保狀態 ✓、需確認滿半年）。

### 已完成 9 家方案

```
priority   公司           金額  特點
─────────────────────────────────────
78         21機車25萬     25    嚴格（20~50、車齡 ≤15、財務 3 選 1）
75         亞太機車25萬   25    無動保（單車貸款）
73         亞太機車15萬   15    有動保（二車貸款）⭐ auto 動保偵測
70         亞太工會機車   15    工會保
65         亞太商品       12    無車齡限制
62         和裕機車       15    車齡 ≤20、信用卡 auto 判
60         和裕商品       12    信用卡 auto 判
55         21商品         12    寬鬆（18~60 軟性、55+ 補保人）
53         21機車12萬     12    同上
```

### 共用紅線

#### 亞太系列共用紅線（APT_RED_LINES）
- 罰單金額 ≤ 3 萬（auto）
- 無酒駕/毒駕（manual）
- 近三月沒送過亞太（auto、`eval_sent_3m_detail not_contains 亞太`）
- 中華民國身分證（每家規則內、tw_id）

#### 加分項（COMMON_BONUS_ITEMS、所有方案共用、不影響判別）
- 信用卡（不能卡循/遲繳/強制停卡）
- 證照、上市櫃任職、股票/基金、定存/存款

### UI（/case-edit + /adminb）

按「📋 對照規則表」黃色按鈕、AJAX `GET /api/check-eligibility?case_id=xxx` → 顯示：
- 每家方案排序好的清單（可送 → 不可送、金額大優先）
- 各條規則 ✅/❌/⚠️ + 實際值
- 📎 必要文件
- ✨ 加分項

### 重要原則（用戶教過、不要再搞混）⚠️

1. **「擇一」就是擇一**：oneof 任一 ✅ 整個 ✅、不要把每項都當紅線
2. **加分項 ≠ 紅線**：信用卡有遲繳只是加分無效、不能擋整個方案
3. **信用卡是和裕「財力擇一」其中一項**、不是全部方案的紅線
4. **「跳過」≠「婉拒」**：照會跳過 current 時、advance_route_to 用 status="跳過"
5. **金額對齊 PLAN_INFO**、不要亂打（亞太工會 15、亞太商品 12、21機25萬 25）
6. **「亞太婉拒過 → 不送和裕」**：目前用 manual hint、未來可加 auto 檢查 route_plan history

### 下一步（pending）
- 等用戶教其他公司規則（喬美 / 貸救補 / 第一 / 麻吉 / 分貝 / 興達 / 合信）
- 整合到 /adminb 加同樣判別按鈕
- 加 conditional 規則（如「45+ 才看勞保」）
- 自動偵測「亞太婉拒過 → 和裕直接 fail」

---

## 車貸專有名詞（2026/05/02 用戶教）

| 名詞 | 意思 | 對應 |
|------|------|------|
| **原融** | 原車融資 = 機車**沒有貸款**的意思 | `not_has_dynbao`（debt_list 無動保）|
| **他行代償** | 別家有貸款、要代償其他公司前貸 | 看 debt_list 是否有他家車貸 |
| **借新還舊** | 原本有貸款在同一家、再貸一次（同公司）| 業務情境、無特定 op |

### 規則表常見用法

- 21機車：「機車無貸款」= 原融（用戶教法）
- 亞太機車25萬：「代償專案、原機車有貸款須前貸是 中租/和潤/裕融」= 他行代償
- 亞太機車15萬：「客戶有動保 + 要貸的是另一台無貸款」= 二車情境（不是借新還舊）

### 規則表對應寫法

```python
# 原融（無貸款）
{"op": "not_has_dynbao"}

# 他行代償（有貸款 + 限定前貸公司）
# 目前 manual、未來可加 op：has_dynbao_from_companies
```
