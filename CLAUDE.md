# CLAUDE.md - 貸款案件管理系統

## 工作規則

- **Agent 自動分配：** Claude 在執行任務時可自由啟動 Agent（Explore、Plan、general-purpose 等）來並行處理子任務，不需要逐一手動批准。可根據任務複雜度自行決定 Agent 數量與類型。

---

## 專案概覽

這是一個 **貸款案件管理系統**，整合 LINE Bot + Web 管理介面，用於追蹤貸款客戶從進件、送件、核准到撥款的完整流程。系統以 FastAPI 為後端，SQLite 為資料庫，部署在 Render 平台上。

**主要檔案：** `main (2).py`（單檔架構，約 4400 行）

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

### 12. 主要業務邏輯（L1490-2019）
- `handle_new_case_block()`：處理新案件建立
- `handle_route_order_block()`：處理送件順序設定
- `handle_a_case_block()`：A 群訊息處理（回貼到業務群、核准金額、婉拒推進）
- `handle_bc_case_block()`：業務群訊息處理（含 @AI 補件回貼 A 群功能）

### 13. 按鈕指令處理（L1664-1799）
處理 Quick Reply 回調：`FORCE_CREATE_NEW|`、`USE_EXISTING_CASE|`、`CONFIRM_TRANSFER|`、`KEEP_OLD_CASE|`、`SELECT_CASE|`、`REOPEN_CASE|`、`CREATE_NEW_CASE|` 等

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
