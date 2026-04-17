"""LINE Bot 核心邏輯回歸測試。

執行：pytest tests/ -v

涵蓋已修復的 bug 與新功能：
- 送件順序別名、PLAN_INFO 21 系列、TRANSFER 多公司、待核准不誤判
- 照會金額 notify_amount、送件金額 ≠ 核准金額、@AI 送件金額指令
- 補件/補申覆 待補 vs 已補、公司名不誤判姓名
- 跨群組獨立案 + A 群回貼跳按鈕選擇
"""
import json

import pytest


# ===== 送件順序 & 公司辨識 =====
class TestRouteOrderAndCompany:
    def test_21_maps_to_shangpin(self, tmp_db):
        main, _ = tmp_db
        assert main.PLAN_INFO["21"] == ("21商品", "12萬/24期")
        assert main.PLAN_INFO["21機"] == ("21機車12萬", "12萬/24期")
        assert main.PLAN_INFO["21機25"] == ("21機車25萬", "25萬/48期")

    def test_21_series_all_normalize_to_21(self, tmp_db):
        main, _ = tmp_db
        for co in ["21商品", "21機車12萬", "21機車25萬", "21汽車", "21機", "21機25萬"]:
            assert main.normalize_section(co) == "21", f"{co} 未歸到 21 區塊"

    def test_route_order_alias(self, tmp_db):
        """貸10/鄉/銀/C/商/代 應轉成 canonical 名稱"""
        main, _ = tmp_db
        r = main.parse_route_order_line("4/17-測試乙-貸10/鄉/銀/C/商/代")
        assert r["companies"] == ["貸就補", "鄉民", "銀行", "零卡", "商品貸", "代書"]

    def test_route_order_alias_qiao_and_loan(self, tmp_db):
        """喬 → 喬美；貸救補 → 貸就補（向下相容）"""
        main, _ = tmp_db
        r = main.parse_route_order_line("4/17-郭小名-21/喬/貸10")
        assert r["companies"] == ["21商品", "喬美", "貸就補"]
        # 舊資料用「貸救補」也能歸到貸就補區塊
        assert main.normalize_section("貸救補") == "貸就補"

    def test_route_order_alias_yan_and_dingduo(self, tmp_db):
        """研 → 商品貸；鼎多 → 喬美"""
        main, _ = tmp_db
        r = main.parse_route_order_line("4/17-王甲-研/鼎多")
        assert r["companies"] == ["商品貸", "喬美"]
        # 日報區塊：研 歸到商品貸；鼎多 歸到喬美
        assert main.normalize_section("研") == "商品貸"
        assert main.normalize_section("鼎多") == "喬美"

    def test_21_machine_alias(self, tmp_db):
        """21/21機12萬/21機25萬 轉成完整名稱"""
        main, _ = tmp_db
        r = main.parse_route_order_line("4/15-測試甲-21/21機12萬/21機25萬")
        assert r["companies"] == ["21商品", "21機車12萬", "21機車25萬"]


# ===== TRANSFER 多公司 =====
class TestTransferMultiCompany:
    def test_transfer_parse_a_plus_b(self, tmp_db):
        main, _ = tmp_db
        r = main.parse_transfer_line("4/17-王思婷-轉喬美+麻吉機 @AI")
        assert r["targets"] == ["喬美", "麻吉機"]

    def test_transfer_parse_single(self, tmp_db):
        main, _ = tmp_db
        r = main.parse_transfer_line("8/5-戴君哲-轉21")
        assert r["targets"] == ["21商品"]


# ===== 待核准不誤判 =====
class TestPendingApprovalNotMisjudged:
    def test_update_amount_excludes_pending(self, tmp_db):
        """『待核准』不該走 update_amount 路徑"""
        main, _ = tmp_db
        cmd = main.parse_special_command("吳承諺 第一 待核准 20萬", "g1")
        assert not cmd or cmd.get("type") != "update_amount"

    def test_update_amount_valid(self, tmp_db):
        main, _ = tmp_db
        cmd = main.parse_special_command("吳瑞銘 房地 核准 2000萬", "g1")
        assert cmd["type"] == "update_amount"
        assert cmd["amount"] == "2000萬"

    def test_single_approval_re_excludes_pending(self, tmp_db):
        main, _ = tmp_db
        assert main.SINGLE_APPROVAL_RE.match("04/17-陳某-房地核准50萬")
        assert not main.SINGLE_APPROVAL_RE.match("04/17-吳承諺-第一待核准")


# ===== 照會金額 & 送件金額 =====
class TestNotifyAmount:
    def test_set_notify_command(self, tmp_db):
        """@AI 姓名 公司[+公司] 金額/期數 → set_notify"""
        main, _ = tmp_db
        cmd = main.parse_special_command("吳承諺 亞太+房地 100/24", "g1")
        assert cmd["type"] == "set_notify"
        assert cmd["companies"] == ["亞太", "房地"]
        assert cmd["amount"] == "100" and cmd["period"] == "24"

    def test_set_notify_single_company_with_wan_qi(self, tmp_db):
        main, _ = tmp_db
        cmd = main.parse_special_command("王小明 第一 30萬/24期", "g1")
        assert cmd["type"] == "set_notify"
        assert cmd["companies"] == ["第一"]

    def test_extract_notify_amount_period_tail(self, tmp_db):
        """送件順序尾巴 @AI 前的 N/M 應被抓到"""
        main, _ = tmp_db
        a, p = main.extract_notify_amount_period("4/17-吳承諺-亞太+房地 100/24 @AI")
        assert a == "100" and p == "24"

    def test_extract_not_misjudge_date(self, tmp_db):
        """只有日期沒送件金額時不該誤抓"""
        main, _ = tmp_db
        a, p = main.extract_notify_amount_period("4/17-吳承諺-亞太+房地 @AI")
        assert a is None

    def test_extract_requires_ai_tag(self, tmp_db):
        """無 @AI 觸發不抓（避免干擾日期）"""
        main, _ = tmp_db
        a, p = main.extract_notify_amount_period("4/17-吳承諺-亞太+房地 100/24")
        assert a is None

    def test_notification_text_uses_manual_amount(self, tmp_db):
        main, _ = tmp_db
        r = {
            "customer_name": "吳", "current_company": "第一",
            "notify_amount": "100", "notify_period": "24",
            "live_years": "0", "company_years": "0", "company_salary": "0",
        }
        txt = main.generate_notification_text(r, "第一")
        assert "100萬/24期" in txt

    def test_notification_no_fallback_to_approved(self, tmp_db):
        """核准金額不該 fallback 進照會訊息（送件金額 ≠ 核准金額）"""
        main, _ = tmp_db
        r = {
            "customer_name": "X", "current_company": "21汽車",
            "notify_amount": "", "notify_period": "",
            "approved_amount": "50萬",
            "live_years": "0", "company_years": "0", "company_salary": "0",
        }
        assert "✅50萬" not in main.generate_notification_text(r, "21汽車")


# ===== 照會訊息處理 =====
class TestNotificationBriefing:
    def test_parse_notification_fields_notify_amount(self, tmp_db):
        main, _ = tmp_db
        fields = main.parse_notification_fields("✅30萬/24期")
        assert fields.get("notify_amount") == "30"
        assert fields.get("notify_period") == "24"
        assert "approved_amount" not in fields

    def test_briefing_writes_notify_not_approved(self, tmp_db):
        """A 群貼照會 → 寫 notify_amount 而非 approved_amount；不誤移到待撥款"""
        main, _ = tmp_db
        main.create_customer_record("王小明", "", "第一", "g1", "init")
        brief = "王小明\n照會注意事項\n✅居住5年 自有 年資3年 月薪4萬\n✅30萬/24期\n"
        main.handle_notification_briefing(brief, "g1", "fake_token")
        c = main.find_active_by_name("王小明")[0]
        assert c["notify_amount"] == "30"
        assert c["notify_period"] == "24"
        assert not c["approved_amount"]
        assert c["report_section"] != "待撥款"


# ===== 補件/補申覆 待補 vs 已補 =====
class TestWaitingVsDone:
    @pytest.mark.parametrize("text,expected", [
        ("陳志昇 21 補申覆", "待補申覆"),
        ("補申覆【1】請提供保證人", "待補申覆"),
        ("王小明 第一 補件", "待補資料"),
        ("陳某 補資料", "待補資料"),
        ("某客 補行照", "待補資料"),
        ("某客 補聯徵", "待補資料"),
    ])
    def test_pending_supplement(self, tmp_db, text, expected):
        main, _ = tmp_db
        assert expected in main.extract_status_summary(text, "王某")

    @pytest.mark.parametrize("text,expected", [
        ("王小明 已補件", "已補資料"),
        ("陳某 已補資料", "已補資料"),
        ("陳某 已補申覆", "已補申覆"),
        ("王小明 申覆通過", "已補申覆"),
        ("王小明 補好了", "已補資料"),
        ("某客 補完了", "已補資料"),
    ])
    def test_done_supplement(self, tmp_db, text, expected):
        main, _ = tmp_db
        assert main.extract_status_summary(text, "王某") == expected


# ===== 公司名不誤判姓名 =====
class TestCompanyNameNotAsCustomer:
    @pytest.mark.parametrize("text", [
        "喬美", "麻吉", "麻吉10", "分貝機", "裕融", "裕融100萬/72期",
        "亞太", "和裕", "第一",
    ])
    def test_company_not_as_name(self, tmp_db, text):
        main, _ = tmp_db
        assert main.extract_name(text) == "", f"{text} 被誤判為姓名"

    def test_real_name_extracted(self, tmp_db):
        main, _ = tmp_db
        assert main.extract_name("4/17-王思婷-亞太") == "王思婷"
        assert main.extract_name("吳承諺 亞太 補件") == "吳承諺"


# ===== 跨群組獨立案 + A 群回貼按鈕 =====
class TestCrossGroupIndependentCases:
    def test_find_all_active_by_id_no(self, tmp_db):
        main, _ = tmp_db
        main.create_customer_record("王小明", "A123456789", "亞太", "g_B", "init1")
        main.create_customer_record("王小明", "A123456789", "第一", "g_C", "init2")
        rows = main.find_all_active_by_id_no("A123456789")
        assert len(rows) == 2
        groups = {r["source_group_id"] for r in rows}
        assert groups == {"g_B", "g_C"}

    def test_find_in_group(self, tmp_db):
        main, _ = tmp_db
        main.create_customer_record("王小明", "A123456789", "亞太", "g_B", "init1")
        main.create_customer_record("王小明", "A123456789", "第一", "g_C", "init2")
        bg = main.find_active_by_id_no_in_group("A123456789", "g_B")
        cg = main.find_active_by_id_no_in_group("A123456789", "g_C")
        assert bg["source_group_id"] == "g_B"
        assert cg["source_group_id"] == "g_C"
        assert bg["case_id"] != cg["case_id"]


# ===== DB schema =====
class TestDBSchema:
    def test_notify_columns_exist(self, tmp_db):
        main, path = tmp_db
        import sqlite3
        conn = sqlite3.connect(path)
        cols = [r[1] for r in conn.execute("PRAGMA table_info(customers)").fetchall()]
        conn.close()
        assert "notify_amount" in cols
        assert "notify_period" in cols
        assert "adminb_21car_amount" in cols


# ===== 多公司轉送（同送）日報雙區塊 =====
class TestMultiCompanyTransferShowsBothSections:
    def test_double_section(self, tmp_db):
        main, path = tmp_db
        import sqlite3
        main.create_customer_record("王思婷", "", "亞太", "g_test", "init",
                                    route_plan=main.make_route_json(["亞太", "和裕"]),
                                    current_company="亞太")
        c = main.find_active_by_name("王思婷")[0]
        # 模擬轉 喬美+麻吉機車 成同送
        main.update_customer(
            c["case_id"],
            route_plan=main.make_route_json(["喬美", "麻吉機車"]),
            current_company="喬美", concurrent_companies="麻吉機車",
            from_group_id="g_test", text="王思婷 轉喬美+麻吉機車",
        )
        conn = sqlite3.connect(path)
        conn.row_factory = sqlite3.Row
        rows = conn.execute("SELECT * FROM customers WHERE status='ACTIVE'").fetchall()
        conn.close()
        smap = main.build_section_map(rows)
        secs_for_wang = [s for s, items in smap.items() if any("王思婷" in it for it in items)]
        assert "喬美" in secs_for_wang
        assert "麻吉" in secs_for_wang
