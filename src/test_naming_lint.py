from __future__ import annotations

import unittest

from src.naming_lint import analyze_pinyin_tokens, validate_slot_name


class NamingLintTest(unittest.TestCase):
    def test_detects_mostly_pinyin_name(self) -> None:
        issues = validate_slot_name("hu_ji_di_pai_chu_suo_bian_ma")
        self.assertTrue(any("拼音直译" in issue for issue in issues))

    def test_keeps_semantic_english_name(self) -> None:
        issues = validate_slot_name("household_police_station_code")
        self.assertEqual(issues, [])

    def test_pinyin_ratio_for_region_name(self) -> None:
        analysis = analyze_pinyin_tokens("sheng_shi_xian_qu")
        self.assertTrue(analysis["is_mostly_pinyin"])
        self.assertGreaterEqual(analysis["pinyin_ratio"], 0.6)


if __name__ == "__main__":
    unittest.main()
