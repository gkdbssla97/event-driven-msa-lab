"""grade.py 채점 기준 오프라인 테스트. 구독 사용량 없이 정답/오답 리포트로 판정 로직만 확인한다.

    python3 -m unittest scripts/incidents/test_grade.py
"""
import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(__file__))
from grade import EXPECTED, grade  # noqa: E402


def report(component, category, is_incident=True, confidence=0.9, sources=("db",)):
    return {
        "summary": "test",
        "is_incident": is_incident,
        "candidates": [
            {"rank": 2, "component": "kafka", "category": "unknown", "cause": "-", "confidence": 0.1,
             "evidence": [{"source": "topics", "detail": "-"}]},
            {"rank": 1, "component": component, "category": category, "cause": "-", "confidence": confidence,
             "evidence": [{"source": s, "detail": "-"} for s in sources]},
        ],
    }


class GradeTest(unittest.TestCase):

    def test_S1_정답(self):
        g = grade(report("payment-service", "service_down", sources=("health", "db")), EXPECTED["S1"])
        self.assertTrue(g["hit"])
        self.assertTrue(g["evidence_ok"])
        self.assertEqual(g["flags"], [])

    def test_S1을_토픽_오설정으로_보면_혼동(self):
        g = grade(report("payment-service", "config_error", sources=("health", "db")), EXPECTED["S1"])
        self.assertFalse(g["hit"])
        self.assertIn("S3(토픽 오설정)와 혼동", g["flags"])

    def test_S2는_발행측과_소비측_모두_인정(self):
        for component in ("order-service", "inventory-service"):
            g = grade(report(component, "poison_message", sources=("dlq",)), EXPECTED["S2"])
            self.assertTrue(g["hit"], component)

    def test_S2에서_보상_예외를_원인으로_보면_오답(self):
        g = grade(report("inventory-service", "code_defect", sources=("dlq",)), EXPECTED["S2"])
        self.assertFalse(g["hit"])
        self.assertIn("부수 증상(보상 IOOBE)을 원인으로 봄", g["flags"])

    def test_S3_필수_증거_누락(self):
        g = grade(report("payment-service", "config_error", sources=("db",)), EXPECTED["S3"])
        self.assertTrue(g["hit"])
        self.assertFalse(g["evidence_ok"])
        self.assertEqual(g["missing_sources"], ["logs"])

    def test_S3을_서비스_중단으로_보면_과신_오답(self):
        g = grade(report("payment-service", "service_down", confidence=0.9, sources=("db", "logs")), EXPECTED["S3"])
        self.assertFalse(g["hit"])
        self.assertIn("S1(서비스 중단)과 혼동", g["flags"])
        self.assertIn("과신(0.9)", g["flags"])

    def test_S4_장애_아님(self):
        g = grade(report("inventory-service", "business_failure", is_incident=False), EXPECTED["S4"])
        self.assertTrue(g["hit"])

    def test_S4를_장애로_높은_확신도로_보고하면_최대_감점(self):
        g = grade(report("inventory-service", "code_defect", is_incident=True, confidence=0.8), EXPECTED["S4"])
        self.assertFalse(g["hit"])
        self.assertIn("오탐(장애 아님을 장애로 보고)", g["flags"])
        self.assertIn("과신(0.8) + 오탐, 최대 감점", g["flags"])

    def test_원인은_맞혀도_장애_여부가_틀리면_오답(self):
        g = grade(report("payment-service", "service_down", is_incident=False, sources=("health", "db")),
                  EXPECTED["S1"])
        self.assertFalse(g["hit"])
        self.assertIn("미탐(장애를 정상으로 판단)", g["flags"])

    def test_적중했지만_확신도가_낮으면_표시(self):
        g = grade(report("payment-service", "service_down", confidence=0.3, sources=("health", "db")),
                  EXPECTED["S1"])
        self.assertTrue(g["hit"])
        self.assertIn("확신도 낮음(0.3)", g["flags"])


if __name__ == "__main__":
    unittest.main()
