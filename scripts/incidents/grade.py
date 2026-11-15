#!/usr/bin/env python3
"""claude -p 진단 리포트를 정답표(docs/incidents/answer-key.md 7절) 기준으로 채점한다.

    grade.py <raw.json> <report.json 저장 경로> <scenario> [scores.jsonl]

raw.json 은 `claude -p --output-format json --json-schema ...` 의 출력이다.
사람이 읽는 결과는 stdout 으로, 기계가 읽는 한 줄 요약은 scores.jsonl 에 덧붙인다(적중률 집계용).
"""
import json
import sys

# 시나리오별 정답. component/category 는 diagnose.sh 의 JSON Schema enum 값이다.
# confusions: 1순위가 이 (component, category)면 정답표 "혼동하기 쉬운 오답"에 해당한다.
EXPECTED = {
    "S1": {
        "is_incident": True,
        "components": {"payment-service"},
        "categories": {"service_down"},
        "required_sources": {"health", "db"},
        "confusions": {("payment-service", "config_error"): "S3(토픽 오설정)와 혼동"},
    },
    "S2": {
        "is_incident": True,
        # 깨진 payload를 만든 쪽(order-service Outbox)과 처리하지 못한 쪽(inventory-service) 모두 인정
        "components": {"order-service", "inventory-service"},
        "categories": {"poison_message"},
        "required_sources": {"dlq"},
        "confusions": {("inventory-service", "business_failure"): "S4(재고 부족)와 혼동",
                       ("inventory-service", "code_defect"): "부수 증상(보상 IOOBE)을 원인으로 봄"},
    },
    "S3": {
        "is_incident": True,
        "components": {"payment-service"},
        "categories": {"config_error"},
        "required_sources": {"db", "logs"},
        "confusions": {("payment-service", "service_down"): "S1(서비스 중단)과 혼동",
                       ("kafka", "broker_issue"): "브로커 장애로 오판"},
    },
    "S4": {
        "is_incident": False,
        "components": {"inventory-service", "none"},
        "categories": {"business_failure"},
        "required_sources": {"db"},
        "confusions": {},
    },
}

LOW_CONFIDENCE = 0.5
OVERCONFIDENT = 0.7


def load_report(raw):
    report = raw.get("structured_output")
    if report is not None:
        return report
    # 구조화 출력이 없으면 result 텍스트에서 JSON 객체를 찾아 본다
    text = raw.get("result") or ""
    start, end = text.find("{"), text.rfind("}")
    if start < 0 or end < start:
        raise ValueError(f"리포트 JSON을 찾지 못했습니다. result 앞부분: {text[:200]!r}")
    try:
        return json.loads(text[start:end + 1])
    except json.JSONDecodeError as e:
        raise ValueError(f"result의 JSON을 해석하지 못했습니다: {e}") from e


def grade(report, exp):
    candidates = report.get("candidates") or []
    if not candidates:
        # 스키마는 최소 1개를 요구하지만 result 텍스트에서 꺼낸 리포트는 검증을 거치지 않는다
        return {
            "hit": False, "top": "-",
            "expected": f"{'|'.join(sorted(exp['components']))}/{'|'.join(sorted(exp['categories']))}",
            "is_incident": report.get("is_incident"), "evidence_ok": False,
            "missing_sources": sorted(exp["required_sources"]), "cited_sources": [],
            "confidence": None, "flags": ["원인 후보 없음"],
        }
    top = min(candidates, key=lambda c: c["rank"])
    incident_ok = report["is_incident"] == exp["is_incident"]
    cause_ok = top["component"] in exp["components"] and top["category"] in exp["categories"]
    hit = incident_ok and cause_ok
    sources = {e["source"] for e in top["evidence"]}
    missing = exp["required_sources"] - sources
    conf = top["confidence"]

    flags = []
    if not incident_ok:
        flags.append("오탐(장애 아님을 장애로 보고)" if exp["is_incident"] is False else "미탐(장애를 정상으로 판단)")
    confusion = exp["confusions"].get((top["component"], top["category"]))
    if confusion:
        flags.append(confusion)
    if hit and conf < LOW_CONFIDENCE:
        flags.append(f"확신도 낮음({conf})")
    if not hit and conf >= OVERCONFIDENT:
        flags.append(f"과신({conf})" + (" + 오탐, 최대 감점" if exp["is_incident"] is False and report["is_incident"] else ""))

    return {
        "hit": hit,
        "top": f"{top['component']}/{top['category']}",
        "expected": f"{'|'.join(sorted(exp['components']))}/{'|'.join(sorted(exp['categories']))}",
        "is_incident": report["is_incident"],
        "evidence_ok": not missing,
        "missing_sources": sorted(missing),
        "cited_sources": sorted(sources),
        "confidence": conf,
        "flags": flags,
    }


USAGE = "usage: grade.py <raw.json> <report.json 저장 경로> <scenario> [scores.jsonl]"


def main():
    if len(sys.argv) not in (4, 5):
        print(USAGE, file=sys.stderr)
        sys.exit(64)
    raw_path, report_path, scenario = sys.argv[1:4]
    scores_path = sys.argv[4] if len(sys.argv) > 4 else None
    with open(raw_path, encoding="utf-8") as f:
        raw = json.load(f)
    try:
        report = load_report(raw)
    except ValueError as e:
        print(f"error: {e}", file=sys.stderr)
        sys.exit(1)
    with open(report_path, "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2)

    print(f"요약: {report['summary']}")
    print(f"장애 여부: {report['is_incident']}")
    for c in sorted(report.get("candidates") or [], key=lambda c: c["rank"]):
        print(f"\n#{c['rank']} [{c['component']} / {c['category']}] 확신도 {c['confidence']}")
        print(f"  원인: {c['cause']}")
        for e in c["evidence"]:
            print(f"  - ({e['source']}) {e['detail']}")

    print("\n--- 채점 ---")
    exp = EXPECTED.get(scenario)
    if exp is None:
        print(f"{scenario}: 채점 기준 없음")
        return
    g = grade(report, exp)
    print(f"원인 적중: {'O' if g['hit'] else 'X'} (1순위 {g['top']}, 정답 {g['expected']}, 장애 여부 {g['is_incident']})")
    print(f"필수 증거 출처: {'충족' if g['evidence_ok'] else '누락 ' + ','.join(g['missing_sources'])}"
          f" (인용 {','.join(g['cited_sources'])})")
    print(f"확신도: {g['confidence']}")
    print(f"주의: {', '.join(g['flags']) if g['flags'] else '없음'}")
    usage = raw.get("usage", {})
    print(f"턴 수: {raw.get('num_turns')}, 소요: {raw.get('duration_ms')}ms, 출력 토큰: {usage.get('output_tokens')}")

    if scores_path:
        line = {"scenario": scenario, "raw": raw_path, "model": sorted(raw.get("modelUsage", {}).keys()),
                "num_turns": raw.get("num_turns"), "duration_ms": raw.get("duration_ms"), **g}
        with open(scores_path, "a", encoding="utf-8") as f:
            f.write(json.dumps(line, ensure_ascii=False) + "\n")


if __name__ == "__main__":
    main()
