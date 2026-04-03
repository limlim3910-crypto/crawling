import os
import json
from pathlib import Path

from openai import OpenAI

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
NEW_ITEMS_FILE = DATA_DIR / "new_items.json"
ENRICHED_ITEMS_FILE = DATA_DIR / "enriched_items.json"

client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))


def load_new_items():
    if not NEW_ITEMS_FILE.exists():
        return []
    try:
        return json.loads(NEW_ITEMS_FILE.read_text(encoding="utf-8"))
    except Exception:
        return []


def save_enriched_items(items):
    ENRICHED_ITEMS_FILE.write_text(
        json.dumps(items, ensure_ascii=False, indent=2),
        encoding="utf-8"
    )


def build_prompt(item):
    return f"""
당신은 지역 행사 모니터링 분석가이다.
아래 행사/공지 정보를 바탕으로, 통신 운용 관점에서 요약과 혼잡도 추정을 수행하라.

[입력 정보]
- 사이트: {item.get("site_name", "")}
- 제목: {item.get("title", "")}
- 기간/작성일: {item.get("published", "")}
- 부서: {item.get("department", "")}
- 링크: {item.get("link", "")}

[출력 규칙]
반드시 JSON만 출력하라.
키는 아래 4개만 사용하라:
- summary: 문자열, 2~3문장
- crowd_level: 문자열, "낮음", "보통", "높음", "매우 높음" 중 하나
- crowd_reason: 문자열 배열, 2~4개
- network_risk: 문자열 배열, 2~4개

[판단 원칙]
- 명확한 정보가 부족하면 보수적으로 추정하라.
- 허위 사실을 만들지 말고, 제목/기간/행사 성격 기반으로 합리적으로 추정하라.
- 통신 품질 관점에서 행사장, 진입로, 주차장, SNS 업로드, 체류 인원 증가 가능성을 고려하라.
""".strip()


def enrich_one_item(item):
    prompt = build_prompt(item)

    response = client.responses.create(
        model="gpt-5.1-mini",
        input=prompt
    )

    output_text = response.output_text.strip()

    try:
        enriched = json.loads(output_text)
    except Exception:
        enriched = {
            "summary": output_text,
            "crowd_level": "보통",
            "crowd_reason": ["응답 JSON 파싱 실패"],
            "network_risk": ["수동 검토 필요"]
        }

    merged = dict(item)
    merged["ai"] = enriched
    return merged


def main():
    items = load_new_items()

    if not items:
        print("신규 항목 없음 -> AI 요약 생략")
        save_enriched_items([])
        return

    enriched_items = []
    for item in items:
        enriched_items.append(enrich_one_item(item))

    save_enriched_items(enriched_items)
    print(f"AI 요약 완료: {len(enriched_items)}건")


if __name__ == "__main__":
    main()
