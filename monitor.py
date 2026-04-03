import json
import hashlib
from datetime import datetime, timezone
from pathlib import Path

import feedparser

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
FEEDS_FILE = BASE_DIR / "feeds.txt"
SEEN_FILE = DATA_DIR / "seen.json"
RESULTS_FILE = DATA_DIR / "results.json"

KEYWORDS = [
    "부산", "울산", "경남", "경상남도",
    "창원", "김해", "양산", "진주", "거제", "통영",
    "축제", "행사", "공연", "페스티벌", "콘서트", "초대가수"
]


def ensure_files():
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    if not SEEN_FILE.exists():
        SEEN_FILE.write_text("[]", encoding="utf-8")

    if not RESULTS_FILE.exists():
        RESULTS_FILE.write_text("[]", encoding="utf-8")


def load_seen():
    try:
        return set(json.loads(SEEN_FILE.read_text(encoding="utf-8")))
    except Exception:
        return set()


def save_seen(seen_set):
    SEEN_FILE.write_text(
        json.dumps(sorted(list(seen_set)), ensure_ascii=False, indent=2),
        encoding="utf-8"
    )


def save_results(results):
    RESULTS_FILE.write_text(
        json.dumps(results, ensure_ascii=False, indent=2),
        encoding="utf-8"
    )


def load_feed_urls():
    if not FEEDS_FILE.exists():
        raise FileNotFoundError("feeds.txt 파일이 없습니다.")

    urls = []
    for line in FEEDS_FILE.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        urls.append(line)
    return urls


def normalize_text(value):
    if not value:
        return ""
    return " ".join(str(value).split())


def make_item_id(title, link):
    raw = f"{title}|{link}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def matches_keywords(text):
    text_lower = text.lower()
    for keyword in KEYWORDS:
        if keyword.lower() in text_lower:
            return True
    return False


def parse_published(entry):
    candidates = [
        entry.get("published"),
        entry.get("updated"),
        entry.get("created"),
    ]
    for value in candidates:
        if value:
            return normalize_text(value)
    return ""


def collect_entries(feed_urls):
    collected = []

    for url in feed_urls:
        parsed = feedparser.parse(url)

        for entry in parsed.entries:
            title = normalize_text(entry.get("title", ""))
            link = normalize_text(entry.get("link", ""))
            summary = normalize_text(entry.get("summary", ""))
            published = parse_published(entry)

            merged_text = f"{title} {summary}"

            if not matches_keywords(merged_text):
                continue

            item_id = make_item_id(title, link)

            collected.append({
                "id": item_id,
                "title": title,
                "link": link,
                "published": published,
                "summary": summary,
                "source_feed": url,
                "collected_at_utc": datetime.now(timezone.utc).isoformat()
            })

    unique = {}
    for item in collected:
        unique[item["id"]] = item

    return list(unique.values())


def main():
    ensure_files()

    seen = load_seen()
    feed_urls = load_feed_urls()
    all_items = collect_entries(feed_urls)

    new_items = [item for item in all_items if item["id"] not in seen]

    for item in new_items:
        seen.add(item["id"])

    save_seen(seen)
    save_results(all_items)

    print(f"전체 감지 항목 수: {len(all_items)}")
    print(f"신규 항목 수: {len(new_items)}")

    for item in new_items[:20]:
        print("-", item["title"])
        print(" ", item["link"])


if __name__ == "__main__":
    main()
