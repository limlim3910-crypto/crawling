import os
import re
import json
import hashlib
from datetime import datetime
from zoneinfo import ZoneInfo
from pathlib import Path
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

print("### MONITOR PIPELINE ###")

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
SEEN_FILE = DATA_DIR / "seen.json"
RESULTS_FILE = DATA_DIR / "results.json"
NEW_ITEMS_FILE = DATA_DIR / "new_items.json"

SITES = [
    {
        "site_name": "부산시",
        "target_url": "https://www.busan.go.kr/nbnews",
        "html_file": "busan_nbnews.html",
        "parser_type": "busan_table",
    },
    {
        "site_name": "경상남도",
        "target_url": "https://festa.gyeongnam.go.kr/",
        "html_file": "gyeongnam_festa.html",
        "parser_type": "gyeongnam_festa",
    }
]

HEADERS = {
    "User-Agent": "Mozilla/5.0"
}

KEYWORDS = [
    "행사", "축제", "공연", "페스티벌", "콘서트", "초대가수",
    "모집", "체험", "전시", "개최", "수련원", "박람회", "문화"
]


def ensure_files():
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    if not SEEN_FILE.exists():
        SEEN_FILE.write_text("{}", encoding="utf-8")

    if not RESULTS_FILE.exists():
        RESULTS_FILE.write_text("[]", encoding="utf-8")

    if not NEW_ITEMS_FILE.exists():
        NEW_ITEMS_FILE.write_text("[]", encoding="utf-8")


def load_seen():
    try:
        data = json.loads(SEEN_FILE.read_text(encoding="utf-8"))

        if isinstance(data, dict):
            normalized = {}
            for site_name, ids in data.items():
                if isinstance(ids, list):
                    normalized[site_name] = set(ids)
                else:
                    normalized[site_name] = set()
            return normalized

        return {}

    except Exception:
        return {}


def save_seen(seen_dict):
    serializable = {}

    for site_name, ids in seen_dict.items():
        serializable[site_name] = sorted(list(ids))

    SEEN_FILE.write_text(
        json.dumps(serializable, ensure_ascii=False, indent=2),
        encoding="utf-8"
    )


def save_results(results):
    RESULTS_FILE.write_text(
        json.dumps(results, ensure_ascii=False, indent=2),
        encoding="utf-8"
    )


def save_new_items(items):
    NEW_ITEMS_FILE.write_text(
        json.dumps(items, ensure_ascii=False, indent=2),
        encoding="utf-8"
    )


def normalize_text(value):
    if not value:
        return ""
    return " ".join(str(value).split())

def extract_address_from_festival(festival: dict) -> str:
    candidate_keys = [
        "address",
        "addr",
        "roadAddr",
        "jibunAddr",
        "place",
        "placeNm",
        "placeName",
        "eventPlace",
        "festivalPlace",
        "location",
        "locationName",
        "venue",
        "venueName",
        "spot",
        "spotName",
    ]

    for key in candidate_keys:
        value = normalize_text(festival.get(key, ""))
        if value:
            return value

    return ""



def make_item_id(title, link):
    raw = f"{title}|{link}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def matches_keywords(text):
    text_lower = text.lower()
    for keyword in KEYWORDS:
        if keyword.lower() in text_lower:
            return True
    return False


def extract_date_range_from_published(published: str):
    if not published:
        return None, None

    matches = re.findall(r"\d{4}-\d{2}-\d{2}", published)
    if len(matches) >= 2:
        try:
            start_date = datetime.strptime(matches[0], "%Y-%m-%d").date()
            end_date = datetime.strptime(matches[1], "%Y-%m-%d").date()
            return start_date, end_date
        except Exception:
            return None, None

    return None, None


def is_expired_event(published: str) -> bool:
    start_date, end_date = extract_date_range_from_published(published)

    # 기간형 문자열이 아니면 종료 행사 판정 안 함
    if start_date is None or end_date is None:
        return False

    today_kst = datetime.now(ZoneInfo("Asia/Seoul")).date()
    return end_date < today_kst


def fetch_page(site):
    html_file = site.get("html_file", "").strip()

    if html_file:
        html_path = Path(html_file)
        if html_path.exists():
            print(f"[{site['site_name']}] 로컬 HTML 파일 사용: {html_file}")
            return html_path.read_text(encoding="utf-8", errors="ignore")
        else:
            print(f"[{site['site_name']}] HTML 파일 없음: {html_file}")

    print(f"[{site['site_name']}] 원격 페이지 요청: {site['target_url']}")
    response = requests.get(site["target_url"], headers=HEADERS, timeout=30)
    response.raise_for_status()
    return response.text


def parse_rows_from_table(soup, site):
    rows = []

    for tr in soup.select("table tbody tr"):
        cells = tr.find_all("td")
        if len(cells) < 4:
            continue

        link_tag = tr.find("a", href=True)
        if not link_tag:
            continue

        title = normalize_text(link_tag.get_text(" ", strip=True))
        link = urljoin(site["target_url"], link_tag["href"])

        department = normalize_text(cells[-3].get_text(" ", strip=True)) if len(cells) >= 3 else ""
        published = normalize_text(cells[-2].get_text(" ", strip=True)) if len(cells) >= 2 else ""

        rows.append({
            "title": title,
            "link": link,
            "department": department,
            "published": published,
            "address": "",
        })

    return rows


def parse_rows_fallback(soup, site):
    rows = []

    for tr in soup.find_all("tr"):
        link_tag = tr.find("a", href=True)
        if not link_tag:
            continue

        title = normalize_text(link_tag.get_text(" ", strip=True))
        href = link_tag.get("href", "").strip()
        if not title or not href:
            continue

        link = urljoin(site["target_url"], href)

        texts = [normalize_text(td.get_text(" ", strip=True)) for td in tr.find_all(["td", "th"])]
        texts = [t for t in texts if t]

        published = ""
        department = ""

        for t in reversed(texts):
            if len(t) == 10 and t[4] == "-" and t[7] == "-":
                published = t
                break

        if published and published in texts:
            idx = texts.index(published)
            if idx - 1 >= 0:
                department = texts[idx - 1]

        rows.append({
            "title": title,
            "link": link,
            "department": department,
            "published": published,
            "address": "",
        })

    unique = {}
    for item in rows:
        unique[(item["title"], item["link"])] = item

    return list(unique.values())


def fetch_gyeongnam_festival_api(site):
    api_url = urljoin(site["target_url"], "/api/callFestivalList.do")

    params = {
        "sigunguCode": "",
    }

    response = requests.get(api_url, headers=HEADERS, params=params, timeout=30)
    response.raise_for_status()

    print(f"[{site['site_name']}] API 상태코드: {response.status_code}")
    print(f"[{site['site_name']}] API Content-Type: {response.headers.get('Content-Type', '')}")

    try:
        data = response.json()
    except Exception as e:
        print(f"[{site['site_name']}] API JSON 파싱 실패: {e}")
        return []

    if isinstance(data, list):
        print(f"[{site['site_name']}] API list 응답 감지")
        return data

    if isinstance(data, dict):
        print(f"[{site['site_name']}] API dict keys: {list(data.keys())[:20]}")

        for key in ["resultData", "data", "list", "items", "result", "body"]:
            if key not in data:
                continue

            value = data[key]

            if isinstance(value, list):
                print(f"[{site['site_name']}] 리스트 키 발견: {key}")
                return value

            if isinstance(value, str):
                try:
                    parsed_value = json.loads(value)
                    if isinstance(parsed_value, list):
                        print(f"[{site['site_name']}] 문자열 JSON 리스트 키 발견: {key}")
                        return parsed_value
                    if isinstance(parsed_value, dict):
                        for nested_key in ["resultData", "data", "list", "items"]:
                            if nested_key in parsed_value and isinstance(parsed_value[nested_key], list):
                                print(f"[{site['site_name']}] 문자열 JSON 중첩 리스트 키 발견: {key}.{nested_key}")
                                return parsed_value[nested_key]
                except Exception:
                    pass

            if isinstance(value, dict):
                for nested_key in ["resultData", "data", "list", "items"]:
                    if nested_key in value and isinstance(value[nested_key], list):
                        print(f"[{site['site_name']}] 중첩 리스트 키 발견: {key}.{nested_key}")
                        return value[nested_key]

    print(f"[{site['site_name']}] API 응답 구조를 해석하지 못함")
    return []

def debug_fetch_detail_page_for_address(url: str):
    try:
        response = requests.get(url, headers=HEADERS, timeout=30)
        response.raise_for_status()
        html = response.text

        print("===== 상세페이지 주소 디버그 시작 =====")
        print(f"URL: {url}")
        print(f"HTML 길이: {len(html)}")

        preview_keywords = ["주소", "장소", "위치", "개최장소", "행사장", "오시는길"]
        for keyword in preview_keywords:
            if keyword in html:
                print(f"[키워드 발견] {keyword}")

        soup = BeautifulSoup(html, "lxml")
        text_preview = soup.get_text(" ", strip=True)
        print("텍스트 미리보기:", text_preview[:1000])
        print("===== 상세페이지 주소 디버그 종료 =====")

    except Exception as e:
        print(f"상세페이지 디버그 실패: {e}")



def build_gyeongnam_festival_items_from_api(festival_list, site):
    items = []

    for festival in festival_list:
        site_name = normalize_text(festival.get("siteName", ""))
        link_url = normalize_text(festival.get("linkUrl", ""))
        sub_path = normalize_text(festival.get("subPath", ""))
        start_date = normalize_text(festival.get("festivalStartDate", ""))
        end_date = normalize_text(festival.get("festivalEndDate", ""))
        address = extract_address_from_festival(festival)

        title = site_name
        if not title:
            continue

        if link_url:
            link = link_url
        elif sub_path:
            link = urljoin(site["target_url"], "/" + sub_path.lstrip("/"))
        else:
            link = site["target_url"]

        published = ""
        if start_date or end_date:
            published = f"{start_date} ~ {end_date}".strip(" ~")

        item_id = make_item_id(title, link)

        items.append({
            "id": item_id,
            "site_name": site["site_name"],
            "title": title,
            "link": link,
            "department": site["site_name"],
            "published": published,
            "address": address,
            "source": site["target_url"],
            "collected_at_utc": datetime.now().astimezone().isoformat()
        })

    unique = {}
    for item in items:
        unique[item["id"]] = item

    return list(unique.values())


def collect_entries():
    collected = []

    for site in SITES:
        print(f"사이트 처리 중: {site['site_name']}")
        parser_type = site.get("parser_type", "")

        if parser_type == "busan_table":
            html = fetch_page(site)
            soup = BeautifulSoup(html, "lxml")

            rows = parse_rows_from_table(soup, site)
            if not rows:
                print(f"[{site['site_name']}] 기본 table 파싱 실패, fallback 파싱 시도")
                rows = parse_rows_fallback(soup, site)

        elif parser_type == "gyeongnam_festa":
            html = fetch_page(site)
            _ = BeautifulSoup(html, "lxml")  # 로컬 HTML 존재 확인용
            festival_list = fetch_gyeongnam_festival_api(site)
            print(f"[{site['site_name']}] API 축제 건수: {len(festival_list)}")
            rows = build_gyeongnam_festival_items_from_api(festival_list, site)

        else:
            print(f"[{site['site_name']}] 알 수 없는 parser_type: {parser_type}")
            rows = []

        print(f"[{site['site_name']}] 수집된 행 수(필터 전): {len(rows)}")

        for row in rows:
            merged_text = f"{row['title']} {row['department']}"
            if not matches_keywords(merged_text):
                continue

            if is_expired_event(row["published"]):
                print(f"[{site['site_name']}] 종료된 행사 제외: {row['title']} / {row['published']}")
                continue

            item_id = make_item_id(row["title"], row["link"])

            collected.append({
                "id": item_id,
                "site_name": site["site_name"],
                "title": row["title"],
                "link": row["link"],
                "department": row["department"],
                "published": row["published"],
                "address": row.get("address", ""),
                "source": site["target_url"],
                "collected_at_utc": datetime.now().astimezone().isoformat()
            })

    unique = {}
    for item in collected:
        unique[item["id"]] = item

    return list(unique.values())


def main():
    ensure_files()

    seen = load_seen()
    all_items = collect_entries()
    for item in all_items:
        if item["site_name"] == "경상남도":
            debug_fetch_detail_page_for_address(item["link"])
            break
            
    new_items = []

    for item in all_items:
        site_name = item["site_name"]

        if site_name not in seen:
            seen[site_name] = set()

        if item["id"] not in seen[site_name]:
            new_items.append(item)

    for item in new_items:
        site_name = item["site_name"]
        seen[site_name].add(item["id"])

    save_seen(seen)
    save_results(all_items)
    save_new_items(new_items)

    print(f"전체 감지 항목 수: {len(all_items)}")
    print(f"신규 항목 수: {len(new_items)}")

    for item in new_items[:20]:
        print(f"- [{item['site_name']}] {item['title']}")
        print(" ", item["published"], "|", item["department"])
        if item.get("address"):
            print(" ", "주소:", item["address"])
        print(" ", item["link"])


if __name__ == "__main__":
    main()
