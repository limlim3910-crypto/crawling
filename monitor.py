import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import os
import json
import hashlib
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urljoin

import requests
import urllib3
from bs4 import BeautifulSoup

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
print("### BUSAN HTML VERSION ###")

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
SEEN_FILE = DATA_DIR / "seen.json"
RESULTS_FILE = DATA_DIR / "results.json"

SITES = [
    {
        "site_name": "부산시",
        "target_url": "https://www.busan.go.kr/nbnews",
        "html_file": "busan_nbnews.html",
        "parser_type": "busan_table",
    },
    {
        "site_name": "울산시",
        "target_url": "https://www.ulsan.go.kr/u/rep/main.ulsan",
        "html_file": "ulsan_main.html",
        "parser_type": "ulsan_main",
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

def send_email(new_items):
    if not new_items:
        print("신규 항목 없음 -> 이메일 발송 생략")
        return

    mail_user = os.getenv("MAIL_USERNAME", "").strip()
    mail_password = os.getenv("MAIL_PASSWORD", "").strip()
    mail_to = os.getenv("MAIL_TO", "").strip()

    if not mail_user or not mail_password or not mail_to:
        print("메일 설정값 없음 -> 이메일 발송 생략")
        return

    subject = f"[행사 모니터링] 부산시 신규 공지 {len(new_items)}건"

    lines = []
    lines.append("부산시 신규 행사/공지 감지 결과")
    lines.append("")

    for idx, item in enumerate(new_items, start=1):
        lines.append(f"{idx}. [{item['site_name']}] {item['title']}")
        lines.append(f"- 작성일: {item['published']}")
        lines.append(f"- 부서: {item['department']}")
        lines.append(f"- 링크: {item['link']}")
        lines.append("")

    body = "\n".join(lines)

    msg = MIMEMultipart()
    msg["From"] = mail_user
    msg["To"] = mail_to
    msg["Subject"] = subject
    msg.attach(MIMEText(body, "plain", "utf-8"))

    with smtplib.SMTP("smtp.gmail.com", 587) as server:
        server.starttls()
        server.login(mail_user, mail_password)
        server.sendmail(mail_user, [mail_to], msg.as_string())

    print("이메일 발송 완료")

def ensure_files():
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    if not SEEN_FILE.exists():
        SEEN_FILE.write_text("[]", encoding="utf-8")

    if not RESULTS_FILE.exists():
        RESULTS_FILE.write_text("[]", encoding="utf-8")


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
    """
    부산시 통합 공지사항 목록 표에서 행을 추출한다.
    최대한 구조 변화에 덜 민감하도록 일반적인 table/tr/td 형태를 우선 사용한다.
    """
    rows = []

    # 가장 일반적인 패턴
    for tr in soup.select("table tbody tr"):
        cells = tr.find_all("td")
        if len(cells) < 4:
            continue

        link_tag = tr.find("a", href=True)
        if not link_tag:
            continue

        title = normalize_text(link_tag.get_text(" ", strip=True))
        link = urljoin(site["target_url"], link_tag["href"])

        # 보통 컬럼 구조: 순번 / 제목 / 첨부 / 부서명 / 작성일 / 조회수
        department = normalize_text(cells[-3].get_text(" ", strip=True)) if len(cells) >= 3 else ""
        published = normalize_text(cells[-2].get_text(" ", strip=True)) if len(cells) >= 2 else ""

        rows.append({
            "title": title,
            "link": link,
            "department": department,
            "published": published,
        })

    return rows


def parse_rows_fallback(soup, site):
    """
    table tbody tr 파싱이 안 될 경우를 대비한 백업 로직.
    """
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

        # 뒤쪽 셀에서 날짜/부서명 추정
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
        })

    # 중복 제거
    unique = {}
    for item in rows:
        unique[(item["title"], item["link"])] = item

    return list(unique.values())

def parse_rows_ulsan_main(soup, site):
    rows = []

    for link_tag in soup.find_all("a", href=True):
        title = normalize_text(link_tag.get_text(" ", strip=True))
        href = link_tag.get("href", "").strip()

        if not title or len(title) < 4:
            continue

        if not href:
            continue

        link = urljoin(site["target_url"], href)

        allow_patterns = [
            "/bbs/view.ulsan",
            "/testPblanc/view.ulsan",
        ]

        if not any(pattern in link for pattern in allow_patterns):
            continue

        rows.append({
            "title": title,
            "link": link,
            "department": site["site_name"],
            "published": "",
        })

    unique = {}
    for item in rows:
        unique[(item["title"], item["link"])] = item

    return list(unique.values())
    

def collect_entries():
    collected = []

    for site in SITES:
        print(f"사이트 처리 중: {site['site_name']}")
        html = fetch_page(site)
        soup = BeautifulSoup(html, "lxml")

        parser_type = site.get("parser_type", "")

        if parser_type == "busan_table":
            rows = parse_rows_from_table(soup, site)
            if not rows:
                print(f"[{site['site_name']}] 기본 table 파싱 실패, fallback 파싱 시도")
                rows = parse_rows_fallback(soup, site)

        elif parser_type == "ulsan_main":
            rows = parse_rows_ulsan_main(soup, site)

        else:
            print(f"[{site['site_name']}] 알 수 없는 parser_type: {parser_type}")
            rows = []

        print(f"[{site['site_name']}] 수집된 행 수(필터 전): {len(rows)}")

        for row in rows:
            merged_text = f"{row['title']} {row['department']}"
            if not matches_keywords(merged_text):
                continue

            item_id = make_item_id(row["title"], row["link"])

            collected.append({
                "id": item_id,
                "site_name": site["site_name"],
                "title": row["title"],
                "link": row["link"],
                "department": row["department"],
                "published": row["published"],
                "source": site["target_url"],
                "collected_at_utc": datetime.now(timezone.utc).isoformat()
            })

    unique = {}
    for item in collected:
        unique[item["id"]] = item

    return list(unique.values())


def main():
    ensure_files()

    seen = load_seen()
    all_items = collect_entries()

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
    send_email(new_items)
    print(f"전체 감지 항목 수: {len(all_items)}")
    print(f"신규 항목 수: {len(new_items)}")

    for item in new_items[:20]:
        print(f"- [{item['site_name']}] {item['title']}")
        print(" ", item["published"], "|", item["department"])
        print(" ", item["link"])


if __name__ == "__main__":
    main()
