import os
import json
import smtplib
from pathlib import Path
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
ENRICHED_ITEMS_FILE = DATA_DIR / "enriched_items.json"


def load_enriched_items():
    if not ENRICHED_ITEMS_FILE.exists():
        return []

    try:
        return json.loads(ENRICHED_ITEMS_FILE.read_text(encoding="utf-8"))
    except Exception as e:
        print(f"enriched_items.json 읽기 실패: {e}")
        return []


def build_mail_body(items):
    lines = []
    lines.append("부산/경남 신규 행사/공지 감지 결과")
    lines.append("")

    for idx, item in enumerate(items, start=1):
        ai = item.get("ai", {})
        summary = ai.get("summary", "AI 요약 없음")
        crowd_level = ai.get("crowd_level", "미분류")
        crowd_reason = ai.get("crowd_reason", [])
        network_risk = ai.get("network_risk", [])

        lines.append(f"{idx}. [{item.get('site_name', '')}] {item.get('title', '')}")
        lines.append(f"- 기간/작성일: {item.get('published', '')}")
        lines.append(f"- 부서: {item.get('department', '')}")

        address = (item.get("address") or "").strip()
        if address:
            lines.append(f"- 주소/장소: {address}")

        lines.append(f"- 링크: {item.get('link', '')}")
        lines.append(f"- AI 요약: {summary}")
        lines.append(f"- 예상 운집 수준: {crowd_level}")

        if crowd_reason:
            lines.append("- 운집 판단 근거:")
            for reason in crowd_reason:
                lines.append(f"  • {reason}")

        if network_risk:
            lines.append("- 통신 관점 체크포인트:")
            for risk in network_risk:
                lines.append(f"  • {risk}")

        lines.append("")

    return "\n".join(lines)


def send_email(items):
    if not items:
        print("발송할 항목 없음 -> 이메일 발송 생략")
        return

    mail_user = os.getenv("MAIL_USERNAME", "").strip()
    mail_password = os.getenv("MAIL_PASSWORD", "").strip()
    mail_to = os.getenv("MAIL_TO", "").strip()

    if not mail_user or not mail_password or not mail_to:
        print("메일 설정값 없음 -> 이메일 발송 생략")
        return

    subject = f"[행사 모니터링] 신규 행사/공지 {len(items)}건"
    body = build_mail_body(items)

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


def main():
    items = load_enriched_items()
    send_email(items)


if __name__ == "__main__":
    main()
