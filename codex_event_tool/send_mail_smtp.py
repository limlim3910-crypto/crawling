from __future__ import annotations

import argparse
import json
import mimetypes
import os
import smtplib
from email.message import EmailMessage
from pathlib import Path
from typing import Any, Dict, List


BASE_DIR = Path(__file__).resolve().parent
RUN_SUMMARY = BASE_DIR / "run_summary.json"


def clean(value: Any) -> str:
    return str(value or "").strip()


def load_summary(path: Path) -> Dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"run_summary.json 없음: {path}")
    return json.loads(path.read_text(encoding="utf-8"))


def split_addresses(value: str) -> List[str]:
    return [part.strip() for part in value.replace(";", ",").split(",") if part.strip()]


def attach_file(message: EmailMessage, path: Path) -> None:
    if not path.exists():
        return
    mime_type, _ = mimetypes.guess_type(path.name)
    maintype, subtype = (mime_type or "application/octet-stream").split("/", 1)
    message.add_attachment(
        path.read_bytes(),
        maintype=maintype,
        subtype=subtype,
        filename=path.name,
    )


def build_message(summary: Dict[str, Any]) -> EmailMessage:
    mail_from = clean(os.getenv("MAIL_FROM")) or clean(os.getenv("MAIL_USERNAME"))
    mail_to = split_addresses(clean(os.getenv("MAIL_TO")))
    mail_cc = split_addresses(clean(os.getenv("MAIL_CC")))
    subject_prefix = clean(os.getenv("MAIL_SUBJECT_PREFIX")) or "[부울경 행사 특별소통 알림]"

    if not mail_from:
        raise ValueError("MAIL_FROM 또는 MAIL_USERNAME Secret이 필요합니다.")
    if not mail_to:
        raise ValueError("MAIL_TO Secret이 필요합니다.")

    html_path = Path(clean(summary.get("html_path")))
    xlsx_path = Path(clean(summary.get("xlsx_path")))
    html_body = html_path.read_text(encoding="utf-8") if html_path.exists() else "<p>요약 파일을 찾지 못했습니다.</p>"

    new_count = int(summary.get("new_count", 0))
    candidate_count = int(summary.get("candidate_count", 0))
    generated_at = clean(summary.get("generated_at"))

    message = EmailMessage()
    message["From"] = mail_from
    message["To"] = ", ".join(mail_to)
    if mail_cc:
        message["Cc"] = ", ".join(mail_cc)
    message["Subject"] = f"{subject_prefix} 신규 {new_count}건"
    message.set_content(
        f"부울경 행사 모니터링 결과\n\n"
        f"- 생성시각: {generated_at}\n"
        f"- 전체 후보: {candidate_count}건\n"
        f"- 신규 감지: {new_count}건\n\n"
        f"HTML 메일이 보이지 않으면 첨부된 관리대장 엑셀을 확인하세요.",
        charset="utf-8",
    )
    message.add_alternative(html_body, subtype="html")
    attach_file(message, xlsx_path)
    return message


def send_message(message: EmailMessage) -> None:
    username = clean(os.getenv("MAIL_USERNAME"))
    password = clean(os.getenv("MAIL_PASSWORD"))
    smtp_host = clean(os.getenv("SMTP_HOST")) or "smtp.gmail.com"
    smtp_port = int(clean(os.getenv("SMTP_PORT")) or "587")
    smtp_tls = clean(os.getenv("SMTP_USE_TLS") or "true").lower() not in {"0", "false", "no"}

    recipients = split_addresses(message["To"] or "") + split_addresses(message.get("Cc", ""))
    if not recipients:
        raise ValueError("수신자가 없습니다.")

    with smtplib.SMTP(smtp_host, smtp_port, timeout=30) as server:
        if smtp_tls:
            server.starttls()
        if username and password:
            server.login(username, password)
        server.send_message(message, to_addrs=recipients)


def main() -> int:
    parser = argparse.ArgumentParser(description="GitHub Actions용 SMTP 메일 발송")
    parser.add_argument("--summary", default=str(RUN_SUMMARY), help="run_summary.json 경로")
    parser.add_argument("--dry-run", action="store_true", help="메일 발송 없이 검증만 수행")
    parser.add_argument("--send-empty", action="store_true", help="신규 0건이어도 메일 발송")
    args = parser.parse_args()

    summary = load_summary(Path(args.summary))
    new_count = int(summary.get("new_count", 0))
    if new_count == 0 and not args.send_empty:
        print("신규 항목 0건 -> 메일 발송 생략")
        return 0

    message = build_message(summary)
    if args.dry_run:
        print(f"메일 검증 완료: {message['Subject']}")
        return 0

    send_message(message)
    print(f"메일 발송 완료: {message['Subject']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
