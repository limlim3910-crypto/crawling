# 부울경 행사 모니터링 GitHub 자동화

GitHub Actions에서 부울경 행사/축제/공연/집회 정보를 자동 확인하고, 신규 건이 있으면 사내메일로 발송하는 프로젝트입니다.

## 바로 쓰는 순서

1. 이 폴더 내용을 새 GitHub 저장소 루트에 올립니다.
2. GitHub 저장소에서 `Settings > Secrets and variables > Actions`로 이동합니다.
3. 아래 Secrets를 등록합니다.
4. `Actions > Event Monitor > Run workflow`를 눌러 수동 실행합니다.
5. 정상 동작하면 이후에는 3시간마다 자동 실행됩니다.

## 필수 Secrets

- `MAIL_USERNAME`: SMTP 로그인 계정
- `MAIL_PASSWORD`: SMTP 비밀번호 또는 앱 비밀번호
- `MAIL_TO`: 받을 사람 메일 주소

## 선택 Secrets

- `MAIL_FROM`: 보내는 사람 주소, 없으면 `MAIL_USERNAME` 사용
- `MAIL_CC`: 참조 주소
- `SMTP_HOST`: SMTP 서버, 없으면 `smtp.gmail.com`
- `SMTP_PORT`: SMTP 포트, 없으면 `587`
- `SMTP_USE_TLS`: 보통 `true`
- `MAIL_SUBJECT_PREFIX`: 메일 제목 앞부분

사내 SMTP를 쓰고 있다면 기존 자동화에서 쓰던 서버 정보를 `SMTP_HOST`, `SMTP_PORT`에 넣으면 됩니다.

## 실행 결과

- `data/results.json`: 마지막 실행 전체 후보
- `data/new_items.json`: 마지막 실행 신규 항목
- `data/history.jsonl`: 신규 항목 누적 이력
- `state.json`: 이미 본 게시물 기록
- `run_summary.json`: 마지막 실행 요약
- `reports/*.xlsx`: 메일에 첨부되는 관리대장 엑셀
- `reports/*.html`: 메일 본문용 요약 HTML

GitHub 저장소에는 `data/`, `state.json`, `run_summary.json`만 자동 커밋합니다. 엑셀/HTML 리포트는 메일 첨부와 Actions artifact로 확인합니다.

## 사이트 추가

`config.json`의 `sources`에 추가하면 됩니다.

RSS 소스는 기본적으로 `filters.rss` 규칙으로 한 번 더 거릅니다.

- `strong_keywords`: 축제, 공연, 집회, 시위처럼 실제 사람이 모일 가능성이 큰 신호
- `weak_keywords`: 행사, 운영, 안내, 캠페인, 봉사처럼 더미성 기사에 자주 나오는 신호
- `min_signal_score`: RSS가 통과해야 하는 최소 점수

RSS에서 더미가 많이 섞이면 이 값부터 조정하면 됩니다.

RSS 예시:

```json
{
  "name": "예시 행사 뉴스",
  "type": "rss",
  "url": "https://example.com/rss"
}
```

일반 홈페이지 예시:

```json
{
  "name": "예시 공식 홈페이지",
  "type": "html",
  "url": "https://example.com/events",
  "item_selector": "article, li, .item",
  "title_selector": "a, h3",
  "link_selector": "a",
  "body_selector": "p, .summary",
  "date_selector": "time, .date"
}
```

## 관리대장 컬럼

사진으로 공유한 기존 양식을 기준으로 아래 컬럼을 생성합니다.

- 구분
- 기간
- Event명
- 장소
- Type
- 시작일
- 종료일
- 이벤트 등급
- 이벤트 특성 구분
- 공동망 여부
- 예상운집인원(Peak Time)

추가 확인을 위해 출처, 게시일, 원문링크, 요약, 수집일, 상태/비고도 함께 넣습니다.

## 실행 주기 변경

`.github/workflows/monitor.yml`의 아래 줄을 바꾸면 됩니다.

```yaml
- cron: "17 */3 * * *"
```

현재 설정은 3시간마다 실행입니다. GitHub cron은 UTC 기준이라 한국시간과 9시간 차이가 있습니다.
