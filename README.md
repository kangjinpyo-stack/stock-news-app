# IPO Peer Valuation Auto Generator

IPO 기업의 핵심 지표와 Peer Group 재무데이터를 입력하면, 아래를 자동 생성하는 투자 분석 툴입니다.

- Peer별 멀티플 자동 계산: `PER`, `PSR`, `EV/EBITDA`
- 멀티플 기반 적정 시가총액 범위
- 주당 공모가 범위
- Markdown 리포트 + Excel(.xlsx) 출력
- Streamlit 웹 UI 지원

## 1) CLI 실행

```powershell
python -m ipo_valuation_generator.main --input examples/sample_input.json --output ipo_valuation_report.md --output-xlsx ipo_valuation_report.xlsx
```

옵션:
- `--output`: Markdown 경로
- `--output-xlsx`: Excel 경로(선택)

## 2) 웹 UI 실행

```powershell
streamlit run streamlit_app.py
```

브라우저에서 JSON 입력 후 `분석 실행` 버튼을 누르면 결과가 바로 표시됩니다.

## 3) 입력 포맷

템플릿: [examples/sample_input.json](examples/sample_input.json)

필수 필드:
- `company_name` (string)
- `currency` (string)
- `shares_outstanding` (number)
- `ipo_metrics` (object): `revenue`, `net_income`, `ebitda`, `net_debt`
- `peer_group` (array): `name`, `market_cap`, `net_debt`, `revenue`, `net_income`, `ebitda`

## 4) 계산식

- `PER = market_cap / net_income`
- `PSR = market_cap / revenue`
- `EV/EBITDA = (market_cap + net_debt) / ebitda`

각 멀티플에서 `Low/Median/High = min/median/max`를 만들고,
IPO 지표에 적용해 적정 시총 밴드를 계산합니다.

- PER 기반 시총: `PER x IPO net_income`
- PSR 기반 시총: `PSR x IPO revenue`
- EV/EBITDA 기반 시총: `(EV/EBITDA x IPO ebitda) - IPO net_debt`

## 5) 주식 정보 + 최근 뉴스 조회 프로그램

종목명(또는 종목코드)을 입력하면, KRX(FinanceDataReader) 기반 주식 정보와 Google News 기반 최신 뉴스를 출력합니다.

```powershell
python stock_info_news.py
```

출력 항목:
- 회사명 / 종목코드 / 시장 / 통화
- 최근 종가 / 전일 대비 / 거래량
- 52주 고저가
- 최근 뉴스 5건(제목, 발행일, 링크)

### 주식 조회 웹 UI (Streamlit)

```powershell
streamlit run stock_streamlit_app.py
```

## 6) 링크로 배포 (Streamlit Community Cloud)

배포하면 `https://...streamlit.app` 형태의 링크로 다른 사람도 사용할 수 있습니다.

1. GitHub에 이 프로젝트를 업로드합니다.
2. [Streamlit Community Cloud](https://share.streamlit.io/)에 로그인합니다.
3. `New app` 선택 후 리포지토리/브랜치 설정:
- Main file path: `stock_streamlit_app.py`
4. `Deploy` 클릭

배포 후 참고:
- KRX/뉴스 데이터는 외부 네트워크 상태에 따라 일부 항목이 비어 보일 수 있습니다.
- 종목명 검색이 불안정하면 종목코드(예: `348370`, `005930`)를 사용하세요.
