# KRX Stock Pulse

종목명/종목코드를 입력하면 주가, 뉴스, 테마 흐름, 관련 종목을 확인할 수 있는 Streamlit 앱입니다.

## 실행 방법

```powershell
pip install -r requirements.txt
streamlit run stock_streamlit_app.py
```

## 주요 기능

- 종목 검색: 종목명/종목코드 조회
- 시세 정보: 최근 종가, 전일 대비, 거래량, 52주 범위
- 뉴스: 최신 뉴스 목록
- 테마 흐름: 상승/하락 테마 및 테마별 종목
- 급등/급락 종목 TOP 100
- 동종업계 비교, 재무/수급 요약

## 배포

Streamlit Community Cloud에서 `Main file path`를 `stock_streamlit_app.py`로 설정해 배포할 수 있습니다.
