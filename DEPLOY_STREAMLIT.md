# Streamlit Cloud 배포 가이드

## 1) GitHub 업로드
- 이 폴더 전체를 GitHub 저장소에 올립니다.
- 필수 파일:
  - `stock_streamlit_app.py`
  - `stock_info_news.py`
  - `requirements.txt`

## 2) Streamlit Community Cloud 배포
1. [https://share.streamlit.io/](https://share.streamlit.io/) 접속
2. `New app` 클릭
3. Repository 선택
4. Main file path에 `stock_streamlit_app.py` 입력
5. `Deploy` 클릭

## 3) 사용자 안내
- KRX 종목은 종목코드로 입력하면 가장 안정적입니다.
  - 예: `348370`, `005930`
- 글로벌 종목은 티커로 입력하세요.
  - 예: `TSLA`, `AAPL`
