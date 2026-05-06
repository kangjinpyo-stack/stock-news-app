import datetime as dt

import streamlit as st

from stock_info_news import (
    disable_broken_proxy_env,
    enrich_company_profile,
    get_recent_news,
    get_stock_snapshot,
    search_symbol,
)

disable_broken_proxy_env()

st.set_page_config(page_title="KRX Stock Pulse", page_icon="📊", layout="wide")

st.markdown(
    """
    <style>
    :root {
      --bg1: #f3f7ff;
      --bg2: #eefaf4;
      --card: #ffffff;
      --ink: #0f172a;
      --muted: #475569;
      --line: #dbe4f0;
      --accent: #0f766e;
    }
    .stApp {
      background: radial-gradient(circle at 5% 5%, var(--bg1), transparent 45%),
                  radial-gradient(circle at 95% 10%, var(--bg2), transparent 45%),
                  #f8fafc;
    }
    .hero {
      background: linear-gradient(135deg, #0f172a 0%, #1e293b 100%);
      color: #e2e8f0;
      border-radius: 16px;
      padding: 20px 24px;
      border: 1px solid #334155;
      margin-bottom: 16px;
    }
    .kpi {
      background: var(--card);
      border: 1px solid var(--line);
      border-radius: 14px;
      padding: 14px;
      box-shadow: 0 8px 22px rgba(15, 23, 42, 0.05);
      height: 100%;
    }
    .kpi-label { color: var(--muted); font-size: 0.85rem; }
    .kpi-value { color: var(--ink); font-weight: 700; font-size: 1.25rem; margin-top: 4px; }
    .news-item {
      background: #ffffff;
      border: 1px solid var(--line);
      border-radius: 12px;
      padding: 12px 14px;
      margin-bottom: 10px;
    }
    .news-meta { color: var(--muted); font-size: 0.85rem; }
    </style>
    """,
    unsafe_allow_html=True,
)

st.markdown(
    """
    <div class="hero">
      <h2 style="margin:0;">KRX Stock Pulse</h2>
      <p style="margin:8px 0 0 0;">종목명 또는 종목코드로 주가와 최신 뉴스를 빠르게 조회합니다.</p>
    </div>
    """,
    unsafe_allow_html=True,
)

with st.form("search_form", clear_on_submit=False):
    col_in, col_btn = st.columns([5, 1])
    query = col_in.text_input("종목명 또는 종목코드", placeholder="예: 엔켐 또는 348370")
    run = col_btn.form_submit_button("조회", type="primary", use_container_width=True)

if run:
    if not query.strip():
        st.warning("종목명 또는 코드를 입력해 주세요.")
    else:
        try:
            match = search_symbol(query)
            if not match:
                st.error("해당 종목을 찾지 못했습니다.")
            else:
                snapshot = get_stock_snapshot(match["symbol"], match.get("market_type", "KRX"))
                snapshot = enrich_company_profile(match, snapshot)
                news_query = match["name"] if match.get("market_type") == "KRX" else match["symbol"]
                news_items = get_recent_news(news_query)

                st.caption(f"조회 시각: {dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
                st.subheader(f"{match['name']} ({match['symbol']})")
                st.caption(f"시장: {match['exchange']} | 통화: {snapshot['currency']}")

                sector = snapshot.get("sector") or match.get("sector") or "N/A"
                industry = snapshot.get("industry") or match.get("industry") or "N/A"
                products = snapshot.get("products") or match.get("products") or "N/A"
                if match.get("market_type") == "KRX":
                    description = f"{match['name']}은(는) KRX 상장 기업입니다."
                    if sector != "N/A" or industry != "N/A":
                        description = f"{match['name']}은(는) {sector} / {industry} 분야 기업입니다."
                else:
                    description = snapshot.get("company_description") or "회사 설명 정보가 없습니다."
                if description == "회사 설명 정보가 없습니다.":
                    description = "정보 없음"

                st.markdown("### 회사 기본정보")
                st.write(f"- 업종/섹터: {sector}")
                st.write(f"- 산업: {industry}")
                st.write(f"- 주요 제품/사업: {products}")
                st.write(description)

                change_pct = snapshot.get("change_pct")
                change_str = "N/A" if change_pct is None else f"{change_pct:+.2f}%"

                c1, c2, c3, c4 = st.columns(4)
                c1.markdown(
                    f"<div class='kpi'><div class='kpi-label'>최근 종가</div><div class='kpi-value'>{snapshot['latest_close']:,.2f}</div></div>",
                    unsafe_allow_html=True,
                )
                c2.markdown(
                    f"<div class='kpi'><div class='kpi-label'>전일 대비</div><div class='kpi-value'>{change_str}</div></div>",
                    unsafe_allow_html=True,
                )
                c3.markdown(
                    f"<div class='kpi'><div class='kpi-label'>거래량</div><div class='kpi-value'>{snapshot['volume']:,.0f}</div></div>",
                    unsafe_allow_html=True,
                )
                c4.markdown(
                    f"<div class='kpi'><div class='kpi-label'>52주 범위</div><div class='kpi-value'>{snapshot['fifty_two_week_low']:,.0f} ~ {snapshot['fifty_two_week_high']:,.0f}</div></div>",
                    unsafe_allow_html=True,
                )

                st.subheader("최근 뉴스")
                if not news_items:
                    st.info("검색된 뉴스가 없습니다.")
                else:
                    for item in news_items:
                        published = item.get("published", "")
                        st.markdown(
                            f"""
                            <div class="news-item">
                              <div><a href="{item['link']}" target="_blank">{item['title']}</a></div>
                              <div class="news-meta">{published}</div>
                            </div>
                            """,
                            unsafe_allow_html=True,
                        )
        except Exception as exc:
            st.error(f"조회 중 오류가 발생했습니다: {exc}")

st.divider()
st.markdown("실행: `streamlit run stock_streamlit_app.py`")
