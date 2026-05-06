import datetime as dt

import streamlit as st

from stock_info_news import (
    disable_broken_proxy_env,
    enrich_company_profile,
    get_krx_fundamentals_and_flow,
    get_related_stocks,
    get_recent_news,
    get_stock_snapshot,
    search_symbol,
)

disable_broken_proxy_env()

if "query_input" not in st.session_state:
    st.session_state["query_input"] = ""
if "auto_search" not in st.session_state:
    st.session_state["auto_search"] = False
if "pending_query" not in st.session_state:
    st.session_state["pending_query"] = None
if "last_match" not in st.session_state:
    st.session_state["last_match"] = None
if "last_snapshot" not in st.session_state:
    st.session_state["last_snapshot"] = None
if "last_news_items" not in st.session_state:
    st.session_state["last_news_items"] = []

if st.session_state.get("pending_query"):
    st.session_state["query_input"] = st.session_state["pending_query"]
    st.session_state["pending_query"] = None

st.set_page_config(page_title="KRX Stock Pulse", page_icon="📊", layout="wide")


def first_sentence(text: str, limit: int = 170) -> str:
    if not text:
        return ""
    t = text.strip()
    for sep in ("다.", ". ", ".\n"):
        idx = t.find(sep)
        if idx != -1:
            t = t[: idx + 1]
            break
    if len(t) > limit:
        t = t[:limit].rstrip() + "..."
    return t


def build_company_profile_line(name: str, industry: str, products: str, desc: str) -> str:
    chunks = []
    if industry and industry != "N/A":
        chunks.append(industry)
    if products and products != "N/A":
        chunks.append(f"주요 제품/사업: {products}")
    summary = first_sentence(desc)
    if summary:
        chunks.append(summary)
    if not chunks:
        return f"{name}의 상세 업종 정보가 부족합니다."
    return " | ".join(chunks)


def fmt_num(value, pattern: str = ",.2f") -> str:
    if value is None:
        return "N/A"
    try:
        return format(float(value), pattern)
    except Exception:
        return "N/A"

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
    div[data-testid="metric-container"] {
      background: #ffffff;
      border: 1px solid #dbe4f0;
      border-radius: 14px;
      padding: 12px 14px;
      box-shadow: 0 8px 18px rgba(15, 23, 42, 0.05);
    }
    div[data-testid="metric-container"] > label {
      color: #475569 !important;
      font-weight: 600 !important;
    }
    div[data-testid="metric-container"] [data-testid="stMetricValue"] {
      color: #0f172a;
      font-weight: 800;
    }
    div[data-testid="stDataFrame"] {
      border: 1px solid #dbe4f0;
      border-radius: 12px;
      overflow: hidden;
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
    .section-card {
      background: #ffffff;
      border: 1px solid #dbe4f0;
      border-radius: 16px;
      padding: 14px 16px;
      box-shadow: 0 10px 24px rgba(15, 23, 42, 0.05);
      margin-bottom: 14px;
    }
    .news-meta { color: var(--muted); font-size: 0.85rem; }
    .rel-card {
      background: #ffffff;
      border: 1px solid var(--line);
      border-radius: 14px;
      padding: 10px 12px;
      margin-bottom: 10px;
      box-shadow: 0 8px 20px rgba(15, 23, 42, 0.05);
    }
    .rel-name { font-weight: 600; color: var(--ink); margin-right:8px; }
    .rel-row { display:flex; align-items:center; gap:10px; flex-wrap:wrap; }
    .rel-sub { color: var(--muted); font-size: 0.82rem; white-space: nowrap; }
    .score-wrap { display:flex; align-items:center; gap:6px; margin-left:auto; }
    .score-row { display:flex; align-items:center; gap:4px; }
    .score-label { width:62px; font-size:0.74rem; color:var(--muted); }
    .score-bar-bg {
      width:64px; height:6px; background:#e2e8f0; border-radius:999px; overflow:hidden;
    }
    .score-bar-theme { height:100%; background:linear-gradient(90deg,#0ea5e9,#0369a1); }
    .score-bar-text { height:100%; background:linear-gradient(90deg,#22c55e,#15803d); }
    .score-num { width:28px; text-align:right; font-size:0.7rem; color:var(--muted); }
    @media (max-width: 900px) {
      .rel-sub { white-space: normal; }
      .score-wrap { width: 100%; margin-left: 0; }
    }
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
    query = col_in.text_input(
        "종목명 또는 종목코드",
        placeholder="예: 엔켐 또는 348370",
        key="query_input",
    )
    run = col_btn.form_submit_button("조회", type="primary", use_container_width=True)

effective_run = run or st.session_state.get("auto_search", False)
if effective_run or st.session_state.get("last_match"):
    st.session_state["auto_search"] = False
    if effective_run:
        if not query.strip():
            st.warning("종목명 또는 코드를 입력해 주세요.")
            st.stop()
        try:
            match = search_symbol(query)
            if not match:
                st.error("해당 종목을 찾지 못했습니다.")
                st.stop()
            snapshot = get_stock_snapshot(match["symbol"], match.get("market_type", "KRX"))
            snapshot = enrich_company_profile(match, snapshot)
            news_query = match["name"] if match.get("market_type") == "KRX" else match["symbol"]
            news_items = get_recent_news(news_query)
            st.session_state["last_match"] = match
            st.session_state["last_snapshot"] = snapshot
            st.session_state["last_news_items"] = news_items
        except Exception as exc:
            st.error(f"조회 중 오류가 발생했습니다: {exc}")
            st.stop()

    match = st.session_state.get("last_match")
    snapshot = st.session_state.get("last_snapshot")
    news_items = st.session_state.get("last_news_items", [])
    if match and snapshot:
        try:
                st.caption(f"조회 시각: {dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
                st.subheader(f"{match['name']} ({match['symbol']})")
                st.caption(f"시장: {match['exchange']} | 통화: {snapshot['currency']}")

                market_dept = match.get("market_dept") or snapshot.get("market_dept") or "N/A"
                industry = snapshot.get("industry") or match.get("industry") or "N/A"
                products = snapshot.get("products") or match.get("products") or "N/A"
                detailed_industry = industry
                if products and products != "N/A":
                    detailed_industry = f"{industry} ({products})" if industry != "N/A" else products
                if match.get("market_type") == "KRX":
                    description = f"{match['name']}은(는) KRX 상장 기업입니다."
                    if detailed_industry != "N/A":
                        description = f"{match['name']}은(는) {detailed_industry} 분야 기업입니다."
                else:
                    description = snapshot.get("company_description") or "회사 설명 정보가 없습니다."
                if description == "회사 설명 정보가 없습니다.":
                    description = "정보 없음"

                with st.container(border=True):
                    st.markdown("### 회사 기본정보")
                    profile_line = build_company_profile_line(
                        match["name"],
                        industry,
                        products,
                        description if description != "정보 없음" else "",
                    )
                    st.write(f"- 업종(상세): {profile_line}")
                    if market_dept != "N/A":
                        st.write(f"- 시장 소속부: {market_dept}")
                    st.write(f"- 주요 제품/사업: {products}")
                    st.write(description)

                if match.get("market_type") == "KRX":
                    with st.container(border=True):
                        st.markdown("### 최근 재무 / 수급")
                        extra = get_krx_fundamentals_and_flow(match["symbol"])
                        f = extra.get("fundamental", {}) if isinstance(extra, dict) else {}
                        flow = extra.get("flow", {}) if isinstance(extra, dict) else {}
                        flow_table = extra.get("flow_table", []) if isinstance(extra, dict) else []
                        flow_error = extra.get("error") if isinstance(extra, dict) else None

                        cfa, cfb, cfc = st.columns(3)
                        cfa.metric("PER", fmt_num(f.get("PER"), ",.2f"))
                        cfb.metric("PBR", fmt_num(f.get("PBR"), ",.2f"))
                        cfc.metric("EPS", fmt_num(f.get("EPS"), ",.0f"))

                        cfd, cfe = st.columns(2)
                        cfd.metric("기관 5일 수급(주수합계)", fmt_num(flow.get("inst_5d"), ",.0f"))
                        cfe.metric("외인 5일 수급(주수합계)", fmt_num(flow.get("foreign_5d"), ",.0f"))

                        if flow_table:
                            st.dataframe(flow_table, use_container_width=True)
                        else:
                            if flow_error:
                                st.warning("수급 데이터를 불러오지 못했습니다. 현재 네트워크/프록시 설정으로 KRX 접속이 차단된 상태일 수 있습니다.")
                                st.caption(f"상세 오류: {flow_error}")
                            else:
                                st.caption("수급 데이터가 아직 제공되지 않았습니다.")

                st.markdown("### 같은 테마 종목")
                related = get_related_stocks(match, limit=8)
                if related:
                    theme_vals = [int(x.get("theme_score", "0")) for x in related]
                    keyword_vals = [int(x.get("keyword_score", "0")) for x in related]
                    max_theme = max(theme_vals) if theme_vals else 1
                    max_keyword = max(keyword_vals) if keyword_vals else 1
                    if max_theme == 0:
                        max_theme = 1
                    if max_keyword == 0:
                        max_keyword = 1
                    for item in related:
                        c_left, c_right = st.columns([4, 1])
                        theme_raw = int(item.get("theme_score", "0"))
                        keyword_raw = int(item.get("keyword_score", "0"))
                        theme_score = round((theme_raw / max_theme) * 5, 1)
                        keyword_score = round((keyword_raw / max_keyword) * 5, 1)
                        theme_pct = int((theme_score / 5) * 100)
                        keyword_pct = int((keyword_score / 5) * 100)
                        product_industry = item.get("products") or item.get("industry", "N/A")
                        keyword_text = item.get("matched_keywords", "").strip()
                        if not keyword_text:
                            keyword_text = "뉴스동시언급 기반"

                        c_left.markdown(
                            f"""
                            <div class="rel-card">
                              <div class="rel-row">
                                <div class="rel-name">{item['name']} ({item['symbol']})</div>
                                <div class="rel-sub">주요제품/산업: {product_industry}</div>
                                <div class="rel-sub">테마: {item.get('matched_themes', '')}</div>
                                <div class="rel-sub">키워드: {keyword_text}</div>
                                <div class="score-wrap">
                                  <div class="score-row">
                                    <div class="score-label">테마</div>
                                    <div class="score-bar-bg"><div class="score-bar-theme" style="width:{theme_pct}%"></div></div>
                                    <div class="score-num">{theme_score}</div>
                                  </div>
                                  <div class="score-row">
                                    <div class="score-label">키워드</div>
                                    <div class="score-bar-bg"><div class="score-bar-text" style="width:{keyword_pct}%"></div></div>
                                    <div class="score-num">{keyword_score}</div>
                                  </div>
                                </div>
                              </div>
                            </div>
                            """,
                            unsafe_allow_html=True,
                        )
                        if c_right.button("조회", key=f"rel_{item['symbol']}"):
                            st.session_state["pending_query"] = item["symbol"]
                            st.session_state["auto_search"] = True
                            st.rerun()
                else:
                    if match.get("market_type") == "KRX":
                        st.write("같은 산업/업종 기준 추천 종목이 없거나 데이터가 부족합니다.")
                    else:
                        st.write("해외 종목의 테마 추천은 다음 업데이트에서 확장 예정입니다.")

                change_pct = snapshot.get("change_pct")
                change_str = "N/A" if change_pct is None else f"{change_pct:+.2f}%"

                c1, c2, c3, c4 = st.columns(4)
                c1.markdown(
                    f"<div class='kpi'><div class='kpi-label'>최근 종가</div><div class='kpi-value'>{fmt_num(snapshot.get('latest_close'), ',.2f')}</div></div>",
                    unsafe_allow_html=True,
                )
                c2.markdown(
                    f"<div class='kpi'><div class='kpi-label'>전일 대비</div><div class='kpi-value'>{change_str}</div></div>",
                    unsafe_allow_html=True,
                )
                c3.markdown(
                    f"<div class='kpi'><div class='kpi-label'>거래량</div><div class='kpi-value'>{fmt_num(snapshot.get('volume'), ',.0f')}</div></div>",
                    unsafe_allow_html=True,
                )
                c4.markdown(
                    f"<div class='kpi'><div class='kpi-label'>52주 범위</div><div class='kpi-value'>{fmt_num(snapshot.get('fifty_two_week_low'), ',.0f')} ~ {fmt_num(snapshot.get('fifty_two_week_high'), ',.0f')}</div></div>",
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
