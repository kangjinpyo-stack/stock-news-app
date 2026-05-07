import datetime as dt
from collections import OrderedDict

import re

import altair as alt
import pandas as pd
import streamlit as st

from stock_info_news import (
    disable_broken_proxy_env,
    enrich_company_profile,
    get_krx_financial_table,
    get_krx_fundamentals_and_flow,
    get_krx_peer_comparison,
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
if "last_related" not in st.session_state:
    st.session_state["last_related"] = []
if "last_extra" not in st.session_state:
    st.session_state["last_extra"] = {}
if "last_peers" not in st.session_state:
    st.session_state["last_peers"] = []
if "last_financial_table" not in st.session_state:
    st.session_state["last_financial_table"] = []

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


def clean_products_text(products: str, industry: str, desc: str) -> str:
    text = (products or "").strip()
    if text and text != "N/A":
        for token in ["제품 등", "사업 등", "제조 및 판매", "제조", "도매", "소매"]:
            text = text.replace(token, "")
        text = text.replace("(", ", ").replace(")", "")
        for sep in ["/", "·", ";"]:
            text = text.replace(sep, ",")
        parts = [x.strip() for x in text.split(",") if x.strip()]
        uniq = []
        for item in parts:
            if item not in uniq:
                uniq.append(item)
        short = ", ".join(uniq[:3])
        if short:
            return short

    if industry and industry != "N/A":
        return industry

    desc_sentence = first_sentence(desc, limit=60)
    return desc_sentence if desc_sentence else "확인 필요"


def infer_core_business_labels(industry: str, products: str, desc: str) -> str:
    base = " ".join([str(industry or ""), str(products or ""), str(desc or "")]).lower()
    labels = []
    keyword_groups = [
        ("DRAM", ["dram"]),
        ("NAND", ["nand"]),
        ("HBM", ["hbm"]),
        ("메모리 반도체", ["메모리", "반도체", "semiconductor"]),
        ("파운드리", ["파운드리", "foundry"]),
        ("배터리", ["배터리", "2차전지", "이차전지"]),
        ("전해액", ["전해액"]),
        ("양극재", ["양극재"]),
        ("음극재", ["음극재"]),
        ("분리막", ["분리막"]),
        ("석유화학", ["석유화학", "납사"]),
        ("정유", ["정유", "원유", "휘발유", "경유"]),
        ("디스플레이", ["디스플레이", "oled", "lcd"]),
        ("유리기판", ["유리기판"]),
        ("바이오", ["바이오", "제약", "신약", "의약품"]),
        ("자동차부품", ["자동차부품", "전장", "모터"]),
        ("태양광", ["태양광"]),
        ("전력기기", ["전력기기", "변압기", "전선"]),
    ]
    for label, keywords in keyword_groups:
        if any(keyword in base for keyword in keywords):
            labels.append(label)
    deduped = []
    for label in labels:
        if label not in deduped:
            deduped.append(label)
    if deduped:
        return ", ".join(deduped[:3])
    return clean_products_text(products, industry, desc)


def build_company_three_line_summary(name, industry, products, desc, news_items):
    industry_text = industry if industry and industry != "N/A" else "업종 정보 확인 필요"
    core_business = infer_core_business_labels(industry, products, desc)
    if not core_business or core_business in {"-", "N/A", "확인 필요"}:
        core_business = clean_products_text(products, industry, desc)

    overview_sentences = []
    if desc and desc != "정보 없음":
        parts = re.split(r"(?<=[.!?])\s+|(?<=함\.)\s*|(?<=있음\.)\s*", str(desc).strip())
        for part in parts:
            cleaned = part.strip()
            if not cleaned:
                continue
            cleaned = cleaned.replace("동사는", name + "는")
            cleaned = cleaned.replace("  ", " ").strip()
            # Drop broken tail fragments such as ") 분야 기업입니다."
            if len(cleaned) < 12:
                continue
            if cleaned.startswith(")") or cleaned.startswith("("):
                continue
            if cleaned in {"분야 기업입니다.", ")분야 기업입니다.", ") 분야 기업입니다."}:
                continue
            if cleaned not in overview_sentences:
                overview_sentences.append(cleaned)

    lines = []
    if overview_sentences:
        lines.append(overview_sentences[0])
    else:
        lines.append(f"{name}은(는) {industry_text} 중심의 회사입니다.")

    if len(overview_sentences) >= 2:
        lines.append(overview_sentences[1])
    else:
        lines.append(f"핵심 사업은 {core_business}이며, 이 부문의 경쟁력과 수요가 실적에 큰 영향을 줍니다.")

    if len(overview_sentences) >= 3:
        lines.append(overview_sentences[2])
    else:
        lines.append(f"주력 사업 구조를 보면 {industry_text} 업황 변화와 고객사 투자 사이클을 함께 보는 것이 좋습니다.")

    if core_business not in {"-", "N/A", "확인 필요"}:
        lines.append(f"특히 {core_business} 관련 수요, 수익성, 투자 확대 여부가 주가 흐름에 중요한 변수로 작용할 수 있습니다.")

    if news_items:
        title = str(news_items[0].get("title", "")).strip()
        if title:
            title = title.replace("-v.daum.net", "").replace("- daum", "").strip()
            if len(title) > 58:
                title = title[:58].rstrip() + "..."
            lines.append(f"최근에는 '{title}' 같은 이슈와 함께 시장에서 언급되고 있습니다.")

    cleaned_lines = []
    for line in lines:
        if line and line not in cleaned_lines:
            cleaned_lines.append(line)
    return cleaned_lines[:5]


def fmt_num(value, pattern: str = ",.2f") -> str:
    if value is None:
        return "N/A"
    try:
        return format(float(value), pattern)
    except Exception:
        return "N/A"


def build_investment_points(match, snapshot, fundamentals, related):
    points = []
    ch = snapshot.get("change_pct")
    if ch is None:
        points.append("단기 주가 방향성: 변동률 데이터가 부족해 추세 판단이 제한됩니다.")
    elif ch >= 2:
        points.append(f"단기 모멘텀: 최근 등락률이 {ch:+.2f}%로 강한 편입니다.")
    elif ch <= -2:
        points.append(f"단기 모멘텀: 최근 등락률이 {ch:+.2f}%로 약한 편입니다.")
    else:
        points.append(f"단기 모멘텀: 최근 등락률이 {ch:+.2f}%로 중립 구간입니다.")

    per = fundamentals.get("PER")
    pbr = fundamentals.get("PBR")
    if per is None and pbr is None:
        points.append("밸류에이션: PER/PBR 데이터가 제한되어 절대평가가 어렵습니다.")
    else:
        points.append(f"밸류에이션: PER {fmt_num(per, ',.2f')} / PBR {fmt_num(pbr, ',.2f')} 수준입니다.")

    if related:
        top_theme = related[0].get("matched_themes") or "테마 데이터 부족"
        points.append(f"시장 연관 테마: 현재 종목은 `{top_theme}` 축에서 함께 움직일 가능성이 큽니다.")
    else:
        points.append("시장 연관 테마: 동행 종목 데이터가 부족해 추가 확인이 필요합니다.")
    return points[:3]


def _to_float_or_none(value):
    try:
        text = str(value).replace(",", "").strip()
        if text in {"", "N/A", "nan", "None"}:
            return None
        return float(text)
    except Exception:
        return None


def build_financial_summary_from_table(financial_table):
    if not financial_table:
        return {}, {}

    row_map = {}
    for row in financial_table:
        key = str(row.get("주요재무정보", "")).strip()
        if key:
            row_map[key] = row

    per_row = row_map.get("PER(배)", {})
    pbr_row = row_map.get("PBR(배)", {})
    eps_row = row_map.get("EPS(원)", {})

    annual_cols = [k for k in per_row.keys() if k.startswith("연간 ")]
    annual_cols.sort()
    chosen_col = None
    for col in reversed(annual_cols):
        if _to_float_or_none(per_row.get(col)) is not None:
            chosen_col = col
            break
    if chosen_col is None:
        for col in reversed(annual_cols):
            if _to_float_or_none(pbr_row.get(col)) is not None or _to_float_or_none(eps_row.get(col)) is not None:
                chosen_col = col
                break

    if chosen_col is None:
        return {}, {}

    basis = chosen_col.replace("연간 ", "")
    return (
        {
            "PER": _to_float_or_none(per_row.get(chosen_col)),
            "PBR": _to_float_or_none(pbr_row.get(chosen_col)),
            "EPS": _to_float_or_none(eps_row.get(chosen_col)),
        },
        {
            "PER": basis,
            "PBR": basis,
            "EPS": basis,
        },
    )




def group_related_items(related):
    grouped = OrderedDict()
    preferred_order = [
        "??? ???",
        "??? ???",
        "??? ???",
        "??? ???",
        "AI/??? ???",
        "?? ???",
        "?? ???",
    ]
    for label in preferred_order:
        grouped[label] = []
    for item in related:
        label = item.get("relation_bucket", "").strip() or "?? ???"
        grouped.setdefault(label, [])
        grouped[label].append(item)
    return [(label, items) for label, items in grouped.items() if items]

@st.cache_data(ttl=600, show_spinner=False)
def load_snapshot_cached(symbol: str, market_type: str):
    return enrich_company_profile({"symbol": symbol, "market_type": market_type}, get_stock_snapshot(symbol, market_type))


@st.cache_data(ttl=600, show_spinner=False)
def load_news_cached(news_query: str):
    return get_recent_news(news_query)


@st.cache_data(ttl=600, show_spinner=False)
def load_related_cached(
    symbol: str,
    name: str,
    market_type: str,
    exchange: str,
    industry: str = "",
    products: str = "",
    company_description: str = "",
):
    return get_related_stocks(
        {
            "symbol": symbol,
            "name": name,
            "market_type": market_type,
            "exchange": exchange,
            "industry": industry,
            "products": products,
            "company_description": company_description,
        },
        limit=8,
    )


@st.cache_data(ttl=600, show_spinner=False)
def load_extra_cached(symbol: str):
    return get_krx_fundamentals_and_flow(symbol)


@st.cache_data(ttl=600, show_spinner=False)
def load_peers_cached(symbol: str):
    return get_krx_peer_comparison(symbol)


@st.cache_data(ttl=600, show_spinner=False)
def load_financial_table_cached(symbol: str):
    return get_krx_financial_table(symbol)

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
    .block-container,
    [data-testid="stAppViewContainer"] > .main .block-container {
      max-width: 1180px;
      padding-top: 1.2rem;
      padding-bottom: 2rem;
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
    .rel-row { display:flex; align-items:center; gap:8px; flex-wrap:nowrap; min-width:0; }
    .rel-main { min-width:0; flex:1; display:flex; align-items:center; gap:8px; overflow:hidden; }
    .rel-name { flex:0 0 auto; }
    .rel-sub { color: var(--muted); font-size: 0.79rem; white-space: nowrap; overflow:hidden; text-overflow:ellipsis; }
    .score-wrap { display:flex; align-items:center; gap:4px; margin-left:auto; flex:0 0 auto; }
    .score-row { display:flex; align-items:center; gap:4px; }
    .score-label { width:48px; font-size:0.72rem; color:var(--muted); }
    .score-bar-bg {
      width:52px; height:6px; background:#e2e8f0; border-radius:999px; overflow:hidden;
    }
    .score-bar-theme { height:100%; background:linear-gradient(90deg,#0ea5e9,#0369a1); }
    .score-bar-text { height:100%; background:linear-gradient(90deg,#22c55e,#15803d); }
    .score-num { width:24px; text-align:right; font-size:0.68rem; color:var(--muted); }
    .search-wrap { max-width: 400px; margin: 0 auto 8px auto; }
    div[data-testid="stTextInput"] input {
      height: 44px;
      border-radius: 10px;
    }
    div[data-testid="stFormSubmitButton"] button {
      height: 44px;
      border-radius: 10px;
      margin-top: 0 !important;
    }
    .quote-card {
      background: #ffffff;
      border: 1px solid #dbe4f0;
      border-radius: 16px;
      padding: 16px 18px;
      box-shadow: 0 8px 20px rgba(15, 23, 42, 0.05);
      margin-bottom: 14px;
    }
    @media (max-width: 900px) {
      .rel-row { flex-wrap:wrap; }
      .rel-main { width:100%; flex-wrap:wrap; }
      .rel-sub { white-space: normal; }
      .score-wrap { width: 100%; margin-left: 0; }
    }
    </style>
    """,
    unsafe_allow_html=True,
)

hero_left, hero_center, hero_right = st.columns([1, 1.2, 1])
with hero_center:
    st.markdown(
        """
        <div class="hero">
          <h2 style="margin:0;">KRX Stock Pulse</h2>
          <p style="margin:8px 0 0 0;">종목명 또는 종목코드로 주가와 최신 뉴스를 빠르게 조회합니다.</p>
        </div>
        """,
        unsafe_allow_html=True,
    )

outer_left, outer_center, outer_right = st.columns([1, 1.2, 1])
with outer_center:
    st.markdown("<div class='search-wrap'>", unsafe_allow_html=True)
    with st.form("search_form", clear_on_submit=False):
        st.caption("종목명 또는 종목코드")
        col_in, col_btn = st.columns([4.2, 1], vertical_alignment="bottom")
        query = col_in.text_input(
            "종목명 또는 종목코드",
            placeholder="예: 엔켐 또는 348370",
            key="query_input",
            label_visibility="collapsed",
        )
        run = col_btn.form_submit_button("조회", type="primary", use_container_width=True)
    st.markdown("</div>", unsafe_allow_html=True)

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
            snapshot = load_snapshot_cached(match["symbol"], match.get("market_type", "KRX"))
            snapshot = enrich_company_profile(match, snapshot)
            news_query = match["name"] if match.get("market_type") == "KRX" else match["symbol"]
            news_items = load_news_cached(news_query)
            related = load_related_cached(
                match["symbol"],
                match["name"],
                match.get("market_type", "KRX"),
                match.get("exchange", ""),
                snapshot.get("industry", "") or match.get("industry", "") or "",
                snapshot.get("products", "") or match.get("products", "") or "",
                snapshot.get("company_description", "") or "",
            )
            extra = load_extra_cached(match["symbol"]) if (match.get("market_type") or "").upper() != "GLOBAL" else {}
            peers = load_peers_cached(match["symbol"]) if (match.get("market_type") or "").upper() != "GLOBAL" else []
            financial_table = load_financial_table_cached(match["symbol"]) if (match.get("market_type") or "").upper() != "GLOBAL" else []
            st.session_state["last_match"] = match
            st.session_state["last_snapshot"] = snapshot
            st.session_state["last_news_items"] = news_items
            st.session_state["last_related"] = related
            st.session_state["last_extra"] = extra
            st.session_state["last_peers"] = peers
            st.session_state["last_financial_table"] = financial_table
        except Exception as exc:
            st.error(f"조회 중 오류가 발생했습니다: {exc}")
            st.stop()

    match = st.session_state.get("last_match")
    snapshot = st.session_state.get("last_snapshot")
    news_items = st.session_state.get("last_news_items", [])
    related = st.session_state.get("last_related", [])
    extra = st.session_state.get("last_extra", {})
    peers = st.session_state.get("last_peers", [])
    financial_table = st.session_state.get("last_financial_table", [])
    if match and snapshot:
        try:
                st.markdown(
                    f"""
                    <div class="quote-card">
                      <div style="color:#64748b; font-size:0.85rem; margin-bottom:6px;">조회 시각: {dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</div>
                      <div style="font-size:2rem; font-weight:800; color:#0f172a;">{match['name']} ({match['symbol']})</div>
                      <div style="color:#64748b; margin-top:6px;">시장: {match['exchange']} | 통화: {snapshot['currency']}</div>
                    </div>
                    """,
                    unsafe_allow_html=True,
                )

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
                    summary_lines = build_company_three_line_summary(
                        match["name"],
                        industry,
                        products,
                        description if description != "정보 없음" else "",
                        news_items,
                    )
                    for line in summary_lines:
                        st.write(f"- {line}")

                is_krx_like = (match.get("market_type") or "").upper() != "GLOBAL"
                f = extra.get("fundamental", {}) if isinstance(extra, dict) else {}
                f_basis = extra.get("fundamental_basis", {}) if isinstance(extra, dict) else {}
                flow = extra.get("flow", {}) if isinstance(extra, dict) else {}
                flow_table = extra.get("flow_table", []) if isinstance(extra, dict) else []
                flow_error = extra.get("error") if isinstance(extra, dict) else None
                if is_krx_like and financial_table:
                    table_f, table_basis = build_financial_summary_from_table(financial_table)
                    if table_f:
                        f = table_f
                        f_basis = table_basis

                    with st.container(border=True):
                        st.markdown("### 투자 포인트 3줄")
                        for p in build_investment_points(match, snapshot, f, related[:3]):
                            st.write(f"- {p}")

                    with st.container(border=True):
                        st.markdown("### 동종업계 비교")
                        if peers:
                            sales_basis = peers[0].get("sales_basis", "최근 분기")
                            op_basis = peers[0].get("op_basis", "최근 분기")
                            peers_df = pd.DataFrame(peers).rename(
                                columns={
                                    "name": "종목명",
                                    "price": "현재가(조회시점)",
                                    "market_cap_100m": "시가총액(억, 조회시점)",
                                    "sales_100m": f"매출액(억, {sales_basis} 기준)",
                                    "op_100m": f"영업이익(억, {op_basis} 기준)",
                                }
                            )
                            for col in [
                                "현재가(조회시점)",
                                "시가총액(억, 조회시점)",
                                f"매출액(억, {sales_basis} 기준)",
                                f"영업이익(억, {op_basis} 기준)",
                            ]:
                                if col in peers_df.columns:
                                    peers_df[col] = peers_df[col].apply(
                                        lambda x: fmt_num(str(x).replace(",", ""), ",.0f") if str(x).strip() not in {"", "N/A"} else "N/A"
                                    )
                            peers_df = peers_df.drop(columns=["sales_basis", "op_basis", "symbol"], errors="ignore")
                            st.dataframe(peers_df, use_container_width=True, hide_index=True)
                            st.caption(f"기준: 현재가/시가총액은 조회 시점 기준입니다. 매출액/영업이익은 네이버 종목비교 표의 최근 실제 분기인 {sales_basis} 값을 사용합니다.")
                        else:
                            st.caption("동종업계 비교 데이터를 가져오지 못했습니다.")

                st.markdown("### 같은 테마 종목")
                if related:
                    theme_vals = [int(x.get("theme_score", "0")) for x in related]
                    keyword_vals = [int(x.get("keyword_score", "0")) for x in related]
                    max_theme = max(theme_vals) if theme_vals else 1
                    max_keyword = max(keyword_vals) if keyword_vals else 1
                    if max_theme == 0:
                        max_theme = 1
                    if max_keyword == 0:
                        max_keyword = 1
                    grouped_related = group_related_items(related)
                    for group_label, group_items in grouped_related:
                        st.markdown(f"#### {group_label}")
                        for item in group_items:
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
                                keyword_text = item.get("theme_reason", "").strip() or "사업/뉴스 연관도 기반"
                            origin_text = item.get("theme_origin", "").strip()

                            c_left.markdown(
                                f"""
                                <div class="rel-card">
                                  <div class="rel-row">
                                    <div class="rel-main">
                                      <div class="rel-name">{item['name']} ({item['symbol']})</div>
                                      <div class="rel-sub">주요제품/산업: {product_industry}</div>
                                      <div class="rel-sub">테마: {item.get('matched_themes', '')}</div>
                                      <div class="rel-sub">근거: {keyword_text}</div>
                                      <div class="rel-sub">출처: {origin_text or '회사정보 우선'}</div>
                                    </div>
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
                            if c_right.button("조회", key=f"rel_{group_label}_{item['symbol']}"):
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

                if is_krx_like:
                    with st.container(border=True):
                        st.markdown("### 최근 재무 / 수급")
                        cfa, cfb, cfc = st.columns(3)
                        per_label = "PER" if not f_basis.get("PER") else f"PER({f_basis.get('PER')})"
                        pbr_label = "PBR" if not f_basis.get("PBR") else f"PBR({f_basis.get('PBR')})"
                        eps_label = "EPS" if not f_basis.get("EPS") else f"EPS({f_basis.get('EPS')})"
                        cfa.metric(per_label, fmt_num(f.get("PER"), ",.2f"))
                        cfb.metric(pbr_label, fmt_num(f.get("PBR"), ",.2f"))
                        cfc.metric(eps_label, fmt_num(f.get("EPS"), ",.0f"))

                        cfd, cfe = st.columns(2)
                        cfd.metric("기관 5일 수급(주수합계)", fmt_num(flow.get("inst_5d"), ",.0f"))
                        cfe.metric("외인 5일 수급(주수합계)", fmt_num(flow.get("foreign_5d"), ",.0f"))

                        if financial_table:
                            financial_df = pd.DataFrame(financial_table)
                            for col in financial_df.columns:
                                if col == "주요재무정보":
                                    continue
                                financial_df[col] = financial_df[col].apply(
                                    lambda x: fmt_num(str(x).replace(",", ""), ",.0f")
                                    if str(x).strip() not in {"", "N/A", "nan", "None"}
                                    else ""
                                )
                            st.caption("기업실적분석")
                            st.dataframe(financial_df, use_container_width=True, hide_index=True)

                        if flow_table:
                            flow_df = pd.DataFrame(flow_table).rename(
                                columns={
                                    "date": "날짜",
                                    "institution": "기관",
                                    "foreign": "외인",
                                }
                            )
                            for col in ("기관", "외인"):
                                if col in flow_df.columns:
                                    flow_df[col] = flow_df[col].apply(lambda x: fmt_num(x, ",.0f"))
                            st.dataframe(flow_df, use_container_width=True, hide_index=True)
                            st.caption("최근 수급 추세")
                            chart_df = pd.DataFrame(
                                {
                                    "날짜": [x.get("date", "") for x in flow_table],
                                    "기관": [x.get("institution", 0.0) for x in flow_table],
                                    "외인": [x.get("foreign", 0.0) for x in flow_table],
                                }
                            )
                            chart_long = chart_df.melt(
                                id_vars=["날짜"],
                                value_vars=["기관", "외인"],
                                var_name="구분",
                                value_name="수급",
                            )
                            flow_chart = (
                                alt.Chart(chart_long)
                                .mark_line(point=True)
                                .encode(
                                    x=alt.X("날짜:N", sort=None, axis=alt.Axis(labelAngle=0, title=None)),
                                    y=alt.Y("수급:Q", title=None),
                                    color=alt.Color("구분:N", title=None),
                                    tooltip=["날짜", "구분", alt.Tooltip("수급:Q", format=",.0f")],
                                )
                                .properties(height=240)
                            )
                            st.altair_chart(flow_chart, use_container_width=True)
                        else:
                            if flow_error:
                                st.warning("수급 데이터를 불러오지 못했습니다. 현재 네트워크/프록시 설정으로 KRX 접속이 차단된 상태일 수 있습니다.")
                                st.caption(f"상세 오류: {flow_error}")
                            else:
                                st.caption("수급 데이터가 아직 제공되지 않았습니다.")
        except Exception as exc:
            st.error(f"조회 중 오류가 발생했습니다: {exc}")

st.divider()
st.markdown("실행: `streamlit run stock_streamlit_app.py`")
