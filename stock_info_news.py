import datetime as dt
import json
import os
import re
from functools import lru_cache
from typing import Any, Dict, List, Optional

import feedparser
import FinanceDataReader as fdr
import pandas as pd
import requests
import yfinance as yf

GOOGLE_NEWS_RSS = "https://news.google.com/rss/search"
WIKIPEDIA_API_KO = "https://ko.wikipedia.org/api/rest_v1/page/summary/"
WIKIPEDIA_API_EN = "https://en.wikipedia.org/api/rest_v1/page/summary/"
THEME_MAP_PATH = "krx_theme_map.json"
AUTO_THEME_RULES: Dict[str, List[str]] = {
    "2차전지": ["2차전지", "배터리", "전해액", "양극재", "음극재", "분리막", "리튬", "셀"],
    "반도체": ["반도체", "메모리", "파운드리", "후공정", "전공정", "칩", "웨이퍼"],
    "AI": ["ai", "인공지능", "llm", "gpu", "데이터센터", "클라우드"],
    "바이오": ["바이오", "제약", "항체", "의약", "헬스케어", "cdmo", "백신"],
    "로봇": ["로봇", "자동화", "협동로봇", "모빌리티"],
    "전력인프라": ["변압기", "전력", "전선", "배전", "송전", "전력기기"],
    "전기차": ["전기차", "ev", "자율주행", "충전"],
    "인터넷플랫폼": ["플랫폼", "포털", "커머스", "메신저", "콘텐츠"],
}


def to_text(value: Any) -> str:
    if value is None:
        return ""
    try:
        if pd.isna(value):
            return ""
    except Exception:
        pass
    return str(value)


def disable_broken_proxy_env() -> None:
    for key in ("HTTP_PROXY", "HTTPS_PROXY", "http_proxy", "https_proxy"):
        os.environ.pop(key, None)
    os.environ["NO_PROXY"] = "*"
    os.environ["no_proxy"] = "*"


@lru_cache(maxsize=1)
def get_krx_listing() -> Any:
    try:
        return fdr.StockListing("KRX")
    except Exception:
        frames = []
        for market in ("KOSPI", "KOSDAQ", "KONEX"):
            try:
                frames.append(fdr.StockListing(market))
            except Exception:
                continue
        if not frames:
            raise RuntimeError("종목 목록을 가져오지 못했습니다. 종목코드(예: 348370)로 조회해 주세요.")
        return pd.concat(frames, ignore_index=True).drop_duplicates(subset=["Code"], keep="first")


@lru_cache(maxsize=1)
def get_krx_desc_listing() -> Any:
    try:
        return fdr.StockListing("KRX-DESC")
    except Exception:
        return pd.DataFrame(columns=["Code", "Name", "Sector", "Industry", "Products"])


@lru_cache(maxsize=1)
def load_krx_theme_map() -> Dict[str, List[str]]:
    try:
        with open(THEME_MAP_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, dict):
            return {str(k): [str(t) for t in v] for k, v in data.items() if isinstance(v, list)}
    except Exception:
        pass
    return {}


def get_krx_profile_by_code(symbol: str) -> Dict[str, Any]:
    desc = get_krx_desc_listing()
    if desc.empty:
        return {}
    m = desc[desc["Code"] == symbol]
    if m.empty:
        return {}
    row = m.iloc[0]
    return {
        "name": row.get("Name"),
        "sector": row.get("Sector"),
        "industry": row.get("Industry"),
        "products": row.get("Products"),
    }


def looks_like_krx_code(query: str) -> bool:
    return bool(re.fullmatch(r"\d{6}(?:\.K[QS])?", query.strip().upper()))


def looks_like_global_ticker(query: str) -> bool:
    return bool(re.fullmatch(r"[A-Za-z][A-Za-z0-9.-]{0,9}", query.strip()))


def normalize_query_text(query: str) -> str:
    cleaned = query.strip()
    while len(cleaned) >= 2 and cleaned[0] == cleaned[-1] and cleaned[0] in {"'", '"'}:
        cleaned = cleaned[1:-1].strip()
    return cleaned


def normalize_krx_symbol(query: str) -> str:
    raw = query.strip().upper()
    if raw.endswith(".KS") or raw.endswith(".KQ"):
        return raw.split(".")[0]
    return raw


def search_symbol(query: str) -> Optional[Dict[str, str]]:
    cleaned = normalize_query_text(query)
    if not cleaned:
        return None

    if looks_like_krx_code(cleaned):
        symbol = normalize_krx_symbol(cleaned)
        profile = get_krx_profile_by_code(symbol)
        try:
            listing = get_krx_listing()
            m = listing[listing["Code"] == symbol]
            if not m.empty:
                row = m.iloc[0]
                return {
                    "symbol": symbol,
                    "name": profile.get("name") or row.get("Name", symbol),
                    "exchange": str(row.get("Market", "N/A")),
                    "market_type": "KRX",
                    "sector": profile.get("sector"),
                    "industry": profile.get("industry"),
                    "products": profile.get("products"),
                }
        except Exception:
            pass
        return {
            "symbol": symbol,
            "name": profile.get("name") or symbol,
            "exchange": "KRX",
            "market_type": "KRX",
            "sector": profile.get("sector"),
            "industry": profile.get("industry"),
            "products": profile.get("products"),
        }

    try:
        listing = get_krx_listing()
        m = listing[listing["Name"] == cleaned]
        if m.empty:
            m = listing[listing["Name"].str.contains(cleaned, case=False, na=False, regex=False)]
        if not m.empty:
            row = m.iloc[0]
            profile = get_krx_profile_by_code(row["Code"])
            return {
                "symbol": row["Code"],
                "name": profile.get("name") or row.get("Name", row["Code"]),
                "exchange": str(row.get("Market", "N/A")),
                "market_type": "KRX",
                "sector": profile.get("sector"),
                "industry": profile.get("industry"),
                "products": profile.get("products"),
            }
    except Exception:
        pass

    if looks_like_global_ticker(cleaned):
        ticker = cleaned.upper()
        return {"symbol": ticker, "name": ticker, "exchange": "GLOBAL", "market_type": "GLOBAL"}

    return None


def get_stock_snapshot_krx(symbol: str) -> Dict[str, Any]:
    hist = fdr.DataReader(symbol, start=(dt.datetime.now() - dt.timedelta(days=400)).strftime("%Y-%m-%d"))
    if hist.empty:
        raise ValueError("가격 데이터를 가져오지 못했습니다.")
    latest = hist.iloc[-1]
    prev = hist.iloc[-2] if len(hist) > 1 else None
    latest_close = float(latest["Close"])
    change_pct = None if prev is None or float(prev["Close"]) == 0 else ((latest_close - float(prev["Close"])) / float(prev["Close"])) * 100
    return {
        "symbol": symbol,
        "latest_close": latest_close,
        "change_pct": change_pct,
        "fifty_two_week_high": float(hist["High"].tail(252).max()),
        "fifty_two_week_low": float(hist["Low"].tail(252).min()),
        "volume": float(latest["Volume"]),
        "currency": "KRW",
        "company_description": "국내 상장 기업(KRX) 데이터",
    }


def get_stock_snapshot_global(symbol: str) -> Dict[str, Any]:
    ticker = yf.Ticker(symbol)
    hist = ticker.history(period="1y")
    info = ticker.info or {}
    if hist.empty:
        raise ValueError("해외 종목 가격 데이터를 가져오지 못했습니다.")
    latest = hist.iloc[-1]
    prev = hist.iloc[-2] if len(hist) > 1 else None
    latest_close = float(latest["Close"])
    change_pct = None if prev is None or float(prev["Close"]) == 0 else ((latest_close - float(prev["Close"])) / float(prev["Close"])) * 100
    return {
        "symbol": symbol,
        "latest_close": latest_close,
        "change_pct": change_pct,
        "fifty_two_week_high": float(hist["High"].max()),
        "fifty_two_week_low": float(hist["Low"].min()),
        "volume": float(latest["Volume"]),
        "currency": info.get("currency", "USD"),
        "company_description": info.get("longBusinessSummary") or "회사 설명 정보가 없습니다.",
        "sector": info.get("sector"),
        "industry": info.get("industry"),
        "company_name": info.get("longName") or info.get("shortName") or symbol,
    }


def fetch_wikipedia_summary(term: str) -> Optional[str]:
    if not term:
        return None
    slug = term.strip().replace(" ", "_")
    for base_url in (WIKIPEDIA_API_KO, WIKIPEDIA_API_EN):
        try:
            r = requests.get(base_url + slug, timeout=8)
            if r.status_code == 200:
                summary = r.json().get("extract")
                if summary:
                    return summary
        except Exception:
            continue
    return None


def enrich_company_profile(match: Dict[str, Any], snapshot: Dict[str, Any]) -> Dict[str, Any]:
    profile = dict(snapshot)
    profile.setdefault("sector", match.get("sector"))
    profile.setdefault("industry", match.get("industry"))
    profile.setdefault("products", match.get("products"))
    desc = profile.get("company_description")
    if desc and "정보가 없습니다" not in str(desc):
        return profile
    wiki_term = match.get("name") or profile.get("company_name") or match.get("symbol")
    wiki_summary = fetch_wikipedia_summary(str(wiki_term))
    if wiki_summary:
        profile["company_description"] = wiki_summary
    return profile


def tokenize_kr_text(text: str) -> List[str]:
    if not text:
        return []
    tokens = re.findall(r"[가-힣A-Za-z0-9]{2,}", str(text).lower())
    stop = {"주식", "기업", "회사", "기준", "사업", "제품", "정보", "데이터", "국내", "상장", "분야"}
    return [t for t in tokens if t not in stop]


def infer_themes_from_text(text: str) -> List[str]:
    lowered = (text or "").lower()
    inferred: List[str] = []
    for theme, keywords in AUTO_THEME_RULES.items():
        for kw in keywords:
            if kw.lower() in lowered:
                inferred.append(theme)
                break
    return inferred


def extract_theme_keyword_hits(text: str) -> Dict[str, set]:
    lowered = (text or "").lower()
    hits: Dict[str, set] = {}
    for theme, keywords in AUTO_THEME_RULES.items():
        matched = {kw for kw in keywords if kw.lower() in lowered}
        if matched:
            hits[theme] = matched
    return hits


def get_stock_snapshot(symbol: str, market_type: str = "KRX") -> Dict[str, Any]:
    return get_stock_snapshot_global(symbol) if market_type == "GLOBAL" else get_stock_snapshot_krx(symbol)


def get_related_stocks(match: Dict[str, Any], limit: int = 8) -> List[Dict[str, str]]:
    if match.get("market_type") != "KRX":
        return []

    symbol = str(match.get("symbol", ""))
    theme_map = load_krx_theme_map()
    desc = get_krx_desc_listing()
    base_text = " ".join(
        [
            to_text(match.get("name", "")),
            to_text(match.get("industry", "")),
            to_text(match.get("sector", "")),
            to_text(match.get("products", "")),
        ]
    )
    base_auto_themes = set(infer_themes_from_text(base_text))
    base_theme_hits = extract_theme_keyword_hits(base_text)
    base_themes = set(theme_map.get(symbol, [])) | base_auto_themes

    if base_themes:
        base_tokens = set(
            tokenize_kr_text(
                " ".join(
                    [
                        to_text(match.get("name", "")),
                        to_text(match.get("industry", "")),
                        to_text(match.get("sector", "")),
                        to_text(match.get("products", "")),
                    ]
                )
            )
        )
        scored: List[Dict[str, Any]] = []
        candidate_codes: set[str] = set(theme_map.keys())
        if not desc.empty and "Code" in desc.columns:
            candidate_codes |= set(desc["Code"].astype(str).tolist())

        for code in candidate_codes:
            if code == symbol:
                continue
            themes = set(theme_map.get(code, []))
            overlap = sorted(base_themes.intersection(set(themes)))
            name, industry, products, sector = code, "", "", ""
            if not desc.empty:
                m = desc[desc["Code"] == code]
                if not m.empty:
                    row = m.iloc[0]
                    name = str(row.get("Name", code))
                    industry = str(row.get("Industry", ""))
                    products = str(row.get("Products", ""))
                    sector = str(row.get("Sector", ""))
            candidate_product_industry_text = " ".join([to_text(industry), to_text(products)])
            auto_themes = set(infer_themes_from_text(" ".join([name, industry, products, sector])))
            cand_theme_hits = extract_theme_keyword_hits(candidate_product_industry_text)
            overlap = sorted(base_themes.intersection(themes | auto_themes))
            if not overlap:
                continue
            cand_tokens = set(
                tokenize_kr_text(" ".join([to_text(name), to_text(industry), to_text(products), to_text(sector)]))
            )
            text_overlap = len(base_tokens.intersection(cand_tokens))
            keyword_overlap_count = 0
            matched_keywords: List[str] = []
            for t in overlap:
                base_kw = base_theme_hits.get(t, set())
                cand_kw = cand_theme_hits.get(t, set())
                inter = sorted(base_kw.intersection(cand_kw))
                if inter:
                    keyword_overlap_count += len(inter)
                    matched_keywords.extend([f"{t}:{k}" for k in inter])

            # prioritize concrete product/industry keyword matches
            total_score = (len(overlap) * 10) + (keyword_overlap_count * 6) + text_overlap
            scored.append({
                "symbol": code,
                "name": name,
                "industry": industry,
                "products": products,
                "matched_themes": ", ".join(overlap),
                "theme_score": len(overlap),
                "keyword_score": keyword_overlap_count,
                "text_score": text_overlap,
                "matched_keywords": ", ".join(matched_keywords[:6]),
                "score": total_score,
            })
        scored.sort(
            key=lambda x: (
                -x["score"],
                -x["keyword_score"],
                -x["theme_score"],
                -x["text_score"],
                x["name"],
            )
        )
        return [{k: str(v) for k, v in row.items() if k != "score"} for row in scored[:limit]]

    # fallback
    if desc.empty:
        return []
    industry = match.get("industry")
    sector = match.get("sector")
    candidates = desc[desc["Industry"] == industry] if industry else desc[desc["Sector"] == sector] if sector else pd.DataFrame()
    if candidates.empty:
        return []
    candidates = candidates[candidates["Code"] != symbol]
    result = []
    for _, row in candidates.head(limit).iterrows():
        result.append({
            "symbol": str(row.get("Code", "")),
            "name": str(row.get("Name", "")),
            "industry": str(row.get("Industry", "")),
            "products": str(row.get("Products", "")),
            "matched_themes": "업종 기반 추천",
            "theme_score": "0",
            "keyword_score": "0",
            "text_score": "0",
            "matched_keywords": "",
        })
    return result


def get_recent_news(query: str, limit: int = 5) -> List[Dict[str, str]]:
    r = requests.get(GOOGLE_NEWS_RSS, params={"q": query, "hl": "ko", "gl": "KR", "ceid": "KR:ko"}, timeout=10)
    r.raise_for_status()
    feed = feedparser.parse(r.text)
    return [{"title": e.get("title", "제목 없음"), "link": e.get("link", ""), "published": e.get("published", "")} for e in feed.entries[:limit]]


def main() -> None:
    disable_broken_proxy_env()
    print("주식 종목 정보를 조회합니다. (KRX + GLOBAL)")
    print("종료하려면 q 를 입력하세요.")
    while True:
        q = input("\n종목명 또는 종목코드를 입력하세요: ").strip()
        if not q:
            continue
        if q.lower() in {"q", "quit", "exit"}:
            break
        m = search_symbol(q)
        if not m:
            print("해당 종목을 찾지 못했습니다.")
            continue
        s = enrich_company_profile(m, get_stock_snapshot(m["symbol"], m.get("market_type", "KRX")))
        print(m["name"], s.get("latest_close"))


if __name__ == "__main__":
    main()
