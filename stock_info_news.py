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
    "바이오": ["바이오", "제약", "항체", "의약품", "cdmo", "백신", "신약"],
    "정유화학": ["정유", "석유", "석유화학", "윤활유", "아스팔트", "납사", "정제품"],
    "로봇": ["로봇", "자동화", "협동로봇", "모빌리티"],
    "전력인프라": ["변압기", "전력", "전선", "배전", "송전", "전력기기"],
    "전기차": ["전기차", "ev", "자율주행", "충전"],
    "인터넷플랫폼": ["플랫폼", "포털", "커머스", "메신저", "콘텐츠"],
}

THEME_EXCLUSIVE_GROUPS: Dict[str, str] = {
    "2차전지": "battery",
    "전기차": "battery",
    "전력인프라": "energy",
    "반도체": "semiconductor",
    "AI": "it",
    "인터넷플랫폼": "it",
    "바이오": "bio",
    "정유화학": "energy",
    "로봇": "robotics",
}
GLOBAL_SEED_TICKERS: List[str] = [
    "AAPL",
    "MSFT",
    "NVDA",
    "AMZN",
    "GOOGL",
    "META",
    "TSLA",
    "AMD",
    "AVGO",
    "ORCL",
    "NFLX",
    "CRM",
    "INTC",
    "QCOM",
    "MU",
]

# Keep yfinance cache in a writable temp folder across OSes.
try:
    import tempfile

    _yf_tmp = os.path.join(tempfile.gettempdir(), "yfinance-cache")
    os.makedirs(_yf_tmp, exist_ok=True)
    yf.set_tz_cache_location(_yf_tmp)
except Exception:
    pass


def to_text(value: Any) -> str:
    if value is None:
        return ""
    try:
        if pd.isna(value):
            return ""
    except Exception:
        pass
    return str(value)


MARKET_DEPT_LABELS = {
    "중견기업부",
    "벤처기업부",
    "우량기업부",
    "기술성장기업부",
    "일반기업부",
}


def normalize_sector_for_display(sector: Any) -> Dict[str, str]:
    sector_text = to_text(sector).strip()
    if not sector_text:
        return {"sector": "", "market_dept": ""}
    if sector_text in MARKET_DEPT_LABELS:
        return {"sector": "", "market_dept": sector_text}
    return {"sector": sector_text, "market_dept": ""}


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
                    "sector": normalize_sector_for_display(profile.get("sector")).get("sector"),
                    "market_dept": normalize_sector_for_display(profile.get("sector")).get("market_dept"),
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
            "sector": normalize_sector_for_display(profile.get("sector")).get("sector"),
            "market_dept": normalize_sector_for_display(profile.get("sector")).get("market_dept"),
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
                "sector": normalize_sector_for_display(profile.get("sector")).get("sector"),
                "market_dept": normalize_sector_for_display(profile.get("sector")).get("market_dept"),
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
    import tempfile
    import yfinance.cache as yf_cache

    try:
        cache_dir = os.path.join(tempfile.gettempdir(), "yfinance-cache")
        os.makedirs(cache_dir, exist_ok=True)
        yf_cache.set_cache_location(cache_dir)
        yf.set_tz_cache_location(cache_dir)
    except Exception:
        pass

    hist = yf.download(symbol, period="1y", progress=False, auto_adjust=False, threads=False)
    if isinstance(hist.columns, pd.MultiIndex):
        # flatten multi-index columns from yfinance download
        hist.columns = [c[0] if isinstance(c, tuple) else c for c in hist.columns]

    if hist.empty:
        try:
            hist = yf.Ticker(symbol).history(period="1y", auto_adjust=False)
        except Exception:
            hist = pd.DataFrame()

    if isinstance(hist.columns, pd.MultiIndex):
        hist.columns = [c[0] if isinstance(c, tuple) else c for c in hist.columns]

    if hist.empty:
        try:
            hist = yf.download(symbol, period="6mo", progress=False, auto_adjust=False, threads=False)
            if isinstance(hist.columns, pd.MultiIndex):
                hist.columns = [c[0] if isinstance(c, tuple) else c for c in hist.columns]
        except Exception:
            hist = pd.DataFrame()

    info = {}
    try:
        info = yf.Ticker(symbol).info or {}
    except Exception:
        info = {}
    if hist.empty:
        return {
            "symbol": symbol,
            "latest_close": None,
            "change_pct": None,
            "fifty_two_week_high": None,
            "fifty_two_week_low": None,
            "volume": None,
            "currency": info.get("currency", "USD"),
            "company_description": info.get("longBusinessSummary") or "해외 종목 가격 데이터를 가져오지 못했습니다.",
            "sector": info.get("sector"),
            "industry": info.get("industry"),
            "company_name": info.get("longName") or info.get("shortName") or symbol,
        }
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


@lru_cache(maxsize=1)
def get_global_seed_profiles() -> List[Dict[str, str]]:
    profiles: List[Dict[str, str]] = []
    for ticker in GLOBAL_SEED_TICKERS:
        try:
            info = {}
            try:
                info = yf.Ticker(ticker).info or {}
            except Exception:
                info = {}
            profiles.append(
                {
                    "symbol": ticker,
                    "name": str(info.get("longName") or info.get("shortName") or ticker),
                    "sector": str(info.get("sector") or ""),
                    "industry": str(info.get("industry") or ""),
                    "summary": str(info.get("longBusinessSummary") or ""),
                }
            )
        except Exception:
            continue
    return profiles


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


def infer_theme_groups(themes: List[str]) -> set:
    groups = set()
    for t in themes:
        g = THEME_EXCLUSIVE_GROUPS.get(t)
        if g:
            groups.add(g)
    return groups


def get_stock_snapshot(symbol: str, market_type: str = "KRX") -> Dict[str, Any]:
    return get_stock_snapshot_global(symbol) if market_type == "GLOBAL" else get_stock_snapshot_krx(symbol)


def get_related_stocks(match: Dict[str, Any], limit: int = 8) -> List[Dict[str, str]]:
    if match.get("market_type") != "KRX":
        base_symbol = str(match.get("symbol", "")).upper()
        base_info = {}
        try:
            base_info = yf.Ticker(base_symbol).info or {}
        except Exception:
            base_info = {}

        base_sector = str(base_info.get("sector") or "")
        base_industry = str(base_info.get("industry") or "")
        base_summary = str(base_info.get("longBusinessSummary") or "")
        base_tokens = set(tokenize_kr_text(base_summary + " " + base_industry + " " + base_sector))

        candidates = get_global_seed_profiles()
        scored: List[Dict[str, Any]] = []
        for c in candidates:
            symbol = c.get("symbol", "").upper()
            if symbol == base_symbol:
                continue
            same_industry = int(bool(base_industry and c.get("industry") == base_industry))
            same_sector = int(bool(base_sector and c.get("sector") == base_sector))
            cand_tokens = set(tokenize_kr_text(c.get("summary", "") + " " + c.get("industry", "") + " " + c.get("sector", "")))
            text_overlap = len(base_tokens.intersection(cand_tokens))
            score = (same_industry * 15) + (same_sector * 8) + text_overlap
            if score <= 0:
                continue
            matched_theme = c.get("industry") or c.get("sector") or "GLOBAL"
            scored.append(
                {
                    "symbol": symbol,
                    "name": c.get("name", symbol),
                    "industry": c.get("industry", ""),
                    "products": c.get("industry", ""),
                    "matched_themes": matched_theme,
                    "theme_score": same_industry + same_sector,
                    "keyword_score": text_overlap,
                    "text_score": text_overlap,
                    "matched_keywords": "global-profile-match",
                    "score": score,
                }
            )
        scored.sort(key=lambda x: (-x["score"], x["symbol"]))
        return [{k: str(v) for k, v in row.items() if k != "score"} for row in scored[:limit]]

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
    base_groups = infer_theme_groups(list(base_themes))

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
            cand_groups = infer_theme_groups(list(themes | auto_themes))
            if base_groups and cand_groups and base_groups.isdisjoint(cand_groups):
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

            # Strict filter: require at least one concrete business keyword overlap.
            if keyword_overlap_count < 1:
                continue

            # prioritize concrete product/industry keyword matches
            total_score = (len(overlap) * 12) + (keyword_overlap_count * 10) + text_overlap
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
