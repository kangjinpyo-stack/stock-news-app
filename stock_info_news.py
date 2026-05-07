import datetime as dt
import json
import os
import re
import tempfile
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
THEME_ONTOLOGY_PATH = "theme_ontology.json"
AUTO_THEME_RULES: Dict[str, List[str]] = {
    "2차전지": ["2차전지", "배터리", "전해액", "양극재", "음극재", "분리막", "리튬", "셀"],
    "반도체": ["반도체", "메모리", "파운드리", "후공정", "전공정", "칩", "웨이퍼"],
    "HBM": ["hbm", "고대역폭메모리", "3d 적층", "2.5d", "advanced packaging"],
    "유리기판": ["유리기판", "글라스기판", "tgv", "패키징기판", "반도체기판"],
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
    "HBM": "semiconductor",
    "유리기판": "semiconductor",
    "AI": "it",
    "인터넷플랫폼": "it",
    "바이오": "bio",
    "정유화학": "energy",
    "로봇": "robotics",
}
THEME_WEIGHT_CORE = 4.0
THEME_WEIGHT_NEWS = 0.6
THEME_WEIGHT_MAPPED = 3.0
RELATED_SCORE_FOCUS = 24
RELATED_SCORE_OVERLAP = 10
RELATED_SCORE_KEYWORD = 12
RELATED_SCORE_ONTOLOGY = 7
RELATED_SCORE_MAPPED = 8
RELATED_SCORE_NEWS = 2
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


def fetch_naver_company_overview(symbol: str) -> Dict[str, str]:
    if not symbol or not re.fullmatch(r"\d{6}", str(symbol).strip()):
        return {}
    try:
        disable_broken_proxy_env()
        r = requests.get(
            f"https://finance.naver.com/item/coinfo.naver?code={symbol}",
            headers={"User-Agent": "Mozilla/5.0"},
            timeout=10,
        )
        r.encoding = "euc-kr"
        html = r.text
    except Exception:
        return {}

    m = re.search(r'<div id="summary_info" class="summary_info">(.*?)<div class="txt_notice">', html, re.S)
    if not m:
        return {}

    block = m.group(1)
    paragraphs = re.findall(r"<p>(.*?)</p>", block, re.S)
    cleaned: List[str] = []
    for p in paragraphs:
        text = re.sub(r"<.*?>", "", p)
        text = re.sub(r"\s+", " ", text).strip()
        if len(text) >= 20:
            cleaned.append(text)
    if not cleaned:
        return {}

    description = " ".join(cleaned[:3]).strip()
    products_text = " ".join(cleaned[1:3]).strip() if len(cleaned) >= 2 else cleaned[0]
    return {
        "company_description": description,
        "products_text": products_text,
    }


def enrich_company_profile(match: Dict[str, Any], snapshot: Dict[str, Any]) -> Dict[str, Any]:
    profile = dict(snapshot)
    profile.setdefault("sector", match.get("sector"))
    profile.setdefault("industry", match.get("industry"))
    profile.setdefault("products", match.get("products"))
    naver_overview = fetch_naver_company_overview(str(match.get("symbol", "")))
    if naver_overview.get("company_description"):
        profile["company_description"] = naver_overview["company_description"]
    if (not to_text(profile.get("products")).strip()) and naver_overview.get("products_text"):
        profile["products"] = naver_overview["products_text"]
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


@lru_cache(maxsize=1)
def build_theme_keyword_bank() -> Dict[str, List[str]]:
    bank: Dict[str, List[str]] = {theme: list(keywords) for theme, keywords in AUTO_THEME_RULES.items()}
    for item in load_theme_ontology().get("themes", []):
        name = to_text(item.get("name"))
        if not name:
            continue
        merged = list(bank.get(name, []))
        merged.extend([to_text(x) for x in item.get("core_keywords", []) if to_text(x)])
        merged.extend([to_text(x) for x in item.get("value_chain", []) if to_text(x)])
        deduped: List[str] = []
        for kw in merged:
            if kw and kw not in deduped:
                deduped.append(kw)
        bank[name] = deduped
    return bank


def score_theme_evidence(core_text: str, news_titles: Optional[List[str]] = None, mapped_themes: Optional[List[str]] = None) -> Dict[str, Dict[str, Any]]:
    core_lower = to_text(core_text).lower()
    news_titles = news_titles or []
    mapped_themes = mapped_themes or []
    evidence: Dict[str, Dict[str, Any]] = {}
    keyword_bank = build_theme_keyword_bank()

    for theme, keywords in keyword_bank.items():
        score = 0.0
        hits: List[str] = []
        core_hits: List[str] = []
        news_hits: List[str] = []
        for kw in keywords:
            kw_text = to_text(kw).strip()
            if not kw_text:
                continue
            kw_lower = kw_text.lower()
            if kw_lower and kw_lower in core_lower:
                score += THEME_WEIGHT_CORE
                if kw_text not in hits:
                    hits.append(kw_text)
                if kw_text not in core_hits:
                    core_hits.append(kw_text)
            for title in news_titles:
                title_text = to_text(title)
                if kw_lower and kw_lower in title_text.lower():
                    score += THEME_WEIGHT_NEWS
                    if kw_text not in hits:
                        hits.append(kw_text)
                    if kw_text not in news_hits:
                        news_hits.append(kw_text)
        if theme in mapped_themes:
            score += THEME_WEIGHT_MAPPED
        if score > 0:
            evidence[theme] = {
                "score": score,
                "hits": hits[:8],
                "core_hits": core_hits[:8],
                "news_hits": news_hits[:8],
                "mapped": theme in mapped_themes,
                "specificity": max(1, len(hits)),
            }
    return evidence


def select_focus_themes(theme_scores: Dict[str, Dict[str, Any]]) -> List[str]:
    if not theme_scores:
        return []
    general_theme_penalty = {
        "반도체": "semiconductor",
        "2차전지": "battery",
        "정유화학": "energy",
        "AI": "it",
        "바이오": "bio",
        "로봇": "robotics",
        "전력인프라": "energy",
        "전기차": "battery",
        "인터넷플랫폼": "it",
    }
    ranked = sorted(
        theme_scores.items(),
        key=lambda item: (
            -float(item[1].get("score", 0)),
            -int(item[1].get("specificity", 0)),
            item[0],
        ),
    )
    selected: List[str] = []
    for theme, data in ranked:
        core_hits = data.get("core_hits", []) or []
        mapped = bool(data.get("mapped"))
        # News-only signals are too noisy for the main theme.
        # Keep themes that are explicitly mapped or directly visible in the company business text.
        if not core_hits and not mapped:
            continue
        ontology_groups = infer_theme_groups_from_ontology([theme])
        group = THEME_EXCLUSIVE_GROUPS.get(theme) or (next(iter(ontology_groups)) if ontology_groups else "")
        if theme in general_theme_penalty and group:
            more_specific_exists = any(
                other != theme
                and (
                    THEME_EXCLUSIVE_GROUPS.get(other) == group
                    or group in infer_theme_groups_from_ontology([other])
                )
                and ((other_data.get("core_hits", []) or []) or other_data.get("mapped"))
                and float(other_data.get("score", 0)) >= float(data.get("score", 0)) * 0.65
                for other, other_data in ranked
            )
            if more_specific_exists:
                continue
        selected.append(theme)
        if len(selected) >= 3:
            break
    return selected


def build_theme_reason(theme: str, base_hits: List[str], cand_hits: List[str], co_mention_score: int) -> str:
    shared = [kw for kw in base_hits if kw in cand_hits]
    if shared:
        return f"{theme} 키워드 일치: {', '.join(shared[:3])}"
    if co_mention_score > 0:
        return f"{theme} 뉴스 동시언급 {co_mention_score}회"
    if cand_hits:
        return f"{theme} 연관 키워드: {', '.join(cand_hits[:3])}"
    return f"{theme} 테마 일치"


def has_business_theme_alignment(theme: str, theme_data: Dict[str, Any], company_text: str) -> bool:
    core_hits = theme_data.get("core_hits", []) or []
    if core_hits:
        return True
    if theme_data.get("mapped"):
        return True
    company_lower = to_text(company_text).lower()
    stricter_checks = {
        "2차전지": ["배터리", "전해액", "양극재", "음극재", "분리막", "리튬", "셀"],
        "전기차": ["전기차", "ev", "충전", "자율주행"],
        "정유화학": ["정유", "석유", "석유화학", "윤활유", "납사", "정제품"],
        "반도체": ["반도체", "메모리", "웨이퍼", "파운드리", "후공정", "전공정"],
        "HBM": ["hbm", "고대역폭메모리", "advanced packaging", "3d 적층", "2.5d"],
        "유리기판": ["유리기판", "글라스기판", "tgv", "패키징기판"],
    }
    required = stricter_checks.get(theme, [])
    return any(token.lower() in company_lower for token in required)


def classify_theme_origin(theme_data: Dict[str, Any]) -> str:
    core_hits = theme_data.get("core_hits", []) or []
    mapped = bool(theme_data.get("mapped"))
    news_hits = theme_data.get("news_hits", []) or []
    if core_hits and mapped:
        return "회사정보+보정"
    if core_hits:
        return "회사정보"
    if mapped:
        return "보정테마"
    if news_hits:
        return "뉴스"
    return "기타"


def categorize_relation_bucket(themes: List[str], industry: str = "", products: str = "") -> str:
    theme_set = set([to_text(x).strip() for x in themes if to_text(x).strip()])
    text = " ".join([to_text(industry), to_text(products)]).lower()

    if theme_set == {"뉴스동시언급"} or not theme_set:
        return "기타 관련주"

    if theme_set.intersection({"2차전지", "전기차"}):
        return "배터리 관련주"
    if theme_set.intersection({"정유화학", "전력인프라"}):
        return "에너지 관련주"
    if theme_set.intersection({"유리기판", "HBM", "반도체"}):
        return "반도체 관련주"
    if theme_set.intersection({"바이오"}):
        return "바이오 관련주"
    if theme_set.intersection({"AI", "인터넷플랫폼"}):
        return "AI/플랫폼 관련주"
    if theme_set.intersection({"로봇"}):
        return "로봇 관련주"

    if any(x in text for x in ["배터리", "전해액", "양극재", "음극재", "분리막", "리튬"]):
        return "배터리 관련주"
    if any(x in text for x in ["정유", "석유", "석유화학", "윤활유", "납사", "정제품", "에너지"]):
        return "에너지 관련주"
    if any(x in text for x in ["반도체", "유리기판", "hbm", "메모리", "파운드리"]):
        return "반도체 관련주"
    return "기타 관련주"


@lru_cache(maxsize=1)
def load_theme_ontology() -> Dict[str, Any]:
    try:
        with open(THEME_ONTOLOGY_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, dict):
            return data
    except Exception:
        pass
    return {"themes": []}


def extract_ontology_hits(text: str) -> Dict[str, set]:
    lowered = to_text(text).lower()
    out: Dict[str, set] = {}
    for t in load_theme_ontology().get("themes", []):
        name = to_text(t.get("name"))
        kws = [to_text(x).lower() for x in t.get("core_keywords", [])]
        hits = {k for k in kws if k and k in lowered}
        if hits:
            out[name] = hits
    return out


def infer_theme_groups_from_ontology(themes: List[str]) -> set:
    groups = set()
    theme_set = set(themes)
    for t in load_theme_ontology().get("themes", []):
        name = to_text(t.get("name"))
        if name in theme_set:
            g = to_text(t.get("group"))
            if g:
                groups.add(g)
    return groups


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
            to_text(match.get("company_description", "")),
        ]
    )
    base_theme_hits = extract_theme_keyword_hits(base_text)
    base_ontology_hits = extract_ontology_hits(base_text)
    news_query = to_text(match.get("name")) or symbol
    base_news_titles = get_news_titles_multi(
        [
            news_query,
            f"{news_query} 관련주",
            f"{news_query} 수혜주",
            f"{news_query} 공급",
            f"{news_query} 고객사",
        ],
        limit_each=8,
    )
    base_score_map = score_theme_evidence(
        base_text,
        news_titles=base_news_titles,
        mapped_themes=list(theme_map.get(symbol, [])),
    )
    base_focus_themes = [
        theme
        for theme in select_focus_themes(base_score_map)
        if has_business_theme_alignment(theme, base_score_map.get(theme, {}), base_text)
    ]
    base_themes = set(base_score_map.keys())
    base_groups = infer_theme_groups(list(base_themes)) | infer_theme_groups_from_ontology(list(base_themes))

    if base_themes:
        base_tokens = set(
            tokenize_kr_text(
                " ".join([to_text(match.get("name", "")), to_text(match.get("industry", "")), to_text(match.get("sector", "")), to_text(match.get("products", "")), to_text(match.get("company_description", ""))])
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
            candidate_text = " ".join([to_text(name), to_text(industry), to_text(products), to_text(sector)])
            cand_theme_hits = extract_theme_keyword_hits(candidate_text)
            cand_ontology_hits = extract_ontology_hits(candidate_text)
            cand_score_map = score_theme_evidence(candidate_text, mapped_themes=list(themes))
            cand_focus_themes = [
                theme
                for theme in select_focus_themes(cand_score_map)
                if has_business_theme_alignment(theme, cand_score_map.get(theme, {}), candidate_text)
            ]
            overlap = sorted(set(base_focus_themes).intersection(set(cand_focus_themes)))
            broad_overlap = sorted(
                theme
                for theme in base_themes.intersection(set(cand_score_map.keys()))
                if has_business_theme_alignment(theme, base_score_map.get(theme, {}), base_text)
                and has_business_theme_alignment(theme, cand_score_map.get(theme, {}), candidate_text)
            )
            co_mention_score = 0
            for title in base_news_titles:
                tline = to_text(title)
                if not tline:
                    continue
                if len(to_text(name).strip()) < 3:
                    continue
                if name and name in tline:
                    co_mention_score += 1

            # If a stock has a specific semiconductor subtheme such as HBM or 유리기판,
            # don't keep candidates that only overlap on the broad "반도체" label.
            specific_semiconductor_themes = {"HBM", "유리기판"}
            base_specific_semis = set(base_focus_themes).intersection(specific_semiconductor_themes)
            cand_specific_semis = set(cand_focus_themes).intersection(specific_semiconductor_themes)
            if base_specific_semis:
                if overlap == ["반도체"] and not cand_specific_semis:
                    continue
                if cand_specific_semis and base_specific_semis.isdisjoint(cand_specific_semis) and co_mention_score < 2:
                    continue

            if not overlap and co_mention_score < 1:
                continue
            if not overlap and broad_overlap:
                overlap = broad_overlap[:2]
            cand_groups = infer_theme_groups(list(cand_score_map.keys())) | infer_theme_groups_from_ontology(list(cand_score_map.keys()))
            if base_groups and cand_groups and base_groups.isdisjoint(cand_groups) and co_mention_score < 1:
                continue
            cand_tokens = set(
                tokenize_kr_text(" ".join([to_text(name), to_text(industry), to_text(products), to_text(sector)]))
            )
            text_overlap = len(base_tokens.intersection(cand_tokens))
            if not overlap and co_mention_score >= 1:
                if text_overlap < 3:
                    continue
                overlap = ["뉴스동시언급"]
            keyword_overlap_count = 0
            matched_keywords: List[str] = []
            theme_reason_parts: List[str] = []
            for t in overlap:
                base_kw = set(base_theme_hits.get(t, set())).union(base_ontology_hits.get(t, set()))
                cand_kw = set(cand_theme_hits.get(t, set())).union(cand_ontology_hits.get(t, set()))
                base_kw = base_kw.union(set(base_score_map.get(t, {}).get("hits", [])))
                cand_kw = cand_kw.union(set(cand_score_map.get(t, {}).get("hits", [])))
                inter = sorted(base_kw.intersection(cand_kw))
                if inter:
                    keyword_overlap_count += len(inter)
                    matched_keywords.extend([f"{t}:{k}" for k in inter])
                theme_reason_parts.append(
                    build_theme_reason(
                        t,
                        list(base_kw),
                        list(cand_kw),
                        co_mention_score,
                    )
                )

            # Strict filter:
            # 1) Prefer concrete business-keyword overlap on the selected focus themes.
            # 2) News co-mention alone should not create a relation unless there is at least
            #    weak business text overlap or mapped-theme support.
            mapped_focus_overlap = len(
                {
                    theme
                    for theme in set(theme_map.get(code, [])).intersection(set(base_focus_themes))
                    if has_business_theme_alignment(theme, base_score_map.get(theme, {}), base_text)
                }
            )
            if keyword_overlap_count < 1 and co_mention_score < 1 and mapped_focus_overlap < 1:
                continue
            if keyword_overlap_count < 1 and co_mention_score >= 1 and text_overlap < 3 and mapped_focus_overlap < 1:
                continue

            # prioritize concrete product/industry overlap on the most likely theme
            ontology_overlap_count = len(set(overlap).intersection(set(cand_ontology_hits.keys())))
            focus_overlap_count = len(set(base_focus_themes).intersection(set(cand_focus_themes)))
            base_focus_score = sum(float(base_score_map.get(t, {}).get("score", 0)) for t in overlap)
            cand_focus_score = sum(float(cand_score_map.get(t, {}).get("score", 0)) for t in overlap)
            base_origin_bonus = sum(
                2
                if classify_theme_origin(base_score_map.get(t, {})) == "회사정보+보정"
                else 1
                if classify_theme_origin(base_score_map.get(t, {})) in {"회사정보", "보정테마"}
                else 0
                for t in overlap
            )
            cand_origin_bonus = sum(
                2
                if classify_theme_origin(cand_score_map.get(t, {})) == "회사정보+보정"
                else 1
                if classify_theme_origin(cand_score_map.get(t, {})) in {"회사정보", "보정테마"}
                else 0
                for t in overlap
            )
            total_score = (
                (focus_overlap_count * RELATED_SCORE_FOCUS)
                + (len(overlap) * RELATED_SCORE_OVERLAP)
                + (keyword_overlap_count * RELATED_SCORE_KEYWORD)
                + (ontology_overlap_count * RELATED_SCORE_ONTOLOGY)
                + (mapped_focus_overlap * RELATED_SCORE_MAPPED)
                + min(base_focus_score + cand_focus_score, 24)
                + (base_origin_bonus + cand_origin_bonus)
                + (co_mention_score * RELATED_SCORE_NEWS)
                + text_overlap
            )
            scored.append({
                "symbol": code,
                "name": name,
                "industry": industry,
                "products": products,
                "matched_themes": ", ".join(overlap),
                "relation_bucket": categorize_relation_bucket(overlap, industry, products),
                "theme_score": focus_overlap_count or len(overlap),
                "keyword_score": keyword_overlap_count,
                "text_score": text_overlap,
                "ontology_score": ontology_overlap_count,
                "co_mention_score": co_mention_score,
                "theme_origin": ", ".join(
                    [
                        f"{t}:{classify_theme_origin(base_score_map.get(t, {}))}/{classify_theme_origin(cand_score_map.get(t, {}))}"
                        for t in overlap[:3]
                    ]
                ),
                "matched_keywords": ", ".join(matched_keywords[:6]),
                "theme_reason": " | ".join([x for x in theme_reason_parts if x][:2]),
                "score": total_score,
            })
        scored.sort(
            key=lambda x: (
                -x["score"],
                -x["theme_score"],
                -x["keyword_score"],
                -x["ontology_score"],
                -x["co_mention_score"],
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
            "relation_bucket": "기타 관련주",
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


def infer_themes_from_news(query: str, limit: int = 12) -> Dict[str, Any]:
    try:
        items = get_recent_news(query, limit=limit)
    except Exception:
        return {"themes": set(), "hits": {}}

    titles = " ".join([to_text(x.get("title", "")) for x in items])
    themes = set(infer_themes_from_text(titles))
    hits = extract_theme_keyword_hits(titles)
    return {"themes": themes, "hits": hits}


def get_news_titles(query: str, limit: int = 20) -> List[str]:
    try:
        items = get_recent_news(query, limit=limit)
        return [to_text(x.get("title", "")) for x in items]
    except Exception:
        return []


def get_news_titles_multi(queries: List[str], limit_each: int = 10) -> List[str]:
    seen = set()
    out: List[str] = []
    for q in queries:
        for title in get_news_titles(q, limit=limit_each):
            t = to_text(title).strip()
            if not t or t in seen:
                continue
            seen.add(t)
            out.append(t)
    return out


def get_krx_fundamentals_and_flow(symbol: str) -> Dict[str, Any]:
    """KRX용 최근 재무지표/수급 요약. pykrx 미설치 또는 조회 실패 시 빈값 반환."""
    result: Dict[str, Any] = {
        "fundamental": {},
        "fundamental_basis": {},
        "flow": {},
        "flow_table": [],
        "error": None,
    }
    # pykrx import 시 matplotlib 캐시 경로 권한 이슈 방지
    os.environ.setdefault("MPLCONFIGDIR", os.path.join(tempfile.gettempdir(), "matplotlib"))

    try:
        from pykrx import stock  # type: ignore
    except Exception:
        stock = None  # type: ignore

    now = dt.datetime.now()
    today = now.strftime("%Y%m%d")
    start_60 = (now - dt.timedelta(days=60)).strftime("%Y%m%d")
    start_20 = (now - dt.timedelta(days=20)).strftime("%Y%m%d")

    if stock is not None:
        def _pick_val(obj: Any, keys: List[str]) -> Optional[float]:
            for k in keys:
                if k in obj and pd.notna(obj[k]):
                    try:
                        return float(obj[k])
                    except Exception:
                        continue
            return None

        def _find_recent_business_day(sym: str, back_days: int = 14) -> Optional[str]:
            for i in range(back_days + 1):
                d = (now - dt.timedelta(days=i)).strftime("%Y%m%d")
                try:
                    chk = stock.get_market_fundamental_by_date(d, d, sym)
                    if chk is not None and not chk.empty:
                        return d
                except Exception:
                    continue
            return None

        end_day = _find_recent_business_day(symbol) or today
        start_60 = (dt.datetime.strptime(end_day, "%Y%m%d") - dt.timedelta(days=60)).strftime("%Y%m%d")
        start_20 = (dt.datetime.strptime(end_day, "%Y%m%d") - dt.timedelta(days=20)).strftime("%Y%m%d")

        try:
            f = stock.get_market_fundamental_by_date(start_60, end_day, symbol)
            if not f.empty:
                last = f.iloc[-1]
                result["fundamental"] = {
                    "BPS": _pick_val(last, ["BPS", "bps", "주당순자산가치"]),
                    "PER": _pick_val(last, ["PER", "per"]),
                    "PBR": _pick_val(last, ["PBR", "pbr"]),
                    "EPS": _pick_val(last, ["EPS", "eps", "주당순이익"]),
                    "DIV": _pick_val(last, ["DIV", "div", "배당수익률"]),
                    "DPS": _pick_val(last, ["DPS", "dps", "주당배당금"]),
                }
            else:
                if not result.get("error"):
                    result["error"] = "KRX 재무 데이터가 비어 있습니다."
        except Exception as e:
            msg = str(e)
            if "None of [Index(['BPS', 'PER', 'PBR', 'EPS', 'DIV', 'DPS']" in msg:
                result["error"] = "KRX 재무 데이터 포맷이 변경되어 재무 지표를 가져오지 못했습니다."
            else:
                result["error"] = msg

        try:
            t = stock.get_market_trading_value_by_date(start_20, end_day, symbol)
            if not t.empty:
                cols = [c for c in ["기관합계", "외국인합계"] if c in t.columns]
                if cols:
                    recent = t[cols].tail(5).copy()
                    flow_table = []
                    for idx, row in recent.iterrows():
                        flow_table.append(
                            {
                                "date": str(idx.date()) if hasattr(idx, "date") else str(idx),
                                "institution": float(row["기관합계"]) if "기관합계" in row else 0.0,
                                "foreign": float(row["외국인합계"]) if "외국인합계" in row else 0.0,
                            }
                        )
                    result["flow_table"] = flow_table

                    inst_5d = float(recent["기관합계"].sum()) if "기관합계" in recent else 0.0
                    for_5d = float(recent["외국인합계"].sum()) if "외국인합계" in recent else 0.0
                    result["flow"] = {
                        "inst_5d": inst_5d,
                        "foreign_5d": for_5d,
                    }
            else:
                # fallback 2: 투자자별 거래대금 API로 재시도
                try:
                    investors = ["기관합계", "외국인", "외국인합계"]
                    rows: List[Dict[str, Any]] = []
                    for inv in investors:
                        try:
                            iv = stock.get_market_trading_value_by_investor(
                                start_20, end_day, symbol, inv
                            )
                            if iv is not None and not iv.empty:
                                rows.append({"inv": inv, "df": iv})
                        except Exception:
                            continue
                    if rows:
                        base = rows[0]["df"].copy()
                        base_cols = {}
                        for r in rows:
                            df_inv = r["df"]
                            col_name = "institution" if "기관" in r["inv"] else "foreign"
                            val_col = "순매수" if "순매수" in df_inv.columns else (
                                df_inv.columns[0] if len(df_inv.columns) > 0 else None
                            )
                            if val_col is None:
                                continue
                            base_cols[col_name] = df_inv[val_col]
                        if base_cols:
                            recent_df = pd.DataFrame(base_cols).tail(5).fillna(0.0)
                            flow_table = []
                            for idx, row in recent_df.iterrows():
                                flow_table.append(
                                    {
                                        "date": str(idx.date()) if hasattr(idx, "date") else str(idx),
                                        "institution": float(row.get("institution", 0.0)),
                                        "foreign": float(row.get("foreign", 0.0)),
                                    }
                                )
                            result["flow_table"] = flow_table
                            result["flow"] = {
                                "inst_5d": float(recent_df.get("institution", pd.Series(dtype=float)).sum()),
                                "foreign_5d": float(recent_df.get("foreign", pd.Series(dtype=float)).sum()),
                            }
                    elif not result.get("error"):
                        result["error"] = "KRX 수급 데이터가 비어 있습니다."
                except Exception as e2:
                    if not result.get("error"):
                        result["error"] = f"KRX 수급 fallback 실패: {e2}"
        except Exception as e:
            if not result.get("error"):
                result["error"] = str(e)

    # pykrx 실패/공란 시 KRX 티커(.KS/.KQ) 대상으로 Yahoo fallback
    if not result.get("fundamental"):
        result["fundamental"] = {}
    if any(result["fundamental"].get(k) is None for k in ["PER", "PBR", "EPS"]):
        for suffix in [".KS", ".KQ"]:
            try:
                info = yf.Ticker(f"{symbol}{suffix}").info or {}
                per = info.get("trailingPE")
                pbr = info.get("priceToBook")
                eps = info.get("trailingEps")
                if per is None and pbr is None and eps is None:
                    continue
                result["fundamental"]["PER"] = result["fundamental"].get("PER")
                if result["fundamental"]["PER"] is None and per is not None:
                    result["fundamental"]["PER"] = float(per)
                result["fundamental"]["PBR"] = result["fundamental"].get("PBR")
                if result["fundamental"]["PBR"] is None and pbr is not None:
                    result["fundamental"]["PBR"] = float(pbr)
                result["fundamental"]["EPS"] = result["fundamental"].get("EPS")
                if result["fundamental"]["EPS"] is None and eps is not None:
                    result["fundamental"]["EPS"] = float(eps)
                if not result.get("error"):
                    result["error"] = "KRX 원본 실패로 Yahoo 재무 fallback 사용 중(수급은 KRX 필요)."
                break
            except Exception:
                continue

    if not result.get("fundamental") and not result.get("flow_table") and not result.get("error"):
        result["error"] = "재무/수급 데이터 소스에서 유효 데이터를 받지 못했습니다."

    # 최종 fallback: 네이버 증권 HTML 파싱
    if not result.get("fundamental") or not result.get("flow_table"):
        try:
            disable_broken_proxy_env()
            naver_url = f"https://finance.naver.com/item/main.naver?code={symbol}"
            tables = pd.read_html(naver_url)

            if not result.get("flow_table"):
                for df in tables:
                    cols = [str(c) for c in df.columns]
                    if all(x in cols for x in ["날짜", "외국인", "기관"]):
                        tmp = df.copy()
                        tmp = tmp.dropna(subset=["날짜"])
                        if tmp.empty:
                            continue
                        tmp["외국인"] = pd.to_numeric(tmp["외국인"], errors="coerce")
                        tmp["기관"] = pd.to_numeric(tmp["기관"], errors="coerce")
                        tmp = tmp.dropna(subset=["외국인", "기관"]).head(5)
                        if tmp.empty:
                            continue
                        flow_table = []
                        for _, row in tmp.iterrows():
                            flow_table.append(
                                {
                                    "date": str(row["날짜"]),
                                    "institution": float(row["기관"]),
                                    "foreign": float(row["외국인"]),
                                }
                            )
                        result["flow_table"] = flow_table
                        result["flow"] = {
                            "inst_5d": float(tmp["기관"].sum()),
                            "foreign_5d": float(tmp["외국인"].sum()),
                        }
                        if result.get("error"):
                            result["error"] = "KRX 수급 실패로 네이버 수급 fallback 사용 중"
                        break

            if any(result["fundamental"].get(k) is None for k in ["PER", "PBR", "EPS"]):
                # 1) 요약지표 테이블 우선 (PERlEPS / PBRlBPS)
                for df in tables:
                    try:
                        if df.shape[1] != 2:
                            continue
                        first = df.iloc[:, 0].astype(str)
                        if not first.str.contains("PERlEPS|PBRlBPS|PBR\\|BPS", regex=True).any():
                            continue
                        per = pbr = eps = None
                        per_basis = pbr_basis = eps_basis = None
                        for _, row in df.iterrows():
                            k = str(row.iloc[0])
                            v = str(row.iloc[1])
                            if "추정PER" in k:
                                token = v.split("l")[0].strip().replace("배", "").replace(",", "")
                                est_per = float(token) if token and token.upper() != "N/A" else None
                                if "l" in v:
                                    rhs = v.split("l")[-1].strip().replace("원", "").replace(",", "")
                                    est_eps = float(rhs) if rhs and rhs.upper() != "N/A" else None
                                else:
                                    est_eps = None
                                if per is None and est_per is not None:
                                    per = est_per
                                    per_basis = "추정"
                                if est_per is not None and est_eps is not None:
                                    eps = est_eps
                                    eps_basis = "추정"
                            elif "PER" in k:
                                token = v.split("l")[0].strip().replace("배", "").replace(",", "")
                                actual_per = float(token) if token and token.upper() != "N/A" else None
                                if actual_per is not None:
                                    per = actual_per
                                    basis_text = k.replace("PERlEPS", "").strip("() ")
                                    per_basis = basis_text or "실적"
                                if "l" in v:
                                    rhs = v.split("l")[-1].strip().replace("원", "").replace(",", "")
                                    actual_eps = float(rhs) if rhs and rhs.upper() != "N/A" else None
                                    if actual_eps is not None:
                                        eps = actual_eps
                                        basis_text = k.replace("PERlEPS", "").strip("() ")
                                        eps_basis = basis_text or "실적"
                            elif "PBR" in k:
                                token = v.split("l")[0].strip().replace("배", "").replace(",", "")
                                pbr = float(token) if token and token.upper() != "N/A" else None
                                basis_text = k.replace("PBRlBPS", "").replace("PBR|BPS", "").strip("() ")
                                pbr_basis = basis_text or "실적"
                                if eps is None and "l" in v:
                                    rhs = v.split("l")[-1].strip().replace("원", "").replace(",", "")
                                    if rhs and rhs.upper() != "N/A":
                                        eps = float(rhs)
                        if per is not None or pbr is not None or eps is not None:
                            result["fundamental"]["PER"] = result["fundamental"].get("PER", per)
                            if result["fundamental"]["PER"] is None:
                                result["fundamental"]["PER"] = per
                            result["fundamental"]["PBR"] = result["fundamental"].get("PBR", pbr)
                            if result["fundamental"]["PBR"] is None:
                                result["fundamental"]["PBR"] = pbr
                            result["fundamental"]["EPS"] = result["fundamental"].get("EPS", eps)
                            if result["fundamental"]["EPS"] is None:
                                result["fundamental"]["EPS"] = eps
                            if per_basis:
                                result["fundamental_basis"]["PER"] = per_basis
                            if pbr_basis:
                                result["fundamental_basis"]["PBR"] = pbr_basis
                            if eps_basis:
                                result["fundamental_basis"]["EPS"] = eps_basis
                            if result.get("error"):
                                result["error"] = "KRX 재무 실패로 네이버 요약지표 fallback 사용 중"
                            break
                    except Exception:
                        continue

            if any(result["fundamental"].get(k) is None for k in ["PER", "PBR", "EPS"]):
                # 2) 주요재무정보 테이블 보조 fallback
                for df in tables:
                    if len(df.columns) < 2:
                        continue
                    first_col = str(df.columns[0])
                    if "주요재무정보" not in first_col:
                        continue
                    dfx = df.copy()
                    dfx.columns = ["__item__"] + [f"c{i}" for i in range(1, len(dfx.columns))]
                    dfx["__item__"] = dfx["__item__"].astype(str)

                    def _extract_metric(name_prefix: str) -> Optional[float]:
                        rows = dfx[dfx["__item__"].str.startswith(name_prefix)]
                        if rows.empty:
                            return None
                        vals = pd.to_numeric(rows.iloc[0, 1:], errors="coerce").dropna()
                        if vals.empty:
                            return None
                        return float(vals.iloc[-1])

                    per = _extract_metric("PER")
                    pbr = _extract_metric("PBR")
                    eps = _extract_metric("EPS")
                    if per is not None or pbr is not None or eps is not None:
                        if result["fundamental"].get("PER") is None:
                            result["fundamental"]["PER"] = per
                        if result["fundamental"].get("PBR") is None:
                            result["fundamental"]["PBR"] = pbr
                        if result["fundamental"].get("EPS") is None:
                            result["fundamental"]["EPS"] = eps
                        if result.get("error"):
                            result["error"] = "KRX 재무 실패로 네이버 재무 fallback 사용 중"
                        break
        except Exception:
            pass

    return result


def get_krx_peer_comparison(symbol: str) -> List[Dict[str, Any]]:
    """네이버 종목 페이지의 동종업계 비교 테이블을 파싱."""
    try:
        disable_broken_proxy_env()
        tables = pd.read_html(f"https://finance.naver.com/item/main.naver?code={symbol}")
    except Exception:
        return []

    latest_actual_quarter = ""
    for df in tables:
        try:
            if not isinstance(df.columns, pd.MultiIndex):
                continue
            quarter_labels: List[str] = []
            for col in df.columns:
                if len(col) >= 2 and str(col[0]).strip() == "최근 분기 실적":
                    label = str(col[1]).strip()
                    if label and "(E)" not in label:
                        quarter_labels.append(label)
            if quarter_labels:
                latest_actual_quarter = quarter_labels[-1]
                break
        except Exception:
            continue

    for df in tables:
        try:
            cols = [str(c) for c in df.columns]
            if len(cols) < 2 or "종목명" not in cols[0]:
                continue
            label_col = cols[0]
            row_map: Dict[str, Any] = {}
            for _, row in df.iterrows():
                key = to_text(row.iloc[0]).strip()
                if key:
                    row_map[key] = row

            out: List[Dict[str, Any]] = []
            for j in range(1, len(cols)):
                col_name = cols[j]
                peer_name = col_name.split("*")[0].strip()
                peer_symbol = col_name.split("*")[-1].strip() if "*" in col_name else ""
                if not peer_name:
                    continue
                if peer_symbol == symbol:
                    continue
                price = row_map.get("현재가", pd.Series()).iloc[j] if "현재가" in row_map else None
                mcap = row_map.get("시가총액(억)", pd.Series()).iloc[j] if "시가총액(억)" in row_map else None
                sales = row_map.get("매출액(억)", pd.Series()).iloc[j] if "매출액(억)" in row_map else None
                op = row_map.get("영업이익(억)", pd.Series()).iloc[j] if "영업이익(억)" in row_map else None
                out.append(
                    {
                        "name": peer_name,
                        "symbol": peer_symbol,
                        "price": to_text(price),
                        "market_cap_100m": to_text(mcap),
                        "sales_100m": to_text(sales),
                        "op_100m": to_text(op),
                        "sales_basis": latest_actual_quarter or "최근 분기",
                        "op_basis": latest_actual_quarter or "최근 분기",
                    }
                )
            if out:
                return out
        except Exception:
            continue
    return []


def get_krx_financial_table(symbol: str) -> List[Dict[str, Any]]:
    """네이버 기업실적분석 표를 화면용 행 데이터로 변환."""
    try:
        disable_broken_proxy_env()
        tables = pd.read_html(f"https://finance.naver.com/item/main.naver?code={symbol}")
    except Exception:
        return []

    for df in tables:
        try:
            if not isinstance(df.columns, pd.MultiIndex):
                continue
            first_col = str(df.columns[0][0]).strip()
            if first_col != "주요재무정보":
                continue

            out_rows: List[Dict[str, Any]] = []
            flat_columns = []
            for idx, col in enumerate(df.columns):
                if idx == 0:
                    flat_columns.append("주요재무정보")
                else:
                    group_name = str(col[0]).strip()
                    period_name = str(col[1]).strip()
                    prefix = "연간" if group_name == "최근 연간 실적" else "분기"
                    flat_columns.append(f"{prefix} {period_name}")

            tmp = df.copy()
            tmp.columns = flat_columns

            for _, row in tmp.iterrows():
                item_name = to_text(row.iloc[0]).strip()
                if not item_name:
                    continue
                row_data: Dict[str, Any] = {"주요재무정보": item_name}
                for col_name in flat_columns[1:]:
                    row_data[col_name] = row.get(col_name)
                out_rows.append(row_data)

            if out_rows:
                return out_rows
        except Exception:
            continue
    return []


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
