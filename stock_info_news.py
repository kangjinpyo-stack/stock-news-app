import datetime as dt
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
            raise RuntimeError(
                "종목 목록을 가져오지 못했습니다. 종목코드(예: 348370)로 조회해 주세요."
            )
        return pd.concat(frames, ignore_index=True).drop_duplicates(subset=["Code"], keep="first")


@lru_cache(maxsize=1)
def get_krx_desc_listing() -> Any:
    try:
        return fdr.StockListing("KRX-DESC")
    except Exception:
        return pd.DataFrame(columns=["Code", "Name", "Sector", "Industry"])


def get_krx_profile_by_code(symbol: str) -> Dict[str, Any]:
    desc = get_krx_desc_listing()
    if desc.empty:
        return {}
    matched = desc[desc["Code"] == symbol]
    if matched.empty:
        return {}
    row = matched.iloc[0]
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
            matched = listing[listing["Code"] == symbol]
            if not matched.empty:
                row = matched.iloc[0]
                market = row.get("Market", "N/A")
                return {
                    "symbol": symbol,
                    "name": profile.get("name") or row.get("Name", symbol),
                    "exchange": str(market),
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
        matched = listing[listing["Name"] == cleaned]
        if matched.empty:
            matched = listing[listing["Name"].str.contains(cleaned, case=False, na=False, regex=False)]
        if not matched.empty:
            row = matched.iloc[0]
            profile = get_krx_profile_by_code(row["Code"])
            market = row.get("Market", "N/A")
            return {
                "symbol": row["Code"],
                "name": profile.get("name") or row.get("Name", row["Code"]),
                "exchange": str(market),
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

    latest_close = float(latest["Close"]) if "Close" in latest else None
    change_pct = None
    if prev is not None and "Close" in prev and float(prev["Close"]) != 0:
        change_pct = ((latest_close - float(prev["Close"])) / float(prev["Close"])) * 100

    fifty_two_week_high = float(hist["High"].tail(252).max()) if "High" in hist else None
    fifty_two_week_low = float(hist["Low"].tail(252).min()) if "Low" in hist else None

    return {
        "symbol": symbol,
        "latest_close": latest_close,
        "change_pct": change_pct,
        "fifty_two_week_high": fifty_two_week_high,
        "fifty_two_week_low": fifty_two_week_low,
        "volume": float(latest["Volume"]) if "Volume" in latest else None,
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

    latest_close = float(latest["Close"]) if "Close" in latest else None
    change_pct = None
    if prev is not None and "Close" in prev and float(prev["Close"]) != 0:
        change_pct = ((latest_close - float(prev["Close"])) / float(prev["Close"])) * 100

    return {
        "symbol": symbol,
        "latest_close": latest_close,
        "change_pct": change_pct,
        "fifty_two_week_high": float(hist["High"].max()) if "High" in hist else None,
        "fifty_two_week_low": float(hist["Low"].min()) if "Low" in hist else None,
        "volume": float(latest["Volume"]) if "Volume" in latest else None,
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
            resp = requests.get(base_url + slug, timeout=8)
            if resp.status_code != 200:
                continue
            data = resp.json()
            summary = data.get("extract")
            if summary:
                return summary
        except Exception:
            continue
    return None


def enrich_company_profile(match: Dict[str, Any], snapshot: Dict[str, Any]) -> Dict[str, Any]:
    profile = dict(snapshot)
    if not profile.get("sector"):
        profile["sector"] = match.get("sector")
    if not profile.get("industry"):
        profile["industry"] = match.get("industry")

    description = profile.get("company_description")
    if description and "정보가 없습니다" not in description:
        return profile

    wiki_term = match.get("name") or profile.get("company_name") or match.get("symbol")
    wiki_summary = fetch_wikipedia_summary(str(wiki_term))
    if wiki_summary:
        profile["company_description"] = wiki_summary
    return profile


def get_stock_snapshot(symbol: str, market_type: str = "KRX") -> Dict[str, Any]:
    if market_type == "GLOBAL":
        return get_stock_snapshot_global(symbol)
    return get_stock_snapshot_krx(symbol)


def get_recent_news(query: str, limit: int = 5) -> List[Dict[str, str]]:
    params = {"q": query, "hl": "ko", "gl": "KR", "ceid": "KR:ko"}
    response = requests.get(GOOGLE_NEWS_RSS, params=params, timeout=10)
    response.raise_for_status()
    feed = feedparser.parse(response.text)

    news_items: List[Dict[str, str]] = []
    for entry in feed.entries[:limit]:
        news_items.append(
            {
                "title": entry.get("title", "제목 없음"),
                "link": entry.get("link", ""),
                "published": entry.get("published", ""),
            }
        )
    return news_items


def format_number(value: Optional[float]) -> str:
    if value is None:
        return "N/A"
    return f"{value:,.2f}" if abs(value) < 1_000_000 else f"{value:,.0f}"


def print_stock_info(match: Dict[str, str], snapshot: Dict[str, Any]) -> None:
    print("\n=== 주식 정보 ===")
    print(f"기업명: {match['name']}")
    print(f"종목코드: {snapshot['symbol']}")
    print(f"시장: {match['exchange']}")
    print(f"통화: {snapshot['currency']}")
    print(f"최근 종가: {format_number(snapshot['latest_close'])}")

    if snapshot.get("change_pct") is not None:
        sign = "+" if snapshot["change_pct"] >= 0 else ""
        print(f"전일 대비: {sign}{snapshot['change_pct']:.2f}%")
    else:
        print("전일 대비: N/A")

    print(f"거래량: {format_number(snapshot['volume'])}")
    print(
        "52주 범위: "
        f"{format_number(snapshot['fifty_two_week_low'])} ~ {format_number(snapshot['fifty_two_week_high'])}"
    )


def print_news(news_items: List[Dict[str, str]]) -> None:
    print("\n=== 최근 뉴스 ===")
    if not news_items:
        print("검색된 뉴스가 없습니다.")
        return

    for idx, item in enumerate(news_items, start=1):
        print(f"[{idx}] {item['title']}")
        if item.get("published"):
            print(f"    발행일: {item['published']}")
        print(f"    링크: {item['link']}")


def main() -> None:
    disable_broken_proxy_env()
    print("주식 종목 정보를 조회합니다. (KRX + GLOBAL)")
    print("종료하려면 q 를 입력하세요.")

    while True:
        query = input("\n종목명 또는 종목코드를 입력하세요: ").strip()

        if not query:
            print("입력이 비어 있습니다. 다시 입력하세요.")
            continue
        if query.lower() in {"q", "quit", "exit"}:
            print("프로그램을 종료합니다.")
            break

        try:
            match = search_symbol(query)
            if not match:
                print("해당 종목을 찾지 못했습니다.")
                continue

            print(f"\n검색 결과: {match['name']} ({match['symbol']}) / {match['exchange']}")
            snapshot = get_stock_snapshot(match["symbol"], match.get("market_type", "KRX"))
            snapshot = enrich_company_profile(match, snapshot)
            news_query = match["name"] if match.get("market_type") == "KRX" else match["symbol"]
            news_items = get_recent_news(news_query)

            print_stock_info(match, snapshot)
            print_news(news_items)
            print(f"\n조회 시각: {dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        except requests.RequestException as exc:
            print(f"네트워크 요청 중 오류가 발생했습니다: {exc}")
        except Exception as exc:
            print(f"예상치 못한 오류가 발생했습니다: {exc}")


if __name__ == "__main__":
    main()
