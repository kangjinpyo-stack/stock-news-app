import datetime as dt
from collections import OrderedDict
from urllib.parse import quote_plus, unquote_plus
from concurrent.futures import ThreadPoolExecutor
import os

import re

import altair as alt
import pandas as pd
import streamlit as st

from stock_info_news import (
    disable_broken_proxy_env,
    enrich_company_profile,
    get_krx_financial_table,
    get_krx_fundamentals_and_flow,
    get_market_wide_movers,
    get_krx_peer_comparison,
    get_related_stocks,
    get_recent_news,
    get_stock_snapshot,
    get_today_theme_movers,
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
if "theme_popup_key" not in st.session_state:
    st.session_state["theme_popup_key"] = ""
if "ai_detail_cache" not in st.session_state:
    st.session_state["ai_detail_cache"] = {}

applied_pending_query = ""
pending_query = st.session_state.get("pending_query")
if pending_query:
    st.session_state["query_input"] = pending_query
    applied_pending_query = pending_query
    st.session_state["pending_query"] = None
if "last_peers" not in st.session_state:
    st.session_state["last_peers"] = []
if "last_financial_table" not in st.session_state:
    st.session_state["last_financial_table"] = []

if st.session_state.get("pending_query"):
    st.session_state["query_input"] = st.session_state["pending_query"]
    st.session_state["pending_query"] = None

qp_query = st.query_params.get("q")
if qp_query:
    normalized_q = qp_query[0] if isinstance(qp_query, list) else str(qp_query)
    st.session_state["query_input"] = normalized_q
    applied_pending_query = normalized_q
    st.session_state["auto_search"] = True
    st.query_params.clear()

qp_theme = st.query_params.get("t")
if qp_theme:
    normalized_t = qp_theme[0] if isinstance(qp_theme, list) else str(qp_theme)
    st.session_state["theme_popup_key"] = unquote_plus(normalized_t)
    st.query_params.clear()

st.set_page_config(page_title="KRX Stock Pulse", page_icon="📊", layout="wide")

_mover_click_bridge = st.components.v2.component(
    "mover_click_bridge",
    html="<div id='mover-click-bridge'></div>",
    js="""
    export default function(component) {
      const { parentElement, setTriggerValue } = component;
      const doc = parentElement.ownerDocument;
      if (doc.__moverDelegationBound) return;
      doc.__moverDelegationBound = true;
      doc.addEventListener("click", (e) => {
        const target = e.target.closest(".mover-row-link[data-symbol]");
        if (!target) return;
        e.preventDefault();
        e.stopPropagation();
        const symbol = target.getAttribute("data-symbol") || "";
        if (symbol) setTriggerValue("symbol_click", symbol);
      }, true);
    }
    """,
)

_theme_click_bridge = st.components.v2.component(
    "theme_click_bridge",
    html="<div id='theme-click-bridge'></div>",
    js="""
    export default function(component) {
      const { parentElement, setTriggerValue } = component;
      const doc = parentElement.ownerDocument;
      if (doc.__themeDelegationBound) return;
      doc.__themeDelegationBound = true;
      doc.addEventListener("click", (e) => {
        const target = e.target.closest(".theme-bubble-link[data-theme-key]");
        if (target) {
          // 클릭 순간에도 즉시 최상단 레이어로 올려 깜빡 가림 방지
          doc.querySelectorAll(".theme-bubble-item.instant-active").forEach((el) => el.classList.remove("instant-active"));
          const item = target.closest(".theme-bubble-item");
          if (item) item.classList.add("instant-active");
          e.preventDefault();
          e.stopPropagation();
          const key = target.getAttribute("data-theme-key") || "";
          if (key) setTriggerValue("theme_click", key);
          return;
        }
        const popup = e.target.closest(".theme-member-popup");
        if (popup) return;
        const board = e.target.closest(".theme-bubble-board");
        if (!board) return;
        setTriggerValue("theme_click", "__CLEAR__");
      }, true);
    }
    """,
)

_related_click_bridge = st.components.v2.component(
    "related_click_bridge",
    html="<div id='related-click-bridge'></div>",
    js="""
    export default function(component) {
      const { parentElement, setTriggerValue } = component;
      const doc = parentElement.ownerDocument;
      const links = doc.querySelectorAll("a.rel-name-link[data-symbol]");
      links.forEach((link) => {
        if (!link.dataset.boundRelatedClick) {
          link.addEventListener("click", (event) => {
            event.preventDefault();
            const symbol = link.dataset.symbol;
            if (symbol) setTriggerValue("symbol_click", symbol);
          });
          link.dataset.boundRelatedClick = "1";
        }
      });
      return () => {};
    }
    """,
)

_hero_click_bridge = st.components.v2.component(
    "hero_click_bridge",
    html="<div id='hero-click-bridge'></div>",
    js="""
    export default function(component) {
      const { parentElement, setTriggerValue } = component;
      const doc = parentElement.ownerDocument;
      const title = doc.querySelector("[data-hero-home='1']");
      if (!title) return;
      if (title.dataset.boundHeroClick === "1") return;
      title.dataset.boundHeroClick = "1";
      title.style.cursor = "pointer";
      title.addEventListener("click", (e) => {
        e.preventDefault();
        setTriggerValue("go_home_reload", true);
      });
    }
    """,
)

_copy_shortcut_bridge = st.components.v2.component(
    "copy_shortcut_bridge",
    html="<div id='copy-shortcut-bridge'></div>",
    js="""
    export default function(component) {
      const { parentElement } = component;
      const doc = parentElement.ownerDocument;
      if (doc.__copyShortcutBound) return;
      doc.__copyShortcutBound = true;
      doc.addEventListener("keydown", (e) => {
        const isCopy = (e.ctrlKey || e.metaKey) && String(e.key || "").toLowerCase() === "c";
        if (!isCopy) return;
        // Keep native copy, but block Streamlit/global shortcut handlers.
        e.stopPropagation();
        if (typeof e.stopImmediatePropagation === "function") e.stopImmediatePropagation();
      }, true);
    }
    """,
)

_theme_drag_bridge = st.components.v2.component(
    "theme_drag_bridge",
    html="<div id='theme-drag-bridge'></div>",
    js="""
    export default function(component) {
      const { parentElement } = component;
      const doc = parentElement.ownerDocument;
      if (doc.__themeDragBound) return;
      doc.__themeDragBound = true;

      let dragState = null;
      const GAP = 6;

      const clamp = (v, lo, hi) => Math.max(lo, Math.min(hi, v));

      const applyXY = (el, x, y) => {
        el.style.transform = `translate(${x}px, ${y}px)`;
        el.dataset.tx = String(x);
        el.dataset.ty = String(y);
      };

      const getRectInLane = (el, laneRect) => {
        const r = el.getBoundingClientRect();
        return {
          left: r.left - laneRect.left,
          right: r.right - laneRect.left,
          top: r.top - laneRect.top,
          bottom: r.bottom - laneRect.top,
          w: r.width,
          h: r.height,
          cx: (r.left + r.right) / 2 - laneRect.left,
          cy: (r.top + r.bottom) / 2 - laneRect.top,
        };
      };

      const separatePair = (a, b, state, laneRect) => {
        const ar = getRectInLane(a, laneRect);
        const br = getRectInLane(b, laneRect);
        const ra = Math.min(ar.w, ar.h) / 2;
        const rb = Math.min(br.w, br.h) / 2;
        const dx = br.cx - ar.cx;
        const dy = br.cy - ar.cy;
        const dist = Math.hypot(dx, dy) || 0.0001;
        const overlap = (ra + rb + GAP) - dist;
        if (overlap <= 0) return { moved: false, blocked: false };
        const moveB = (b !== state.item); // dragging item은 최대한 고정, 다른 원을 우선 이동
        const target = moveB ? b : a;
        const ux = dx / dist;
        const uy = dy / dist;
        let tx = Number(target.dataset.tx || "0");
        let ty = Number(target.dataset.ty || "0");
        const push = overlap + 0.6;
        if (moveB) {
          tx += ux * push;
          ty += uy * push;
        } else {
          tx -= ux * push;
          ty -= uy * push;
        }
        const prevX = Number(target.dataset.tx || "0");
        const prevY = Number(target.dataset.ty || "0");
        tx = clamp(tx, state.minXMap.get(target), state.maxXMap.get(target));
        ty = clamp(ty, state.minYMap.get(target), state.maxYMap.get(target));
        // 경계에 막혀 실제 좌표 변화가 없으면 더 밀지 않는다.
        if (Math.abs(tx - prevX) < 0.01 && Math.abs(ty - prevY) < 0.01) {
          return { moved: false, blocked: true };
        }
        applyXY(target, tx, ty);
        return { moved: true, blocked: false };
      };

      const resolveCollisions = (state) => {
        const laneRect = state.lane.getBoundingClientRect();
        const nodes = state.nodes;
        // chain collision 정리: 여러 번 반복해서 남은 겹침까지 제거
        let blocked = false;
        for (let iter = 0; iter < 16; iter++) {
          let moved = false;
          for (let i = 0; i < nodes.length; i++) {
            for (let j = i + 1; j < nodes.length; j++) {
              const res = separatePair(nodes[i], nodes[j], state, laneRect);
              if (res.moved) moved = true;
              if (res.blocked) blocked = true;
            }
          }
          if (!moved) break;
        }
        return blocked;
      };

      const hasOverlapWithOthers = (state) => {
        const laneRect = state.lane.getBoundingClientRect();
        const active = getRectInLane(state.item, laneRect);
        const ra = Math.min(active.w, active.h) / 2;
        for (const n of state.nodes) {
          if (n === state.item) continue;
          const r = getRectInLane(n, laneRect);
          const rb = Math.min(r.w, r.h) / 2;
          const dist = Math.hypot(active.cx - r.cx, active.cy - r.cy);
          if (dist < (ra + rb + GAP)) return true;
        }
        return false;
      };

      const onMove = (e) => {
        if (!dragState) return;
        if (e.cancelable) e.preventDefault();
        const p = e.touches ? e.touches[0] : e;
        const dx = p.clientX - dragState.startX;
        const dy = p.clientY - dragState.startY;
        let nx = dragState.baseX + dx;
        let ny = dragState.baseY + dy;
        nx = Math.max(dragState.minX, Math.min(dragState.maxX, nx));
        ny = Math.max(dragState.minY, Math.min(dragState.maxY, ny));
        const snapshot = new Map();
        for (const n of dragState.nodes) {
          snapshot.set(n, {
            x: Number(n.dataset.tx || "0"),
            y: Number(n.dataset.ty || "0"),
          });
        }
        applyXY(dragState.item, nx, ny);
        resolveCollisions(dragState);
        // 밀 수 없어서 겹침이 남으면 이번 프레임 이동은 취소
        if (hasOverlapWithOthers(dragState)) {
          for (const [node, pos] of snapshot.entries()) {
            applyXY(node, pos.x, pos.y);
          }
        }
      };

      const endDrag = () => {
        if (!dragState) return;
        dragState.item.classList.remove("dragging");
        dragState = null;
      };

      doc.addEventListener("mousemove", onMove, true);
      doc.addEventListener("touchmove", onMove, { capture: true, passive: false });
      doc.addEventListener("mouseup", endDrag, true);
      doc.addEventListener("touchend", endDrag, true);

      doc.addEventListener("mousedown", (e) => {
        const item = e.target.closest(".theme-bubble-item");
        if (!item) return;
        if (e.target.closest(".theme-member-popup")) return;
        const lane = item.closest(".theme-bubble-lane");
        if (!lane) return;
        const itemRect = item.getBoundingClientRect();
        const laneRect = lane.getBoundingClientRect();
        const baseX = Number(item.dataset.tx || "0");
        const baseY = Number(item.dataset.ty || "0");
        const minXMap = new Map();
        const maxXMap = new Map();
        const minYMap = new Map();
        const maxYMap = new Map();
        const nodes = Array.from(lane.querySelectorAll(".theme-bubble-item"));
        for (const b of nodes) {
          const r = b.getBoundingClientRect();
          minXMap.set(b, -Math.max(0, r.left - laneRect.left));
          maxXMap.set(b, Math.max(0, laneRect.right - r.right));
          minYMap.set(b, -Math.max(0, r.top - laneRect.top));
          maxYMap.set(b, Math.max(0, laneRect.bottom - r.bottom));
        }
        dragState = {
          item,
          lane,
          startX: e.clientX,
          startY: e.clientY,
          baseX,
          baseY,
          minX: -Math.max(0, itemRect.left - laneRect.left),
          maxX: Math.max(0, laneRect.right - itemRect.right),
          minY: -Math.max(0, itemRect.top - laneRect.top),
          maxY: Math.max(0, laneRect.bottom - itemRect.bottom),
          minXMap,
          maxXMap,
          minYMap,
          maxYMap,
          nodes,
        };
        item.classList.add("dragging");
      }, true);
    }
    """,
)


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
    text = str(products or "").strip()
    if text.lower() in {"nan", "none"}:
        text = ""
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

    industry_text = str(industry or "").strip()
    if industry_text.lower() in {"nan", "none"}:
        industry_text = ""
    if industry_text and industry_text != "N/A":
        return industry_text

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


def normalize_broken_parentheses(text: str) -> str:
    cleaned = str(text or "").strip()
    if not cleaned:
        return ""
    if cleaned.count("(") != cleaned.count(")"):
        cleaned = cleaned.replace("(", "").replace(")", "")
    cleaned = re.sub(r"\s+\)", ")", cleaned)
    cleaned = re.sub(r"\(\s+", "(", cleaned)
    cleaned = re.sub(r"\s{2,}", " ", cleaned).strip()
    return cleaned


def _split_core_items(text: str, limit: int = 4) -> list[str]:
    raw = str(text or "")
    if not raw:
        return []
    normalized = raw.replace("/", ",").replace("·", ",").replace(";", ",").replace("|", ",")
    parts = [normalize_broken_parentheses(x).strip() for x in normalized.split(",")]
    out = []
    skip = {"", "-", "N/A", "nan", "none", "확인 필요"}
    for p in parts:
        if p.lower() in skip or p in skip:
            continue
        if p not in out:
            out.append(p)
        if len(out) >= limit:
            break
    return out


def _clean_description_sentences(name: str, desc: str, limit: int = 3) -> list[str]:
    if not desc or desc == "정보 없음":
        return []
    sentences = []
    parts = re.split(r"(?<=[.!?])\s+|(?<=함\.)\s*|(?<=있음\.)\s*|(?<=다\.)\s*", str(desc).strip())
    bad_exact = {"분야 기업입니다.", ")분야 기업입니다.", ") 분야 기업입니다."}
    for part in parts:
        s = normalize_broken_parentheses(part.replace("동사는", f"{name}는").strip())
        if not s or len(s) < 14:
            continue
        if s.startswith(")") or s.startswith("("):
            continue
        if s in bad_exact:
            continue
        if "nan" in s.lower():
            continue
        if s not in sentences:
            sentences.append(s)
        if len(sentences) >= limit:
            break
    return sentences


def _plain_sentence(text: str, max_len: int = 88) -> str:
    s = normalize_broken_parentheses(str(text or ""))
    if not s:
        return ""
    s = s.replace("동사는", "").replace("당사는", "")
    s = re.sub(r"\s+", " ", s).strip(" .")
    if "," in s:
        s = s.split(",", 1)[0].strip()
    if "이며" in s and len(s) > max_len:
        s = s.split("이며", 1)[0].strip()
    if len(s) > max_len:
        s = s[:max_len].rstrip() + "..."
    if s and not s.endswith("."):
        s += "."
    return s


def build_company_profile_block(name, industry, products, desc):
    industry_text = normalize_broken_parentheses(industry if industry and industry != "N/A" else "업종 정보 확인 필요")
    core_business = normalize_broken_parentheses(infer_core_business_labels(industry, products, desc))
    if not core_business or core_business in {"-", "N/A", "확인 필요"}:
        core_business = normalize_broken_parentheses(clean_products_text(products, industry, desc))

    product_items = _split_core_items(products, limit=4)
    core_items = _split_core_items(core_business, limit=4)
    desc_lines = _clean_description_sentences(name, desc, limit=4)

    if desc_lines:
        head = _plain_sentence(desc_lines[0], max_len=92)
    else:
        head = f"{name}은(는) {industry_text} 중심의 사업을 하는 회사입니다."

    if product_items:
        product_line = " · ".join(product_items[:4])
    elif core_items:
        product_line = " · ".join(core_items[:4])
    else:
        product_line = "주요 제품/사업 정보 확인 필요"

    if len(desc_lines) >= 2:
        business_line = _plain_sentence(desc_lines[1], max_len=92)
    else:
        business_line = f"주요 사업축은 {core_business}이며, 이 부문 실적이 주가에 큰 영향을 줍니다."
    point_line = f"핵심 체크포인트는 {industry_text} 업황, 고객사 투자, 원가/환율 변화입니다."

    return [
        f"한눈에 보기: {head}",
        f"무엇을 파는 회사인가: {product_line}",
        f"어디서 돈을 버는가: {business_line}",
        f"볼 포인트: {point_line}",
    ]


def build_company_detailed_report(name, industry, products, desc, news_items, related, snapshot):
    industry_text = normalize_broken_parentheses(industry if industry and industry != "N/A" else "업종 정보 확인 필요")
    core_business = normalize_broken_parentheses(infer_core_business_labels(industry, products, desc))
    product_items = _split_core_items(products, limit=6)
    desc_lines = _clean_description_sentences(name, desc, limit=5)

    report = []
    if len(desc_lines) >= 3:
        report.append(f"회사 이해 포인트: {_plain_sentence(desc_lines[2], max_len=92)}")

    if product_items:
        report.append(f"대표 제품/서비스: {' · '.join(product_items[:5])}")
    elif core_business:
        report.append(f"대표 제품/서비스: {core_business}")

    if related:
        top_theme = normalize_broken_parentheses(str(related[0].get("matched_themes") or "연관 테마 확인 필요"))
        reason = normalize_broken_parentheses(str(related[0].get("theme_reason") or related[0].get("matched_keywords") or "사업 키워드 중첩"))
        if ":" in reason:
            reason = reason.split(":", 1)[-1].strip()
        if len(reason) > 54:
            reason = reason[:54].rstrip() + "..."
        report.append(f"시장에서 묶이는 테마: {top_theme}")
        report.append(f"테마 근거: {reason}")

    if news_items:
        titles = []
        for n in news_items[:1]:
            t = normalize_broken_parentheses(str(n.get("title", "")).replace("-v.daum.net", "").replace("- daum", "").strip())
            if t:
                titles.append(t if len(t) <= 52 else t[:52].rstrip() + "...")
        if titles:
            report.append(f"최근 이슈: {' / '.join(titles)}")

    report.append("확인하면 좋은 항목: 실적 발표 일정, 고객사 투자(CAPEX), 원가/환율, 동종사 밸류에이션")
    return report[:5]


def merge_company_info_lines(summary_lines, detailed_lines, max_lines: int = 7):
    def _norm(s: str) -> str:
        t = str(s or "").strip().lower()
        t = re.sub(
            r"^(요약|회사 개요|핵심 사업|핵심사업|사업 상세|주요 제품/서비스|주요 사업 구조|사업 구조|사업구조|체크포인트|최근 이슈|시장에서는 이렇게 봄|한눈에 보기|무엇을 파는 회사인가|어디서 돈을 버는가|볼 포인트|회사 이해 포인트|대표 제품/서비스|시장에서 묶이는 테마|테마 근거|확인하면 좋은 항목)\s*:\s*",
            "",
            t,
        )
        t = t.replace("(", " ").replace(")", " ").replace("/", " ").replace("·", " ")
        t = re.sub(r"[^\w\s가-힣%+-]", " ", t)
        t = re.sub(r"\s+", " ", t)
        return t

    def _token_set(s: str) -> set:
        toks = [x for x in _norm(s).split(" ") if x and len(x) >= 2]
        stop = {
            "회사", "사업", "중심", "관련", "분야", "기준", "확인", "가능", "현재",
            "주요", "구조", "서비스", "제품", "시장", "체크포인트", "데이터"
        }
        return set([t for t in toks if t not in stop])

    merged = []
    seen_text = []
    seen_tokens = []
    for line in (summary_lines or []) + (detailed_lines or []):
        raw = str(line or "").strip()
        if not raw:
            continue
        n = _norm(raw)
        tok = _token_set(raw)
        if len(n) < 8:
            continue
        duplicated = False
        for i, existing in enumerate(seen_text):
            if n == existing or n in existing or existing in n:
                duplicated = True
                break
            etok = seen_tokens[i]
            if tok and etok:
                inter = len(tok.intersection(etok))
                union = len(tok.union(etok))
                jacc = (inter / union) if union else 0.0
                if jacc >= 0.58 or inter >= 5:
                    duplicated = True
                    break
        if duplicated:
            continue
        seen_text.append(n)
        seen_tokens.append(tok)
        merged.append(raw)
        if len(merged) >= max_lines:
            break
    return merged


def generate_ai_detailed_report(match, snapshot, related, news_items, peers, financial_table):
    api_key = ""
    try:
        api_key = st.secrets.get("OPENAI_API_KEY", "")
    except Exception:
        api_key = ""
    if not api_key:
        api_key = os.getenv("OPENAI_API_KEY", "")
    if not api_key:
        return {"ok": False, "message": "OPENAI_API_KEY가 설정되지 않았습니다. (환경변수 또는 Streamlit Secrets)"}
    api_key = str(api_key).strip()
    try:
        api_key.encode("ascii")
    except Exception:
        return {"ok": False, "message": "OPENAI_API_KEY 값에 한글/특수문자가 포함되어 있습니다. 실제 OpenAI 키(sk-...)로 교체해 주세요."}
    if not api_key.startswith("sk-"):
        return {"ok": False, "message": "OPENAI_API_KEY 형식이 올바르지 않습니다. `sk-`로 시작하는 실제 키를 입력해 주세요."}

    try:
        from openai import OpenAI
    except Exception:
        return {"ok": False, "message": "openai 패키지가 설치되지 않았습니다. requirements.txt에 openai를 추가해 주세요."}

    company_name = str(match.get("name", ""))
    symbol = str(match.get("symbol", ""))
    exchange = str(match.get("exchange", ""))
    currency = str(snapshot.get("currency", ""))
    latest_close = fmt_num(snapshot.get("latest_close"), ",.2f")
    change_pct = snapshot.get("change_pct")
    change_text = "N/A" if change_pct is None else f"{change_pct:+.2f}%"
    industry = str(snapshot.get("industry") or match.get("industry") or "N/A")
    products = str(snapshot.get("products") or match.get("products") or "N/A")
    description = str(snapshot.get("company_description") or "").strip()

    prompt = f"""
너는 한국 주식 리서치 어시스턴트다.
아래 데이터만 근거로 쉬운 한국어 분석을 작성해라.
모르는 내용은 추정하지 말고 '데이터 확인 필요'라고 써라.

[회사]
- 회사명: {company_name}
- 종목코드: {symbol}
- 시장: {exchange}
- 통화: {currency}
- 최근종가: {latest_close}
- 당일등락률: {change_text}
- 업종: {industry}
- 주요제품/사업: {products}
- 회사설명: {description}

[같은 테마 종목 상위]
{related[:6] if related else []}

[최근 뉴스]
{news_items[:6] if news_items else []}

[동종업계 비교 상위]
{peers[:6] if peers else []}

[재무표 일부]
{financial_table[:8] if financial_table else []}

출력 형식:
1) 회사 한줄 정의
2) 핵심 사업 구조 (3~5개 불릿)
3) 시장에서 보는 관점 (3개 불릿)
4) 투자포인트 (긍정 3개 / 리스크 3개)
5) 체크리스트 (숫자/공시 4개)
6) 요약 결론 (2문장)
"""

    try:
        client = OpenAI(api_key=api_key)
        resp = client.responses.create(
            model="gpt-5.5",
            input=prompt,
            max_output_tokens=1200,
        )
        text = (getattr(resp, "output_text", "") or "").strip()
        if not text:
            text = "분석 결과를 생성하지 못했습니다."
        return {"ok": True, "text": text}
    except Exception as exc:
        return {"ok": False, "message": f"AI 상세분석 호출 실패: {exc}"}


def fmt_num(value, pattern: str = ",.2f") -> str:
    if value is None:
        return "N/A"
    try:
        return format(float(value), pattern)
    except Exception:
        return "N/A"


def build_investment_points(match, snapshot, fundamentals, related):
    points = []
    flow = fundamentals.get("flow") if isinstance(fundamentals, dict) else None
    ch = snapshot.get("change_pct")
    if ch is None:
        points.append("모멘텀: 당일 변동률 데이터가 부족해 단기 탄력 판단이 제한됩니다.")
    elif ch >= 2:
        points.append(f"모멘텀: 당일 등락률이 {ch:+.2f}%로 강한 구간입니다.")
    elif ch <= -2:
        points.append(f"모멘텀: 당일 등락률이 {ch:+.2f}%로 약세 구간입니다.")
    else:
        points.append(f"모멘텀: 당일 등락률이 {ch:+.2f}%로 중립권입니다.")

    per = fundamentals.get("PER")
    pbr = fundamentals.get("PBR")
    eps = fundamentals.get("EPS")
    if per is None and pbr is None:
        points.append("밸류에이션: PER/PBR 데이터가 제한적이라 절대평가 신뢰도가 낮습니다.")
    else:
        eps_text = fmt_num(eps, ",.0f")
        points.append(f"밸류에이션: PER {fmt_num(per, ',.2f')} / PBR {fmt_num(pbr, ',.2f')} / EPS {eps_text} 기준입니다.")

    if isinstance(flow, dict):
        inst_5d = flow.get("institution_5d")
        foreign_5d = flow.get("foreign_5d")
        if inst_5d is not None or foreign_5d is not None:
            points.append(
                f"수급: 최근 5거래일 기관 {fmt_num(inst_5d, ',.0f')}주 / 외국인 {fmt_num(foreign_5d, ',.0f')}주 순매수 흐름입니다."
            )

    if related:
        top_theme = related[0].get("matched_themes") or "테마 데이터 부족"
        reason = related[0].get("theme_reason") or related[0].get("matched_keywords") or "관련 사업 키워드 중첩"
        reason = normalize_broken_parentheses(str(reason))
        if len(reason) > 58:
            reason = reason[:58].rstrip() + "..."
        points.append(f"테마 연동: `{top_theme}` 축 연관도가 높고, 근거는 {reason} 입니다.")
    else:
        points.append("테마 연동: 동행 종목 데이터가 부족해 테마 해석은 보수적으로 볼 필요가 있습니다.")
    points.append("리스크 체크: 실적 발표 일정, 가이던스 변화, 대외 변수(금리/환율/원자재)를 함께 확인하는 것이 좋습니다.")
    return points[:5]


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


def build_theme_tile_style(avg_change: float) -> str:
    strength = min(max(abs(avg_change), 0.0), 10.0) / 10.0
    if avg_change >= 0:
        base = (255, 99, 71)
        alpha = 0.24 + (0.58 * strength)
        text = "#fff7ed" if strength > 0.35 else "#7f1d1d"
    else:
        base = (37, 99, 235)
        alpha = 0.20 + (0.52 * strength)
        text = "#eff6ff" if strength > 0.35 else "#1e3a8a"
    return f"background: rgba({base[0]}, {base[1]}, {base[2]}, {alpha:.2f}); color: {text};"


def build_theme_tile_span(avg_change: float, member_count: int) -> tuple[int, int]:
    magnitude = abs(avg_change)
    if magnitude >= 4 or member_count >= 4:
        return 2, 2
    if magnitude >= 2:
        return 2, 1
    return 1, 1


def chunked_rows(items, per_row: int = 2):
    return [items[idx : idx + per_row] for idx in range(0, len(items), per_row)]


def build_theme_bubble_style(avg_change: float, member_count: int, relative_strength: float = 0.5) -> tuple[int, str, str]:
    intensity = min(max(abs(avg_change), 0.0), 12.0) / 12.0
    # Relative strength makes stronger themes clearly bigger within each lane.
    rel = min(max(relative_strength, 0.0), 1.0)
    size = 88 + int(rel * 120) + min(max(member_count, 1), 6) * 4
    size = min(max(size, 88), 230)
    tone = min(max((intensity * 0.42) + (rel * 0.58), 0.0), 1.0)
    if avg_change >= 0:
        alpha = 0.82 + 0.14 * tone
        bg = (
            f"linear-gradient(145deg, rgba(255,255,255,0.10) 0%, rgba(248,113,113,{alpha:.2f}) 22%, "
            f"rgba(225,29,72,{alpha:.2f}) 66%, rgba(127,29,29,{0.76 + tone * 0.16:.2f}) 100%)"
        )
        text = "#ffffff"
    else:
        alpha = 0.82 + 0.14 * tone
        bg = (
            f"linear-gradient(145deg, rgba(255,255,255,0.10) 0%, rgba(96,165,250,{alpha:.2f}) 22%, "
            f"rgba(37,99,235,{alpha:.2f}) 66%, rgba(30,58,138,{0.76 + tone * 0.16:.2f}) 100%)"
        )
        text = "#ffffff"
    return size, bg, text


def render_theme_bubble_cluster(rows, positive: bool = True, popup_theme: str = "") -> str:
    if not rows:
        empty_label = "상승 테마를 아직 만들지 못했습니다." if positive else "하락 테마를 아직 만들지 못했습니다."
        return f"<div class='theme-bubble-empty'>{empty_label}</div>"

    bubbles = []
    if positive:
        sorted_rows = sorted(
            rows,
            key=lambda x: (float(x.get("avg_change", 0)), int(x.get("member_count", 0))),
            reverse=True,
        )[:10]
    else:
        sorted_rows = sorted(
            rows,
            key=lambda x: (float(x.get("avg_change", 0)), -int(x.get("member_count", 0))),
        )[:10]
    max_abs_change = max([abs(float(x.get("avg_change", 0))) for x in sorted_rows], default=1.0)
    max_abs_change = max(max_abs_change, 0.1)
    lane_key = "UP" if positive else "DOWN"
    for row in sorted_rows:
        avg_change = float(row.get("avg_change", 0))
        member_count = int(row.get("member_count", 0))
        theme_name = str(row.get("theme", "")).strip()
        theme_pick_key = quote_plus(f"{lane_key}|{theme_name}")
        relative_strength = min(max(abs(avg_change) / max_abs_change, 0.0), 1.0)
        size, bg, text = build_theme_bubble_style(avg_change, member_count, relative_strength)
        if not positive:
            size = max(78, int(size * 0.82))
        fs_name = max(7, min(16, int(size * 0.13)))
        fs_change = max(8, min(17, int(size * 0.15)))
        fs_meta = max(7, min(13, int(size * 0.11)))
        name_len = len(theme_name.replace(" ", ""))
        if name_len >= 6:
            fs_name = max(8, fs_name - 1)
        if name_len >= 8:
            fs_name = max(7, fs_name - 1)
        if name_len >= 9:
            fs_name = max(7, fs_name - 1)
        sign = "+" if avg_change > 0 else ""
        strength = min(max(abs(avg_change), 0.0), 12.0) / 12.0
        ring = 1
        ring_alpha = 0.28 + (0.22 * strength)
        ring_color = f"rgba(127,29,29,{ring_alpha:.2f})" if avg_change >= 0 else f"rgba(30,58,138,{ring_alpha:.2f})"
        popup_html = ""
        if popup_theme and theme_name == popup_theme:
            popup_row = next((x for x in sorted_rows if str(x.get("theme", "")).strip() == popup_theme), None)
            if popup_row:
                popup_html = render_theme_member_popup_html(popup_row, side=lane_key)
        item_cls = "theme-bubble-item active-popup" if popup_html else "theme-bubble-item"
        bubbles.append(
            f"""
            <div class="{item_cls}">
              <div class="theme-bubble-link" data-theme-key="{theme_pick_key}">
                <div class="theme-bubble" style="--bubble-size:{size}px; background:{bg}; color:{text}; border:{ring}px solid {ring_color};">
                  <div class="theme-bubble-name" style="font-size:{fs_name}px !important; line-height:1.14 !important;">{theme_name}</div>
                  <div class="theme-bubble-change" style="font-size:{fs_change}px !important;">{sign}{avg_change:.2f}%</div>
                  <div class="theme-bubble-meta" style="font-size:{fs_meta}px !important;">{member_count}종목</div>
                </div>
              </div>
              {popup_html}
            </div>
            """
        )
    return "".join(bubbles)


def render_theme_member_popup_html(row: dict, side: str = "UP") -> str:
    theme_name = str(row.get("theme", "")).strip()
    members = row.get("members", [])[:16]
    if not theme_name or not members:
        return ""
    popup_class = "up" if side == "UP" else "down"
    lines = [
        f"<div class='theme-member-popup floating {popup_class}'><div class='theme-member-title'>{theme_name} 관련 종목</div>"
    ]
    for idx, member in enumerate(members, start=1):
        symbol = quote_plus(str(member.get("symbol") or member.get("name") or ""))
        name = str(member.get("name", "")).strip()
        pct = float(member.get("change_pct", 0))
        pct_text = f"+{pct:.2f}%" if pct > 0 else f"{pct:.2f}%"
        pct_cls = "pct-up" if pct > 0 else "pct-down"
        lines.append(
            f"<div class='mover-row-link' data-symbol='{unquote_plus(symbol)}' style='margin-bottom:4px;'><span class='rank-pill'>{idx}</span><span class='mover-name-text'>{name}</span><span class='{pct_cls}'>{pct_text}</span></div>"
        )
    lines.append("<div style='margin-top:6px; font-size:0.78rem; color:#64748b;'>다른 원을 클릭하면 내용이 바뀝니다.</div></div>")
    return "".join(lines)


def collect_theme_leaderboard(theme_movers):
    seen = {}
    source_items = theme_movers.get("all_items") or []
    if source_items:
        for member in source_items:
            key = str(member.get("symbol", "")).strip()
            if not key:
                continue
            existing = seen.get(key)
            if existing is None or abs(float(member.get("change_pct", 0))) > abs(float(existing.get("change_pct", 0))):
                seen[key] = member
    else:
        for group_name in ("up", "down"):
            for row in theme_movers.get(group_name, []):
                for member in row.get("members", []):
                    key = str(member.get("symbol", "")).strip()
                    if not key:
                        continue
                    existing = seen.get(key)
                    if existing is None or abs(float(member.get("change_pct", 0))) > abs(float(existing.get("change_pct", 0))):
                        seen[key] = member
    items = list(seen.values())
    rising = sorted([x for x in items if float(x.get("change_pct", 0)) > 0], key=lambda x: -float(x.get("change_pct", 0)))
    falling = sorted([x for x in items if float(x.get("change_pct", 0)) < 0], key=lambda x: float(x.get("change_pct", 0)))
    return rising[:6], falling[:6]

@st.cache_data(ttl=600, show_spinner=False)
def load_snapshot_cached(symbol: str, market_type: str):
    return enrich_company_profile({"symbol": symbol, "market_type": market_type}, get_stock_snapshot(symbol, market_type))

@st.cache_data(ttl=600, show_spinner=False)
def load_match_cached(query_text: str):
    return search_symbol(query_text)


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


THEME_MOVER_CACHE_VERSION = "theme-v15"
MARKET_MOVER_UI_VERSION = "bubble-v25"
MARKET_WIDE_MOVER_CACHE_VERSION = "mover-top100-v1"


@st.cache_data(ttl=1800, show_spinner=False)
def load_today_theme_movers_cached(_version: str = THEME_MOVER_CACHE_VERSION):
    return get_today_theme_movers(limit_themes=30, members_per_theme=12)


@st.cache_data(ttl=1800, show_spinner=False)
def load_market_wide_movers_cached(_version: str = MARKET_WIDE_MOVER_CACHE_VERSION):
    return get_market_wide_movers(limit_each_market=120, top_n=100)

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
      max-width: 1420px;
      padding-top: 1.2rem;
      padding-bottom: 1rem;
    }
    .hero-shell {
      background: #ffffff;
      border: 1px solid #dbe4f0;
      border-radius: 18px;
      padding: 18px 20px;
      box-shadow: 0 10px 24px rgba(15, 23, 42, 0.05);
      margin-bottom: 14px;
    }
    .hero-top {
      display:flex;
      justify-content:space-between;
      align-items:flex-start;
      gap: 12px;
    }
    .hero-title {
      font-family: "Pretendard", "Noto Sans KR", sans-serif;
      font-size: 1.78rem;
      font-weight: 800;
      letter-spacing: -0.5px;
      color: #0f1f3d;
      margin: 0;
    }
    .live-badge {
      display:inline-flex;
      align-items:center;
      gap: 6px;
      margin-left: 10px;
      padding: 4px 10px;
      border-radius: 999px;
      font-size: 0.82rem;
      font-weight: 700;
      color: #0f766e;
      background: #e6fffb;
      border: 1px solid #c6f2eb;
      vertical-align: middle;
    }
    .hero-sub {
      margin-top: 4px;
      margin-bottom: 6px;
      color: #5b6784;
      font-size: 0.94rem;
      font-weight: 500;
      letter-spacing: -0.1px;
    }
    .hero-right {
      color:#64748b;
      font-size:0.92rem;
      margin-top:6px;
      white-space:nowrap;
    }
    .search-card {
      margin-top: 14px;
      border: 1px solid #dbe4f0;
      border-radius: 14px;
      padding: 12px 14px;
      background: linear-gradient(180deg,#ffffff 0%,#f9fbff 100%);
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
    .rel-name-link,
    .rel-name-link:visited,
    .rel-name-link:hover,
    .rel-name-link:active {
      color: var(--ink) !important;
      text-decoration: none !important;
      font-weight: inherit;
      cursor: pointer !important;
    }
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
    .search-wrap { max-width: 100%; margin: 0; }
    div[data-testid="stTextInput"] input {
      height: 44px;
      border-radius: 10px;
    }
    div[data-testid="stFormSubmitButton"] button {
      height: 44px;
      border-radius: 10px;
      margin-top: 0 !important;
    }
    div[data-testid="stButton"] button {
      white-space: nowrap !important;
      word-break: keep-all !important;
      min-width: 72px;
      height: 40px;
      border-radius: 10px;
      padding: 0 12px;
    }
    [data-testid="column"] div[data-testid="stButton"] button[kind="secondary"] {
      height: 34px !important;
      min-width: 64px !important;
      padding: 0 10px !important;
      border-radius: 8px !important;
      font-size: 0.86rem !important;
    }
    .quote-card {
      background: #ffffff;
      border: 1px solid #dbe4f0;
      border-radius: 16px;
      padding: 16px 18px;
      box-shadow: 0 8px 20px rgba(15, 23, 42, 0.05);
      margin-bottom: 14px;
    }
    .theme-board {
      background: #ffffff;
      border: 1px solid #dbe4f0;
      border-radius: 16px;
      padding: 14px 16px;
      box-shadow: 0 8px 20px rgba(15, 23, 42, 0.05);
      margin: 10px 0 18px 0;
    }
    .theme-head {
      display:flex;
      justify-content:space-between;
      align-items:center;
      margin-bottom:10px;
    }
    .theme-chip-row {
      display:flex;
      flex-wrap:wrap;
      gap:8px;
      margin-top:8px;
    }
    .theme-chip {
      display:inline-flex;
      align-items:center;
      gap:6px;
      border-radius:999px;
      padding:6px 10px;
      font-size:0.8rem;
      border:1px solid #dbe4f0;
      background:#f8fafc;
      color:#0f172a;
      white-space:nowrap;
    }
    .theme-item {
      border-top: 1px solid #edf2f7;
      padding-top: 10px;
      margin-top: 10px;
    }
    .theme-title {
      font-weight: 700;
      color: #0f172a;
    }
    .theme-change-up { color:#15803d; font-weight:700; }
    .theme-change-down { color:#b91c1c; font-weight:700; }
    .market-title {
      font-family: "Pretendard", "Noto Sans KR", "Apple SD Gothic Neo", sans-serif;
      letter-spacing: -0.25px;
      font-weight: 800;
      font-size: 1.45rem;
      margin: 2px 0 12px 0;
      color: #0b1736;
    }
    .theme-bubble-board {
      --bubble-scale: 1;
      background:
        radial-gradient(circle at 15% 15%, rgba(254,226,226,0.55), transparent 24%),
        radial-gradient(circle at 85% 18%, rgba(219,234,254,0.72), transparent 28%),
        linear-gradient(180deg, #fffdfd 0%, #f8fbff 100%);
      border:1px solid #dbe4f0;
      border-radius:22px;
      padding:12px 12px 12px 12px;
      box-shadow: 0 10px 28px rgba(15, 23, 42, 0.06);
      position: relative;
      overflow: visible;
    }
    .theme-bubble-grid {
      display:grid;
      grid-template-columns: 1fr 1fr;
      gap: 10px;
    }
    .theme-bubble-lane {
      min-height: 176px;
      border-radius: 20px;
      padding: 10px;
      border: 1px solid rgba(219,228,240,0.9);
      background: rgba(255,255,255,0.74);
      display:flex;
      flex-direction:column;
      overflow: visible;
    }
    .theme-bubble-lane.up {
      background: linear-gradient(180deg, rgba(255,245,245,0.96) 0%, rgba(255,255,255,0.80) 100%);
    }
    .theme-bubble-lane.down {
      background: linear-gradient(180deg, rgba(239,246,255,0.96) 0%, rgba(255,255,255,0.82) 100%);
    }
    .theme-bubble-lane-title {
      font-size: 0.92rem;
      font-weight: 800;
      color: #0f172a;
      margin-bottom: 8px;
    }
    .theme-bubble-wrap {
      display:flex;
      flex-wrap:wrap;
      align-items:center;
      align-content:flex-start;
      justify-content:flex-start;
      gap: 8px;
      flex:1;
    }
    .theme-bubble-link {
      text-decoration:none !important;
      display:inline-flex;
      cursor: pointer;
    }
    .theme-bubble-item {
      position: relative;
      display: inline-flex;
      cursor: grab;
      user-select: none;
      z-index: 1;
    }
    .theme-bubble-item.instant-active { z-index: 11900; }
    .theme-bubble-item.active-popup { z-index: 12000; }
    .theme-bubble-item.dragging {
      cursor: grabbing;
      z-index: 13000;
    }
    .theme-bubble {
      width: calc(var(--bubble-size, 120px) * var(--bubble-scale));
      height: calc(var(--bubble-size, 120px) * var(--bubble-scale));
      --bubble-base: calc(var(--bubble-size, 120px) * var(--bubble-scale));
      --fs-name: clamp(8px, calc(var(--bubble-base) * 0.16), 18px);
      --fs-change: clamp(8px, calc(var(--bubble-base) * 0.15), 17px);
      --fs-meta: clamp(7px, calc(var(--bubble-base) * 0.11), 13px);
      border-radius: 999px;
      display:flex;
      flex-direction:column;
      align-items:center;
      justify-content:center;
      text-align:center;
      padding: 9px;
      box-shadow:
        inset 0 1px 0 rgba(255,255,255,0.24),
        inset 0 -14px 22px rgba(15,23,42,0.10),
        0 14px 24px rgba(15,23,42,0.13),
        0 1px 0 rgba(255,255,255,0.85);
      transition: transform 140ms ease, box-shadow 140ms ease, filter 140ms ease;
      overflow: hidden;
      position: relative;
      isolation: isolate;
    }
    .theme-bubble::before,
    .theme-bubble::after {
      content: none;
    }
    .theme-member-popup {
      margin-top: 10px;
      border: 1px solid #dbe4f0;
      background: #ffffff;
      border-radius: 12px;
      padding: 10px 12px;
      box-shadow: 0 10px 24px rgba(15,23,42,0.18);
      z-index: 9999;
    }
    .theme-member-popup.floating {
      position: absolute;
      left: 74%;
      top: 74%;
      transform: translate(0, 0);
      width: min(320px, 92vw);
      margin: 0;
    }
    .theme-member-popup.floating.up {
      left: 74%;
    }
    .theme-member-popup.floating.down {
      left: 74%;
      right: auto;
    }
    .theme-member-title {
      font-size: 0.92rem;
      font-weight: 800;
      color: #0f172a;
      margin-bottom: 8px;
    }
    .theme-bubble:hover {
      transform: translateY(-2px);
      filter: saturate(1.04);
      box-shadow:
        inset 0 1px 0 rgba(255,255,255,0.28),
        inset 0 -16px 24px rgba(15,23,42,0.12),
        0 18px 32px rgba(15,23,42,0.16),
        0 1px 0 rgba(255,255,255,0.9);
    }
    .theme-bubble-name {
      font-family: "Pretendard Variable", "Noto Sans KR", "Apple SD Gothic Neo", sans-serif;
      font-size: var(--fs-name);
      font-weight: 700;
      line-height: 1.1;
      max-width: 88%;
      white-space: nowrap;
      overflow: hidden;
      text-overflow: ellipsis;
      text-shadow: 0 1px 2px rgba(15,23,42,0.22);
    }
    .theme-bubble-change {
      font-family: "Pretendard Variable", "Noto Sans KR", "Apple SD Gothic Neo", sans-serif;
      font-size: var(--fs-change);
      font-weight: 600;
      margin-top: 5px;
      max-width: 84%;
      white-space: nowrap;
      overflow: hidden;
      text-overflow: ellipsis;
      text-shadow: 0 1px 2px rgba(15,23,42,0.20);
    }
    .theme-bubble-meta {
      font-family: "Pretendard Variable", "Noto Sans KR", "Apple SD Gothic Neo", sans-serif;
      font-size: var(--fs-meta);
      margin-top: 4px;
      opacity: 0.95;
      max-width: 84%;
      white-space: nowrap;
      overflow: hidden;
      text-overflow: ellipsis;
      text-shadow: 0 1px 2px rgba(15,23,42,0.16);
    }
    .theme-bubble-empty {
      color:#64748b;
      font-size:0.88rem;
      padding:18px 8px;
    }
    .theme-mover-grid {
      display:grid;
      grid-template-columns: 1fr 1fr;
      gap: 18px;
      margin-top: 8px;
    }
    .theme-rank-card {
      background:#ffffff;
      border:1px solid #dbe4f0;
      border-radius:16px;
      padding:12px 14px;
      box-shadow: 0 8px 20px rgba(15, 23, 42, 0.05);
      height:100%;
    }
    .theme-rank-row {
      display:flex;
      align-items:center;
      justify-content:space-between;
      gap:10px;
      min-height:40px;
    }
    .theme-rank-divider {
      border-top:1px solid #edf2f7;
      margin:8px 0 10px 0;
    }
    .theme-rank-name {
      font-weight:700;
      color:#0f172a;
      display:flex;
      align-items:baseline;
      gap:6px;
      white-space:nowrap;
    }
    .theme-rank-sub {
      font-size:0.78rem;
      color:#64748b;
    }
    .theme-rank-code {
      font-size:0.8rem;
      color:#64748b;
      font-weight:500;
      white-space:nowrap;
    }
    .st-key-mover-list-up div[data-testid="stButton"] > button,
    .st-key-mover-list-down div[data-testid="stButton"] > button {
      font-family: "Pretendard Variable", "Noto Sans KR", "Apple SD Gothic Neo", sans-serif;
      text-align: left;
      justify-content: flex-start;
      align-items: center;
      white-space: nowrap !important;
      overflow: hidden;
      text-overflow: ellipsis;
      border: none !important;
      padding: 0 !important;
      background: transparent !important;
      box-shadow: none !important;
      min-height: 24px !important;
      height: 24px !important;
      min-width: 0;
      border-radius: 0;
      font-weight: 700;
      color: #0f172a;
      line-height: 24px !important;
      font-size: 0.82rem;
      letter-spacing: 0;
      transition: none;
      display:flex !important;
      align-items:center !important;
    }
    .st-key-mover-list-up div[data-testid="stButton"],
    .st-key-mover-list-down div[data-testid="stButton"] {
      margin: 0 !important;
      padding: 0 !important;
      line-height: 24px !important;
      min-height: 24px !important;
    }
    .st-key-mover-list-up div[data-testid="stButton"] > div,
    .st-key-mover-list-down div[data-testid="stButton"] > div {
      margin: 0 !important;
      padding: 0 !important;
      min-height: 24px !important;
      display:flex !important;
      align-items:center !important;
    }
    .st-key-mover-list-up div[data-testid="stButton"] > button p,
    .st-key-mover-list-up div[data-testid="stButton"] > button span,
    .st-key-mover-list-down div[data-testid="stButton"] > button p,
    .st-key-mover-list-down div[data-testid="stButton"] > button span {
      white-space: nowrap !important;
      word-break: keep-all !important;
      overflow: hidden !important;
      text-overflow: ellipsis !important;
      line-height: 24px !important;
      margin: 0 !important;
      height: 24px !important;
      display: flex !important;
      align-items: center !important;
    }
    .st-key-mover-list-up div[data-testid="stButton"] > button > div,
    .st-key-mover-list-down div[data-testid="stButton"] > button > div {
      height: 24px !important;
      line-height: 24px !important;
      display:flex !important;
      align-items:center !important;
    }
    .st-key-mover-list-up div[data-testid="stMarkdown"] p,
    .st-key-mover-list-down div[data-testid="stMarkdown"] p {
      margin: 0 !important;
      line-height: 1.2 !important;
    }
    .st-key-mover-list-up div[data-testid="stButton"] > button:hover,
    .st-key-mover-list-down div[data-testid="stButton"] > button:hover { transform:none; }
    .st-key-mover-list-up div[data-testid="stButton"] > button {
      color: #111827;
    }
    .st-key-mover-list-down div[data-testid="stButton"] > button {
      color: #111827;
    }
    .mover-list-scroll {
      max-height: 300px;
      overflow-y: auto;
      padding-right: 6px;
    }
    .mover-list-scroll::-webkit-scrollbar {
      width: 8px;
    }
    .mover-list-scroll::-webkit-scrollbar-thumb {
      background: #cbd5e1;
      border-radius: 999px;
    }
    .mover-list-scroll::-webkit-scrollbar-track {
      background: transparent;
    }
    .mover-col {
      display:flex;
      flex-direction:column;
      gap:6px;
    }
    .mover-head-row {
      display:flex;
      justify-content:space-between;
      align-items:center;
      margin-bottom:10px;
    }
    .mover-sub {
      color:#64748b;
      font-size:0.83rem;
      font-weight:600;
    }
    .mover-rank-note {
      color:#94a3b8;
      font-size:0.76rem;
      margin-top:2px;
    }
    .mover-head-row .market-title {
      margin: 0;
      font-size: 0.96rem;
      font-weight: 800;
      color: #1b2433;
      letter-spacing: 0;
      line-height: 1.25;
    }
    .mover-more-pill {
      display:inline-flex;
      align-items:center;
      gap:4px;
      padding:6px 12px;
      border:1px solid #e7edf6;
      border-radius:999px;
      color:#4b5b73;
      background:#ffffff;
      font-size:0.75rem;
      font-weight:700;
      box-shadow:0 2px 6px rgba(15,23,42,0.04);
    }
    .mover-footer-link {
      text-align:center;
      color:#2d5bcf;
      font-size:0.8rem;
      font-weight:700;
      margin-top:8px;
    }
    .rank-pill {
      min-width:24px;
      height:24px;
      border-radius:2px;
      display:flex;
      align-items:center;
      justify-content:center;
      line-height:24px;
      font-size:0.74rem;
      font-weight:800;
      background:#ffffff;
      border:1px solid #e5ebf4;
    }
    .st-key-mover-list-up .rank-pill {
      color:#ef6b73;
      border-color:#f8dde0;
      background:#fff7f8;
      box-shadow:none;
    }
    .st-key-mover-list-down .rank-pill {
      color:#5f8fe9;
      border-color:#dee8ff;
      background:#f7faff;
      box-shadow:none;
    }
    .pct-up {
      color:#f0737b;
      font-size:0.8rem;
      font-weight:800;
      text-align:right;
      white-space:nowrap;
      line-height:24px;
      margin:0;
      font-variant-numeric: tabular-nums;
      height:24px;
      display:flex;
      align-items:center;
      justify-content:flex-end;
      position: relative;
      top: 0;
    }
    .pct-down {
      color:#6c95eb;
      font-size:0.8rem;
      font-weight:800;
      text-align:right;
      white-space:nowrap;
      line-height:24px;
      margin:0;
      font-variant-numeric: tabular-nums;
      height:24px;
      display:flex;
      align-items:center;
      justify-content:flex-end;
      position: relative;
      top: 0;
    }
    .mover-row-link {
      display:flex;
      align-items:center;
      justify-content:center;
      gap:10px;
      width:100%;
      text-decoration:none !important;
      min-height:24px;
      transform: none;
      cursor: pointer;
    }
    .mover-row-link:hover .mover-name-text {
      text-decoration: underline;
    }
    .mover-name-text {
      flex:1;
      min-width:0;
      color:#536273;
      font-size:0.82rem;
      font-weight:600;
      line-height:24px;
      white-space:nowrap;
      overflow:hidden;
      text-overflow:ellipsis;
    }
    .mover-name-btn div[data-testid="stButton"] > button {
      border: none !important;
      background: transparent !important;
      box-shadow: none !important;
      padding: 0 !important;
      height: 22px !important;
      min-height: 22px !important;
      min-width: 0 !important;
      border-radius: 0 !important;
      color: #111827 !important;
      font-size: 0.86rem !important;
      font-weight: 600 !important;
      text-align: left !important;
      justify-content: flex-start !important;
      line-height: 1.1 !important;
      margin: 0 !important;
    }
    .mover-name-btn div[data-testid="stButton"] {
      margin: 0 !important;
      padding: 0 !important;
    }
    .mover-name-btn div[data-testid="stButton"] > button p {
      margin: 0 !important;
      line-height: 1.1 !important;
    }
    .st-key-mover-list-up [data-testid="stVerticalBlock"],
    .st-key-mover-list-down [data-testid="stVerticalBlock"] {
      gap: 0.28rem !important;
    }
    .st-key-mover-panel-up div[data-testid="stVerticalBlockBorderWrapper"],
    .st-key-mover-panel-down div[data-testid="stVerticalBlockBorderWrapper"] {
      background:#ffffff !important;
      border:1px solid #e9eef6 !important;
      border-radius:0 !important;
      box-shadow:0 3px 10px rgba(15,23,42,0.045) !important;
    }
    .st-key-mover-panel-up div[data-testid="stVerticalBlockBorderWrapper"] > div,
    .st-key-mover-panel-down div[data-testid="stVerticalBlockBorderWrapper"] > div {
      padding:12px 12px 10px 12px !important;
    }
    .st-key-mover-list-up div[data-testid="stVerticalBlockBorderWrapper"] {
      border: 1px solid #eaedf2 !important;
      background: #fffdfd !important;
      border-radius: 0 !important;
      box-shadow: none !important;
      width: 96% !important;
      margin-left: auto !important;
      margin-right: auto !important;
    }
    .st-key-mover-list-down div[data-testid="stVerticalBlockBorderWrapper"] {
      border: 1px solid #eaedf2 !important;
      background: #fcfdff !important;
      border-radius: 0 !important;
      box-shadow: none !important;
      width: 96% !important;
      margin-left: auto !important;
      margin-right: auto !important;
    }
    .st-key-mover-list-up div[data-testid="stVerticalBlockBorderWrapper"] > div,
    .st-key-mover-list-down div[data-testid="stVerticalBlockBorderWrapper"] > div {
      padding: 4px 8px !important;
      min-height: 34px !important;
      display:flex !important;
      align-items:center !important;
      justify-content:center !important;
    }
    .st-key-mover-list-up div[data-testid="stVerticalBlockBorderWrapper"] {
      background: #fffdfd !important;
    }
    .st-key-mover-list-down div[data-testid="stVerticalBlockBorderWrapper"] {
      background: #fcfdff !important;
    }
    .st-key-mover-list-up div[data-testid="column"],
    .st-key-mover-list-down div[data-testid="column"] {
      min-width:0 !important;
    }
    .st-key-mover-list-up div[data-testid="column"] > div,
    .st-key-mover-list-down div[data-testid="column"] > div {
      height: 100%;
      display: flex;
      min-height: 24px;
      align-items: center;
    }
    .st-key-mover-list-up div[data-testid="stButton"],
    .st-key-mover-list-down div[data-testid="stButton"],
    .st-key-mover-list-up div[data-testid="stMarkdown"],
    .st-key-mover-list-down div[data-testid="stMarkdown"] {
      margin: 0 !important;
      width: 100%;
      display: flex;
      align-items: center;
    }
    .st-key-mover-list-up div[data-testid="column"]:first-child > div,
    .st-key-mover-list-down div[data-testid="column"]:first-child > div {
      justify-content: flex-start !important;
    }
    .st-key-mover-list-up div[data-testid="column"]:nth-child(2) > div,
    .st-key-mover-list-down div[data-testid="column"]:nth-child(2) > div {
      justify-content: flex-start !important;
    }
    .st-key-mover-list-up div[data-testid="column"]:last-child > div,
    .st-key-mover-list-down div[data-testid="column"]:last-child > div {
      justify-content: flex-end !important;
    }
    .st-key-mover-list-up div[data-testid="stButton"] > button {
      width: 96% !important;
      margin: 0 auto 4px auto !important;
      border: 1px solid #eceff4 !important;
      background: #fffefe !important;
      color: #7f1d1d !important;
      box-shadow: none !important;
      font-family: "Pretendard Variable", "Noto Sans KR", "Apple SD Gothic Neo", sans-serif !important;
      font-size: 0.88rem !important;
      font-weight: 700 !important;
      text-align: left !important;
      justify-content: flex-start !important;
      padding: 0 10px !important;
      height: 34px !important;
      min-height: 34px !important;
      border-radius: 0 !important;
    }
    .st-key-mover-list-down div[data-testid="stButton"] > button {
      width: 96% !important;
      margin: 0 auto 4px auto !important;
      border: 1px solid #eceff4 !important;
      background: #fcfdff !important;
      color: #1e3a8a !important;
      box-shadow: none !important;
      font-family: "Pretendard Variable", "Noto Sans KR", "Apple SD Gothic Neo", sans-serif !important;
      font-size: 0.88rem !important;
      font-weight: 700 !important;
      text-align: left !important;
      justify-content: flex-start !important;
      padding: 0 10px !important;
      height: 34px !important;
      min-height: 34px !important;
      border-radius: 0 !important;
    }
    @media (max-width: 900px) {
      .theme-bubble-board { --bubble-scale: 0.86; }
      .theme-bubble-grid { grid-template-columns: 1fr; }
      .theme-bubble-lane { min-height: 220px; }
      .theme-mover-grid { grid-template-columns: 1fr; }
      .rel-row { flex-wrap:wrap; }
      .rel-main { width:100%; flex-wrap:wrap; }
      .rel-sub { white-space: normal; }
      .score-wrap { width: 100%; margin-left: 0; }
    }
    @media (max-width: 760px) {
      .theme-bubble-board { --bubble-scale: 0.72; }
    }
    </style>
    """,
    unsafe_allow_html=True,
)

head_left, head_right = st.columns([6.2, 1.8], vertical_alignment="center")
with head_left:
    st.markdown(
        "<h1 class='hero-title' data-hero-home='1'>KRX Stock Pulse <span class='live-badge'>실시간</span></h1>",
        unsafe_allow_html=True,
    )
    st.markdown(
        "<div class='hero-sub'>종목명 또는 종목코드로 주가와 최신 뉴스를 빠르게 조회합니다.</div>",
        unsafe_allow_html=True,
    )
with head_right:
    st.markdown(
        f"<div style='text-align:right; color:#64748b; font-size:0.92rem;'>{dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</div>",
        unsafe_allow_html=True,
    )

hero_result = _hero_click_bridge(on_go_home_change=lambda: None, isolate_styles=False, key="hero-click-bridge")
_copy_shortcut_bridge(isolate_styles=False, key="copy-shortcut-bridge")
if getattr(hero_result, "go_home_reload", None):
    st.markdown(
        """
        <script>
          window.location.reload();
        </script>
        """,
        unsafe_allow_html=True,
    )
    st.session_state["query_input"] = ""
    st.session_state["auto_search"] = False
    st.session_state["pending_query"] = None
    st.session_state["last_match"] = None
    st.session_state["last_snapshot"] = None
    st.session_state["last_news_items"] = []
    st.session_state["last_related"] = []
    st.session_state["last_extra"] = {}
    st.session_state["last_peers"] = []
    st.session_state["last_financial_table"] = []
    st.rerun()

with st.form("search_form", clear_on_submit=False):
    col_in, col_btn = st.columns([5.2, 1], vertical_alignment="bottom")
    query = col_in.text_input(
        "종목명 또는 종목코드",
        placeholder="예: 엔켐 또는 348370",
        key="query_input",
        label_visibility="collapsed",
    )
    run = col_btn.form_submit_button("조회", type="primary", use_container_width=True)

effective_query = str(applied_pending_query or query or "")
effective_run = run or st.session_state.get("auto_search", False)

if not effective_run and not st.session_state.get("last_match"):
    theme_movers = load_today_theme_movers_cached(THEME_MOVER_CACHE_VERSION)
    market_wide_movers = load_market_wide_movers_cached(MARKET_WIDE_MOVER_CACHE_VERSION)
    with st.container(border=True):
        st.markdown("<h2 class='market-title'>오늘 테마 흐름</h2>", unsafe_allow_html=True)
        st.caption(f"기준일: {theme_movers.get('as_of', dt.datetime.now().strftime('%Y-%m-%d'))}  |  화면 버전: {MARKET_MOVER_UI_VERSION}")
        if theme_movers.get("error"):
            st.warning(theme_movers.get("error"))
        all_theme_rows = sorted(
            theme_movers.get("up", []) + theme_movers.get("down", []),
            key=lambda x: abs(float(x.get("avg_change", 0))),
            reverse=True,
        )[:80]
        top_risers = market_wide_movers.get("rise", [])
        top_fallers = market_wide_movers.get("fall", [])
        up_theme_rows = [row for row in all_theme_rows if float(row.get("avg_change", 0)) >= 0]
        down_theme_rows = [row for row in all_theme_rows if float(row.get("avg_change", 0)) < 0]

        if all_theme_rows:
            popup_key = str(st.session_state.get("theme_popup_key", "")).strip()
            popup_side = ""
            popup_theme = ""
            if popup_key and "|" in popup_key:
                popup_side, popup_theme = popup_key.split("|", 1)
            popup_up = popup_theme if popup_side == "UP" else ""
            popup_down = popup_theme if popup_side == "DOWN" else ""
            st.markdown(
                f"""
                <div class="theme-bubble-board">
                  <div class="theme-bubble-grid">
                    <div class="theme-bubble-lane up">
                      <div class="theme-bubble-lane-title">강한 상승 테마</div>
                      <div class="theme-bubble-wrap">{render_theme_bubble_cluster(up_theme_rows, positive=True, popup_theme=popup_up)}</div>
                    </div>
                    <div class="theme-bubble-lane down">
                      <div class="theme-bubble-lane-title">약세/하락 테마</div>
                      <div class="theme-bubble-wrap">{render_theme_bubble_cluster(down_theme_rows, positive=False, popup_theme=popup_down)}</div>
                    </div>
                  </div>
                </div>
                """,
                unsafe_allow_html=True,
            )
            theme_bridge_result = _theme_click_bridge(on_theme_click_change=lambda: None, isolate_styles=False, key="theme-bridge")
            _theme_drag_bridge(isolate_styles=False, key="theme-drag-bridge")
            if getattr(theme_bridge_result, "theme_click", None):
                clicked = unquote_plus(str(theme_bridge_result.theme_click))
                st.session_state["theme_popup_key"] = "" if clicked == "__CLEAR__" else clicked
                st.rerun()
        else:
            st.caption("오늘 테마 맵을 아직 만들지 못했습니다.")
            stats = theme_movers.get("stats", {})
            if stats:
                st.caption(f"조회 성공 {stats.get('success', 0)}건 / 실패 {stats.get('failed', 0)}건")

        st.markdown("<div style='height:10px;'></div>", unsafe_allow_html=True)

        mover_left, mover_right = st.columns(2, vertical_alignment="top")

        with mover_left:
            with st.container(border=True, key="mover-panel-up"):
                st.markdown(
                    "<div class='mover-head-row'><h4 class='market-title'>🔥 시장 급등 종목 TOP 100</h4></div>",
                    unsafe_allow_html=True,
                )
                if top_risers:
                    with st.container(height=192, border=False, key="mover-list-up"):
                        display_risers = top_risers[:100]
                        for row_idx, pair in enumerate(chunked_rows(display_risers, 2)):
                            cols = st.columns(2, vertical_alignment="center", gap="small")
                            for i, item in enumerate(pair):
                                rank = row_idx * 2 + i + 1
                                query_value = quote_plus(str(item.get("symbol") or item["name"]))
                                cols[i].markdown(
                                    f"""
                                    <div style="width:96%; margin:0 auto 4px auto; border:1px solid #eceff4; background:#fffefe; border-radius:0; padding:4px 8px; min-height:34px; display:flex; align-items:center;">
                                      <div class='mover-row-link' data-symbol='{str(item.get("symbol") or item["name"])}' style="width:100%;">
                                        <span class='rank-pill'>{rank}</span>
                                        <span class='mover-name-text'>{item['name']}</span>
                                        <span class='pct-up'>+{float(item['change_pct']):.2f}%</span>
                                      </div>
                                    </div>
                                    """,
                                    unsafe_allow_html=True,
                                )
                else:
                    st.caption("급등 종목 데이터를 아직 불러오지 못했습니다.")

        with mover_right:
            with st.container(border=True, key="mover-panel-down"):
                st.markdown(
                    "<div class='mover-head-row'><h4 class='market-title'>〽 시장 급락 종목 TOP 100</h4></div>",
                    unsafe_allow_html=True,
                )
                if top_fallers:
                    with st.container(height=192, border=False, key="mover-list-down"):
                        display_fallers = top_fallers[:100]
                        for row_idx, pair in enumerate(chunked_rows(display_fallers, 2)):
                            cols = st.columns(2, vertical_alignment="center", gap="small")
                            for i, item in enumerate(pair):
                                rank = row_idx * 2 + i + 1
                                query_value = quote_plus(str(item.get("symbol") or item["name"]))
                                cols[i].markdown(
                                    f"""
                                    <div style="width:96%; margin:0 auto 4px auto; border:1px solid #eceff4; background:#fcfdff; border-radius:0; padding:4px 8px; min-height:34px; display:flex; align-items:center;">
                                      <div class='mover-row-link' data-symbol='{str(item.get("symbol") or item["name"])}' style="width:100%;">
                                        <span class='rank-pill'>{rank}</span>
                                        <span class='mover-name-text'>{item['name']}</span>
                                        <span class='pct-down'>{float(item['change_pct']):.2f}%</span>
                                      </div>
                                    </div>
                                    """,
                                    unsafe_allow_html=True,
                                )
                else:
                    st.caption("급락 종목 데이터를 아직 불러오지 못했습니다.")

        bridge_result = _mover_click_bridge(on_symbol_click_change=lambda: None, isolate_styles=False, key="mover-bridge")
        if getattr(bridge_result, "symbol_click", None):
            st.session_state["pending_query"] = bridge_result.symbol_click
            st.session_state["auto_search"] = True
            st.rerun()

if effective_run or st.session_state.get("last_match"):
    st.session_state["auto_search"] = False
    if effective_run:
        if not effective_query.strip():
            st.warning("종목명 또는 코드를 입력해 주세요.")
            st.stop()
        try:
            match = load_match_cached(effective_query)
            if not match:
                st.error("해당 종목을 찾지 못했습니다.")
                st.stop()
            snapshot = load_snapshot_cached(match["symbol"], match.get("market_type", "KRX"))
            news_query = match["name"] if match.get("market_type") == "KRX" else match["symbol"]
            is_global = (match.get("market_type") or "").upper() == "GLOBAL"
            with ThreadPoolExecutor(max_workers=5) as executor:
                f_news = executor.submit(load_news_cached, news_query)
                f_related = executor.submit(
                    load_related_cached,
                    match["symbol"],
                    match["name"],
                    match.get("market_type", "KRX"),
                    match.get("exchange", ""),
                    snapshot.get("industry", "") or match.get("industry", "") or "",
                    snapshot.get("products", "") or match.get("products", "") or "",
                    snapshot.get("company_description", "") or "",
                )
                f_peers = executor.submit(load_peers_cached, match["symbol"]) if not is_global else None
                f_financial = executor.submit(load_financial_table_cached, match["symbol"]) if not is_global else None
                news_items = f_news.result()
                related = f_related.result()
                extra = {}
                peers = f_peers.result() if f_peers else []
                financial_table = f_financial.result() if f_financial else []
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
                    summary_lines = build_company_profile_block(
                        match["name"],
                        industry,
                        products,
                        description if description != "정보 없음" else "",
                    )
                    detailed_lines = build_company_detailed_report(
                        match["name"],
                        industry,
                        products,
                        description if description != "정보 없음" else "",
                        news_items,
                        related,
                        snapshot,
                    )
                    merged_lines = merge_company_info_lines(summary_lines, detailed_lines, max_lines=9)
                    for line in merged_lines:
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
                    if isinstance(flow, dict) and flow:
                        f["flow"] = flow

                    with st.container(border=True):
                        st.markdown("### 투자 포인트")
                        for p in build_investment_points(match, snapshot, f, related[:3]):
                            st.write(f"- {p}")

                    with st.container(border=True):
                        st.markdown("### 동종업계 비교")
                        if peers:
                            sales_basis = peers[0].get("sales_basis", "최근 분기")
                            op_basis = peers[0].get("op_basis", "최근 분기")
                            current_symbol = str(match.get("symbol", "")).strip()
                            current_name = str(match.get("name", "")).strip()
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
                            if "symbol" in peers_df.columns:
                                peers_df["_is_current"] = (
                                    (peers_df["symbol"].astype(str).str.strip() == current_symbol)
                                    | (peers_df["종목명"].astype(str).str.strip() == current_name)
                                )
                                peers_df = peers_df.sort_values(by="_is_current", ascending=False, kind="stable").reset_index(drop=True)
                            else:
                                peers_df["_is_current"] = False
                            peers_df = peers_df.drop(columns=["sales_basis", "op_basis"], errors="ignore")
                            styled_peers = peers_df.drop(columns=["symbol", "_is_current"], errors="ignore").style.apply(
                                lambda row: ["font-weight: 800; background-color: #eef6ff; color: #0f172a;" for _ in row] if bool(peers_df.loc[row.name, "_is_current"]) else ["" for _ in row],
                                axis=1,
                            )
                            st.dataframe(styled_peers, use_container_width=True, hide_index=True)
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

                            st.markdown(
                                f"""
                                <div class="rel-card">
                                  <div class="rel-row">
                                    <div class="rel-main">
                                      <div class="rel-name">
                                        <a href="?q={item['symbol']}" class="rel-name-link" data-symbol="{item['symbol']}">{item['name']} ({item['symbol']})</a>
                                      </div>
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
                    related_bridge_result = _related_click_bridge(on_symbol_click_change=lambda: None, isolate_styles=False, key="related-bridge")
                    if getattr(related_bridge_result, "symbol_click", None):
                        st.session_state["pending_query"] = related_bridge_result.symbol_click
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
