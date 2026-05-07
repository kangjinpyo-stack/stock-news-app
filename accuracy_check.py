import argparse
import json
from pathlib import Path
from typing import Any, Dict, List, Tuple

from stock_info_news import disable_broken_proxy_env, enrich_company_profile, get_related_stocks, search_symbol, get_stock_snapshot


def load_cases(path: Path) -> List[Dict[str, Any]]:
    with path.open('r', encoding='utf-8-sig') as f:
        data = json.load(f)
    if not isinstance(data, list):
        raise ValueError('테스트 파일은 리스트(JSON array)여야 합니다.')
    return data


def resolve_match(case: Dict[str, Any]) -> Tuple[Dict[str, Any], str]:
    if case.get('match_override'):
        return dict(case['match_override']), 'override'

    query = str(case.get('query', '')).strip()
    if not query:
        raise ValueError('query 또는 match_override가 필요합니다.')

    match = search_symbol(query)
    if not match:
        raise RuntimeError(f'종목 검색 실패: {query}')

    try:
        snapshot = get_stock_snapshot(match['symbol'], match.get('market_type', 'KRX'))
        match = enrich_company_profile(match, snapshot)
    except Exception:
        pass
    return match, 'search'


def evaluate_case(case: Dict[str, Any], related: List[Dict[str, str]]) -> Dict[str, Any]:
    names = {str(x.get('name', '')).strip() for x in related}
    buckets = {str(x.get('relation_bucket', '')).strip() for x in related}
    themes = set()
    for item in related:
        raw = str(item.get('matched_themes', '')).strip()
        if not raw:
            continue
        for part in raw.split(','):
            part = part.strip()
            if part:
                themes.add(part)

    failures: List[str] = []

    for expected in case.get('must_include', []):
        if expected not in names:
            failures.append(f"포함 필요 종목 누락: {expected}")
    for banned in case.get('must_exclude', []):
        if banned in names:
            failures.append(f"제외 필요 종목 포함: {banned}")
    for expected in case.get('must_include_buckets', []):
        if expected not in buckets:
            failures.append(f"포함 필요 분류 누락: {expected}")
    for banned in case.get('must_exclude_buckets', []):
        if banned in buckets:
            failures.append(f"제외 필요 분류 포함: {banned}")
    for expected in case.get('must_include_themes', []):
        if expected not in themes:
            failures.append(f"포함 필요 테마 누락: {expected}")
    for banned in case.get('must_exclude_themes', []):
        if banned in themes:
            failures.append(f"제외 필요 테마 포함: {banned}")

    return {
        'passed': not failures,
        'failures': failures,
        'names': sorted([x for x in names if x]),
        'buckets': sorted([x for x in buckets if x]),
        'themes': sorted([x for x in themes if x]),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description='관련주 추천 정확도 자동 점검')
    parser.add_argument('--cases', default='related_test_cases.json', help='테스트 케이스 JSON 경로')
    parser.add_argument('--limit', type=int, default=8, help='관련주 최대 개수')
    parser.add_argument('--save-report', default='', help='결과 JSON 저장 경로(optional)')
    args = parser.parse_args()

    disable_broken_proxy_env()
    cases = load_cases(Path(args.cases))

    report: List[Dict[str, Any]] = []
    passed = 0
    failed = 0
    skipped = 0

    for idx, case in enumerate(cases, start=1):
        name = str(case.get('name', f'case-{idx}'))
        query = str(case.get('query', ''))
        try:
            match, source = resolve_match(case)
            related = get_related_stocks(match, limit=args.limit)
            result = evaluate_case(case, related)
            status = 'PASS' if result['passed'] else 'FAIL'
            if result['passed']:
                passed += 1
            else:
                failed += 1
            report_item = {
                'name': name,
                'query': query,
                'status': status,
                'source': source,
                'resolved_match': match,
                'result': result,
            }
        except Exception as exc:
            skipped += 1
            report_item = {
                'name': name,
                'query': query,
                'status': 'SKIP',
                'error': str(exc),
            }
        report.append(report_item)

    total = len(cases)
    evaluated = passed + failed
    accuracy = (passed / evaluated * 100.0) if evaluated else 0.0

    print(f'총 케이스: {total}')
    print(f'평가 완료: {evaluated}')
    print(f'통과: {passed}')
    print(f'실패: {failed}')
    print(f'스킵: {skipped}')
    print(f'정확도(스킵 제외): {accuracy:.1f}%')
    print('-' * 60)

    for item in report:
        print(f"[{item['status']}] {item['name']}")
        if item['status'] == 'SKIP':
            print(f"  사유: {item['error']}")
            continue
        result = item['result']
        print(f"  분류: {', '.join(result['buckets']) or '-'}")
        print(f"  테마: {', '.join(result['themes']) or '-'}")
        if item['status'] == 'FAIL':
            for msg in result['failures']:
                print(f"  실패: {msg}")
        print(f"  종목: {', '.join(result['names'][:8]) or '-'}")

    if args.save_report:
        Path(args.save_report).write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding='utf-8')
        print('-' * 60)
        print(f'리포트 저장: {args.save_report}')

    return 0 if failed == 0 else 1


if __name__ == '__main__':
    raise SystemExit(main())
