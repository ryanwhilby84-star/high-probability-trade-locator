"""Keep startup bounded; retain the complete archive for older date selections."""
import json
from pathlib import Path


def build_bootstrap(doc, weeks=16):
    records = doc.get('records') or []
    row_date = lambda row: str(row.get('date') or row.get('week') or row.get('cot_report_date') or row.get('latest_report_date') or '')[:10]
    dates = sorted({row_date(row) for row in records if row_date(row)})
    if not dates:
        raise ValueError('Confluence export has no dated records')
    cutoff = dates[-weeks] if len(dates) >= weeks else dates[0]
    recent = [row for row in records if row_date(row) >= cutoff]
    # Preserve the most recent observation for any market absent in this window.
    present = {row.get('market') or row.get('raw_cftc_market_name') for row in recent}
    fallback = {}
    for row in records:
        market = row.get('market') or row.get('raw_cftc_market_name')
        if market not in present and (market not in fallback or row_date(row) > row_date(fallback[market])):
            fallback[market] = row
    return {**doc, 'records': recent + list(fallback.values()),
            'bootstrap': {'version': 1, 'dates': dates, 'from_date': cutoff,
                          'latest_date': dates[-1], 'full_record_count': len(records)}}


if __name__ == '__main__':
    folder = Path(__file__).resolve().parents[1] / 'web-dashboard/public/data'
    doc = json.loads((folder / 'confluence_history_latest.json').read_text(encoding='utf-8'))
    result = build_bootstrap(doc)
    target = folder / 'confluence_bootstrap_latest.json'
    target.write_text(json.dumps(result, separators=(',', ':'), allow_nan=False), encoding='utf-8')
    print(f'Confluence startup: {len(result["records"])}/{len(doc["records"])} rows; {target.stat().st_size} bytes; latest {result["bootstrap"]["latest_date"]}')
