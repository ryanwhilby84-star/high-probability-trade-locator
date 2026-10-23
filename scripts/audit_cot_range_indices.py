"""Independent stdlib audit: chart nets -> all packed range indices and 3Y changes.
Usage: python scripts/audit_cot_range_indices.py --inspector FILE --source FILE
Does not import production calculation helpers. Does not certify upstream CFTC records.
"""
import argparse
import json
from pathlib import Path


def audit(inspector, source):
    checks = 0
    windows = ((156, 4), (260, 12), (520, 14), (780, 13), (None, 15))
    for name, market in inspector['markets'].items():
        rows = market.get('rows', [])
        if not rows:
            continue
        raw = source['markets'][name]['series']
        assert len(raw) == len(rows), (name, 'source coverage differs')
        assert len(set(r[0] for r in rows)) == len(rows), (name, 'duplicate weeks')
        assert [r[0] for r in rows] == sorted(r[0] for r in rows), (name, 'unsorted weeks')
        for group, key in enumerate(('commercial_net', 'institutional_net', 'retail_net'), 1):
            expected_3y = []
            for i, row in enumerate(rows):
                assert raw[i]['date'] == row[0], (name, 'date mismatch')
                assert raw[i].get(key) == row[group][0], (name, row[0], key, 'net mismatch')
                checks += 1
                for width, offset in windows:
                    history = raw[max(0, i-width+1) if width else 0:i+1]
                    values = [r[key] for r in history if r.get(key) is not None]
                    current = raw[i].get(key)
                    expected = None
                    if current is not None and len(values) >= 2 and max(values) != min(values):
                        expected = round(100 * (current-min(values)) / (max(values)-min(values)), 2)
                    actual = row[group][offset]
                    assert (expected is None and actual is None) or (expected is not None and actual is not None and abs(expected-actual) <= .02), (name,row[0],key,width,expected,actual)
                    if width == 156:
                        expected_3y.append(expected)
                    checks += 1
                for lag, offset in ((1,5),(4,6),(12,7)):
                    current = expected_3y[i]
                    previous = expected_3y[i-lag] if i >= lag else None
                    expected = current-previous if current is not None and previous is not None else None
                    actual = row[group][offset]
                    assert (expected is None and actual is None) or (expected is not None and actual is not None and abs(expected-actual) <= .03), (name,row[0],key,'3Y change',lag,expected,actual)
                    checks += 1
    return checks


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--inspector', default='web-dashboard/public/data/cot_weekly_inspector_latest.json')
    parser.add_argument('--source', default='web-dashboard/public/data/cot_3y_series_latest.json')
    args = parser.parse_args()
    doc = json.loads(Path(args.inspector).read_text())
    checks = audit(doc, json.loads(Path(args.source).read_text()))
    print(f'PASS: {checks:,} independent net/index/movement checks across {len(doc["markets"])} markets; export {doc.get("generated_at")}')
