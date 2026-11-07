import copy
import importlib.util
from datetime import datetime, timezone
from pathlib import Path
import pytest

spec = importlib.util.spec_from_file_location('release', Path(__file__).parents[1] / 'scripts/validate_cot_release.py')
release = importlib.util.module_from_spec(spec)
spec.loader.exec_module(release)


def fixture():
    group = [10, 1, 2, 3, 50] + [0] * 11
    doc = {'engine': 'weekly_inspector_flow', 'measure': release.MEASURE, 'markets': {str(i): {'available': True, 'rows': [['2026-11-03', group, group, group]]} for i in range(26)}}
    gate = {'report_date': '2026-11-03', 'checked_count': 26, 'passed_count': 26, 'failed_count': 0, 'exit_code': 0}
    return copy.deepcopy(doc), gate


def test_complete_release():
    doc, gate = fixture()
    assert release.validate(doc, gate, '2026-11-03') == '2026-11-03'


@pytest.mark.parametrize('fault', ['old_market', 'missing_market', 'missing_group', 'failed_gate', 'old_gate'])
def test_partial_release_rejected(fault):
    doc, gate = fixture()
    if fault == 'old_market': doc['markets']['0']['rows'][-1][0] = '2026-10-27'
    if fault == 'missing_market': del doc['markets']['0']
    if fault == 'missing_group': doc['markets']['0']['rows'][-1][1][4] = None
    if fault == 'failed_gate': gate['failed_count'] = 1
    if fault == 'old_gate': gate['report_date'] = '2026-10-27'
    with pytest.raises(ValueError): release.validate(doc, gate, '2026-11-03')


def test_friday_release_time_and_saturday():
    for stamp, week in [('2026-11-06T20:29:00', '2026-10-27'), ('2026-11-06T20:30:00', '2026-11-03'), ('2026-11-07T08:00:00', '2026-11-03')]:
        assert release.expected_week(datetime.fromisoformat(stamp).replace(tzinfo=timezone.utc)) == week
