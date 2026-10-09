import importlib.util
import json
import tempfile
import unittest
import zipfile
from pathlib import Path

SCRIPT = Path(__file__).resolve().parents[1] / 'scripts/audit_sugar_archive_source.py'
spec = importlib.util.spec_from_file_location('sugar_source_audit', SCRIPT)
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)


class SugarSourceAuditTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.path = Path(self.temp.name) / 'contracts.zip'
        with zipfile.ZipFile(self.path, 'w') as source:
            for code, closes in [('SB00V', [100, 101, 99]), ('SB01H', [199, 200, 202])]:
                source.writestr(code + '.txt', '\n'.join(f'{d},{c},{c+1},{c-1},{c},100,1000' for d, c in zip(['000928', '000929', '001002'], closes)))
        self.archive = {'first_date': '2000-09-28', 'last_date': '2000-10-02', 'rows': [
            ['2000-09-29', '2000-09-28', .01, 1, 'SB00V'],
            ['2000-10-02', '2000-09-29', .01, 1, 'SB01H'],
        ]}

    def test_roll_price_gap_is_excluded_but_new_contract_daily_return_is_included(self):
        result = module.audit_archive(self.path, self.archive)
        self.assertTrue(result['passed'], result['failures'])
        self.assertEqual(result['contract_switches_checked'], 1)

    def test_artificial_roll_jump_is_detected(self):
        self.archive['rows'][1][2] = 202 / 101 - 1
        result = module.audit_archive(self.path, self.archive)
        self.assertFalse(result['passed'])
        self.assertTrue(any('source closes' in failure for failure in result['failures']))

    def test_false_quality_flag_and_missing_return_are_detected(self):
        self.archive['rows'][0][3] = 0
        self.archive['rows'].pop()
        result = module.audit_archive(self.path, self.archive)
        self.assertFalse(result['passed'])
        self.assertTrue(any('quality flag' in failure for failure in result['failures']))
        self.assertTrue(any('session count' in failure for failure in result['failures']))


if __name__ == '__main__':
    unittest.main()
