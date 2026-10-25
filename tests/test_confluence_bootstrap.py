import importlib.util
from pathlib import Path
import unittest

spec = importlib.util.spec_from_file_location('bootstrap', Path(__file__).resolve().parents[1] / 'scripts/build_confluence_bootstrap.py')
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)


class BootstrapTests(unittest.TestCase):
    def test_recent_window_preserves_whole_records_and_archive_dates(self):
        rows = [{'date': f'2026-10-{day:02d}', 'market': 'Gold', 'nested': {'value': day}} for day in range(1, 21)]
        # A market with delayed reporting must remain resolvable at startup.
        rows += [{'date': '2026-10-01', 'market': 'Delayed', 'net_value': 123}]
        source = {'records': rows, 'latest_cot_report_date': '2026-10-20', 'instrument_registry': {'markets': ['Gold']}}
        result = module.build_bootstrap(source, weeks=16)
        self.assertEqual(result['bootstrap']['from_date'], '2026-10-05')
        self.assertEqual(len(result['bootstrap']['dates']), 20)
        self.assertEqual(result['records'][-1]['market'], 'Delayed')
        self.assertEqual(result['records'][0], rows[4])
        self.assertEqual(result['instrument_registry'], source['instrument_registry'])
        self.assertEqual(len(source['records']), 21)

    def test_empty_export_fails_instead_of_publishing_blank_startup(self):
        with self.assertRaises(ValueError):
            module.build_bootstrap({'records': []})


if __name__ == '__main__':
    unittest.main()
