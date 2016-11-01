import sys
from StringIO import StringIO
from unittest import TestCase

import mock
import os

path = os.path.dirname(os.path.dirname(__file__))
sys.path.insert(0, path)

from pg_view.models.system_stat import SystemStatCollector


class SystemStatCollectorTest(TestCase):
    def setUp(self):
        self.collector = SystemStatCollector()
        super(SystemStatCollectorTest, self).setUp()

    def test_result_should_contain_proper_data_keys(self):
        refreshed_data = self.collector.refresh()
        self.assertIn('stime', refreshed_data)
        self.assertIn('softirq', refreshed_data)
        self.assertIn('iowait', refreshed_data)
        self.assertIn('idle', refreshed_data)
        self.assertIn('ctxt', refreshed_data)
        self.assertIn('running', refreshed_data)
        self.assertIn('guest', refreshed_data)
        self.assertIn('irq', refreshed_data)
        self.assertIn('utime', refreshed_data)
        self.assertIn('steal', refreshed_data)
        self.assertIn('cpu', refreshed_data)

        self.assertIsInstance(refreshed_data['cpu'], list)
        self.assertEqual(10, len(refreshed_data['cpu']))
        self.assertIn('blocked', refreshed_data)

    @mock.patch('pg_view.models.system_stat.SystemStatCollector._refresh_cpu_time_values')
    @mock.patch('pg_view.models.system_stat.SystemStatCollector._do_refresh')
    @mock.patch('pg_view.models.system_stat.SystemStatCollector._read_proc_stat')
    def test_refresh_shold_call_helpers_with_proper_data(self, mocked__read_proc_stat, mocked__do_refresh, mocked__refresh_cpu_time_values):
        stat_data = {
            'cpu': ['46535', '0', '40348', '8412642', '188', '1', '2020', '0', '0', '0'], 'blocked': 0,
            'ctxt': 11530476.0, 'guest': 0.0, 'idle': 8412642.0, 'iowait': 188.0, 'irq': 1.0, 'running': 1,
            'softirq': 2020.0, 'steal': 0.0, 'stime': 40348.0, 'utime': 46535.0
        }
        mocked__read_proc_stat.return_value = stat_data

        cpu_data = {
            'guest': 0.0, 'idle': 8412642.0, 'iowait': 188.0,  'irq': 1.0,
            'softirq': 2020.0, 'steal': 0.0, 'stime': 40348.0, 'utime': 46535.0
        }
        self.collector.refresh()
        mocked__refresh_cpu_time_values.assert_called_once_with(cpu_data)
        merged_data = {}
        merged_data.update(stat_data)
        merged_data.update(cpu_data)
        mocked__do_refresh.assert_called_once_with([merged_data])

    @mock.patch('pg_view.models.system_stat.open')
    def test_result_should_return_data_when_proc_stat_ok(self, mocked_open):
        proc_stat_file = """cpu  46535 0 40348 8412642 188 1 2020 0 0 0
        cpu0 46535 0 40348 8412642 188 1 2020 0 0 0
        intr 5117521 53 10 0 0 0 0 0 0 0 0 0 0 156 0 0 0 0 0 0 645625 413948 29401 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0
        ctxt 11530476
        btime 1477917348
        processes 57175
        procs_running 1
        procs_blocked 0
        """

        mocked_open.return_value = StringIO(proc_stat_file)
        refreshed_data = self.collector.refresh()
        expected_data = {
            'blocked': 0,
            'cpu': ['46535', '0', '40348', '8412642', '188', '1', '2020', '0', '0', '0'],
            'ctxt': 11530476.0,
            'guest': 0.0,
            'idle': 8412642.0,
            'iowait': 188.0,
            'irq': 1.0,
            'running': 1,
            'softirq': 2020.0,
            'steal': 0.0,
            'stime': 40348.0,
            'utime': 46535.0
        }
        self.assertEqual(expected_data, refreshed_data)

    @mock.patch('pg_view.models.system_stat.open')
    def test__read_proc_stat_should_parse_proper_file(self, mocked_open):
        proc_stat_file = """cpu  46535 0 40348 8412642 188 1 2020 0 0 0
        cpu0 46535 0 40348 8412642 188 1 2020 0 0 0
        intr 5117521 53 10 0 0 0 0 0 0 0 0 0 0 156 0 0 0 0 0 0 645625 413948 29401 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0
        ctxt 11530476
        btime 1477917348
        processes 57175
        procs_running 1
        procs_blocked 0
        """

        mocked_open.return_value = StringIO(proc_stat_file)
        refreshed_data = self.collector._read_proc_stat()
        expected_result = {
            'running': 1,
            'blocked': 0,
            'ctxt': 11530476.0,
            'cpu': ['46535', '0', '40348', '8412642', '188', '1', '2020', '0', '0', '0']
        }
        self.assertEqual(expected_result, refreshed_data)

    def test__read_cpu_data_should_transform_input_when_cpu_row_ok(self):
        cpu_row = ['46535', '0', '40348', '8412642', '188', '1', '2020', '0', '0', '0']
        refreshed_cpu = self.collector._read_cpu_data(cpu_row)
        expected_data = {
            'guest': 0.0, 'idle': 8412642.0, 'iowait': 188.0,  'irq': 1.0,
            'softirq': 2020.0, 'steal': 0.0, 'stime': 40348.0, 'utime': 46535.0
        }
        self.assertEqual(expected_data, refreshed_cpu)

    def test__refresh_cpu_time_values_should_update_cpu_when_ok(self):
        cpu_data = {
            'guest': 0.0, 'idle': 8412642.0, 'iowait': 188.0,  'irq': 1.0,
            'softirq': 2020.0, 'steal': 0.0, 'stime': 40348.0, 'utime': 46535.0
        }
        self.collector.current_total_cpu_time = 1.0
        self.collector._refresh_cpu_time_values(cpu_data)

        self.assertEqual(1.0, self.collector.previos_total_cpu_time)
        self.assertEqual(8501734.0, self.collector.current_total_cpu_time)
        self.assertEqual(8501733.0, self.collector.cpu_time_diff)
