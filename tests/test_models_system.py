import sys
from collections import namedtuple
from unittest import TestCase

import mock
import os

from tests.common import TEST_DIR

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
        self.assertIn('blocked', refreshed_data)
        self.assertIn('guest', refreshed_data)
        self.assertIn('irq', refreshed_data)
        self.assertIn('utime', refreshed_data)
        self.assertIn('steal', refreshed_data)

    @mock.patch('pg_view.models.system_stat.SystemStatCollector._refresh_cpu_time_values')
    @mock.patch('pg_view.models.system_stat.SystemStatCollector._do_refresh')
    @mock.patch('pg_view.models.system_stat.SystemStatCollector.read_cpu_stats')
    @mock.patch('pg_view.models.system_stat.SystemStatCollector.read_cpu_times')
    def test_refresh_should_call_helpers_with_proper_data(self, mocked_read_cpu_times, mocked_read_proc_stat,
                                                          mocked__do_refresh, mocked__refresh_cpu_time_values):
        cpu_stats = {
            'cpu': ['46535', '0', '40348', '8412642', '188', '1', '2020', '0', '0', '0'], 'blocked': 0,
            'ctxt': 11530476.0, 'guest': 0.0, 'idle': 8412642.0, 'iowait': 188.0, 'irq': 1.0, 'running': 1,
            'softirq': 2020.0, 'steal': 0.0, 'stime': 40348.0, 'utime': 46535.0
        }

        cpu_times = {
            'guest': 0.0, 'idle': 8412642.0, 'iowait': 188.0, 'irq': 1.0,
            'softirq': 2020.0, 'steal': 0.0, 'stime': 40348.0, 'utime': 46535.0
        }

        mocked_read_proc_stat.return_value = cpu_stats
        mocked_read_cpu_times.return_value = cpu_times
        merged_data = dict(cpu_times, **cpu_stats)

        self.collector.refresh()
        mocked__refresh_cpu_time_values.assert_called_once_with(cpu_times)
        mocked__do_refresh.assert_called_once_with([merged_data])

    @mock.patch('pg_view.models.system_stat.open_binary')
    def test_get_missing_cpu_stat_from_file_should_parse_data_from_proc_stat(self, mocked_open):
        cpu_info_ok = os.path.join(TEST_DIR, 'cpu_info_ok')
        mocked_open.return_value = open(cpu_info_ok, "rU")
        refreshed_data = self.collector.get_missing_cpu_stat_from_file()
        self.assertEqual({'procs_blocked': 0, 'procs_running': 1}, refreshed_data)

    @mock.patch('pg_view.models.system_stat.psutil.cpu_times')
    def test_read_cpu_data_should_transform_input_when_cpu_times_for_linux(self, mocked_cpu_times):
        linux_scputimes = namedtuple('scputimes', 'user nice system idle iowait irq softirq steal guest')
        mocked_cpu_times.return_value = linux_scputimes(
            user=848.31, nice=0.0, system=775.15, idle=105690.03, iowait=2.05, irq=0.01,
            softirq=54.83, steal=0.0, guest=0.0
        )
        refreshed_cpu = self.collector.read_cpu_times()
        expected_data = {
            'guest': 0.0, 'idle': 10569003.0, 'iowait': 204.99999999999997, 'irq': 1.0,
            'softirq': 5483.0, 'steal': 0.0, 'stime': 77515.0, 'utime': 84831.0
        }
        self.assertEqual(expected_data, refreshed_cpu)

    @mock.patch('pg_view.models.system_stat.psutil.cpu_times')
    def test_read_cpu_data_should_transform_input_when_cpu_times_for_macos(self, mocked_cpu_times):
        macos_scputimes = namedtuple('scputimes', 'user system idle')
        mocked_cpu_times.return_value = macos_scputimes(
            user=49618.61, system=28178.55, idle=341331.57)
        refreshed_cpu = self.collector.read_cpu_times()
        expected_data = {
            'guest': 0.0, 'idle': 34133157.0, 'iowait': 0.0, 'irq': 0.0,
            'softirq': 0.0, 'steal': 0.0, 'stime': 2817855.0, 'utime': 4961861.0
        }
        self.assertEqual(expected_data, refreshed_cpu)

    @mock.patch('pg_view.models.system_stat.psutil.cpu_stats')
    @mock.patch('pg_view.models.system_stat.psutil.LINUX', False)
    def test_read_cpu_data_should_transform_input_when_cpu_stats_for_macos(self, mocked_cpu_times):
        macos_scpustats = namedtuple('scpustats', 'ctx_switches interrupts soft_interrupts syscalls')
        mocked_cpu_times.return_value = macos_scpustats(
            ctx_switches=12100, interrupts=888823, soft_interrupts=211467872, syscalls=326368)

        refreshed_cpu = self.collector.read_cpu_stats()
        expected_data = {'running': 0.0, 'ctxt': 12100, 'blocked': 0.0}
        self.assertEqual(expected_data, refreshed_cpu)

    @mock.patch('pg_view.models.system_stat.psutil.cpu_stats')
    @mock.patch('pg_view.models.system_stat.psutil.LINUX', True)
    @mock.patch('pg_view.models.system_stat.SystemStatCollector.get_missing_cpu_stat_from_file')
    def test_read_cpu_data_should_transform_input_when_cpu_stats_for_linux(self, mocked_get_missing_cpu_stat_from_file,
                                                                           mocked_cpu_times):
        macos_scpustats = namedtuple('scpustats', 'ctx_switches interrupts soft_interrupts syscalls')
        mocked_get_missing_cpu_stat_from_file.return_value = {
            'procs_running': 10,
            'procs_blocked': 20,
        }
        mocked_cpu_times.return_value = macos_scpustats(
            ctx_switches=12100, interrupts=888823, soft_interrupts=211467872, syscalls=326368)

        refreshed_cpu = self.collector.read_cpu_stats()
        expected_data = {'running': 10.0, 'ctxt': 12100, 'blocked': 20.0}
        self.assertEqual(expected_data, refreshed_cpu)

    def test__refresh_cpu_time_values_should_update_cpu_when_ok(self):
        cpu_data = {
            'guest': 0.0, 'idle': 8412642.0, 'iowait': 188.0, 'irq': 1.0,
            'softirq': 2020.0, 'steal': 0.0, 'stime': 40348.0, 'utime': 46535.0
        }
        self.collector.current_total_cpu_time = 1.0
        self.collector._refresh_cpu_time_values(cpu_data)

        self.assertEqual(1.0, self.collector.previos_total_cpu_time)
        self.assertEqual(8501734.0, self.collector.current_total_cpu_time)
        self.assertEqual(8501733.0, self.collector.cpu_time_diff)
