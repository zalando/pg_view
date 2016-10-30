import os
from unittest import TestCase

import mock
import sys

from freezegun import freeze_time

path = os.path.dirname(os.path.dirname(__file__))
sys.path.insert(0, path)

from pg_view.models.host_stat import HostStatCollector


class HostStatCollectorTest(TestCase):
    def setUp(self):
        self.collector = HostStatCollector()
        super(HostStatCollectorTest, self).setUp()

    def test_result_should_contain_proper_data_keys(self):
        refreshed_data = self.collector.refresh()
        self.assertIn('cores', refreshed_data)
        self.assertIn('hostname', refreshed_data)
        self.assertIn('loadavg', refreshed_data)
        self.assertIn('uptime', refreshed_data)
        self.assertIn('sysname', refreshed_data)

    @mock.patch('pg_view.models.host_stat.os.getloadavg', return_value=(3.47, 3.16, 2.89))
    def test_refresh_should_call_load_average(self, mocked_getloadavg):
        refreshed_data = self.collector._read_load_average()
        self.assertEqual({'loadavg': '3.47 3.16 2.89'}, refreshed_data)

    @mock.patch('pg_view.models.host_stat.psutil.boot_time', return_value=1477834496.0)
    @freeze_time('2016-10-31 00:25:00')
    def test_refresh_should_call_uptime(self, mocked_boot_time):
        refreshed_data = self.collector._read_uptime()
        self.assertEqual({'uptime': '10:50:04'}, refreshed_data)

    @mock.patch('pg_view.models.host_stat.socket.gethostname', return_value='Macbook-Pro')
    def test_refresh_should_call_hostname(self, mocked_gethostname):
        refreshed_data = self.collector._read_hostname()
        self.assertEqual({'hostname': 'Macbook-Pro'}, refreshed_data)

    @mock.patch('pg_view.models.host_stat.os.uname', return_value=('Darwin', 'MacBook-Pro', '15.6.0', 'KV 15.6.0: Thu Sep 1 PDT 2016; root:xnu-3248', 'x86_64'))
    def test_refresh_should_call_uname(self, mocked_uname):
        refreshed_data = self.collector._read_uname()
        self.assertEqual({'sysname': 'Darwin 15.6.0'}, refreshed_data)

    @mock.patch('pg_view.models.host_stat.cpu_count', return_value=1)
    def test_refresh_should_call_cpus_count_when_ok(self, mocked_cpu_count):
        refreshed_data = self.collector._read_cpus()
        self.assertEqual({'cores': 1}, refreshed_data)

    @mock.patch('pg_view.models.host_stat.cpu_count')
    @mock.patch('pg_view.models.host_stat.logger')
    def test_refresh_should_call_cpus_count_when_not_implemented_raised(self, mocked_logging, mocked_cpu_count):
        mocked_cpu_count.side_effect = NotImplementedError
        refreshed_data = self.collector._read_cpus()
        self.assertEqual({'cores': 0}, refreshed_data)
        mocked_logging.error.assert_called_with('multiprocessing does not support cpu_count')
