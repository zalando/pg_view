import json
from datetime import datetime
from unittest import TestCase

import mock
from freezegun import freeze_time

from pg_view.factories import get_displayer_by_class
from pg_view.models.collector_host import HostStatCollector
from pg_view.models.displayers import ColumnType, OUTPUT_METHOD


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

    @mock.patch('pg_view.models.collector_host.os.getloadavg', return_value=(3.47, 3.16, 2.89))
    def test_refresh_should_call_load_average(self, mocked_getloadavg):
        refreshed_data = self.collector._read_load_average()
        self.assertEqual({'loadavg': '3.47 3.16 2.89'}, refreshed_data)

    @mock.patch('pg_view.models.collector_host.psutil.boot_time', return_value=1477834496.0)
    @freeze_time('2016-10-31 00:25:00')
    def test_refresh_should_call_uptime(self, mocked_boot_time):
        refreshed_data = self.collector._read_uptime()
        expected_uptime = datetime(2016, 10, 31, 0, 25) - datetime.fromtimestamp(1477834496.0)
        self.assertEqual({'uptime': str(expected_uptime)}, refreshed_data)

    @mock.patch('pg_view.models.collector_host.socket.gethostname', return_value='Macbook-Pro')
    def test_refresh_should_call_hostname(self, mocked_gethostname):
        refreshed_data = self.collector._read_hostname()
        self.assertEqual({'hostname': 'Macbook-Pro'}, refreshed_data)

    @mock.patch('pg_view.models.collector_host.os.uname', return_value=(
            'Darwin', 'MacBook-Pro', '15.6.0', 'KV 15.6.0: Thu Sep 1 PDT 2016; root:xnu-3248', 'x86_64'))
    def test_refresh_should_call_uname(self, mocked_uname):
        refreshed_data = self.collector._read_uname()
        self.assertEqual({'sysname': 'Darwin 15.6.0'}, refreshed_data)

    @mock.patch('pg_view.models.collector_host.cpu_count', return_value=1)
    def test_refresh_should_call_cpus_count_when_ok(self, mocked_cpu_count):
        refreshed_data = self.collector._read_cpus()
        self.assertEqual({'cores': 1}, refreshed_data)

    @mock.patch('pg_view.models.collector_host.cpu_count')
    @mock.patch('pg_view.models.collector_host.logger')
    def test_refresh_should_call_cpus_count_when_not_implemented_raised(self, mocked_logging, mocked_cpu_count):
        mocked_cpu_count.side_effect = NotImplementedError
        refreshed_data = self.collector._read_cpus()
        self.assertEqual({'cores': 0}, refreshed_data)
        mocked_logging.error.assert_called_with('multiprocessing does not support cpu_count')

    def test_output_should_raise_not_support_when_unknown_method(self):
        with self.assertRaises(Exception):
            self.collector.output('unknown')

    def test_output_should_return_json_when_output_json(self):
        faked_refresh_data = {
            'sysname': 'Linux 3.13.0-100-generic',
            'uptime': '2 days, 22:04:58',
            'loadavg': '0.06 0.04 0.05',
            'hostname': 'vagrant-ubuntu-trusty-64',
            'cores': 1
        }

        self.collector._do_refresh([faked_refresh_data])
        displayer = get_displayer_by_class(OUTPUT_METHOD.json, self.collector, False, True, False)
        json_data = self.collector.output(displayer)
        expected_resp = {
            'data': [{
                'cores': 1,
                'host': 'vagrant-ubuntu-trusty-64',
                'load average': '0.06 0.04 0.05',
                'name': 'Linux 3.13.0-100-generic',
                'up': '2 days, 22:04:58'
            }],
            'type': 'host'
        }
        self.assertEqual(expected_resp, json.loads(json_data))

    def test_output_should_return_console_output_when_console(self):
        faked_refresh_data = {
            'sysname': 'Linux 3.13.0-100-generic',
            'uptime': '2 days, 22:04:58',
            'loadavg': '0.06 0.04 0.05',
            'hostname': 'vagrant-ubuntu-trusty-64',
            'cores': 1
        }
        displayer = get_displayer_by_class(OUTPUT_METHOD.console, self.collector, False, True, False)
        self.collector._do_refresh([faked_refresh_data])
        console_data = self.collector.output(displayer)
        expected_resp = [
            'Host statistics',
            'load average   up               host                     cores name                    ',
            '0.06 0.04 0.05 2 days, 22:04:58 vagrant-ubuntu-trusty-64 1     Linux 3.13.0-100-generic', '\n'
        ]
        self.assertEqual('\n'.join(expected_resp), console_data)

    def test_output_should_return_ncurses_output_when_ncurses(self):
        faked_refresh_data = {
            'sysname': 'Linux 3.13.0-100-generic',
            'uptime': '2 days, 22:04:58',
            'loadavg': '0.06 0.04 0.05',
            'hostname': 'vagrant-ubuntu-trusty-64',
            'cores': 1
        }
        displayer = get_displayer_by_class(OUTPUT_METHOD.curses, self.collector, False, True, False)
        self.collector._do_refresh([faked_refresh_data])
        console_data = self.collector.output(displayer)
        expected_resp = {
            'host': {
                'rows': [{
                    'cores': ColumnType(value='1', header='cores', header_position=2),
                    'host': ColumnType(value='vagrant-ubuntu-trusty-64', header='', header_position=None),
                    'load average': ColumnType(value='0.06 0.04 0.05', header='load average', header_position=1),
                    'name': ColumnType(value='Linux 3.13.0-100-generic', header='', header_position=None),
                    'up': ColumnType(value='2 days, 22:04:58', header='up', header_position=1)
                }],
                'hide': [],
                'noautohide': {'cores': True, 'host': True, 'load average': True, 'name': True, 'up': True},
                'prepend_column_headers': False,
                'highlights': {'cores': False, 'host': True, 'load average': False, 'name': False, 'up': False},
                'align': {'cores': 0, 'host': 0, 'load average': 0, 'name': 0, 'up': 0},
                'pos': {'cores': 2, 'host': 0, 'load average': 4, 'name': 3, 'up': 1},
                'column_header': {'cores': 2, 'host': 0, 'load average': 1, 'name': 0, 'up': 1},
                'header': False,
                'prefix': None, 'statuses': [{
                    'cores': {0: 0, -1: 0}, 'host': {0: 0, -1: 0}, 'load average': {0: 0, 1: 0, 2: 0},
                    'name': {0: 0, 1: 0, -1: 0}, 'up': {0: 0, 1: 0, 2: 0, -1: 0}
                }],
                'w': {'cores': 7, 'host': 24, 'load average': 27, 'name': 24, 'up': 19},
                'types': {'up': 0, 'cores': 1, 'host': 0, 'load average': 0, 'name': 0}
            }
        }
        self.assertEqual(expected_resp, console_data)

    def test__concat_load_avg_should_return_empty_when_less_than_three_rows(self):
        concatenated_data = self.collector._concat_load_avg('loadavg', (0.16, 0.05), False)
        self.assertEqual('', concatenated_data)

    def test__concat_load_avg_should_return_str_when_than_three_rows(self):
        concatenated_data = self.collector._concat_load_avg('loadavg', (0.16, 0.05, 0.06), False)
        self.assertEqual('0.16 0.05 0.06', concatenated_data)

    def test__construct_sysname_should_return_none_when_less_than_three_elements(self):
        sysname = self.collector._construct_sysname('', ('Linux', 'vagrant-ubuntu-trusty-64'), 'optional')
        self.assertIsNone(sysname)

    def test__construct_sysname_should_return_sysname_when_input_ok(self):
        row = (
            'Linux', 'vagrant-ubuntu-trusty-64', '3.13.0-100-generic', '#147-Ubuntu SMP Tue Oct 18 16:48:51 UTC 2016',
            'x86_64'
        )
        sysname = self.collector._construct_sysname('', row, 'optional')
        self.assertEqual('Linux 3.13.0-100-generic', sysname)
