from datetime import timedelta
from unittest import TestCase

import mock

from pg_view.collectors.host_collector import HostStatCollector
from pg_view.collectors.pg_collector import PgStatCollector
from pg_view.models.formatters import StatusFormatter, FnFormatter


class StatusFormatterTest(TestCase):
    def setUp(self):
        super(StatusFormatterTest, self).setUp()
        self.cluster = {
            'ver': 9.3,
            'name': '/var/lib/postgresql/9.3/main',
            'pid': 1049,
            'reconnect': mock.Mock(),
            'pgcon': mock.MagicMock(),
        }

    def test_load_avg_state_should_return_empty_when_no_load_avg(self):
        collector = HostStatCollector()
        formatter = StatusFormatter(collector)
        row = ['', '2 days, 15:33:30', 'ubuntu-trusty-64', 1, 'Linux 3.13.0-100-generic']
        col = {'warning': 5, 'critical': 20, 'out': 'load average'}
        self.assertEqual({}, formatter.load_avg_state(row, col))

    def test_load_avg_state_should_return_every_state_when_warning_critical_ok(self):
        collector = HostStatCollector()
        formatter = StatusFormatter(collector)
        row = ['0.0 5.01 20.05', '2 days, 15:33:30', 'ubuntu-trusty-64', 1, 'Linux 3.13.0-100-generic']
        col = {'warning': 5, 'critical': 20, 'out': 'load average'}
        self.assertEqual({0: 0, 1: 1, 2: 2}, formatter.load_avg_state(row, col))

    def test_age_status_fn_should_return_critical_when_age_bigger_than_critical(self):
        collector = PgStatCollector.from_cluster(self.cluster, 1049)
        formatter = StatusFormatter(collector)
        row = [
            11139, None, 'backend', None, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 2.9, '20:52:05',
            'postgres', 'postgres', False, 'idle in transaction for 20:51:17'
        ]
        col = {'warning': 300, 'critical': 500, 'out': 'age'}
        self.assertEqual({-1: 2}, formatter.age_status_fn(row, col))

    def test_age_status_fn_should_return_warning_when_age_bigger_than_warning(self):
        collector = PgStatCollector.from_cluster(self.cluster, 1049)
        formatter = StatusFormatter(collector)
        row = [
            11139, None, 'backend', None, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 2.9, '0:06:05',
            'postgres', 'postgres', False, 'idle in transaction for 20:51:17'
        ]
        col = {'warning': 300, 'critical': 500, 'out': 'age'}
        self.assertEqual({-1: 1}, formatter.age_status_fn(row, col))

    def test_age_status_fn_should_return_ok_when_age_less_than_warning(self):
        collector = PgStatCollector.from_cluster(self.cluster, 1049)
        formatter = StatusFormatter(collector)
        row = [
            11139, None, 'backend', None, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 2.9, '0:04:05',
            'postgres', 'postgres', False, 'idle in transaction for 20:51:17'
        ]
        col = {'warning': 300, 'critical': 500, 'out': 'age'}
        self.assertEqual({-1: 0}, formatter.age_status_fn(row, col))

    def test_query_status_fn_should_return_critical_when_waiting(self):
        collector = PgStatCollector.from_cluster(self.cluster, 1049)
        formatter = StatusFormatter(collector)
        row = [
            11139, None, 'backend', None, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 2.9, '21:05:45', 'postgres',
            'postgres', True, 'idle in transaction for 21:04:57'
        ]
        col = {'warning': 'idle in transaction', 'critical': 'locked', 'out': 'query'}
        self.assertEqual({-1: 2}, formatter.query_status_fn(row, col))

    def test_query_status_fn_should_return_warning_when_idle_in_transaction(self):
        collector = PgStatCollector.from_cluster(self.cluster, 1049)
        formatter = StatusFormatter(collector)
        row = [
            11139, None, 'backend', None, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 2.9, '21:05:45', 'postgres',
            'postgres', False, 'idle in transaction '
        ]
        col = {'warning': 'idle in transaction', 'critical': 'locked', 'out': 'query'}
        self.assertEqual({-1: 1}, formatter.query_status_fn(row, col))

    def test_query_status_fn_should_return_warning_when_default_warning(self):
        collector = PgStatCollector.from_cluster(self.cluster, 1049)
        formatter = StatusFormatter(collector)
        row = [
            11139, None, 'backend', None, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 2.9, '21:05:45', 'postgres',
            'postgres', False, '! '
        ]
        col = {'critical': 'locked', 'out': 'query'}
        self.assertEqual({-1: 1}, formatter.query_status_fn(row, col))

    def test_query_status_fn_should_return_ok_when_no_warning(self):
        collector = PgStatCollector.from_cluster(self.cluster, 1049)
        formatter = StatusFormatter(collector)
        row = [
            11139, None, 'backend', None, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 2.9, '21:05:45', 'postgres',
            'postgres', False, 'ok'
        ]
        col = {'warning': 'idle in transaction', 'critical': 'locked', 'out': 'query'}
        self.assertEqual({-1: 0}, formatter.query_status_fn(row, col))


class FnFormatterTest(TestCase):
    def setUp(self):
        super(FnFormatterTest, self).setUp()
        self.cluster = {
            'ver': 9.3,
            'name': '/var/lib/postgresql/9.3/main',
            'pid': 1049,
            'reconnect': mock.Mock(),
            'pgcon': mock.MagicMock(),
        }

    def test_idle_format_fn_should_return_text_when_no_matches(self):
        collector = PgStatCollector.from_cluster(self.cluster, 1049)
        formatter = FnFormatter(collector)
        formatted_idle = formatter.idle_format_fn('return text')
        self.assertEqual('return text', formatted_idle)

    def test_idle_format_fn_should_return_formatted_for_version_bigger_than_92(self):
        collector = PgStatCollector.from_cluster(self.cluster, 1049)
        formatter = FnFormatter(collector)
        formatted_idle = formatter.idle_format_fn('idle in transaction 1')
        self.assertEqual('idle in transaction for 00:01', formatted_idle)

    def test_idle_format_fn_should_return_formatted_for_version_less_than_92(self):
        self.cluster['ver'] = 9.1
        collector = PgStatCollector.from_cluster(self.cluster, 1049)
        formatter = FnFormatter(collector)
        formatted_idle = formatter.idle_format_fn('idle in transaction 1')
        self.assertEqual('idle in transaction 00:01 since the last query start', formatted_idle)

    def test_kb_pretty_print_should_return_formatted_when_mb(self):
        collector = PgStatCollector.from_cluster(self.cluster, 1049)
        formatter = FnFormatter(collector)
        formatted_kb = formatter.kb_pretty_print(501708)
        self.assertEqual('489.9MB', formatted_kb)

    def test_kb_pretty_print_should_return_formatted_when_kb(self):
        collector = PgStatCollector.from_cluster(self.cluster, 1049)
        formatter = FnFormatter(collector)
        formatted_kb = formatter.kb_pretty_print(1024)
        self.assertEqual('1024KB', formatted_kb)

    def test_time_interval_pretty_print_should_return_formatted_when_start_time_number(self):
        collector = PgStatCollector.from_cluster(self.cluster, 1049)
        formatter = FnFormatter(collector)
        formatted_time = formatter.time_pretty_print(68852.0)
        self.assertEqual('19:07:32', formatted_time)

    def test_time_interval_pretty_print_should_return_formatted_when_start_time_timedelta(self):
        collector = PgStatCollector.from_cluster(self.cluster, 1049)
        formatter = FnFormatter(collector)
        formatted_time = formatter.time_pretty_print(timedelta(seconds=30))
        self.assertEqual('00:30', formatted_time)

    def test_time_interval_pretty_print_should_raise_error_when_non_valid_type(self):
        collector = PgStatCollector.from_cluster(self.cluster, 1049)
        formatter = FnFormatter(collector)
        with self.assertRaises(ValueError):
            formatter.time_pretty_print('None')
