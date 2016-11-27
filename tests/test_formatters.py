from unittest import TestCase

import mock

from pg_view.models.formatters import StatusFormatter
from pg_view.models.pg_stat import PgStatCollector


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

    def test_idle_format_fn_should_return_text_when_no_matches(self):
        collector = PgStatCollector.from_cluster(self.cluster, 1049)
        formatter = StatusFormatter(collector)
        formatted_idle = formatter.idle_format_fn('return text')
        self.assertEqual('return text', formatted_idle)

    def test_idle_format_fn_should_return_formatted_for_version_bigger_than_92(self):
        collector = PgStatCollector.from_cluster(self.cluster, 1049)
        formatter = StatusFormatter(collector)
        formatted_idle = formatter.idle_format_fn('idle in transaction 1')
        self.assertEqual('idle in transaction for 00:01', formatted_idle)

    def test_idle_format_fn_should_return_formatted_for_version_less_than_92(self):
        self.cluster['ver'] = 9.1
        collector = PgStatCollector.from_cluster(self.cluster, 1049)
        formatter = StatusFormatter(collector)
        formatted_idle = formatter.idle_format_fn('idle in transaction 1')
        self.assertEqual('idle in transaction 00:01 since the last query start', formatted_idle)

    # def test_query_status_fn_should_return_critical_when_
