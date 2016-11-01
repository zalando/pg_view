import os
from unittest import TestCase

import mock
import sys

path = os.path.dirname(os.path.dirname(__file__))
sys.path.insert(0, path)

from pg_view.models.pg_stat import PgStatCollector


class PgstatCollectorTest(TestCase):
    def setUp(self):

        pg_con = mock.Mock()
        pg_con.fetch_one.return_value = 1
        self.collector = PgStatCollector(
            pgcon=pg_con,
            reconnect='',
            pid=1042,
            dbname='/var/lib/postgresql/9.3/main',
            dbver=9.3,
            always_track_pids=[]
        )
        super(PgstatCollectorTest, self).setUp()

    def test_result_should_contain_proper_data_keys(self):
        refreshed_data = self.collector.refresh()
        self.assertIsInstance(refreshed_data, list)
        first_pg_process_data = refreshed_data[0]
        self.assertIn('write_bytes', first_pg_process_data)
        self.assertIn('vsize', first_pg_process_data)
        self.assertIn('delayacct_blkio_ticks', first_pg_process_data)
        self.assertIn('state', first_pg_process_data)
        self.assertIn('priority', first_pg_process_data)
        self.assertIn('cmdline', first_pg_process_data)
        self.assertIn('read_bytes', first_pg_process_data)
        self.assertIn('uss', first_pg_process_data)
        self.assertIn('stime', first_pg_process_data)
        self.assertIn('starttime', first_pg_process_data)
        self.assertIn('rss', first_pg_process_data)
        self.assertIn('type', first_pg_process_data)
        self.assertIn('checkpointer', first_pg_process_data)
        self.assertIn('guest_time', first_pg_process_data)
        self.assertIn('utime', first_pg_process_data)
