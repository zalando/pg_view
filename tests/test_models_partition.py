import os
from unittest import TestCase

import mock
import sys

from multiprocessing import JoinableQueue

from pg_view.consumers import DiskCollectorConsumer

path = os.path.dirname(os.path.dirname(__file__))
sys.path.insert(0, path)

from pg_view.models.partition_stat import PartitionStatCollector


class PartitionStatCollectorTest(TestCase):
    def setUp(self):
        self.collector = PartitionStatCollector(
            dbname='/var/lib/postgresql/9.3/main',
            dbversion=9.3,
            work_directory='/var/lib/postgresql/9.3/main',
            consumer=DiskCollectorConsumer(JoinableQueue(1))
        )
        super(PartitionStatCollectorTest, self).setUp()

    def _assert_data_has_proper_structure(self, data):
        self.assertIn('type', data)
        self.assertIn('path_size', data)
        self.assertIn('dev', data)
        self.assertIn('space_total', data)
        self.assertIn('path', data)
        self.assertIn('space_left', data)

    def test_result_should_contain_proper_data_keys(self):
        refreshed_data = self.collector.refresh()
        self.assertIsInstance(refreshed_data, list)
        self.assertEqual(2, len(refreshed_data))
        data_type = refreshed_data[0]
        self._assert_data_has_proper_structure(data_type)
        xlog_type = refreshed_data[1]
        self._assert_data_has_proper_structure(xlog_type)


