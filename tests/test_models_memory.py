import os
from unittest import TestCase

import mock
import sys

path = os.path.dirname(os.path.dirname(__file__))
sys.path.insert(0, path)

from pg_view.models.memory_stat import MemoryStatCollector


class MemoryStatCollectorTest(TestCase):
    def setUp(self):
        self.collector = MemoryStatCollector()
        super(MemoryStatCollectorTest, self).setUp()

    def test_result_should_contain_proper_data_keys(self):
        refreshed_data = self.collector.refresh()
        self.assertIn('cached', refreshed_data)
        self.assertIn('commit_limit', refreshed_data)
        self.assertIn('free', refreshed_data)
        self.assertIn('dirty', refreshed_data)
        self.assertIn('commit_left', refreshed_data)
        self.assertIn('total', refreshed_data)
        self.assertIn('buffers', refreshed_data)
        self.assertIn('committed_as', refreshed_data)
