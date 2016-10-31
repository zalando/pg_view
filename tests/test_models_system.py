import os
from unittest import TestCase

import mock
import sys

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

