import os
import unittest
from collections import namedtuple
from unittest import TestCase

import mock

from pg_view.collectors.memory_collector import MemoryStatCollector
from pg_view.utils import open_universal, KB_IN_MB
from tests.common import TEST_DIR


class MemoryStatCollectorTest(TestCase):
    def setUp(self):
        self.collector = MemoryStatCollector()
        super(MemoryStatCollectorTest, self).setUp()

    @unittest.skip('psutil')
    def test_refresh_should_contain_proper_data_keys(self):
        refreshed_data = self.collector.refresh()
        self.assertIn('cached', refreshed_data)
        self.assertIn('commit_limit', refreshed_data)
        self.assertIn('free', refreshed_data)
        self.assertIn('dirty', refreshed_data)
        self.assertIn('commit_left', refreshed_data)
        self.assertIn('total', refreshed_data)
        self.assertIn('buffers', refreshed_data)
        self.assertIn('committed_as', refreshed_data)

    @unittest.skip('psutil')
    @mock.patch('pg_view.models.collector_system.psutil.virtual_memory')
    @mock.patch('pg_view.models.collector_memory.psutil.LINUX', False)
    def test_read_memory_data_should_return_data_when_cpu_virtual_memory_for_macos(self, mocked_virtual_memory):
        linux_svmem = namedtuple('linux_svmem', 'total free buffers cached')
        mocked_virtual_memory.return_value = linux_svmem(
            total=2048 * 1024, free=1024 * 1024, buffers=3072 * 1024, cached=4096 * 1024
        )
        refreshed_cpu = self.collector.read_memory_data()
        expected_data = {
            'MemTotal': 2048,
            'MemFree': 1024,
            'Buffers': 3072,
            'Cached': 4096,
            'Dirty': 0,
            'CommitLimit': 0,
            'Committed_AS': 0,
        }
        self.assertEqual(expected_data, refreshed_cpu)

    @unittest.skip('psutil')
    # @unittest.skipUnless(psutil.LINUX, "Linux only")
    @mock.patch('pg_view.models.collector_memory.psutil._pslinux.open_binary')
    @mock.patch('pg_view.models.collector_memory.psutil.virtual_memory')
    def test__read_memory_data_should_parse_data_from_proc_meminfo_when_linux(self, mocked_virtual_memory,
                                                                              mocked_open_binary):
        meminfo_ok_path = os.path.join(TEST_DIR, 'proc_files', 'meminfo_ok')
        linux_svmem = namedtuple('linux_svmem', 'total free buffers cached')
        mocked_open_binary.return_value = open_universal(meminfo_ok_path)
        mocked_virtual_memory.return_value = linux_svmem(
            total=2048 * 1024, free=1024 * 1024, buffers=3072 * 1024, cached=4096 * 1024
        )
        expected_data = {
            'MemTotal': 2048,
            'Cached': 4096,
            'MemFree': 1024,
            'Buffers': 3072,
            'CommitLimit': 250852,
            'Dirty': 36,
            'Committed_AS': 329264
        }
        refreshed_data = self.collector.read_memory_data()
        self.assertEqual(expected_data, refreshed_data)

    def test__is_commit_should_return_false_when_both_none(self):
        self.assertFalse(self.collector._is_commit({}))

    def test__is_commit_should_return_false_when_commit_limit_none(self):
        self.assertFalse(self.collector._is_commit({'Commited_AS': 10}))

    def test__is_commit_should_return_false_when_commited_as_none(self):
        self.assertFalse(self.collector._is_commit({'CommitLimit': 10}))

    def test__is_commit_should_return_true_when_both_exist(self):
        self.assertFalse(self.collector._is_commit({'CommitLimit': 10, 'Commited_AS': 20}))

    def test__calculate_kb_left_until_limit_should_return_result(self):
        data = self.collector.calculate_kb_left_until_limit(
            'commit_left', {'CommitLimit': 30, 'Committed_AS': 20}, True)
        self.assertEqual(10, data)

    @mock.patch('pg_view.collectors.base_collector.logger')
    def test__calculate_kb_left_until_limit_should_log_warn_when_non_optional_and_not_commit(self, mocked_logger):
        data = self.collector.calculate_kb_left_until_limit('commit_left', {}, False)
        self.assertIsNone(data)
        mocked_logger.error.assert_called_with('Column commit_left is not optional, but input row has no value for it')

    @unittest.skip('psutil')
    # @unittest.skipUnless(psutil.LINUX, "Linux only")
    @mock.patch('pg_view.models.collector_system.psutil._pslinux.open_binary')
    def test_get_missing_memory_stat_from_file_should_parse_data_from_proc_stat(self, mocked_open):
        cpu_info_ok = os.path.join(TEST_DIR, 'proc_files', 'meminfo_ok')
        mocked_open.return_value = open_universal(cpu_info_ok)
        refreshed_data = self.collector.get_missing_memory_stat_from_file()
        expected_data = {
            'CommitLimit:': 250852 * KB_IN_MB, 'Committed_AS:': 329264 * KB_IN_MB, 'Dirty:': 36 * KB_IN_MB}
        self.assertEqual(expected_data, refreshed_data)
