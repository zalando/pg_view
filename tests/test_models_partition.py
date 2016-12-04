import unittest
from collections import namedtuple
from multiprocessing import JoinableQueue
from unittest import TestCase

import mock
import os
import psutil

from pg_view.models.collector_partition import PartitionStatCollector
from pg_view.models.consumers import DiskCollectorConsumer
from tests.common import TEST_DIR

sdiskio = namedtuple(
    'sdiskio', ['read_count', 'write_count', 'read_bytes', 'write_bytes', 'read_time', 'write_time', 'busy_time']
)


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

    def test__dereference_dev_name_should_return_input_when_not_dev(self):
        self.assertEqual('/abc', self.collector._dereference_dev_name('/abc'))

    def test__dereference_dev_name_should_return_none_when_devname_false(self):
        self.assertIsNone(self.collector._dereference_dev_name(''))

    def test__dereference_dev_name_should_replace_dev_when_dev(self):
        dev_name = self.collector._dereference_dev_name('/dev/sda1')
        self.assertEqual('sda1', dev_name)

    @mock.patch('pg_view.models.collector_partition.PartitionStatCollector.get_missing_io_stat_from_file')
    @mock.patch('pg_view.models.collector_partition.psutil.disk_io_counters')
    def test_get_io_data_should_return_data_when_disk_in_pnames(self, mocked_disk_io_counters, mocked_get_missing_io_stat_from_file):
        mocked_get_missing_io_stat_from_file.return_value = {}
        mocked_disk_io_counters.return_value = {
            'sda1': sdiskio(read_count=10712, write_count=4011, read_bytes=157438976,
                            write_bytes=99520512, read_time=6560, write_time=3768, busy_time=5924
                            )
        }
        io_data = self.collector.get_io_data('sda1')
        expected_data = {
            'sda1': {
                'await': 0,
                'sectors_read': 307498,
                'sectors_written': 194376
            }
        }

        self.assertEqual(expected_data, io_data)

    @mock.patch('pg_view.models.collector_partition.psutil.disk_io_counters')
    def test_get_io_data_should_return_empty_when_disk_not_in_pnames(self, mocked_disk_io_counters):
        mocked_disk_io_counters.return_value = {
            'sda1': sdiskio(read_count=10712, write_count=4011, read_bytes=157438976,
                            write_bytes=99520512, read_time=6560, write_time=3768, busy_time=5924
                            )
        }
        io_data = self.collector.get_io_data('sda2')
        self.assertEqual({}, io_data)

    def test_get_name_from_fields_should_return_ok_when_linux_24(self):
        fields = [
            '8', '1', 'sda1', '11523', '7', '383474', '7304', '24134', '24124', '528624', '6276', '0', '5916', '13452']
        name = self.collector.get_name_from_fields(fields)
        self.assertEqual('sda1', name)

    def test_get_name_from_fields_should_return_ok_when_linux_26(self):
        fields = [
            '0', '8', '1', 'sda2', '11523', '7', '383474', '7304', '24134', '24124', '528624', '6276', '0', '5916', '13452']
        name = self.collector.get_name_from_fields(fields)
        self.assertEqual('sda2', name)

    @unittest.skipUnless(psutil.LINUX, "Linux only")
    @mock.patch('pg_view.models.collector_partition.psutil.disk_io_counters')
    @mock.patch('pg_view.models.collector_partition.PartitionStatCollector.get_missing_io_stat_from_file')
    def test_get_io_data_should_parse_data_from_proc_meminfo_when_linux(self, mocked_get_missing_io_stat_from_file,
                                                                        mocked_io_counters):
        mocked_get_missing_io_stat_from_file.return_value = {'sda1': {'await': 100022}}
        io_counters = {
            'sda1': sdiskio(read_count=11523, write_count=24279, read_bytes=196338688, write_bytes=271822848,
                            read_time=7304, write_time=6284, busy_time=5924)
        }
        mocked_io_counters.return_value = io_counters
        expected_data = {
            'sda1': {'await': 100022, 'sectors_read': 383474, 'sectors_written': 530904}
        }
        refreshed_data = self.collector.get_io_data(['sda1'])
        self.assertEqual(expected_data, refreshed_data)
        mocked_get_missing_io_stat_from_file.assert_called_with(['sda1'])

    @unittest.skipUnless(psutil.LINUX, "Linux only")
    @mock.patch('pg_view.models.collector_system.psutil._pslinux.open_text')
    def test_get_missing_io_stat_from_file_should_return_empty_when_no_data_for_name(self, mocked_open_text):
        cpu_info_ok = os.path.join(TEST_DIR, 'proc_files', 'diskstat_24')
        mocked_open_text.return_value = open(cpu_info_ok, "rt")
        refreshed_data = self.collector.get_missing_io_stat_from_file(['sda2'])
        expected_data = {}
        self.assertEqual(expected_data, refreshed_data)

    @unittest.skipUnless(psutil.LINUX, "Linux only")
    @mock.patch('pg_view.models.collector_system.psutil._pslinux.open_text')
    def test_get_missing_io_stat_from_file_should_return_stats_when_data_for_names_24(self, mocked_open_text):
        cpu_info_ok = os.path.join(TEST_DIR, 'proc_files', 'diskstat_24')
        mocked_open_text.return_value = open(cpu_info_ok, "rt")
        refreshed_data = self.collector.get_missing_io_stat_from_file(['sda', 'sda1'])
        expected_data = {'sda': {'await': 13524}, 'sda1': {'await': 13476}}
        self.assertEqual(expected_data, refreshed_data)

    @unittest.skipUnless(psutil.LINUX, "Linux only")
    @mock.patch('pg_view.models.collector_system.psutil._pslinux.open_text')
    def test_get_missing_io_stat_from_file_should_return_stats_when_data_for_names_26(self, mocked_open_text):
        cpu_info_ok = os.path.join(TEST_DIR, 'proc_files', 'diskstat_26')
        mocked_open_text.return_value = open(cpu_info_ok, "rt")
        refreshed_data = self.collector.get_missing_io_stat_from_file(['sda', 'sda1'])
        expected_data = {'sda': {'await': 135241}, 'sda1': {'await': 135241}}
        self.assertEqual(expected_data, refreshed_data)
