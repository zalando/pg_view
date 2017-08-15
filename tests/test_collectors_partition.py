import os
import posix
import unittest
from collections import namedtuple
from multiprocessing import JoinableQueue
from unittest import TestCase

import mock
# import psutil

from common import TEST_DIR, ErrorAfter, CallableExhaustedError
from pg_view.collectors.partition_collector import PartitionStatCollector, DetachedDiskStatCollector, \
    DiskCollectorConsumer

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

    def test_refresh_should_contain_proper_data_keys(self):
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

    @unittest.skip('psutil')
    @mock.patch('pg_view.collectors.partition_collector.PartitionStatCollector.get_missing_io_stat_from_file')
    @mock.patch('pg_view.collectors.partition_collector.psutil.disk_io_counters')
    def test_get_io_data_should_return_data_when_disk_in_pnames(self, mocked_disk_io_counters,
                                                                mocked_get_missing_io_stat_from_file):
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

    @unittest.skip('psutil')
    @mock.patch('pg_view.collectors.partition_collector.psutil.disk_io_counters')
    def test_get_io_data_should_return_empty_when_disk_not_in_pnames(self, mocked_disk_io_counters):
        mocked_disk_io_counters.return_value = {
            'sda1': sdiskio(read_count=10712, write_count=4011, read_bytes=157438976,
                            write_bytes=99520512, read_time=6560, write_time=3768, busy_time=5924
                            )
        }
        io_data = self.collector.get_io_data('sda2')
        self.assertEqual({}, io_data)

    @unittest.skip('psutil')
    def test_get_name_from_fields_should_return_ok_when_linux_24(self):
        fields = [
            '8', '1', 'sda1', '11523', '7', '383474', '7304', '24134', '24124', '528624', '6276', '0', '5916', '13452']
        name = self.collector.get_name_from_fields(fields)
        self.assertEqual('sda1', name)

    @unittest.skip('psutil')
    def test_get_name_from_fields_should_return_ok_when_linux_26(self):
        fields = [
            '0', '8', '1', 'sda2', '11523', '7', '383474', '7304', '24134', '24124', '528624', '6276', '0', '5916',
            '13452'
        ]
        name = self.collector.get_name_from_fields(fields)
        self.assertEqual('sda2', name)

    @unittest.skip('psutil')
    # @unittest.skipUnless(psutil.LINUX, "Linux only")
    @mock.patch('pg_view.collectors.partition_collector.psutil.disk_io_counters')
    @mock.patch('pg_view.collectors.partition_collector.PartitionStatCollector.get_missing_io_stat_from_file')
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

    @unittest.skip('psutil')
    # @unittest.skipUnless(psutil.LINUX, "Linux only")
    @mock.patch('pg_view.collectors.system_collector.psutil._pslinux.open_text')
    def test_get_missing_io_stat_from_file_should_return_empty_when_no_data_for_name(self, mocked_open_text):
        cpu_info_ok = os.path.join(TEST_DIR, 'proc_files', 'diskstat_24')
        mocked_open_text.return_value = open(cpu_info_ok, "rt")
        refreshed_data = self.collector.get_missing_io_stat_from_file(['sda2'])
        expected_data = {}
        self.assertEqual(expected_data, refreshed_data)

    @unittest.skip('psutil')
    # @unittest.skipUnless(psutil.LINUX, "Linux only")
    @mock.patch('pg_view.collectors.system_collector.psutil._pslinux.open_text')
    def test_get_missing_io_stat_from_file_should_return_stats_when_data_for_names_24(self, mocked_open_text):
        cpu_info_ok = os.path.join(TEST_DIR, 'proc_files', 'diskstat_24')
        mocked_open_text.return_value = open(cpu_info_ok, "rt")
        refreshed_data = self.collector.get_missing_io_stat_from_file(['sda', 'sda1'])
        expected_data = {'sda': {'await': 13524}, 'sda1': {'await': 13476}}
        self.assertEqual(expected_data, refreshed_data)

    @unittest.skip('psutil')
    # @unittest.skipUnless(psutil.LINUX, "Linux only")
    @mock.patch('pg_view.collectors.system_collector.psutil._pslinux.open_text')
    def test_get_missing_io_stat_from_file_should_return_stats_when_data_for_names_26(self, mocked_open_text):
        cpu_info_ok = os.path.join(TEST_DIR, 'proc_files', 'diskstat_26')
        mocked_open_text.return_value = open(cpu_info_ok, "rt")
        refreshed_data = self.collector.get_missing_io_stat_from_file(['sda', 'sda1'])
        expected_data = {'sda': {'await': 135241}, 'sda1': {'await': 135241}}
        self.assertEqual(expected_data, refreshed_data)


class DetachedDiskStatCollectorTest(TestCase):
    @mock.patch('pg_view.collectors.partition_collector.DetachedDiskStatCollector.run_du')
    def test_get_du_data_should_run_du_when_work_directory_and_db_version_less_than_10(self, mocked_run_du):
        mocked_run_du.side_effect = [35628, 35620]
        detached_disk = DetachedDiskStatCollector(mock.Mock(), ['/var/lib/postgresql/9.3/main'], 9.3)
        result = detached_disk.get_du_data('/var/lib/postgresql/9.3/main')
        expected_result = {
            'xlog': ('35620', '/var/lib/postgresql/9.3/main/pg_xlog/'),
            'data': ('35628', '/var/lib/postgresql/9.3/main')
        }
        self.assertEqual(expected_result, result)
        mocked_run_du.assert_has_calls([
            mock.call('/var/lib/postgresql/9.3/main'),
            mock.call('/var/lib/postgresql/9.3/main/pg_xlog/')
        ])

    @mock.patch('pg_view.collectors.partition_collector.DetachedDiskStatCollector.run_du')
    def test_get_du_data_should_run_du_when_work_directory_and_db_version_bigger_than_10(self, mocked_run_du):
        mocked_run_du.side_effect = [35628, 35620]
        detached_disk = DetachedDiskStatCollector(mock.Mock(), ['/var/lib/postgresql/9.3/main'], 10.3)
        result = detached_disk.get_du_data('/var/lib/postgresql/10.3/main')
        expected_result = {
            'xlog': ('35620', '/var/lib/postgresql/10.3/main/pg_wal/'),
            'data': ('35628', '/var/lib/postgresql/10.3/main')
        }
        self.assertEqual(expected_result, result)
        mocked_run_du.assert_has_calls([
            mock.call('/var/lib/postgresql/10.3/main'),
            mock.call('/var/lib/postgresql/10.3/main/pg_wal/')
        ])

    @mock.patch('pg_view.collectors.partition_collector.DetachedDiskStatCollector.run_du')
    @mock.patch('pg_view.collectors.partition_collector.logger')
    def test_get_du_data_should_log_error_when_run_du_raises_exception(self, mocked_logger, mocked_run_du):
        mocked_run_du.side_effect = Exception
        detached_disk = DetachedDiskStatCollector(mock.Mock(), ['/var/lib/postgresql/9.3/main'], 9.3)
        detached_disk.get_du_data('/var/lib/postgresql/9.3/main')
        expected_msg = 'Unable to read free space information for the pg_xlog and data directories for the directory ' \
                       '/var/lib/postgresql/9.3/main: '
        mocked_logger.error.assert_called_with(expected_msg)

    @mock.patch('pg_view.collectors.partition_collector.DetachedDiskStatCollector.get_du_data')
    @mock.patch('pg_view.collectors.partition_collector.DetachedDiskStatCollector.get_df_data')
    def test_run_should_loop_forever_processing_both_collectors(self, mocked_get_df_data, mocked_get_du_data):
        mocked_get_du_data.side_effect = ErrorAfter(1)
        mocked_get_df_data.side_effect = ErrorAfter(1)
        queue = mock.Mock()
        detached_disk = DetachedDiskStatCollector(queue, ['/var/lib/postgresql/9.3/main'], 9.3)
        with self.assertRaises(CallableExhaustedError):
            detached_disk.run()

        mocked_get_du_data.assert_called_with('/var/lib/postgresql/9.3/main')
        mocked_get_df_data.assert_called_with('/var/lib/postgresql/9.3/main')
        queue.put.assert_called_once_with(
            {'/var/lib/postgresql/9.3/main': [('/var/lib/postgresql/9.3/main',), ('/var/lib/postgresql/9.3/main',)]})

    @mock.patch('pg_view.collectors.partition_collector.DetachedDiskStatCollector.get_mounted_device',
                return_value='/dev/sda1')
    @mock.patch('pg_view.collectors.partition_collector.DetachedDiskStatCollector.get_mount_point', return_value='/')
    @mock.patch('pg_view.collectors.partition_collector.os.statvfs')
    def test_get_df_data_should_return_proper_data_when_data_dev_and_xlog_dev_are_equal(self, mocked_os_statvfs,
                                                                                        mocked_get_mounted_device,
                                                                                        mocked_get_mount_point):
        seq = (4096, 4096, 10312784, 9823692, 9389714, 2621440, 2537942, 2537942, 4096, 255)
        mocked_os_statvfs.return_value = posix.statvfs_result(sequence=seq)
        detached_disk = DetachedDiskStatCollector(mock.Mock(), ['/var/lib/postgresql/9.3/main'], 9.3)
        df_data = detached_disk.get_df_data('/var/lib/postgresql/9.3/main')
        expected_df_data = {'data': ('/dev/sda1', 41251136, 37558856), 'xlog': ('/dev/sda1', 41251136, 37558856)}
        self.assertEqual(expected_df_data, df_data)

    @mock.patch('pg_view.collectors.partition_collector.DetachedDiskStatCollector.get_mounted_device',
                side_effect=['/dev/sda1', '/dev/sda2'])
    @mock.patch('pg_view.collectors.partition_collector.DetachedDiskStatCollector.get_mount_point', return_value='/')
    @mock.patch('pg_view.collectors.partition_collector.os.statvfs')
    def test_get_df_data_should_return_proper_data_when_data_dev_and_xlog_dev_are_different(self, mocked_os_statvfs,
                                                                                            mocked_get_mounted_device,
                                                                                            mocked_get_mount_point):
        mocked_os_statvfs.side_effect = [
            posix.statvfs_result(
                sequence=(4096, 4096, 10312784, 9823692, 9389714, 2621440, 2537942, 2537942, 4096, 255)),
            posix.statvfs_result(sequence=(1024, 1024, 103127, 9823, 9389, 2621, 2537, 2537, 1024, 255))
        ]
        detached_disk = DetachedDiskStatCollector(mock.Mock(), ['/var/lib/postgresql/9.3/main'], 9.3)
        df_data = detached_disk.get_df_data('/var/lib/postgresql/9.3/main')
        expected_df_data = {'data': ('/dev/sda1', 41251136, 37558856), 'xlog': ('/dev/sda2', 103127, 9389)}
        self.assertEqual(expected_df_data, df_data)

    @mock.patch('pg_view.collectors.partition_collector.os.statvfs', return_value=(4096, 4096))
    def test__get_or_update_df_cache_should_call_os_statvfs_when_empty_cache(self, mocked_os_statvfs):
        detached_disk = DetachedDiskStatCollector(mock.Mock(), ['/var/lib/postgresql/9.3/main'], 9.3)
        df_data = detached_disk._get_or_update_df_cache('/var/lib/postgresql/9.3/main', '/sda/dev1')
        self.assertEqual((4096, 4096,), df_data)
        self.assertEqual((4096, 4096,), detached_disk.df_cache['/sda/dev1'])
        mocked_os_statvfs.assert_called_once_with('/var/lib/postgresql/9.3/main')

    @mock.patch('pg_view.collectors.partition_collector.os.statvfs')
    def test__get_or_update_df_cache_should_get_from_cache_when_entry_exists(self, mocked_os_statvfs):
        detached_disk = DetachedDiskStatCollector(mock.Mock(), ['/var/lib/postgresql/9.3/main'], 9.3)
        detached_disk.df_cache = {'/sda/dev1': (4096, 4096,)}
        df_data = detached_disk._get_or_update_df_cache('/var/lib/postgresql/9.3/main', '/sda/dev1')
        self.assertEqual((4096, 4096,), df_data)
        mocked_os_statvfs.assert_not_called()

    @unittest.skip('psutil')
    @mock.patch('pg_view.collectors.partition_collector.psutil.disk_partitions', return_value=[])
    def test_get_mounted_device_should_return_none_when_no_device_on_pathname(self, mocked_disk_partitions):
        detached_disk = DetachedDiskStatCollector(mock.Mock(), ['/var/lib/postgresql/9.3/main'], 9.3)
        mounted_device = detached_disk.get_mounted_device('/test')
        self.assertIsNone(mounted_device)

    @unittest.skip('psutil')
    @mock.patch('pg_view.collectors.partition_collector.psutil.disk_partitions')
    def test_get_mounted_device_should_return_dev_when_device_on_pathname(self, mocked_disk_partitions):
        device = mock.Mock(mountpoint='/', device='sda1')
        mocked_disk_partitions.return_value = [device]
        detached_disk = DetachedDiskStatCollector(mock.Mock(), ['/var/lib/postgresql/9.3/main'], 9.3)
        mounted_device = detached_disk.get_mounted_device('/')
        self.assertEqual('sda1', mounted_device)


class DiskCollectorConsumerTest(TestCase):
    def test_consume_should_not_get_new_data_from_queue_when_old_not_consumed(self):
        queue = mock.Mock()
        first_data = {'/var/lib/postgresql/9.3/main': [{
            'xlog': ('16388', '/var/lib/postgresql/9.3/main/pg_xlog'),
            'data': ('35620', '/var/lib/postgresql/9.3/main')
        }, {
            'xlog': ('/dev/sda1', 41251136, 37716376),
            'data': ('/dev/sda1', 41251136, 37716376)}
        ]}

        queue.get_nowait.return_value = first_data

        consumer = DiskCollectorConsumer(queue)
        consumer.consume()
        self.assertEqual(first_data, consumer.result)
        self.assertEqual(first_data, consumer.cached_result)

        self.assertIsNone(consumer.consume())
        self.assertEqual(first_data, consumer.result)
        self.assertEqual(first_data, consumer.cached_result)

    def test_consume_should_consume_new_data_when_old_fetched(self):
        queue = mock.Mock()
        first_data = {'/var/lib/postgresql/9.3/main': [{
            'xlog': ('16388', '/var/lib/postgresql/9.3/main/pg_xlog'),
            'data': ('35620', '/var/lib/postgresql/9.3/main')
        }, {
            'xlog': ('/dev/sda1', 41251136, 37716376),
            'data': ('/dev/sda1', 41251136, 37716376)}
        ]}

        second_data = {'/var/lib/postgresql/9.3/main': [{
            'xlog': ('16389', '/var/lib/postgresql/9.3/main/pg_xlog'),
            'data': ('35621', '/var/lib/postgresql/9.3/main')
        }, {
            'xlog': ('/dev/sda1', 41251137, 37716377),
            'data': ('/dev/sda1', 41251137, 37716377)}
        ]}

        queue.get_nowait.side_effect = [first_data.copy(), second_data.copy()]

        consumer = DiskCollectorConsumer(queue)
        consumer.consume()
        self.assertEqual(first_data, consumer.result)
        self.assertEqual(first_data, consumer.cached_result)

        self.assertEqual(first_data['/var/lib/postgresql/9.3/main'], consumer.fetch('/var/lib/postgresql/9.3/main'))
        self.assertEqual({}, consumer.result)
        self.assertEqual(first_data, consumer.cached_result)

        consumer.consume()
        self.assertEqual(second_data, consumer.result)
        self.assertEqual(second_data, consumer.cached_result)

        self.assertEqual(second_data['/var/lib/postgresql/9.3/main'], consumer.fetch('/var/lib/postgresql/9.3/main'))
        self.assertEqual({}, consumer.result)
        self.assertEqual(second_data, consumer.cached_result)
