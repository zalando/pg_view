import posix
from unittest import TestCase

import mock

from pg_view.models.collector_partition import DetachedDiskStatCollector
from tests.common import ErrorAfter, CallableExhaustedError


class DetachedDiskStatCollectorTest(TestCase):
    @mock.patch('pg_view.models.collector_partition.DetachedDiskStatCollector.run_du')
    def test_get_du_data_should_run_du_when_work_directory(self, mocked_run_du):
        mocked_run_du.side_effect = [35628, 35620]
        detached_disk = DetachedDiskStatCollector(mock.Mock(), ['/var/lib/postgresql/9.3/main'])
        result = detached_disk.get_du_data('/var/lib/postgresql/9.3/main')
        expected_result = {
            'xlog': ('35620', '/var/lib/postgresql/9.3/main/pg_xlog'),
            'data': ('35628', '/var/lib/postgresql/9.3/main')
        }
        self.assertEqual(expected_result, result)
        mocked_run_du.assert_has_calls([
            mock.call('/var/lib/postgresql/9.3/main'),
            mock.call('/var/lib/postgresql/9.3/main/pg_xlog/')
        ])

    @mock.patch('pg_view.models.collector_partition.DetachedDiskStatCollector.run_du')
    @mock.patch('pg_view.models.collector_partition.logger')
    def test_get_du_data_should_log_error_when_run_du_raises_exception(self, mocked_logger, mocked_run_du):
        mocked_run_du.side_effect = Exception
        detached_disk = DetachedDiskStatCollector(mock.Mock(), ['/var/lib/postgresql/9.3/main'])
        detached_disk.get_du_data('/var/lib/postgresql/9.3/main')
        expected_msg = 'Unable to read free space information for the pg_xlog and data directories for the directory ' \
                       '/var/lib/postgresql/9.3/main: '
        mocked_logger.error.assert_called_with(expected_msg)

    @mock.patch('pg_view.models.collector_partition.DetachedDiskStatCollector.get_du_data')
    @mock.patch('pg_view.models.collector_partition.DetachedDiskStatCollector.get_df_data')
    def test_run_should_loop_forever_processing_both_collectors(self, mocked_get_df_data, mocked_get_du_data):
        mocked_get_du_data.side_effect = ErrorAfter(1)
        mocked_get_df_data.side_effect = ErrorAfter(1)
        queue = mock.Mock()
        detached_disk = DetachedDiskStatCollector(queue, ['/var/lib/postgresql/9.3/main'])
        with self.assertRaises(CallableExhaustedError):
            detached_disk.run()

        mocked_get_du_data.assert_called_with('/var/lib/postgresql/9.3/main')
        mocked_get_df_data.assert_called_with('/var/lib/postgresql/9.3/main')
        queue.put.assert_called_once_with(
            {'/var/lib/postgresql/9.3/main': [('/var/lib/postgresql/9.3/main',), ('/var/lib/postgresql/9.3/main',)]})

    @mock.patch('pg_view.models.collector_partition.DetachedDiskStatCollector.get_mounted_device',
                return_value='/dev/sda1')
    @mock.patch('pg_view.models.collector_partition.DetachedDiskStatCollector.get_mount_point', return_value='/')
    @mock.patch('pg_view.models.collector_partition.os.statvfs')
    def test_get_df_data_should_return_proper_data_when_data_dev_and_xlog_dev_are_equal(self, mocked_os_statvfs,
                                                                                        mocked_get_mounted_device,
                                                                                        mocked_get_mount_point):
        seq = (4096, 4096, 10312784, 9823692, 9389714, 2621440, 2537942, 2537942, 4096, 255)
        mocked_os_statvfs.return_value = posix.statvfs_result(sequence=seq)
        detached_disk = DetachedDiskStatCollector(mock.Mock(), ['/var/lib/postgresql/9.3/main'])
        df_data = detached_disk.get_df_data('/var/lib/postgresql/9.3/main')
        expected_df_data = {'data': ('/dev/sda1', 41251136, 37558856), 'xlog': ('/dev/sda1', 41251136, 37558856)}
        self.assertEqual(expected_df_data, df_data)

    @mock.patch('pg_view.models.collector_partition.DetachedDiskStatCollector.get_mounted_device',
                side_effect=['/dev/sda1', '/dev/sda2'])
    @mock.patch('pg_view.models.collector_partition.DetachedDiskStatCollector.get_mount_point', return_value='/')
    @mock.patch('pg_view.models.collector_partition.os.statvfs')
    def test_get_df_data_should_return_proper_data_when_data_dev_and_xlog_dev_are_different(self, mocked_os_statvfs,
                                                                                            mocked_get_mounted_device,
                                                                                            mocked_get_mount_point):
        mocked_os_statvfs.side_effect = [
            posix.statvfs_result(
                sequence=(4096, 4096, 10312784, 9823692, 9389714, 2621440, 2537942, 2537942, 4096, 255)),
            posix.statvfs_result(sequence=(1024, 1024, 103127, 9823, 9389, 2621, 2537, 2537, 1024, 255))
        ]
        detached_disk = DetachedDiskStatCollector(mock.Mock(), ['/var/lib/postgresql/9.3/main'])
        df_data = detached_disk.get_df_data('/var/lib/postgresql/9.3/main')
        expected_df_data = {'data': ('/dev/sda1', 41251136, 37558856), 'xlog': ('/dev/sda2', 103127, 9389)}
        self.assertEqual(expected_df_data, df_data)

    @mock.patch('pg_view.models.collector_partition.os.statvfs', return_value=(4096, 4096))
    def test__get_or_update_df_cache_should_call_os_statvfs_when_empty_cache(self, mocked_os_statvfs):
        detached_disk = DetachedDiskStatCollector(mock.Mock(), ['/var/lib/postgresql/9.3/main'])
        df_data = detached_disk._get_or_update_df_cache('/var/lib/postgresql/9.3/main', '/sda/dev1')
        self.assertEqual((4096, 4096,), df_data)
        self.assertEqual((4096, 4096,), detached_disk.df_cache['/sda/dev1'])
        mocked_os_statvfs.assert_called_once_with('/var/lib/postgresql/9.3/main')

    @mock.patch('pg_view.models.collector_partition.os.statvfs')
    def test__get_or_update_df_cache_should_get_from_cache_when_entry_exists(self, mocked_os_statvfs):
        detached_disk = DetachedDiskStatCollector(mock.Mock(), ['/var/lib/postgresql/9.3/main'])
        detached_disk.df_cache = {'/sda/dev1': (4096, 4096,)}
        df_data = detached_disk._get_or_update_df_cache('/var/lib/postgresql/9.3/main', '/sda/dev1')
        self.assertEqual((4096, 4096,), df_data)
        mocked_os_statvfs.assert_not_called()
