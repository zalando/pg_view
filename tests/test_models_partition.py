from collections import namedtuple
from multiprocessing import JoinableQueue
from unittest import TestCase

import mock

from pg_view.models.collector_partition import PartitionStatCollector
from pg_view.models.consumers import DiskCollectorConsumer

sdiskio = namedtuple(
    'sdiskio', ['read_count', 'write_count', 'read_bytes', 'write_bytes', 'read_time', 'write_time']
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

    @mock.patch('pg_view.models.collector_partition.psutil.disk_io_counters')
    def test_get_io_data_should_return_data_when_ok(self, mocked_disk_io_counters):
        mocked_disk_io_counters.return_value = {
            'sda1': sdiskio(read_count=10712, write_count=4011, read_bytes=157438976,
                            write_bytes=99520512, read_time=6560, write_time=3768
                            )
        }
        io_data = self.collector.get_io_data('')
        expected_data = {
            'sda1': {
                'await': 0,
                'sectors_read': 307498,
                'sectors_written': 194376
            }
        }

        self.assertEqual(expected_data, io_data)
