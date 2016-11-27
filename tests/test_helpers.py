import os
from unittest import TestCase

import mock

from pg_view.helpers import UnitConverter, read_configuration
from tests.common import TEST_DIR


class UnitConverterTest(TestCase):
    def test_kb_to_mbytes_should_convert_when_ok(self):
        self.assertEqual(3, UnitConverter.kb_to_mbytes(3072))

    def test_kb_to_mbytes_should_return_none_when_none(self):
        self.assertIsNone(UnitConverter.kb_to_mbytes(None))

    def test_sectors_to_mbytes_should_convert_when_ok(self):
        self.assertEqual(10, UnitConverter.sectors_to_mbytes(20480))

    def test_sectors_to_mbytes_should_return_none_when_none(self):
        self.assertIsNone(UnitConverter.sectors_to_mbytes(None))

    def test_bytes_to_mbytes_should_convert_when_ok(self):
        self.assertEqual(2, UnitConverter.bytes_to_mbytes(2097152))

    def test_bytes_to_mbytes_should_return_none_when_none(self):
        self.assertIsNone(UnitConverter.bytes_to_mbytes(None))

    @mock.patch('pg_view.helpers.consts.USER_HZ', 100)
    def test_ticks_to_seconds_should_convert_when_ok(self):
        self.assertEqual(5, UnitConverter.ticks_to_seconds(500))

    @mock.patch('pg_view.helpers.consts.USER_HZ', 100)
    def test_ticks_to_seconds_should_return_none_when_none(self):
        self.assertIsNone(UnitConverter.ticks_to_seconds(None))

    def test_time_diff_to_percent_should_convert_when_ok(self):
        self.assertEqual(1000.0, UnitConverter.time_diff_to_percent(10))

    def test_time_diff_to_percent_should_return_none_when_none(self):
        self.assertIsNone(UnitConverter.time_diff_to_percent(None))


class ReadConfigurationTest(TestCase):
    def test_read_configuration_should_return_none_when_not_config_file_name(self):
        self.assertIsNone(read_configuration(None))

    @mock.patch('pg_view.models.base.logger')
    def test_read_configuration_should_return_none_when_cannot_read_file(self, mocked_logger):
        config_file_path = os.path.join(TEST_DIR, 'not-existing')
        self.assertIsNone(read_configuration(config_file_path))
        expected_msg = 'Configuration file {0} is empty or not found'.format(config_file_path)
        mocked_logger.error.assert_called_with(expected_msg)

    def test_read_configuration_should_return_config_data_when_config_file_ok(self):
        config_file_path = os.path.join(TEST_DIR, 'configs', 'default_ok.cfg')
        expected_conf = {'testdb': {
            'host': '/var/run/postgresql', 'port': '5432', 'dbname': 'postgres', 'user': 'username'}
        }
        config = read_configuration(config_file_path)
        self.assertDictEqual(expected_conf, config)

    def test_read_configuration_should_skip_empty_options_when_not_exist(self):
        config_file_path = os.path.join(TEST_DIR, 'configs', 'default_with_none_user.cfg')
        expected_conf = {'testdb': {
            'host': '/var/run/postgresql', 'port': '5432', 'dbname': 'postgres'}
        }
        config = read_configuration(config_file_path)
        self.assertDictEqual(expected_conf, config)
