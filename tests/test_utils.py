import os
import subprocess
from unittest import TestCase

import mock

from common import TEST_DIR
from pg_view.exceptions import InvalidConnectionParamError
from pg_view.models.parsers import connection_params
from pg_view.utils import UnitConverter, read_configuration, validate_autodetected_conn_param, \
    exec_command_with_output, output_method_is_valid


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

    @mock.patch('pg_view.consts.USER_HZ', 100)
    def test_ticks_to_seconds_should_convert_when_ok(self):
        self.assertEqual(5, UnitConverter.ticks_to_seconds(500))

    @mock.patch('pg_view.consts.USER_HZ', 100)
    def test_ticks_to_seconds_should_return_none_when_none(self):
        self.assertIsNone(UnitConverter.ticks_to_seconds(None))

    def test_time_diff_to_percent_should_convert_when_ok(self):
        self.assertEqual(1000.0, UnitConverter.time_diff_to_percent(10))

    def test_time_diff_to_percent_should_return_none_when_none(self):
        self.assertIsNone(UnitConverter.time_diff_to_percent(None))


class ReadConfigurationTest(TestCase):
    def test_read_configuration_should_return_none_when_not_config_file_name(self):
        self.assertIsNone(read_configuration(None))

    @mock.patch('pg_view.utils.logger')
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


class ValidateConnParamTest(TestCase):
    def test_validate_autodetected_conn_param_should_return_none_when_no_user_dbname(self):
        self.assertIsNone(validate_autodetected_conn_param(None, '9.3', '/var/run/postgresql', {}))

    def test_validate_autodetected_conn_param_should_raise_invalid_param_when_different_dbnames(self):
        conn_parameters = connection_params(pid=1049, version=9.3, dbname='/var/lib/postgresql/9.3/main')
        with self.assertRaises(InvalidConnectionParamError):
            validate_autodetected_conn_param('/var/lib/postgresql/9.5/main', 9.3, '/var/run/postgresql',
                                             conn_parameters)

    def test_validate_autodetected_conn_param_should_raise_invalid_param_when_no_result_work_dir(self):
        conn_parameters = connection_params(pid=1049, version=9.3, dbname='/var/lib/postgresql/9.3/main')
        with self.assertRaises(InvalidConnectionParamError):
            validate_autodetected_conn_param('/var/lib/postgresql/9.3/main', 9.3, '', conn_parameters)

    def test_validate_autodetected_conn_param_should_raise_invalid_param_when_no_connection_params_pid(self):
        conn_parameters = connection_params(pid=None, version=9.3, dbname='/var/lib/postgresql/9.3/main')
        with self.assertRaises(InvalidConnectionParamError):
            validate_autodetected_conn_param(
                '/var/lib/postgresql/9.3/main', 9.3, '/var/run/postgresql', conn_parameters)

    def test_validate_autodetected_conn_param_should_raise_invalid_param_when_different_versions(self):
        conn_parameters = connection_params(pid=2, version=9.3, dbname='/var/lib/postgresql/9.3/main')
        with self.assertRaises(InvalidConnectionParamError):
            validate_autodetected_conn_param(
                '/var/lib/postgresql/9.3/main', 9.5, '/var/run/postgresql', conn_parameters)


class CommandExecutorTest(TestCase):
    @mock.patch('pg_view.utils.logger')
    @mock.patch('pg_view.utils.subprocess.Popen')
    def test_exec_command_with_output_should_log_info_when_cmd_return_not_zero_exit_code(self, mocked_popen,
                                                                                         mocked_logger):
        cmdline = 'ps -o pid --ppid 1049 --noheaders'
        proc = mock.MagicMock()
        proc.wait.return_value = 1
        proc.stdout.read.return_value = ' 1051\n 1052\n 1053\n 1054\n 1055\n 11139\n 26585\n'
        mocked_popen.return_value = proc
        ret, stdout = exec_command_with_output(cmdline)
        mocked_popen.assert_called_with(cmdline, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
        mocked_logger.info.assert_called_with(
            'The command ps -o pid --ppid 1049 --noheaders returned a non-zero exit code')

        self.assertEqual(1, ret)
        self.assertEqual('1051\n 1052\n 1053\n 1054\n 1055\n 11139\n 26585', stdout)

    @mock.patch('pg_view.utils.logger')
    @mock.patch('pg_view.utils.subprocess.Popen')
    def test_exec_command_with_output_should_return_ret_stdout_when_cmd_return_zero_exit_code(self, mocked_popen,
                                                                                              mocked_logger):
        cmdline = 'ps -o pid --ppid 1049 --noheaders'
        proc = mock.MagicMock()
        proc.wait.return_value = 0
        proc.stdout.read.return_value = ' 1051\n 1052\n 1053\n 1054\n 1055\n 11139\n 26585\n'
        mocked_popen.return_value = proc
        ret, stdout = exec_command_with_output(cmdline)
        mocked_popen.assert_called_with(cmdline, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
        self.assertFalse(mocked_logger.info.called)

        self.assertEqual(0, ret)
        self.assertEqual('1051\n 1052\n 1053\n 1054\n 1055\n 11139\n 26585', stdout)


class ValidatorTest(TestCase):
    def test_output_method_is_valid_should_return_true_when_valid(self):
        ALLOWED_OUTPUTS = ['console', 'json', 'curses']
        for output in ALLOWED_OUTPUTS:
            self.assertTrue(output_method_is_valid(output))

    def test_output_method_is_valid_should_return_false_when_invalid(self):
        ALLOWED_OUTPUTS = ['test', 'foo', 1]
        for output in ALLOWED_OUTPUTS:
            self.assertFalse(output_method_is_valid(output))
