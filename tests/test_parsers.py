from unittest import TestCase

import mock
import os
from psutil._common import sconn

from pg_view.parsers import ProcNetParser, get_dbname_from_path, ProcWorker
from tests.common import TEST_DIR


class ProcNetParserTest(TestCase):
    @mock.patch('pg_view.models.parsers.logger')
    @mock.patch('pg_view.models.parsers.psutil.net_connections')
    def test__get_connection_by_type_should_return_none_when_unix_type_wrong_format(self, mocked_net_connections, mocked_logger):
        parser = ProcNetParser(1048)
        unix_conn = sconn(
            fd=6, family=1, type=1, laddr='/var/run/.s.PGSQQL.5432', raddr=None, status='NONE', pid=1048)
        conn_params = parser._get_connection_by_type('unix', unix_conn)
        self.assertIsNone(conn_params)
        expected_msg = 'unix socket name is not recognized as belonging to PostgreSQL: {0}'.format(unix_conn)
        mocked_logger.warning.assert_called_with(expected_msg)

    @mock.patch('pg_view.models.parsers.psutil.net_connections')
    def test__get_connection_by_type_should_return_conn_params_when_unix_type_ok(self, mocked_net_connections):
        parser = ProcNetParser(1048)
        unix_conn = sconn(
            fd=6, family=1, type=1, laddr='/var/run/postgres/.s.PGSQL.5432', raddr=None, status='NONE', pid=1048)
        conn_params = parser._get_connection_by_type('unix', unix_conn)
        self.assertEqual(('/var/run/postgres', '5432'), conn_params)

    @mock.patch('pg_view.models.parsers.psutil.net_connections')
    def test__get_connection_by_type_should_return_conn_params_when_tcp_type_ok(self, mocked_net_connections):
        parser = ProcNetParser(1048)
        unix_conn = sconn(fd=3, family=2, type=1, laddr=('127.0.0.1', 5432), raddr=(), status='LISTEN', pid=1048)
        conn_params = parser._get_connection_by_type('tcp', unix_conn)
        self.assertEqual(('127.0.0.1', 5432), conn_params)

        conn_params = parser._get_connection_by_type('tcp6', unix_conn)
        self.assertEqual(('127.0.0.1', 5432), conn_params)

    @mock.patch('pg_view.models.parsers.psutil.net_connections')
    def test_get_socket_connections_call_net_connections_with_allowed_conn_types(self, mocked_net_connections):
        parser = ProcNetParser(1048)
        calls = [mock.call('unix'), mock.call('tcp'), mock.call('tcp6')]
        mocked_net_connections.assert_has_calls(calls, any_order=True)

    @mock.patch('pg_view.models.parsers.psutil.net_connections')
    def test_get_socket_connections_exclude_by_pid(self, mocked_net_connections):
        unix_conns = [
            sconn(fd=6, family=1, type=1, laddr='/var/run/postgres/.s.PGSQL.5432', raddr=None, status='NONE', pid=1048),
            sconn(fd=6, family=1, type=1, laddr='/var/run/postgres/.s.PGSQL.5432', raddr=None, status='NONE', pid=1049)
        ]
        tcp_conns = [
            sconn(fd=6, family=1, type=1, laddr=('127.0.0.1', 5432), raddr=None, status='NONE', pid=1048),
            sconn(fd=6, family=1, type=1, laddr=('127.0.0.1', 5432), raddr=None, status='NONE', pid=1049)
        ]

        mocked_net_connections.side_effect = [unix_conns, tcp_conns, []]
        parser = ProcNetParser(1048)

        self.assertEqual(1, len(parser.sockets['unix']))
        self.assertIn(unix_conns[0], parser.sockets['unix'])

        self.assertEqual(1, len(parser.sockets['tcp']))
        self.assertIn(tcp_conns[0], parser.sockets['tcp'])

    @mock.patch('pg_view.models.parsers.psutil.net_connections')
    def test_get_connections_from_sockets_should_return_connections_by_type_when_ok(self, mocked_net_connections):
        unix_conns = [
            sconn(fd=6, family=1, type=1, laddr='/var/run/postgres/.s.PGSQL.5432', raddr=None, status='NONE', pid=1048),
        ]

        tcp_conns = [
            sconn(fd=6, family=1, type=1, laddr=('127.0.0.1', 5432), raddr=None, status='NONE', pid=1048),
            sconn(fd=6, family=1, type=1, laddr=('127.0.0.1', 5432), raddr=None, status='NONE', pid=1049)
        ]
        tcp6_conns = [
            sconn(fd=6, family=1, type=1, laddr=('127.0.0.1', 5432), raddr=None, status='NONE', pid=1048),
        ]

        mocked_net_connections.side_effect = [unix_conns, tcp_conns, tcp6_conns]
        parser = ProcNetParser(1048)
        expected_connections = {
            'unix': [('/var/run/postgres', '5432')],
            'tcp6': [('127.0.0.1', 5432)],
            'tcp': [('127.0.0.1', 5432)]
        }
        self.assertEqual(expected_connections, parser.get_connections_from_sockets())


class UtilsTest(TestCase):
    def test_get_dbname_from_path_should_return_last_when_name(self):
        self.assertEqual('foo', get_dbname_from_path('foo'))

    def test_get_dbname_from_path_should_return_last_when_path(self):
        self.assertEqual('bar', get_dbname_from_path('/pgsql_bar/9.4/data'))


class ProcWorkerTest(TestCase):
    def setUp(self):
        super(ProcWorkerTest, self).setUp()
        self.worker = ProcWorker()

    def test_detect_with_postmaster_pid_should_return_none_when_version_none(self):
        result = self.worker.detect_with_postmaster_pid('', None)
        self.assertIsNone(result)

    def test_detect_with_postmaster_pid_should_return_none_when_version_90(self):
        result = self.worker.detect_with_postmaster_pid('', 9.0)
        self.assertIsNone(result)

    @mock.patch('pg_view.models.parsers.os.access', return_value=False)
    @mock.patch('pg_view.models.parsers.logger')
    def test_detect_with_postmaster_pid_should_return_none_when_no_access_to_postmaster(self, mocked_logger, mocked_os_access):
        result = self.worker.detect_with_postmaster_pid('/var/lib/postgresql/9.3/main/', 9.3)
        self.assertIsNone(result)
        expected_msg = 'cannot access PostgreSQL cluster directory /var/lib/postgresql/9.3/main/: permission denied'
        mocked_logger.warning.assert_called_with(expected_msg)

    @mock.patch('pg_view.models.parsers.readlines_file')
    @mock.patch('pg_view.models.parsers.os.access', return_value=True)
    @mock.patch('pg_view.models.parsers.logger')
    def test_detect_with_postmaster_pid_should_return_none_when_readline_files_error(self, mocked_logger, mocked_os_access, mocked_readlines_file):
        mocked_readlines_file.side_effect = os.error('Msg error')
        result = self.worker.detect_with_postmaster_pid('/var/lib/postgresql/9.3/main', 9.3)
        self.assertIsNone(result)
        expected_msg = 'could not read /var/lib/postgresql/9.3/main/postmaster.pid: Msg error'.format()
        mocked_logger.error.assert_called_with(expected_msg)

    @mock.patch('pg_view.helpers.open_universal')
    @mock.patch('pg_view.models.parsers.os.access', return_value=True)
    @mock.patch('pg_view.models.parsers.logger')
    def test_detect_with_postmaster_pid_should_return_none_when_postmaster_truncated(self, mocked_logger, mocked_os_access,
                                                                                     mocked_open_universal):
        postmaster_info_broken = os.path.join(TEST_DIR, 'postmaster_pg_files', 'postmaster_info_truncated')
        mocked_open_universal.return_value = open(postmaster_info_broken, "rU")
        result = self.worker.detect_with_postmaster_pid('/var/lib/postgresql/9.3/main', 9.3)
        expected_msg = '/var/lib/postgresql/9.3/main/postmaster.pid seems to be truncated, ' \
                       'unable to read connection information'
        mocked_logger.error.assert_called_with(expected_msg)
        self.assertIsNone(result)

    @mock.patch('pg_view.helpers.open_universal')
    @mock.patch('pg_view.models.parsers.os.access', return_value=True)
    @mock.patch('pg_view.models.parsers.logger')
    def test_detect_with_postmaster_pid_should_return_none_when_postmaster_info_missing_data(self, mocked_logger, mocked_os_access,
                                                                                     mocked_open_universal):
        postmaster_info_broken = os.path.join(TEST_DIR, 'postmaster_pg_files', 'postmaster_info_missing_data')
        mocked_open_universal.return_value = open(postmaster_info_broken, "rU")
        result = self.worker.detect_with_postmaster_pid('/var/lib/postgresql/9.3/main', 9.3)
        expected_msg = 'could not acquire a socket postmaster at /var/lib/postgresql/9.3/main is listening on'
        mocked_logger.error.assert_called_with(expected_msg)
        self.assertIsNone(result)

    @mock.patch('pg_view.helpers.open_universal')
    @mock.patch('pg_view.models.parsers.os.access', return_value=True)
    def test_detect_with_postmaster_pid_should_fill_tcp_localhost_when_address_star(self, mocked_os_access,
                                                                                     mocked_open_universal):
        postmaster_info_ok = os.path.join(TEST_DIR, 'postmaster_pg_files', 'postmaster_info_tcp')
        mocked_open_universal.return_value = open(postmaster_info_ok, "rU")
        result = self.worker.detect_with_postmaster_pid('/var/lib/postgresql/9.3/main', 9.3)
        expected_result = {
            'unix': [('/var/run/postgresql', '5432')],
            'tcp': [('127.0.0.1', '5432')]
        }
        self.assertEqual(expected_result, result)

    @mock.patch('pg_view.helpers.open_universal')
    @mock.patch('pg_view.models.parsers.os.access', return_value=True)
    def test_detect_with_postmaster_pid_should_return_conn_params_when_ok(self, mocked_os_access,
                                                                                     mocked_open_universal):
        postmaster_info_ok = os.path.join(TEST_DIR, 'postmaster_pg_files', 'postmaster_info_ok')
        mocked_open_universal.return_value = open(postmaster_info_ok, "rU")
        result = self.worker.detect_with_postmaster_pid('/var/lib/postgresql/9.3/main', 9.3)
        expected_result = {
            'unix': [('/var/run/postgresql', '5432')],
            'tcp': [('localhost', '5432')]
        }
        self.assertEqual(expected_result, result)
