import os
import unittest
from collections import namedtuple
from unittest import TestCase

import mock

from common import TEST_DIR
from pg_view.models.parsers import ProcNetParser, get_dbname_from_path, ProcWorker, connection_params

sconn = namedtuple('sconn', ['fd', 'family', 'type', 'laddr', 'raddr', 'status', 'pid'])


@unittest.skip('psutil')
class ProcNetParserTest(TestCase):
    @mock.patch('pg_view.models.parsers.logger')
    @mock.patch('pg_view.models.parsers.psutil.net_connections')
    def test__get_connection_by_type_should_return_none_when_unix_type_wrong_format(self, mocked_net_connections,
                                                                                    mocked_logger):
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
        ProcNetParser(1048)
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


@unittest.skip('psutil')
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
    def test_detect_with_postmaster_pid_should_return_none_when_no_access_to_postmaster(self, mocked_logger,
                                                                                        mocked_os_access):
        result = self.worker.detect_with_postmaster_pid('/var/lib/postgresql/9.3/main/', 9.3)
        self.assertIsNone(result)
        expected_msg = 'cannot access PostgreSQL cluster directory /var/lib/postgresql/9.3/main/: permission denied'
        mocked_logger.warning.assert_called_with(expected_msg)

    @mock.patch('pg_view.models.parsers.readlines_file')
    @mock.patch('pg_view.models.parsers.os.access', return_value=True)
    @mock.patch('pg_view.models.parsers.logger')
    def test_detect_with_postmaster_pid_should_return_none_when_readline_files_error(self, mocked_logger,
                                                                                     mocked_os_access,
                                                                                     mocked_readlines_file):
        mocked_readlines_file.side_effect = os.error('Msg error')
        result = self.worker.detect_with_postmaster_pid('/var/lib/postgresql/9.3/main', 9.3)
        self.assertIsNone(result)
        expected_msg = 'could not read /var/lib/postgresql/9.3/main/postmaster.pid: Msg error'.format()
        mocked_logger.error.assert_called_with(expected_msg)

    @mock.patch('pg_view.utils.open_universal')
    @mock.patch('pg_view.models.parsers.os.access', return_value=True)
    @mock.patch('pg_view.models.parsers.logger')
    def test_detect_with_postmaster_pid_should_return_none_when_postmaster_truncated(self, mocked_logger,
                                                                                     mocked_os_access,
                                                                                     mocked_open_universal):
        postmaster_info_broken = os.path.join(TEST_DIR, 'postmaster_pg_files', 'postmaster_info_truncated')
        mocked_open_universal.return_value = open(postmaster_info_broken, "rU")
        result = self.worker.detect_with_postmaster_pid('/var/lib/postgresql/9.3/main', 9.3)
        expected_msg = '/var/lib/postgresql/9.3/main/postmaster.pid seems to be truncated, ' \
                       'unable to read connection information'
        mocked_logger.error.assert_called_with(expected_msg)
        self.assertIsNone(result)

    @mock.patch('pg_view.utils.open_universal')
    @mock.patch('pg_view.models.parsers.os.access', return_value=True)
    @mock.patch('pg_view.models.parsers.logger')
    def test_detect_with_postmaster_pid_should_return_none_when_postmaster_info_missing_data(self, mocked_logger,
                                                                                             mocked_os_access,
                                                                                             mocked_open_universal):
        postmaster_info_broken = os.path.join(TEST_DIR, 'postmaster_pg_files', 'postmaster_info_missing_data')
        mocked_open_universal.return_value = open(postmaster_info_broken, "rU")
        result = self.worker.detect_with_postmaster_pid('/var/lib/postgresql/9.3/main', 9.3)
        expected_msg = 'could not acquire a socket postmaster at /var/lib/postgresql/9.3/main is listening on'
        mocked_logger.error.assert_called_with(expected_msg)
        self.assertIsNone(result)

    @mock.patch('pg_view.utils.open_universal')
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

    @mock.patch('pg_view.utils.open_universal')
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

    @mock.patch('pg_view.models.parsers.ProcWorker._get_postgres_processes')
    @mock.patch('pg_view.models.parsers.ProcWorker.get_pg_version_from_file')
    def test_get_postmasters_directories_should_return_postmaster_when_ppid_not_in_candidates(self,
                                                                                              mocked_get_pg_version_from_file,
                                                                                              mocked_get_postgres_processes):
        mocked_get_pg_version_from_file.return_value = connection_params(1056, '9.3', 'db')
        process = mock.Mock(pid=1056, name='postgres')
        process.ppid.return_value = 1
        process.cwd.return_value = '/var/lib/postgresql/9.3/main'
        mocked_get_postgres_processes.return_value = [process]

        result = self.worker.get_postmasters_directories()
        expected_result = {'/var/lib/postgresql/9.3/main': connection_params(pid=1056, version='9.3', dbname='db')}
        self.assertEqual(expected_result, result)
        mocked_get_postgres_processes.assert_called_with()

    @mock.patch('pg_view.models.parsers.ProcWorker._get_postgres_processes')
    @mock.patch('pg_view.models.parsers.ProcWorker.get_pg_version_from_file', return_value=None)
    def test_get_postmasters_directories_should_ignore_process_when_no_pg_version_file(self,
                                                                                       mocked_get_pg_version_from_file,
                                                                                       mocked_get_postgres_processes):
        process = mock.Mock(pid=1056, name='postgres')
        process.ppid.return_value = 1
        process.cwd.return_value = '/var/lib/postgresql/9.3/main'
        mocked_get_postgres_processes.return_value = [process]

        result = self.worker.get_postmasters_directories()
        self.assertEqual({}, result)
        mocked_get_postgres_processes.assert_called_with()

    @mock.patch('pg_view.models.parsers.ProcWorker._get_postgres_processes')
    @mock.patch('pg_view.models.parsers.ProcWorker.get_pg_version_from_file')
    def test_get_postmasters_directories_should_return_two_postmaster_when_ppid_not_in_candidates_and_separate_pd_dir(
            self,
            mocked_get_pg_version_from_file,
            mocked_get_postgres_processes):
        mocked_get_pg_version_from_file.side_effect = [
            connection_params(1056, '9.3', 'db'), connection_params(1057, '9.4', 'test_db')
        ]
        first_process = mock.Mock(pid=1056, name='postgres')
        first_process.ppid.return_value = 1
        first_process.cwd.return_value = '/var/lib/postgresql/9.3/main'

        second_process = mock.Mock(pid=1057, name='postgres')
        second_process.ppid.return_value = 1
        second_process.cwd.return_value = '/var/lib/postgresql/9.4/main'

        mocked_get_postgres_processes.return_value = [first_process, second_process]

        result = self.worker.get_postmasters_directories()
        expected_result = {
            '/var/lib/postgresql/9.3/main': connection_params(pid=1056, version='9.3', dbname='db'),
            '/var/lib/postgresql/9.4/main': connection_params(pid=1057, version='9.4', dbname='test_db')
        }
        self.assertEqual(expected_result, result)
        mocked_get_postgres_processes.assert_called_with()

    @mock.patch('pg_view.models.parsers.ProcWorker._get_postgres_processes')
    @mock.patch('pg_view.models.parsers.ProcWorker.get_pg_version_from_file')
    def test_get_postmasters_directories_should_exclude_second_process_when_same_pd_dir(self,
                                                                                        mocked_get_pg_version_from_file,
                                                                                        mocked_get_postgres_processes):
        mocked_get_pg_version_from_file.side_effect = [
            connection_params(1056, '9.3', 'db'), connection_params(1057, '9.3', 'test_db')
        ]
        first_process = mock.Mock(pid=1056, name='postgres')
        first_process.ppid.return_value = 1
        first_process.cwd.return_value = '/var/lib/postgresql/9.3/main'

        second_process = mock.Mock(pid=1057, name='postgres')
        second_process.ppid.return_value = 1
        second_process.cwd.return_value = '/var/lib/postgresql/9.3/main'

        mocked_get_postgres_processes.return_value = [first_process, second_process]

        result = self.worker.get_postmasters_directories()
        expected_result = {
            '/var/lib/postgresql/9.3/main': connection_params(pid=1056, version='9.3', dbname='db'),
        }
        self.assertEqual(expected_result, result)
        mocked_get_postgres_processes.assert_called_with()

    @mock.patch('pg_view.models.parsers.ProcWorker._get_postgres_processes')
    @mock.patch('pg_view.models.parsers.ProcWorker.get_pg_version_from_file')
    def test_get_postmasters_directories_should_exclude_process_when_ppid_in_process_candidates(self,
                                                                                                mocked_get_pg_version_from_file,
                                                                                                mocked_get_postgres_processes):
        mocked_get_pg_version_from_file.side_effect = [
            connection_params(1056, '9.3', 'db'), connection_params(1057, '9.4', 'test_db')
        ]
        first_process = mock.Mock(pid=1056, name='postgres')
        first_process.ppid.return_value = 1
        first_process.cwd.return_value = '/var/lib/postgresql/9.3/main'

        second_process = mock.Mock(pid=1057, name='postgres')
        second_process.ppid.return_value = 1056
        second_process.cwd.return_value = '/var/lib/postgresql/9.4/main'

        mocked_get_postgres_processes.return_value = [first_process, second_process]

        result = self.worker.get_postmasters_directories()
        expected_result = {
            '/var/lib/postgresql/9.3/main': connection_params(pid=1056, version='9.3', dbname='db'),
        }
        self.assertEqual(expected_result, result)
        mocked_get_postgres_processes.assert_called_with()

    @mock.patch('pg_view.models.parsers.psutil.process_iter')
    def test_get_postgres_processes_should_filter_by_name(self, mocked_process_iter):
        mocked_process_iter.return_value = [
            mock.Mock(**{'name.return_value': 'postgres', 'pid': 1}),
            mock.Mock(**{'name.return_value': 'postmaster', 'pid': 2}),
            mock.Mock(**{'name.return_value': 'test', 'pid': 3}),
            mock.Mock(**{'name.return_value': 'process', 'pid': 4})
        ]

        postgres_processes = self.worker._get_postgres_processes()
        self.assertEqual(2, len(postgres_processes))
        self.assertEqual([1, 2], [p.pid for p in postgres_processes])

    @mock.patch('pg_view.models.parsers.os.access', return_value=False)
    @mock.patch('pg_view.models.parsers.logger')
    def test_get_pg_version_from_file_should_return_none_when_no_access_to_file(self, mocked_logger, mocked_os_access):
        pg_version = self.worker.get_pg_version_from_file(10, '/var/lib/postgresql/9.3/main')
        self.assertIsNone(pg_version)

        expected_msg = 'PostgreSQL candidate directory /var/lib/postgresql/9.3/main is missing PG_VERSION file, ' \
                       'have to skip it'
        mocked_logger.warning.assert_called_once_with(expected_msg)

    @mock.patch('pg_view.models.parsers.os.access', return_value=True)
    @mock.patch('pg_view.models.parsers.read_file', return_value='9.3\n')
    @mock.patch('pg_view.models.parsers.logger')
    def test_get_pg_version_from_file_should_return_params_when_file_ok(self, mocked_logger, mocked_read_file,
                                                                        mocked_os_access):
        pg_version = self.worker.get_pg_version_from_file(10, '/var/lib/postgresql/9.3/main')
        expected_result = connection_params(pid=10, version=9.3, dbname='/var/lib/postgresql/9.3/main')
        self.assertEqual(expected_result, pg_version)

    @mock.patch('pg_view.models.parsers.os.access', return_value=True)
    @mock.patch('pg_view.models.parsers.read_file', return_value='9')
    @mock.patch('pg_view.models.parsers.logger')
    def test_get_pg_version_from_file_should_return_none_when_wrong_db_version_in_file(self, mocked_logger,
                                                                                       mocked_read_file,
                                                                                       mocked_os_access):
        pg_version = self.worker.get_pg_version_from_file(10, '/var/lib/postgresql/9.3/main')
        expected_msg = "PG_VERSION doesn't contain a valid version number: 9"
        self.assertIsNone(pg_version)
        mocked_logger.error.assert_called_once_with(expected_msg)

    @mock.patch('pg_view.models.parsers.os.access', return_value=True)
    @mock.patch('pg_view.models.parsers.read_file', side_effect=os.error)
    @mock.patch('pg_view.models.parsers.logger')
    def test_get_pg_version_from_file_should_return_none_when_error_on_read_file(self, mocked_logger,
                                                                                 mocked_read_file, mocked_os_access):
        pg_version = self.worker.get_pg_version_from_file(10, '/var/lib/postgresql/9.3/main')
        expected_msg = 'unable to read version number from PG_VERSION directory /var/lib/postgresql/9.3/main, have to skip it'
        self.assertIsNone(pg_version)
        mocked_logger.error.assert_called_once_with(expected_msg)
