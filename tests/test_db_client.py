from unittest import TestCase

import mock

from pg_view.models.db_client import read_postmaster_pid, make_cluster_desc, DBConnection, DBConnectionFinder


class DbClientUtilsTest(TestCase):
    @mock.patch('pg_view.models.db_client.logger')
    @mock.patch('__builtin__.open')
    def test_read_postmaster_pid_should_return_none_when_error(self, mocked_open, mocked_logger):
        mocked_open.side_effect = Exception
        data = read_postmaster_pid('/var/lib/postgresql/9.3/main', 'default')
        self.assertIsNone(data)
        expected_msg = 'Unable to read postmaster.pid for {name} at {wd}\n HINT: make sure Postgres is running'
        mocked_logger.error.assert_called_with(
            expected_msg.format(name='default', wd='/var/lib/postgresql/9.3/main'))

    @mock.patch('pg_view.models.db_client.logger')
    @mock.patch('__builtin__.open')
    def test_read_postmaster_pid_should_return_none_when_error_strip(self, mocked_open, mocked_logger):
        mocked_open.return_value.readline.return_value = []
        data = read_postmaster_pid('/var/lib/postgresql/9.3/main', 'default')
        self.assertIsNone(data)
        expected_msg = 'Unable to read postmaster.pid for {name} at {wd}\n HINT: make sure Postgres is running'
        mocked_logger.error.assert_called_with(
            expected_msg.format(name='default', wd='/var/lib/postgresql/9.3/main'))

    @mock.patch('__builtin__.open')
    def test_read_postmaster_pid_should_return_pid_when_read_file(self, mocked_open):
        mocked_open.return_value.readline.return_value = '123 '
        data = read_postmaster_pid('/var/lib/postgresql/9.3/main', 'default')
        self.assertEqual('123', data)

    def test_make_cluster_desc_should_return_dict_when_ok(self):
        cluster_desc = make_cluster_desc('name', 'version', 'workdir', 'pid', 'pgcon', 'con')

        self.assertEqual('name', cluster_desc['name'])
        self.assertEqual('version', cluster_desc['ver'])
        self.assertEqual('workdir', cluster_desc['wd'])
        self.assertEqual('pid', cluster_desc['pid'])
        self.assertEqual('pgcon', cluster_desc['pgcon'])
        self.assertIn('reconnect', cluster_desc)

    def test_build_connection_should_return_only_existing_parameters(self):
        connection = DBConnection('host', '5432', 'user')
        conn = connection.build_connection()
        self.assertEqual({'host': 'host', 'port': '5432', 'user': 'user'}, conn)


class DBConnectionFinderTest(TestCase):
    @mock.patch('pg_view.models.db_client.logger')
    @mock.patch('pg_view.models.db_client.DBConnectionFinder.detect_with_proc_net', return_value=None)
    @mock.patch('pg_view.models.db_client.ProcWorker')
    def test_detect_db_connection_arguments_should_return_none_when_no_conn_args(self, mocked_proc_worker, mocked_detect_with_proc_net, mocked_logger):
        finder = DBConnectionFinder('workdir', 1049, '9.3', 'username', 'atlas')
        mocked_proc_worker.return_value.detect_with_postmaster_pid.return_value = None
        conn_args = finder.detect_db_connection_arguments()
        self.assertIsNone(conn_args)
        mocked_logger.error.assert_called_with('unable to detect connection parameters for the PostgreSQL cluster at workdir')
