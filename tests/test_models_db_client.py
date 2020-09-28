import unittest
from unittest import TestCase

import mock
import psycopg2

from pg_view.exceptions import NotConnectedError, NoPidConnectionError, DuplicatedConnectionError
from pg_view.models.db_client import read_postmaster_pid, make_cluster_desc, DBConnectionFinder, DBClient, \
    prepare_connection_params


class DbClientUtilsTest(TestCase):
    @mock.patch('pg_view.models.db_client.logger')
    @mock.patch('pg_view.models.db_client.open', create=True)
    def test_read_postmaster_pid_should_return_none_when_error(self, mocked_open, mocked_logger):
        mocked_open.side_effect = Exception
        data = read_postmaster_pid('/var/lib/postgresql/9.3/main', 'default')
        self.assertIsNone(data)
        expected_msg = 'Unable to read postmaster.pid for {name} at {wd}\n HINT: make sure Postgres is running'
        mocked_logger.error.assert_called_with(
            expected_msg.format(name='default', wd='/var/lib/postgresql/9.3/main'))

    @mock.patch('pg_view.models.db_client.logger')
    @mock.patch('pg_view.models.db_client.open', create=True)
    def test_read_postmaster_pid_should_return_none_when_error_strip(self, mocked_open, mocked_logger):
        mocked_open.return_value.readline.return_value = []
        data = read_postmaster_pid('/var/lib/postgresql/9.3/main', 'default')
        self.assertIsNone(data)
        expected_msg = 'Unable to read postmaster.pid for {name} at {wd}\n HINT: make sure Postgres is running'
        mocked_logger.error.assert_called_with(
            expected_msg.format(name='default', wd='/var/lib/postgresql/9.3/main'))

    @mock.patch('pg_view.models.db_client.open', create=True)
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

    def test_build_connection_should_create_full_connection(self):
        connection = prepare_connection_params('host', '5432', 'user', 'database')
        self.assertEqual(
            {'host': 'host', 'port': '5432', 'user': 'user', 'database': 'database'}, connection)

    def test_build_connection_should_return_only_existing_parameters(self):
        connection = prepare_connection_params('host', '5432')
        self.assertEqual({'host': 'host', 'port': '5432'}, connection)


class DBConnectionFinderTest(TestCase):
    @mock.patch('pg_view.models.db_client.logger')
    @mock.patch('pg_view.models.db_client.DBConnectionFinder.detect_with_proc_net', return_value=None)
    @mock.patch('pg_view.models.db_client.ProcWorker')
    def test_detect_db_connection_arguments_should_return_none_when_no_conn_args(self, mocked_proc_worker,
                                                                                 mocked_detect_with_proc_net,
                                                                                 mocked_logger):
        finder = DBConnectionFinder('workdir', 1049, '9.3', 'username', 'atlas')
        mocked_proc_worker.return_value.detect_with_postmaster_pid.return_value = None
        conn_args = finder.detect_db_connection_arguments()
        self.assertIsNone(conn_args)
        mocked_logger.error.assert_called_with(
            'unable to detect connection parameters for the PostgreSQL cluster at workdir')

    @mock.patch('pg_view.models.db_client.logger')
    @mock.patch('pg_view.models.db_client.DBConnectionFinder.detect_with_proc_net', return_value=None)
    @mock.patch('pg_view.models.db_client.ProcWorker')
    def test_detect_db_connection_arguments_should_return_none_when_not_pickable_conn_arguments(self,
                                                                                                mocked_proc_worker,
                                                                                                mocked_detect_with_proc_net,
                                                                                                mocked_logger):
        finder = DBConnectionFinder('workdir', 1049, '9.3', 'username', 'atlas')
        conn_params = {'unix_wrong': [('/var/run/postgresql', '5432')], 'tcp_wrong': [('localhost', '5432')]}
        mocked_proc_worker.return_value.detect_with_postmaster_pid.return_value = conn_params
        conn_args = finder.detect_db_connection_arguments()
        self.assertIsNone(conn_args)
        expected_msg = "unable to connect to PostgreSQL cluster at workdir using any of the detected connection " \
                       "options: {0}".format(conn_params)
        mocked_logger.error.assert_called_with(expected_msg)

    @mock.patch('pg_view.models.db_client.DBConnectionFinder.detect_with_proc_net', return_value=None)
    @mock.patch('pg_view.models.db_client.DBConnectionFinder.can_connect_with_connection_arguments', return_value=True)
    @mock.patch('pg_view.models.db_client.ProcWorker')
    def test_detect_db_connection_arguments_should_return_params_when_detect_with_postmaster_pid(self,
                                                                                                 mocked_proc_worker,
                                                                                                 mocked_can_connect,
                                                                                                 mocked_detect_with_proc_net):
        finder = DBConnectionFinder('workdir', 1049, '9.3', 'username', 'atlas')
        mocked_proc_worker.return_value.detect_with_postmaster_pid.return_value = {
            'unix': [('/var/run/postgresql', '5432')],
            'tcp': [('localhost', '5432')]
        }
        conn_args = finder.detect_db_connection_arguments()
        self.assertEqual({'host': '/var/run/postgresql', 'port': '5432'}, conn_args)

    @mock.patch('pg_view.models.db_client.DBConnectionFinder.detect_with_proc_net')
    @mock.patch('pg_view.models.db_client.DBConnectionFinder.can_connect_with_connection_arguments', return_value=True)
    def test_detect_db_connection_arguments_should_return_params_when_detect_with_proc_net(self, mocked_can_connect,
                                                                                           mocked_detect_with_proc_net):
        mocked_detect_with_proc_net.return_value = {
            'unix': [('/var/run/postgresql', '5432')], 'tcp': [('127.0.0.1', 5432)]
        }
        finder = DBConnectionFinder('workdir', 1049, '9.3', 'username', 'atlas')
        conn_args = finder.detect_db_connection_arguments()
        self.assertEqual({'host': '/var/run/postgresql', 'port': '5432'}, conn_args)

    def test_pick_connection_arguments_should_return_empty_when_unknown_conn_types(self):
        conn_args = {'unix1': [('/var/run/postgresql', '5432')], 'tcp1': [('127.0.0.1', 5432)]}
        finder = DBConnectionFinder('workdir', 1049, '9.3', 'username', 'atlas')
        available_connection = finder.pick_connection_arguments(conn_args)
        self.assertEqual({}, available_connection)

    @mock.patch('pg_view.models.db_client.DBConnectionFinder.can_connect_with_connection_arguments')
    def test_pick_connection_arguments_should_return_first_available_conn_when_multiple(self, mocked_can_connect):
        mocked_can_connect.side_effect = [False, True]
        conn_args = {
            'unix': [('/var/run/postgresql', '5432')], 'tcp': [('127.0.0.1', 5431)]
        }

        finder = DBConnectionFinder('workdir', 1049, '9.3', 'username', 'atlas')
        available_connection = finder.pick_connection_arguments(conn_args)
        self.assertEqual({'host': '127.0.0.1', 'port': 5431}, available_connection)

    @mock.patch('pg_view.models.db_client.logger')
    @mock.patch('pg_view.models.db_client.psycopg2.connect', side_effect=psycopg2.OperationalError)
    def test_can_connect_with_connection_arguments_should_return_false_when_no_connection(self, mocked_psycopg2_connect,
                                                                                          mocked_logger):
        finder = DBConnectionFinder('workdir', 1049, '9.3', 'username', 'atlas')
        connection_builder = prepare_connection_params(host='127.0.0.1', port=5431)
        can_connect = finder.can_connect_with_connection_arguments(connection_builder)
        self.assertFalse(can_connect)
        mocked_psycopg2_connect.assert_called_once_with(host='127.0.0.1', port=5431)

    @mock.patch('pg_view.models.db_client.psycopg2.connect')
    def test_can_connect_with_connection_arguments_should_return_true_when_connection_ok(self, mocked_psycopg2_connect):
        finder = DBConnectionFinder('workdir', 1049, '9.3', 'username', 'atlas')
        connection_builder = prepare_connection_params(host='127.0.0.1', port=5431)
        can_connect = finder.can_connect_with_connection_arguments(connection_builder)
        self.assertTrue(can_connect)
        mocked_psycopg2_connect.assert_called_once_with(host='127.0.0.1', port=5431)

    @unittest.skip('psutil')
    @mock.patch('pg_view.models.db_client.logger')
    @mock.patch('pg_view.models.db_client.ProcNetParser')
    def test_detect_with_proc_net_should_return_none_when_no_connections_from_sockets(self, mocked_proc_net_parser,
                                                                                      mocked_logger):
        finder = DBConnectionFinder('workdir', 1049, '9.3', 'username', 'atlas')
        mocked_proc_net_parser.return_value.get_connections_from_sockets.return_value = {}
        conn_param = finder.detect_with_proc_net()

        self.assertIsNone(conn_param)
        expected_msg = 'could not detect connection string from /proc/net for postgres process 1049'
        mocked_logger.error.assert_called_once_with(expected_msg)

    @unittest.skip('psutil')
    @mock.patch('pg_view.models.db_client.ProcNetParser')
    def test_detect_with_proc_net_should_return_result_when_connections_from_socket(self, mocked_proc_net_parser):
        finder = DBConnectionFinder('workdir', 1049, '9.3', 'username', 'atlas')
        mocked_proc_net_parser.return_value.get_connections_from_sockets.return_value = {
            'unix': [('/var/run/postgresql', '5432')]}
        conn_param = finder.detect_with_proc_net()

        self.assertEqual({'unix': [('/var/run/postgresql', '5432')]}, conn_param)


class DBClientTest(TestCase):
    def test_from_config_should_init_class_properly(self):
        config = {'host': 'localhost', 'port': '5432', 'user': 'user', 'database': 'db'}
        client = DBClient.from_config(config)
        self.assertIsInstance(client, DBClient)
        self.assertEqual(config, client.connection_params)

    def test_from_options_should_init_class_properly(self):
        options = mock.Mock(host='localhost', port='5432', username='user', dbname='db')
        client = DBClient.from_options(options)
        self.assertIsInstance(client, DBClient)
        self.assertEqual(
            {'host': 'localhost', 'port': '5432', 'user': 'user', 'database': 'db'},
            client.connection_params
        )

    @mock.patch('pg_view.models.db_client.DBConnectionFinder')
    def test_from_postmasters_should_return_none_when_no_detected_connection(self, mocked_db_finder):
        mocked_db_finder.return_value.detect_db_connection_arguments.return_value = None
        options = mock.Mock(username='username', dbname='db')
        client = DBClient.from_postmasters('/var/lib/postgresql/9.3/main', 1056, 9.3, options)
        self.assertIsNone(client)
        mocked_db_finder.assert_called_once_with('/var/lib/postgresql/9.3/main', 1056, 9.3, 'username', 'db')

    @mock.patch('pg_view.models.db_client.DBConnectionFinder')
    def test_from_postmasters_should_return_instance_when_detected_connection(self, mocked_db_finder):
        mocked_db_finder.return_value.detect_db_connection_arguments.return_value = {
            'host': 'localhost', 'port': '5432', 'user': 'user1', 'database': 'db1'}
        options = mock.Mock(username='username', dbname='db')
        client = DBClient.from_postmasters('/var/lib/postgresql/9.3/main', 1056, 9.3, options)
        self.assertIsInstance(client, DBClient)
        self.assertEqual(
            {'host': 'localhost', 'port': '5432', 'user': 'username', 'database': 'db'},
            client.connection_params
        )

    @mock.patch('pg_view.models.db_client.logger')
    @mock.patch('pg_view.models.db_client.psycopg2')
    def test_establish_user_defined_connection_should_raise_error_when_cant_connect(self, mocked_psycopg2,
                                                                                    mocked_logger):
        mocked_psycopg2.connect.side_effect = Exception
        client = DBClient.from_config({'host': 'localhost', 'port': '5432', 'user': 'user', 'database': 'db'})
        with self.assertRaises(NotConnectedError):
            client.establish_user_defined_connection('instance', [])

        expected_msg = "failed to establish connection to instance via {0}".format(client.connection_params)
        mocked_logger.error.assert_has_calls([mock.call(expected_msg), mock.call('PostgreSQL exception: ')])

    @mock.patch('pg_view.models.db_client.logger')
    @mock.patch('pg_view.models.db_client.psycopg2')
    @mock.patch('pg_view.models.db_client.read_postmaster_pid')
    def test_establish_user_defined_connection_should_raise_error_when_not_pid_postmaster(self,
                                                                                          mocked_read_postmaster_pid,
                                                                                          mocked_psycopg2,
                                                                                          mocked_logger):
        mocked_psycopg2.connect.return_value = mock.Mock(
            **{'cursor.return_value': mock.MagicMock(), 'server_version': 93})
        mocked_read_postmaster_pid.return_value = None

        client = DBClient.from_config({'host': 'localhost', 'port': '5432', 'user': 'user', 'database': 'db'})
        with self.assertRaises(NoPidConnectionError):
            client.establish_user_defined_connection('default', [])

        expected_msg = "failed to read pid of the postmaster on {0}".format(client.connection_params)
        mocked_logger.error.assert_called_once_with(expected_msg)

    @mock.patch('pg_view.models.db_client.logger')
    @mock.patch('pg_view.models.db_client.psycopg2')
    @mock.patch('pg_view.models.db_client.read_postmaster_pid')
    def test_establish_user_defined_connection_should_raise_error_when_duplicated_connections(self,
                                                                                              mocked_read_postmaster_pid,
                                                                                              mocked_psycopg2,
                                                                                              mocked_logger):
        mocked_psycopg2.connect.return_value = mock.Mock(
            **{'cursor.return_value': mock.MagicMock(), 'server_version': 93})
        mocked_read_postmaster_pid.return_value = 10

        client = DBClient.from_config({'host': 'localhost', 'port': '5432', 'user': 'user', 'database': 'db'})
        with self.assertRaises(DuplicatedConnectionError):
            client.establish_user_defined_connection('default', [{'pid': 10, 'name': 'cluster1'}])

        expected_msg = "duplicate connection options detected  for databases default and cluster1, same pid 10, skipping default"
        mocked_logger.error.assert_called_once_with(expected_msg)

    @mock.patch('pg_view.models.db_client.psycopg2')
    @mock.patch('pg_view.models.db_client.read_postmaster_pid')
    def test_establish_user_defined_connection_should_create_cluster_desc_when_ok(self, mocked_read_postmaster_pid,
                                                                                  mocked_psycopg2):
        cursor = mock.MagicMock(**{'fetchone.return_value': ['/var/lib/postgresql/9.3/main']})
        pg_con = mock.Mock(**{'cursor.return_value': cursor, 'server_version': 90314})
        mocked_psycopg2.connect.return_value = pg_con
        mocked_read_postmaster_pid.return_value = 10

        client = DBClient.from_config({'host': 'localhost', 'port': '5432', 'user': 'user', 'database': 'db'})
        expected_cluster_desc = {
            'name': 'default', 'ver': 9.3, 'wd': '/var/lib/postgresql/9.3/main', 'pid': 10, 'pgcon': pg_con}
        cluster_desc = client.establish_user_defined_connection('default', [{'pid': 11, 'name': 'cluster1'}])
        cluster_desc.pop('reconnect')
        self.assertDictEqual(
            expected_cluster_desc, cluster_desc)

    def test_execute_query_and_fetchone_should_call_show_command(self):
        client = DBClient.from_config({'host': 'localhost', 'port': '5432', 'user': 'user', 'database': 'db'})
        cursor = mock.Mock(**{'fetchone.return_value': ['/var/lib/postgresql/9.3/main']})
        pg_conn = mock.Mock(**{'cursor.return_value': cursor})
        work_directory = client.execute_query_and_fetchone(pg_conn)
        cursor.execute.assert_called_once_with('SHOW DATA_DIRECTORY')
        cursor.close.assert_called_once_with()
        pg_conn.commit.assert_called_once_with()
        self.assertEqual('/var/lib/postgresql/9.3/main', work_directory)
