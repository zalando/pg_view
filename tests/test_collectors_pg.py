import datetime
import unittest
from collections import namedtuple
from unittest import TestCase

import mock
# import psutil
import psycopg2

from pg_view.collectors.pg_collector import PgStatCollector
from pg_view.sqls import SHOW_MAX_CONNECTIONS, SELECT_PG_IS_IN_RECOVERY, SELECT_PGSTAT_VERSION_LESS_THAN_92, \
    SELECT_PGSTAT_VERSION_LESS_THAN_96, SELECT_PGSTAT_NEVER_VERSION
from pg_view.utils import dbversion_as_float

pmem = namedtuple('pmem', ['rss', 'vms', 'shared', 'text', 'lib', 'data', 'dirty'])
pio = namedtuple('pio', ['read_count', 'write_count', 'read_bytes', 'write_bytes'])
pcputimes = namedtuple('pcputimes', ['user', 'system', 'children_user', 'children_system'])


class PgStatCollectorTest(TestCase):
    def setUp(self):
        super(PgStatCollectorTest, self).setUp()
        self.cluster = {
            'ver': 9.3,
            'name': '/var/lib/postgresql/9.3/main',
            'pid': 1049,
            'reconnect': mock.Mock(),
            'pgcon': mock.MagicMock(),
        }

    def test_dbversion_as_float_should_return_formatted_version_from_pgcon_version(self):
        self.assertEqual(9.3, dbversion_as_float(90314))

    def test__get_psinfo_should_return_empty_when_no_cmdline(self):
        pstype, action = PgStatCollector.from_cluster(self.cluster, 1049)._get_psinfo('')
        self.assertEqual('unknown', pstype)
        self.assertIsNone(action)

    def test__get_psinfo_should_return_pstype_action_when_cmdline_matches_postgres(self):
        collector = PgStatCollector.from_cluster(self.cluster, 1049)
        pstype, action = collector._get_psinfo('postgres: back')
        self.assertEqual('backend', pstype)
        self.assertIsNone(action)

    def test__get_psinfo_should_return_pstype_action_when_cmdline_matches_postgres_process(self):
        collector = PgStatCollector.from_cluster(self.cluster, 1049)
        pstype, action = collector._get_psinfo('postgres: checkpointer process')
        self.assertEqual('checkpointer', pstype)
        self.assertEqual('', action)

    def test__get_psinfo_should_return_pstype_action_when_cmdline_matches_autovacuum_worker(self):
        collector = PgStatCollector.from_cluster(self.cluster, 1049)
        pstype, action = collector._get_psinfo('postgres: autovacuum worker process')
        self.assertEqual('autovacuum', pstype)
        self.assertEqual('', action)

    def test__get_psinfo_should_return_unknown_when_cmdline_not_match(self):
        collector = PgStatCollector.from_cluster(self.cluster, 1049)
        pstype, action = collector._get_psinfo('postgres1: worker process')
        self.assertEqual('unknown', pstype)
        self.assertIsNone(action)

    @unittest.skip('psutil')
    @mock.patch('pg_view.collectors.pg_collector.psutil.Process')
    @mock.patch('pg_view.collectors.pg_collector.logger')
    def test_get_subprocesses_pid_should_return_empty_when_no_cmd_output(self, mocked_logger, mocked_process):
        mocked_process.return_value.children.return_value = []
        collector = PgStatCollector.from_cluster(self.cluster, 1049)
        self.assertEqual([], collector.get_subprocesses_pid())
        mocked_logger.info.assert_called_with("Couldn't determine the pid of subprocesses for 1049")

    @unittest.skip('psutil')
    @mock.patch('pg_view.collectors.pg_collector.psutil.Process')
    def test_get_subprocesses_pid_should_return_subprocesses_when_children_processes(self, mocked_process):
        subprocesses = [mock.Mock(pid=1051), mock.Mock(pid=1052), mock.Mock(pid=1206)]
        mocked_process.return_value.children.return_value = subprocesses
        collector = PgStatCollector.from_cluster(self.cluster, 1049)
        self.assertEqual([1051, 1052, 1206], collector.get_subprocesses_pid())

    @unittest.skip('psutil')
    @mock.patch('pg_view.collectors.pg_collector.psutil.Process')
    def test__get_memory_usage_should_return_uss_when_memory_info_ok(self, mocked_psutil_process):
        mocked_psutil_process.return_value.memory_info.return_value = pmem(
            rss=1769472, vms=252428288, shared=344064, text=5492736, lib=0, data=1355776, dirty=0)
        collector = PgStatCollector.from_cluster(self.cluster, 1049)
        memory_usage = collector._get_memory_usage(1049)
        self.assertEqual(1425408, memory_usage)

    @mock.patch('pg_view.collectors.pg_collector.PgStatCollector._execute_fetchone_query', return_value={})
    def test__get_max_connections_should_return_zero_when_no_output(self, mocked_execute_fetchone_query):
        collector = PgStatCollector.from_cluster(self.cluster, 1049)
        self.assertEqual(0, collector._get_max_connections())
        mocked_execute_fetchone_query.assert_called_with(SHOW_MAX_CONNECTIONS)

    @mock.patch('pg_view.collectors.pg_collector.PgStatCollector._execute_fetchone_query',
                return_value={'max_connections': '1'})
    def test__get_max_connections_should_return_zero_when_output_ok(self, mocked_execute_fetchone_query):
        collector = PgStatCollector.from_cluster(self.cluster, 1049)
        self.assertEqual(1, collector._get_max_connections())
        mocked_execute_fetchone_query.assert_called_with(SHOW_MAX_CONNECTIONS)

    @mock.patch('pg_view.collectors.pg_collector.PgStatCollector._execute_fetchone_query', return_value={})
    def test__get_recovery_status_should_return_unknown_when_no_output(self, mocked_execute_fetchone_query):
        collector = PgStatCollector.from_cluster(self.cluster, 1049)
        self.assertEqual('unknown', collector._get_recovery_status())
        mocked_execute_fetchone_query.assert_called_with(SELECT_PG_IS_IN_RECOVERY)

    @mock.patch('pg_view.collectors.pg_collector.PgStatCollector._execute_fetchone_query', return_value={'role': 'role'})
    def test__get_recovery_status_should_return_zero_when_output_ok(self, mocked_execute_fetchone_query):
        collector = PgStatCollector.from_cluster(self.cluster, 1049)
        self.assertEqual('role', collector._get_recovery_status())
        mocked_execute_fetchone_query.assert_called_with(SELECT_PG_IS_IN_RECOVERY)

    def test_get_sql_by_pg_version_should_return_92_when_dbver_less_than_92(self):
        cluster = self.cluster.copy()
        cluster['ver'] = 9.1
        collector = PgStatCollector.from_cluster(cluster, 1049)
        self.assertEqual(SELECT_PGSTAT_VERSION_LESS_THAN_92, collector.get_sql_pgstat_by_version())

    def test_get_sql_by_pg_version_should_return_less_than_96_when_dbver_95(self):
        cluster = self.cluster.copy()
        cluster['ver'] = 9.5
        collector = PgStatCollector.from_cluster(cluster, 1049)
        self.assertEqual(SELECT_PGSTAT_VERSION_LESS_THAN_96, collector.get_sql_pgstat_by_version())

    def test_get_sql_by_pg_version_should_return_newer_when_bigger_than_96(self):
        cluster = self.cluster.copy()
        cluster['ver'] = 9.7
        collector = PgStatCollector.from_cluster(cluster, 1049)
        self.assertEqual(SELECT_PGSTAT_NEVER_VERSION, collector.get_sql_pgstat_by_version())

    @unittest.skip('psutil')
    @mock.patch('pg_view.collectors.pg_collector.os')
    @mock.patch('pg_view.collectors.pg_collector.psutil.Process')
    def test__read_proc_should_return_data_when_process_ok(self, mocked_psutil_process, mocked_os):
        collector = PgStatCollector.from_cluster(self.cluster, 1049)
        mocked_os.sysconf.return_value.SC_PAGE_SIZE = 4096
        mocked_process = mocked_psutil_process.return_value
        mocked_process.pid = 1049
        mocked_process.status.return_value = 'status'
        mocked_process.io_counters.return_value = pio(
            read_count=12, write_count=13, read_bytes=655, write_bytes=1)
        cpu_times = pcputimes(user=0.02, system=0.01, children_user=0.0, children_system=0.0)
        memory_info = pmem(
            rss=1769472, vms=252428288, shared=344064, text=5492736, lib=0, data=1355776, dirty=0)
        mocked_process.cpu_times.return_value = cpu_times
        mocked_process.memory_info.return_value = memory_info
        mocked_process.nice.return_value = '10'
        mocked_process.cmdline.return_value = ['backend \n']
        mocked_process.create_time.return_value = 1480777289.0
        proc_stats = collector.get_proc_data(1048)
        expected_proc_stats = {
            'read_bytes': 655,
            'write_bytes': 1,

            'pid': 1049,
            'state': 'status',
            'utime': 0.0002,
            'stime': 0.0001,
            'rss': 432,
            'priority': 10,
            'vsize': 252428288,

            'guest_time': 0.0,
            'starttime': datetime.datetime.fromtimestamp(1480777289.0),
            'delayacct_blkio_ticks': 0,
            'cmdline': 'backend'
        }
        self.assertEqual(expected_proc_stats, proc_stats)

    @unittest.skip('psutil')
    # @unittest.skipUnless(psutil.LINUX, 'Linux only')
    @mock.patch('pg_view.collectors.pg_collector.PgStatCollector._get_psinfo')
    @mock.patch('pg_view.collectors.pg_collector.PgStatCollector._get_memory_usage')
    def test_get_additional_info_should_update_when_not_backend_and_action(self, mocked__get_memory_usage,
                                                                           mocked__get_psinfo):
        collector = PgStatCollector.from_cluster(self.cluster, 1049)
        mocked__get_psinfo.return_value = ('vacuum', 'query')
        mocked__get_memory_usage.return_value = 10
        info = collector.get_additional_proc_info(1049, {'cmdline': ''}, [10])
        self.assertEqual({'type': 'vacuum', 'query': 'query', 'cmdline': '', 'uss': 10}, info)

    @unittest.skip('psutil')
    # @unittest.skipUnless(psutil.LINUX, 'Linux only')
    @mock.patch('pg_view.collectors.pg_collector.PgStatCollector._get_psinfo')
    @mock.patch('pg_view.collectors.pg_collector.PgStatCollector._get_memory_usage')
    def test_get_additional_info_should_update_when_not_backend_and_not_action(self, mocked__get_memory_usage,
                                                                               mocked__get_psinfo):
        collector = PgStatCollector.from_cluster(self.cluster, 1049)
        mocked__get_psinfo.return_value = ('vacuum', None)
        mocked__get_memory_usage.return_value = 10
        info = collector.get_additional_proc_info(1049, {'cmdline': ''}, [10])
        self.assertEqual({'type': 'vacuum', 'cmdline': '', 'uss': 10}, info)

    @unittest.skip('psutil')
    @mock.patch('pg_view.collectors.pg_collector.PgStatCollector._get_psinfo')
    @mock.patch('pg_view.collectors.pg_collector.PgStatCollector._get_memory_usage')
    def test_get_additional_info_should_update_when_backend_and_not_active(self, mocked__get_memory_usage,
                                                                           mocked__get_psinfo):
        collector = PgStatCollector.from_cluster(self.cluster, [1011])
        mocked__get_psinfo.return_value = ('vacuum', None)
        mocked__get_memory_usage.return_value = 10
        info = collector.get_additional_proc_info(1049, {'cmdline': ''}, {1049: {'query': 'idle'}})
        self.assertEqual({'type': 'backend', 'cmdline': ''}, info)

    @unittest.skip('psutil')
    # @unittest.skipUnless(psutil.LINUX, 'Linux only')
    @mock.patch('pg_view.collectors.pg_collector.PgStatCollector._get_psinfo')
    @mock.patch('pg_view.collectors.pg_collector.PgStatCollector._get_memory_usage')
    def test_get_additional_info_should_update_when_backend_and_active_query_not_idle(self, mocked__get_memory_usage,
                                                                                      mocked__get_psinfo):
        collector = PgStatCollector.from_cluster(self.cluster, [1011])
        mocked__get_psinfo.return_value = ('vacuum', None)
        mocked__get_memory_usage.return_value = 10
        info = collector.get_additional_proc_info(1049, {'cmdline': ''}, {1049: {'query': 'not idle'}})
        self.assertEqual({'type': 'backend', 'cmdline': '', 'uss': 10}, info)

    @unittest.skip('psutil')
    # @unittest.skipUnless(psutil.LINUX, 'Linux only')
    @mock.patch('pg_view.collectors.pg_collector.PgStatCollector._get_psinfo')
    @mock.patch('pg_view.collectors.pg_collector.PgStatCollector._get_memory_usage')
    def test_get_additional_info_should_update_when_backend_and_active_pid_in_track_pids(self, mocked__get_memory_usage,
                                                                                         mocked__get_psinfo):
        collector = PgStatCollector.from_cluster(self.cluster, [1049])
        mocked__get_psinfo.return_value = ('vacuum', None)
        mocked__get_memory_usage.return_value = 10
        info = collector.get_additional_proc_info(1049, {'cmdline': ''}, {1049: {'query': 'idle'}})
        self.assertEqual({'type': 'backend', 'cmdline': '', 'uss': 10}, info)

    def test__read_pg_stat_activity_should_parse_pg_stats_when_ok(self):
        results = [{
            'datname': 'postgres',
            'client_addr': None,
            'locked_by': None,
            'pid': 11139,
            'waiting': False,
            'client_port': -1,
            'query': 'idle',
            'age': None,
            'usename': 'postgres'
        }]

        self.cluster['pgcon'].cursor.return_value.fetchall.return_value = results
        collector = PgStatCollector.from_cluster(self.cluster, [1049])
        activity_stats = collector._read_pg_stat_activity()
        expected_stats = {
            11139: {
                'datname': 'postgres',
                'client_addr': None,
                'locked_by': None,
                'pid': 11139,
                'waiting': False,
                'client_port': -1,
                'query': 'idle',
                'age': None,
                'usename': 'postgres'
            }
        }

        self.assertEqual(expected_stats, activity_stats)

    def test_ncurses_produce_prefix_should_return_offline_when_no_pgcon(self):
        self.cluster['pgcon'].get_parameter_status.return_value = '9.3'
        collector = PgStatCollector.from_cluster(self.cluster, [1049])
        collector.pgcon = None
        self.assertEqual('/var/lib/postgresql/9.3/main 9.3 (offline)\n', collector.ncurses_produce_prefix())

    @mock.patch('pg_view.collectors.pg_collector.PgStatCollector._get_max_connections', return_value=10)
    @mock.patch('pg_view.collectors.pg_collector.PgStatCollector._get_recovery_status', return_value='role')
    def test_ncurses_produce_prefix_should_return_online_when_pgcon(self, mocked__status, mocked__max_conn):
        self.cluster['pgcon'].get_parameter_status.return_value = '9.3'
        collector = PgStatCollector.from_cluster(self.cluster, [1049])
        self.assertEqual(
            '/var/lib/postgresql/9.3/main 9.3 role connections: 0 of 10 allocated, 0 active\n',
            collector.ncurses_produce_prefix()
        )

    @unittest.skip('psutil')
    # @unittest.skipUnless(psutil.LINUX, 'Linux only')
    @mock.patch('pg_view.collectors.pg_collector.PgStatCollector._get_memory_usage', return_value=10)
    @mock.patch('pg_view.collectors.pg_collector.PgStatCollector._read_pg_stat_activity')
    @mock.patch('pg_view.collectors.pg_collector.PgStatCollector.get_subprocesses_pid', return_value=[1010])
    @mock.patch('pg_view.collectors.pg_collector.PgStatCollector.get_proc_data')
    @mock.patch('pg_view.collectors.pg_collector.PgStatCollector._do_refresh')
    def test_refresh_should_return_results_when_ok(self, mocked__do_refresh, mocked_get_proc_data,
                                                   mocked_get_subprocesses_pid, mocked__read_pg_stat_activity,
                                                   mocked___get_memory_usage):
        mocked_get_proc_data.return_value = {
            'read_bytes': 655,
            'write_bytes': 1,
            'pid': 1049,
            'status': 'status',
            'utime': 0.0002,
            'stime': 0.0001,
            'rss': 432,
            'priority': 10,
            'vsize': 252428288,
            'guest_time': 0.0,
            'starttime': 911,
            'delayacct_blkio_ticks': 1,
            'cmdline': 'backend'
        }

        mocked__read_pg_stat_activity.return_value = {
            11139: {
                'datname': 'postgres',
                'client_addr': None,
                'locked_by': None,
                'pid': 11139,
                'waiting': False,
                'client_port': -1,
                'query': 'idle',
                'age': None,
                'usename': 'postgres'
            }
        }

        collector = PgStatCollector.from_cluster(self.cluster, [1049])
        result = collector.refresh()
        expected_results = [{
            'status': 'status',
            'write_bytes': 1,
            'vsize': 252428288,
            'delayacct_blkio_ticks': 1,
            'pid': 1049,
            'priority': 10,
            'cmdline': 'backend',
            'read_bytes': 655,
            'uss': 10,
            'stime': 0.0001,
            'starttime': 911,
            'utime': 0.0002,
            'type': 'unknown',
            'guest_time': 0.0,
            'rss': 432
        }]
        self.assertEqual(expected_results, result)
        mocked__do_refresh.assert_called_with(result)

    @unittest.skip('psutil')
    @mock.patch('pg_view.collectors.pg_collector.PgStatCollector._try_reconnect')
    @mock.patch('pg_view.collectors.pg_collector.PgStatCollector._get_memory_usage', return_value=10)
    @mock.patch('pg_view.collectors.pg_collector.PgStatCollector._read_pg_stat_activity')
    @mock.patch('pg_view.collectors.pg_collector.PgStatCollector.get_subprocesses_pid', return_value=[1010])
    @mock.patch('pg_view.collectors.pg_collector.PgStatCollector.get_proc_data')
    @mock.patch('pg_view.collectors.pg_collector.PgStatCollector._do_refresh')
    def test_refresh_should_try_reconnect_whne_no_pgcon(self, mocked__do_refresh, mocked_get_proc_data,
                                                        mocked_get_subprocesses_pid,
                                                        mocked__read_pg_stat_activity, mocked___get_memory_usage,
                                                        mocked_try_reconnect):
        mocked_get_proc_data.return_value = {}
        mocked__read_pg_stat_activity.return_value = {}

        collector = PgStatCollector.from_cluster(self.cluster, [1049])
        collector.pgcon = None
        result = collector.refresh()
        mocked_try_reconnect.assert_called_with()
        mocked__do_refresh.assert_called_with(result)

    @unittest.skip('psutil')
    @mock.patch('pg_view.collectors.pg_collector.PgStatCollector._try_reconnect')
    @mock.patch('pg_view.collectors.pg_collector.PgStatCollector._get_memory_usage', return_value=10)
    @mock.patch('pg_view.collectors.pg_collector.PgStatCollector._read_pg_stat_activity')
    @mock.patch('pg_view.collectors.pg_collector.PgStatCollector.get_subprocesses_pid', return_value=[1010])
    @mock.patch('pg_view.collectors.pg_collector.PgStatCollector.get_proc_data')
    @mock.patch('pg_view.collectors.pg_collector.PgStatCollector._do_refresh')
    def test_refresh_should_return_none_when_try_reconnect_raises_error(self, mocked__do_refresh, mocked_get_proc_data,
                                                                        mocked_get_subprocesses_pid,
                                                                        mocked__read_pg_stat_activity,
                                                                        mocked___get_memory_usage,
                                                                        mocked_try_reconnect):
        mocked_get_proc_data.return_value = {}
        mocked__read_pg_stat_activity.return_value = {}

        collector = PgStatCollector.from_cluster(self.cluster, [1049])
        collector.pgcon = None
        mocked_try_reconnect.side_effect = psycopg2.OperationalError
        result = collector.refresh()
        self.assertIsNone(result)
        mocked__do_refresh.assert_called_with([])
