import resource
import sys
from datetime import datetime

import os
import psutil
import psycopg2
import psycopg2.extras
import re

from pg_view.consts import RD
from pg_view.formatters import StatusFormatter, FnFormatter
from pg_view.models.collector_base import BaseStatCollector, logger
from pg_view.models.displayers import COLALIGN
from pg_view.sqls import SELECT_PGSTAT_VERSION_LESS_THAN_92, SELECT_PGSTAT_VERSION_LESS_THAN_96, \
    SELECT_PGSTAT_NEVER_VERSION, SELECT_PG_IS_IN_RECOVERY, SHOW_MAX_CONNECTIONS

MEM_PAGE_SIZE = resource.getpagesize()
PAGESIZE = os.sysconf("SC_PAGE_SIZE")

if sys.hexversion >= 0x03000000:
    long = int
    maxsize = sys.maxsize
else:
    maxsize = sys.maxint


def dbversion_as_float(server_version):
    version_num = server_version
    version_num /= 100
    return float('{0}.{1}'.format(version_num / 100, version_num % 100))


def process_sort_key(process):
    return process.get('age', maxsize)


class PgStatCollector(BaseStatCollector):
    """ Collect PostgreSQL-related statistics """

    def __init__(self, pgcon, reconnect, pid, dbname, dbver, always_track_pids):
        super(PgStatCollector, self).__init__()
        self.postmaster_pid = pid
        self.pgcon = pgcon
        self.reconnect = reconnect
        self.rows_diff = []
        self.status_formatter = StatusFormatter(self)
        self.fn_formatter = FnFormatter(self)

        # figure out our backend pid
        self.connection_pid = pgcon.get_backend_pid()
        self.max_connections = self._get_max_connections()
        self.recovery_status = self._get_recovery_status()
        self.always_track_pids = always_track_pids
        self.dbname = dbname
        self.dbver = dbver
        self.server_version = pgcon.get_parameter_status('server_version')
        self.filter_aux_processes = True
        self.total_connections = 0
        self.active_connections = 0

        self.transform_dict_data = [
            {'out': 'pid', 'fn': int},
            {'out': 'state'},
            {'out': 'utime', 'fn': self.unit_converter.ticks_to_seconds},
            {'out': 'stime', 'fn': self.unit_converter.ticks_to_seconds},
            {'out': 'rss', 'fn': int},
            {'out': 'read_bytes', 'fn': int, 'optional': True},
            {'out': 'write_bytes', 'fn': int, 'optional': True},
            {'out': 'priority', 'fn': int},
            {'out': 'starttime'},
            {'out': 'vsize', 'fn': int},
            {'out': 'delayacct_blkio_ticks', 'fn': long, 'optional': True},
            {'out': 'guest_time', 'fn': self.unit_converter.ticks_to_seconds, 'optional': True}
        ]

        self.diff_generator_data = [
            {'out': 'pid', 'diff': False},
            {'out': 'type', 'diff': False},
            {'out': 'state', 'diff': False},
            {'out': 'priority', 'diff': False},
            {'out': 'utime'},
            {'out': 'stime'},
            {'out': 'guest_time'},
            {'out': 'delayacct_blkio_ticks'},
            {'out': 'read_bytes'},
            {'out': 'write_bytes'},
            {'out': 'uss', 'diff': False},
            {'out': 'age', 'diff': False},
            {'out': 'datname', 'diff': False},
            {'out': 'usename', 'diff': False},
            {'out': 'waiting', 'diff': False},
            {'out': 'locked_by', 'diff': False},
            {'out': 'query', 'diff': False},
        ]

        self.output_transform_data = [  # query with age 5 and more will have the age column highlighted
            {
                'out': 'pid',
                'in': 'pid',
                'pos': 0,
                'minw': 5,
                'noautohide': True,
            },
            {
                'out': 'lock',
                'in': 'locked_by',
                'pos': 1,
                'minw': 5,
                'noautohide': True,
            },
            {
                'out': 'type',
                'in': 'type',
                'pos': 1
            },
            {
                'out': 'state',
                'in': 'state',
                'pos': 2,
                'status_fn': self.status_formatter.check_ps_state,
                'warning': 'disk-sleep',
            },
            {
                'out': 'utime',
                'in': 'utime',
                'units': '%',
                'fn': self.unit_converter.time_diff_to_percent,
                'round': RD,
                'pos': 4,
                'warning': 90,
                'align': COLALIGN.ca_right,
            },
            {
                'out': 'stime',
                'in': 'stime',
                'units': '%',
                'fn': self.unit_converter.time_diff_to_percent,
                'round': RD,
                'pos': 5,
                'warning': 5,
                'critical': 30,
            },
            {
                'out': 'guest',
                'in': 'guest_time',
                'units': '%',
                'fn': self.unit_converter.time_diff_to_percent,
                'round': RD,
                'pos': 6,
            },
            {
                'out': 'delay_blkio',
                'in': 'delayacct_blkio_ticks',
                'units': '/s',
                'round': RD,
            },
            {
                'out': 'read',
                'in': 'read_bytes',
                'units': 'MB/s',
                'fn': self.unit_converter.bytes_to_mbytes,
                'round': RD,
                'pos': 7,
                'noautohide': True,
            },
            {
                'out': 'write',
                'in': 'write_bytes',
                'units': 'MB/s',
                'fn': self.unit_converter.bytes_to_mbytes,
                'round': RD,
                'pos': 8,
                'noautohide': True,
            },
            {
                'out': 'uss',
                'in': 'uss',
                'units': 'MB',
                'fn': self.unit_converter.bytes_to_mbytes,
                'round': RD,
                'pos': 9,
                'noautohide': True
            },
            {
                'out': 'age',
                'in': 'age',
                'noautohide': True,
                'pos': 9,
                'fn': self.fn_formatter.time_pretty_print,
                'status_fn': self.status_formatter.age_status_fn,
                'align': COLALIGN.ca_right,
                'warning': 300,
            },
            {
                'out': 'db',
                'in': 'datname',
                'pos': 10,
                'noautohide': True,
                'maxw': 14,
            },
            {
                'out': 'user',
                'in': 'usename',
                'pos': 11,
                'noautohide': True,
                'maxw': 14,
            },
            {
                'out': 'w',
                'in': 'waiting',
                'pos': -1,
                'hide_if_ok': True,
            },
            {
                'out': 'query',
                'pos': 12,
                'noautohide': True,
                'fn': self.fn_formatter.idle_format_fn,
                'warning': 'idle in transaction',
                'critical': 'locked',
                'status_fn': self.status_formatter.query_status_fn,
            },
        ]
        self.ncurses_custom_fields = {'header': True, 'prefix': None}
        self.postinit()

    @classmethod
    def from_cluster(cls, cluster, pid):
        return cls(cluster['pgcon'], cluster['reconnect'], cluster['pid'], cluster['name'], cluster['ver'], pid)

    def get_subprocesses_pid(self):
        subprocesses = psutil.Process(self.postmaster_pid).children()
        if not subprocesses:
            logger.info("Couldn't determine the pid of subprocesses for {0}".format(self.postmaster_pid))
            return []
        return [p.pid for p in subprocesses]

    def ident(self):
        return '{0} ({1}/{2})'.format('postgres', self.dbname, self.dbver)

    @staticmethod
    def _get_psinfo(cmdline):
        if not cmdline:
            return 'unknown', None
        m = re.match(r'postgres:\s+(.*)\s+process\s*(.*)$', cmdline)
        if m:
            pstype = m.group(1)
            action = m.group(2)
            return 'autovacuum' if pstype == 'autovacuum worker' else pstype, action
        elif re.match(r'postgres:.*', cmdline):
            # assume it's a backend process
            return 'backend', None
        return 'unknown', None

    @staticmethod
    def _is_auxiliary_process(pstype):
        return pstype not in ('backend', 'autovacuum')

    def set_aux_processes_filter(self, newval):
        self.filter_aux_processes = newval

    def ncurses_filter_row(self, row):
        return self._is_auxiliary_process(row['type']) if self.filter_aux_processes else False

    def refresh(self):
        try:
            if not self.pgcon:
                self._try_reconnect()
            stat_data = self._read_pg_stat_activity()
        except psycopg2.OperationalError as e:
            logger.info("failed to query the server: {}".format(e))
            if self.pgcon and not self.pgcon.closed:
                self.pgcon.close()
            self.pgcon = None
            self._do_refresh([])
            return None

        # fetch up-to-date list of subprocess PIDs
        pids = self.get_subprocesses_pid()
        logger.info("new refresh round")

        result = []
        for pid in pids:
            if pid == self.connection_pid:
                continue
            result_row = {}
            # for each pid, get hash row from /proc/
            proc_data = self.get_proc_data(pid)
            if proc_data:
                self.get_additional_proc_info(pid, proc_data, stat_data)
                result_row.update(proc_data)
            if stat_data and pid in stat_data:
                # ditto for the pg_stat_activity
                result_row.update(stat_data[pid])
            # result is not empty - add it to the list of current rows
            if result_row:
                result.append(result_row)
        # and refresh the rows with this data
        self._do_refresh(result)
        return result

    def get_proc_data(self, pid):
        result = {}
        process = psutil.Process(pid)
        cpu_times = process.cpu_times()
        memory_info = process.memory_info()

        proc_stats = {
            'pid': process.pid,
            'state': process.status(),
            'utime': cpu_times.user,
            'stime': cpu_times.system,
            'rss': memory_info.rss / PAGESIZE,
            'priority': process.nice(),
            'vsize': memory_info.vms,
            'starttime': datetime.fromtimestamp(process.create_time()),
            'locked_by': process.username(),
            'guest_time': cpu_times.guest if hasattr(cpu_times, 'guest') else 0.0,
            'delayacct_blkio_ticks': self.delayactt_blkio_ticks_or_default(cpu_times),
        }

        io_stats = self.get_io_counters(process)
        proc_stats.update(io_stats)
        # Assume we managed to read the row if we can get its PID
        result.update(self._transform_input(proc_stats))
        result['cmdline'] = process.cmdline()[0].strip()
        return result

    def delayactt_blkio_ticks_or_default(self, cpu_times):
        return cpu_times.delayacct_blkio_ticks if hasattr(cpu_times, 'delayacct_blkio_ticks') else 0

    def get_io_counters(self, process):
        if not hasattr(process, 'io_counters'):
            return {}
        io_stats = process.io_counters()
        return {
            'read_bytes': io_stats.read_bytes,
            'write_bytes': io_stats.write_bytes
        }

    def get_additional_proc_info(self, pid, proc_data, stat_data):
        is_backend = pid in stat_data
        is_active = is_backend and (stat_data[pid]['query'] != 'idle' or pid in self.always_track_pids)
        if is_backend:
            proc_data['type'] = 'backend'
        else:
            proc_data['type'], action = self._get_psinfo(proc_data['cmdline'])
            if action:
                proc_data['query'] = action
        if psutil.LINUX and (is_active or not is_backend):
            proc_data['uss'] = self._get_memory_usage(pid)
        return proc_data

    def _try_reconnect(self):
        # if we've lost the connection, try to reconnect and re-initialize all connection invariants
        self.pgcon, self.postmaster_pid = self.reconnect()
        self.connection_pid = self.pgcon.get_backend_pid()
        self.max_connections = self._get_max_connections()
        self.dbver = dbversion_as_float(self.pgcon)
        self.server_version = self.pgcon.get_parameter_status('server_version')

    def _get_memory_usage(self, pid):
        # compute process's own non-shared memory.
        # See http://www.depesz.com/2012/06/09/how-much-ram-is-postgresql-using/ for the explanation of how
        # to measure PostgreSQL process memory usage and the stackexchange answer for details on the unshared counts:
        # http://unix.stackexchange.com/questions/33381/getting-information-about-a-process-memory-usage-from-proc-pid-smaps
        # there is also a good discussion here:
        # http://rhaas.blogspot.de/2012/01/linux-memory-reporting.html
        # we use statm instead of /proc/smaps because of performance considerations. statm is much faster,
        # while providing slightly outdated results.
        memory_info = psutil.Process(pid).memory_info()
        return long(memory_info.rss) - long(memory_info.shared)

    def _get_max_connections(self):
        result = self._execute_fetchone_query(SHOW_MAX_CONNECTIONS)
        return int(result.get('max_connections', 0))

    def _get_recovery_status(self):
        result = self._execute_fetchone_query(SELECT_PG_IS_IN_RECOVERY)
        return result.get('role', 'unknown')

    def _execute_fetchone_query(self, query):
        cur = self.pgcon.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(query)
        result = cur.fetchone()
        cur.close()
        return result

    def _read_pg_stat_activity(self):
        """ Read data from pg_stat_activity """
        self.recovery_status = self._get_recovery_status()
        cur = self.pgcon.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(self.get_sql_pgstat_by_version())
        results = cur.fetchall()

        # fill in the number of total connections, including ourselves
        self.total_connections = len(results) + 1
        self.active_connections = 0
        formatted_results = {}
        for result in results:
            # stick multiline queries together
            if result.get('query'):
                if result['query'] != 'idle':
                    if result['pid'] != self.connection_pid:
                        self.active_connections += 1
                lines = result['query'].splitlines()
                newlines = [re.sub('\s+', ' ', l.strip()) for l in lines]
                result['query'] = ' '.join(newlines)
            formatted_results[result['pid']] = result
        self.pgcon.commit()
        cur.close()
        return formatted_results

    def get_sql_pgstat_by_version(self):
        # the pg_stat_activity format has been changed to 9.2, avoiding ambigiuous meanings for some columns.
        # since it makes more sense then the previous layout, we 'cast' the former versions to 9.2
        if self.dbver < 9.2:
            return SELECT_PGSTAT_VERSION_LESS_THAN_92
        elif self.dbver < 9.6:
            return SELECT_PGSTAT_VERSION_LESS_THAN_96
        return SELECT_PGSTAT_NEVER_VERSION

    def ncurses_produce_prefix(self):
        if self.pgcon:
            return "{dbname} {version} {role} connections: {conns} of {max_conns} allocated, {active_conns} active\n". \
                format(dbname=self.dbname,
                       version=self.server_version,
                       role=self.recovery_status,
                       conns=self.total_connections,
                       max_conns=self.max_connections,
                       active_conns=self.active_connections)
        else:
            return "{dbname} {version} (offline)\n".format(dbname=self.dbname, version=self.server_version)

    def diff(self):
        """ we only diff backend processes if new one is not idle and use pid to identify processes """
        self.rows_diff = []
        self.running_diffs = []
        self.blocked_diffs = {}
        for cur in self.rows_cur:
            if 'query' not in cur or cur['query'] != 'idle' or cur['pid'] in self.always_track_pids:
                # look for the previous row corresponding to the current one
                for x in self.rows_prev:
                    if x['pid'] == cur['pid']:
                        prev = x
                        break
                else:
                    continue
                # now we have a previous and a current row - do the diff
                candidate = self._produce_diff_row(prev, cur)
                if candidate is not None and len(candidate) > 0:
                    if candidate['locked_by'] is None:
                        self.running_diffs.append(candidate)
                    else:
                        # when determining the position where to put the blocked process,
                        # only consider the first blocker. This will provide consustent
                        # results for multiple processes blocked by the same set of blockers,
                        # since the list is sorted by pid.
                        block_pid = int(candidate['locked_by'].split(',')[0])
                        if block_pid not in self.blocked_diffs:
                            self.blocked_diffs[block_pid] = [candidate]
                        else:
                            self.blocked_diffs[block_pid].append(candidate)
        # order the result rows by the start time value
        if len(self.blocked_diffs) == 0:
            self.rows_diff = self.running_diffs
            self.rows_diff.sort(key=process_sort_key, reverse=True)
        else:
            blocked_temp = []
            # we traverse the tree of blocked processes in a depth-first order, building a list
            # to display the blocked processes near the blockers. The reason we need multiple
            # loops here is because there is no way to quickly fetch the processes blocked
            # by the current one from the plain list of process information rows, that's why
            # we use a dictionary of lists of blocked processes with a blocker pid as a key
            # and effectively build a separate tree for each blocker.
            self.running_diffs.sort(key=process_sort_key, reverse=True)
            # sort elements in the blocked lists, so that they still appear in the latest to earliest order
            for key in self.blocked_diffs:
                self.blocked_diffs[key].sort(key=process_sort_key)
            for parent_row in self.running_diffs:
                self.rows_diff.append(parent_row)
                # if no processes blocked by this one - just skip to the next row
                if parent_row['pid'] in self.blocked_diffs:
                    blocked_temp.extend(self.blocked_diffs[parent_row['pid']])
                    del self.blocked_diffs[parent_row['pid']]
                    while len(blocked_temp) > 0:
                        # traverse a tree (in DFS order) of all processes blocked by the current one
                        child_row = blocked_temp.pop()
                        self.rows_diff.append(child_row)
                        if child_row['pid'] in self.blocked_diffs:
                            blocked_temp.extend(self.blocked_diffs[child_row['pid']])
                            del self.blocked_diffs[child_row['pid']]

    def output(self, displayer):
        return super(self.__class__, self).output(displayer, before_string='PostgreSQL processes:', after_string='\n')
