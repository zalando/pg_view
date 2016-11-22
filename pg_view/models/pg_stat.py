import os
import resource
import sys

import psutil
import psycopg2
import psycopg2.extras
import re

from pg_view.models.base import StatCollector, COLALIGN, logger, COLSTATUS
from pg_view.sqls import SELECT_PGSTAT_VERSION_LESS_THAN_92, SELECT_PGSTAT_VERSION_LESS_THAN_96, \
    SELECT_PGSTAT_NEVER_VERSION, SELECT_PG_IS_IN_RECOVERY, SHOW_MAX_CONNECTIONS

MEM_PAGE_SIZE = resource.getpagesize()
PAGESIZE = os.sysconf("SC_PAGE_SIZE")

if sys.hexversion >= 0x03000000:
    long = int
    maxsize = sys.maxsize
else:
    maxsize = sys.maxint


def dbversion_as_float(pgcon):
    version_num = pgcon.server_version
    version_num /= 100
    return float('{0}.{1}'.format(version_num / 100, version_num % 100))


class PgStatCollector(StatCollector):
    """ Collect PostgreSQL-related statistics """

    def __init__(self, pgcon, reconnect, pid, dbname, dbver, always_track_pids):
        super(PgStatCollector, self).__init__()
        self.postmaster_pid = pid
        self.pgcon = pgcon
        self.reconnect = reconnect
        self.pids = []
        self.rows_diff = []

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
            {'out': 'status'},
            {'out': 'utime', 'fn': self.unit_converter.ticks_to_seconds},
            {'out': 'stime', 'fn': self.unit_converter.ticks_to_seconds},
            {'out': 'rss', 'fn': int},
            {'out': 'read_bytes', 'fn': int, 'optional': True},
            {'out': 'write_bytes', 'fn': int, 'optional': True},
            {'out': 'priority', 'fn': int},
            {'out': 'starttime', 'fn': long},
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
                'out': 's',
                'in': 'state',
                'pos': 2,
                'status_fn': self.check_ps_state,
                'warning': 'D',
            },
            {
                'out': 'utime',
                'in': 'utime',
                'units': '%',
                'fn': self.unit_converter.time_diff_to_percent,
                'round': StatCollector.RD,
                'pos': 4,
                'warning': 90,
                'align': COLALIGN.ca_right,
            },
            {
                'out': 'stime',
                'in': 'stime',
                'units': '%',
                'fn': self.unit_converter.time_diff_to_percent,
                'round': StatCollector.RD,
                'pos': 5,
                'warning': 5,
                'critical': 30,
            },
            {
                'out': 'guest',
                'in': 'guest_time',
                'units': '%',
                'fn': self.unit_converter.time_diff_to_percent,
                'round': StatCollector.RD,
                'pos': 6,
            },
            {
                'out': 'delay_blkio',
                'in': 'delayacct_blkio_ticks',
                'units': '/s',
                'round': StatCollector.RD,
            },
            {
                'out': 'read',
                'in': 'read_bytes',
                'units': 'MB/s',
                'fn': self.unit_converter.bytes_to_mbytes,
                'round': StatCollector.RD,
                'pos': 7,
                'noautohide': True,
            },
            {
                'out': 'write',
                'in': 'write_bytes',
                'units': 'MB/s',
                'fn': self.unit_converter.bytes_to_mbytes,
                'round': StatCollector.RD,
                'pos': 8,
                'noautohide': True,
            },
            {
                'out': 'uss',
                'in': 'uss',
                'units': 'MB',
                'fn': self.unit_converter.bytes_to_mbytes,
                'round': StatCollector.RD,
                'pos': 9,
                'noautohide': True
            },
            {
                'out': 'age',
                'in': 'age',
                'noautohide': True,
                'pos': 9,
                'fn': StatCollector.time_pretty_print,
                'status_fn': self.age_status_fn,
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
                'fn': self.idle_format_fn,
                'warning': 'idle in transaction',
                'critical': 'locked',
                'status_fn': self.query_status_fn,
            },
        ]

        self.ncurses_custom_fields = {'header': True, 'prefix': None}
        self.postinit()

    @classmethod
    def from_cluster(cls, cluster, pid):
        return cls(cluster['pgcon'], cluster['reconnect'], cluster['pid'], cluster['name'], cluster['ver'], pid)

    def get_subprocesses_pid(self):
        ppid = self.postmaster_pid
        result = self.exec_command_with_output('ps -o pid --ppid {0} --noheaders'.format(ppid))
        if result[0] != 0:
            logger.info("Couldn't determine the pid of subprocesses for {0}".format(ppid))
            self.pids = []
        self.pids = [int(x) for x in result[1].split()]

    def check_ps_state(self, row, col):
        if row[self.output_column_positions[col['out']]] == col.get('warning', ''):
            return {0: COLSTATUS.cs_warning}
        return {0: COLSTATUS.cs_ok}

    def age_status_fn(self, row, col):
        age_string = row[self.output_column_positions[col['out']]]
        age_seconds = self.time_field_to_seconds(age_string)
        if 'critical' in col and col['critical'] < age_seconds:
            return {-1: COLSTATUS.cs_critical}
        if 'warning' in col and col['warning'] < age_seconds:
            return {-1: COLSTATUS.cs_warning}
        return {-1: COLSTATUS.cs_ok}

    def idle_format_fn(self, text):
        r = re.match(r'idle in transaction (\d+)', text)
        if not r:
            return text
        else:
            if self.dbver >= 9.2:
                return 'idle in transaction for ' + StatCollector.time_pretty_print(int(r.group(1)))
            else:
                return 'idle in transaction ' + StatCollector.time_pretty_print(int(r.group(1))) \
                       + ' since the last query start'

    def query_status_fn(self, row, col):
        if row[self.output_column_positions['w']] is True:
            return {-1: COLSTATUS.cs_critical}
        else:
            val = row[self.output_column_positions[col['out']]]
            if val and val.startswith(col.get('warning', '!')):
                return {-1: COLSTATUS.cs_warning}
        return {-1: COLSTATUS.cs_ok}

    def ident(self):
        return '{0} ({1}/{2})'.format('postgres', self.dbname, self.dbver)

    @staticmethod
    def _get_psinfo(cmdline):
        """ gets PostgreSQL process type from the command-line."""
        pstype = 'unknown'
        action = None
        if cmdline is not None and len(cmdline) > 0:
            # postgres: stats collector process
            m = re.match(r'postgres:\s+(.*)\s+process\s*(.*)$', cmdline)
            if m:
                pstype = m.group(1)
                action = m.group(2)
            else:
                if re.match(r'postgres:.*', cmdline):
                    # assume it's a backend process
                    pstype = 'backend'
        if pstype == 'autovacuum worker':
            pstype = 'autovacuum'
        return pstype, action

    @staticmethod
    def _is_auxiliary_process(pstype):
        return pstype not in ('backend', 'autovacuum')

    def set_aux_processes_filter(self, newval):
        self.filter_aux_processes = newval

    def ncurses_filter_row(self, row):
        return self._is_auxiliary_process(row['type']) if self.filter_aux_processes else False

    def refresh(self):
        """ Reads data from /proc and PostgreSQL stats """
        result = []
        # fetch up-to-date list of subprocess PIDs
        self.get_subprocesses_pid()
        try:
            if not self.pgcon:
                # if we've lost the connection, try to reconnect and
                # re-initialize all connection invariants
                self.pgcon, self.postmaster_pid = self.reconnect()
                self.connection_pid = self.pgcon.get_backend_pid()
                self.max_connections = self._get_max_connections()
                self.dbver = dbversion_as_float(self.pgcon)
                self.server_version = self.pgcon.get_parameter_status('server_version')
            stat_data = self._read_pg_stat_activity()
        except psycopg2.OperationalError as e:
            logger.info("failed to query the server: {}".format(e))
            if self.pgcon and not self.pgcon.closed:
                self.pgcon.close()
            self.pgcon = None
            self._do_refresh([])
            return

        logger.info("new refresh round")
        for pid in self.pids:
            if pid == self.connection_pid:
                continue
            is_backend = pid in stat_data
            is_active = is_backend and (stat_data[pid]['query'] != 'idle' or pid in self.always_track_pids)
            result_row = {}
            # for each pid, get hash row from /proc/
            proc_data = self._read_proc(pid, is_backend, is_active)
            if proc_data:
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

    def _read_proc(self, pid, is_backend, is_active):
        """ see man 5 proc for details (/proc/[pid]/stat) """
        result = {}
        process = psutil.Process(pid)
        io_stats = process.io_counters()

        proc_stats = {
            'read_bytes': io_stats.read_bytes,
            'write_bytes': io_stats.write_bytes,

            'pid': process.pid,
            'status': process.status(),
            'utime': process.cpu_times().user,
            'stime': process.cpu_times().system,
            'rss': process.memory_info().rss / PAGESIZE,
            'priority': process.nice(),
            'vsize': process.memory_info().vms,

            # TODO: Check if correct
            'locked_by': process.username,
            'guest_time': 0.0,
            'starttime': 911L,
            'delayacct_blkio_ticks': 1,
        }

        # Assume we managed to read the row if we can get its PID
        result.update(self._transform_input(proc_stats))
        result['cmdline'] = process.cmdline()[0].strip()

        if not is_backend:
            result['type'], action = self._get_psinfo(result['cmdline'])
            if action:
                result['query'] = action
        else:
            result['type'] = 'backend'
        if is_active or not is_backend:
            result['uss'] = self._get_memory_usage(pid)
        return result

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
        uss = long(memory_info.rss) - long(memory_info.shared)
        return uss

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
        sql_pgstat = self.get_sql_by_pg_version()
        cur.execute(sql_pgstat)
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

    def get_sql_by_pg_version(self):
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

    @staticmethod
    def process_sort_key(process):
        return process.get('age', maxsize)

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
            self.rows_diff.sort(key=self.process_sort_key, reverse=True)
        else:
            blocked_temp = []
            # we traverse the tree of blocked processes in a depth-first order, building a list
            # to display the blocked processes near the blockers. The reason we need multiple
            # loops here is because there is no way to quickly fetch the processes blocked
            # by the current one from the plain list of process information rows, that's why
            # we use a dictionary of lists of blocked processes with a blocker pid as a key
            # and effectively build a separate tree for each blocker.
            self.running_diffs.sort(key=self.process_sort_key, reverse=True)
            # sort elements in the blocked lists, so that they still appear in the latest to earliest order
            for key in self.blocked_diffs:
                self.blocked_diffs[key].sort(key=self.process_sort_key)
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

    def output(self, method):
        return super(self.__class__, self).output(method, before_string='PostgreSQL processes:', after_string='\n')
