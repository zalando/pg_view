import re
import sys

import psycopg2
import psycopg2.extras

from pg_view import consts
from pg_view.collectors.base_collector import BaseStatCollector
from pg_view.loggers import logger
from pg_view.models.formatters import FnFormatter, StatusFormatter
from pg_view.models.outputs import COLALIGN
from pg_view.sqls import SELECT_PG_IS_IN_RECOVERY, SHOW_MAX_CONNECTIONS, SELECT_PGSTAT_VERSION_LESS_THAN_92, \
    SELECT_PGSTAT_VERSION_LESS_THAN_96, SELECT_PGSTAT_NEVER_VERSION
from pg_view.utils import MEM_PAGE_SIZE, exec_command_with_output, dbversion_as_float

if sys.hexversion >= 0x03000000:
    long = int
    maxsize = sys.maxsize
else:
    maxsize = sys.maxint


def process_sort_key(process):
    return process.get('age', maxsize) or maxsize


class PgStatCollector(BaseStatCollector):
    """ Collect PostgreSQL-related statistics """
    STATM_FILENAME = '/proc/{0}/statm'

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

        self.transform_list_data = [
            {'out': 'pid', 'in': 0, 'fn': int},
            {'out': 'state', 'in': 2},
            {'out': 'utime', 'in': 13, 'fn': self.unit_converter.ticks_to_seconds},
            {'out': 'stime', 'in': 14, 'fn': self.unit_converter.ticks_to_seconds},
            {'out': 'priority', 'in': 17, 'fn': int},
            {'out': 'starttime', 'in': 21, 'fn': long},
            {'out': 'vsize', 'in': 22, 'fn': int},
            {'out': 'rss', 'in': 23, 'fn': int},
            {'out': 'delayacct_blkio_ticks', 'in': 41, 'fn': long, 'optional': True},
            {'out': 'guest_time', 'in': 42, 'fn': self.unit_converter.ticks_to_seconds, 'optional': True},
        ]

        self.transform_dict_data = [
            {'out': 'read_bytes', 'fn': int, 'optional': True},
            {'out': 'write_bytes', 'fn': int, 'optional': True}
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
            {'out': 'type', 'pos': 1},
            {
                'out': 's',
                'in': 'state',
                'pos': 2,
                'status_fn': self.status_formatter.check_ps_state,
                'warning': 'D',
            },
            {
                'out': 'utime',
                'units': '%',
                'fn': self.unit_converter.time_diff_to_percent,
                'round': consts.RD,
                'pos': 4,
                'warning': 90,
                'align': COLALIGN.ca_right,
            },
            {
                'out': 'stime',
                'units': '%',
                'fn': self.unit_converter.time_diff_to_percent,
                'round': consts.RD,
                'pos': 5,
                'warning': 5,
                'critical': 30,
            },
            {
                'out': 'guest',
                'in': 'guest_time',
                'units': '%',
                'fn': self.unit_converter.time_diff_to_percent,
                'round': consts.RD,
                'pos': 6,
            },
            {
                'out': 'delay_blkio',
                'in': 'delayacct_blkio_ticks',
                'units': '/s',
                'round': consts.RD,
            },
            {
                'out': 'read',
                'in': 'read_bytes',
                'units': 'MB/s',
                'fn': self.unit_converter.bytes_to_mbytes,
                'round': consts.RD,
                'pos': 7,
                'noautohide': True,
            },
            {
                'out': 'write',
                'in': 'write_bytes',
                'units': 'MB/s',
                'fn': self.unit_converter.bytes_to_mbytes,
                'round': consts.RD,
                'pos': 8,
                'noautohide': True,
            },
            {
                'out': 'uss',
                'in': 'uss',
                'units': 'MB',
                'fn': self.unit_converter.bytes_to_mbytes,
                'round': consts.RD,
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
        ppid = self.postmaster_pid
        result = exec_command_with_output('ps -o pid --ppid {0} --noheaders'.format(ppid))
        if result[0] != 0:
            logger.info("Couldn't determine the pid of subprocesses for {0}".format(ppid))
            return []
        return [int(x) for x in result[1].split()]

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
            return []

        # fetch up-to-date list of subprocess PIDs
        pids = self.get_subprocesses_pid()
        logger.info("new refresh round")

        result = []
        for pid in pids:
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
        raw_result = {}

        fp = None
        # read raw data from /proc/stat, proc/cmdline and /proc/io
        for ftyp, fname in zip(('stat', 'cmd', 'io',), ('/proc/{0}/stat', '/proc/{0}/cmdline', '/proc/{0}/io')):
            try:
                fp = open(fname.format(pid), 'rU')
                if ftyp == 'stat':
                    raw_result[ftyp] = fp.read().strip().split()
                if ftyp == 'cmd':
                    # large number of trailing \0x00 returned by python
                    raw_result[ftyp] = fp.readline().strip('\x00').strip()
                if ftyp == 'io':
                    proc_stat_io_read = {}
                    for line in fp:
                        x = [e.strip(':') for e in line.split()]
                        if len(x) < 2:
                            logger.error(
                                '{0} content not in the "name: value" form: {1}'.format(fname.format(pid), line))
                            continue
                        else:
                            proc_stat_io_read[x[0]] = int(x[1])
                    raw_result[ftyp] = proc_stat_io_read
            except IOError:
                logger.warning('Unable to read {0}, process data will be unavailable'.format(fname.format(pid)))
                return None
            finally:
                fp and fp.close()

        # Assume we managed to read the row if we can get its PID
        for cat in 'stat', 'io':
            result.update(self._transform_input(raw_result.get(cat, {} if cat == 'io' else [])))
        # generated columns
        result['cmdline'] = raw_result.get('cmd', None)
        if not is_backend:
            result['type'], action = self._get_psinfo(result['cmdline'])
            if action:
                result['query'] = action
        else:
            result['type'] = 'backend'
        if is_active or not is_backend:
            result['uss'] = self._get_memory_usage(pid)
        return result

    def _try_reconnect(self):
        # if we've lost the connection, try to reconnect and re-initialize all connection invariants
        self.pgcon, self.postmaster_pid = self.reconnect()
        self.connection_pid = self.pgcon.get_backend_pid()
        self.max_connections = self._get_max_connections()
        self.dbver = dbversion_as_float(self.pgcon)
        self.server_version = self.pgcon.get_parameter_status('server_version')

    def _get_memory_usage(self, pid):
        """ calculate usage of private memory per process """
        # compute process's own non-shared memory.
        # See http://www.depesz.com/2012/06/09/how-much-ram-is-postgresql-using/ for the explanation of how
        # to measure PostgreSQL process memory usage and the stackexchange answer for details on the unshared counts:
        # http://unix.stackexchange.com/questions/33381/getting-information-about-a-process-memory-usage-from-proc-pid-smaps
        # there is also a good discussion here:
        # http://rhaas.blogspot.de/2012/01/linux-memory-reporting.html
        # we use statm instead of /proc/smaps because of performance considerations. statm is much faster,
        # while providing slightly outdated results.
        uss = 0
        statm = None
        fp = None
        try:
            fp = open(self.STATM_FILENAME.format(pid), 'r')
            statm = fp.read().strip().split()
            logger.info("calculating memory for process {0}".format(pid))
        except IOError as e:
            logger.warning(
                'Unable to read {0}: {1}, process memory information will be unavailable'.format(
                    self.STATM_FILENAME.format(pid), e))
        finally:
            fp and fp.close()
        if statm and len(statm) >= 3:
            uss = (long(statm[1]) - long(statm[2])) * MEM_PAGE_SIZE
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

    def output(self, displayer, before_string=None, after_string=None):
        return super(PgStatCollector, self).output(displayer, before_string='PostgreSQL processes:', after_string='\n')
