import re
import sys

import psycopg2

from pg_view import loggers
from pg_view.collectors.base_collector import StatCollector
from pg_view.models.outputs import COLSTATUS, COLALIGN
from pg_view.utils import MEM_PAGE_SIZE

if sys.hexversion >= 0x03000000:
    long = int
    maxsize = sys.maxsize
else:
    maxsize = sys.maxint


def dbversion_as_float(pgcon):
    version_num = pgcon.server_version
    version_num /= 100
    return float('{0}.{1}'.format(version_num / 100, version_num % 100))


class PgstatCollector(StatCollector):
    """ Collect PostgreSQL-related statistics """

    STATM_FILENAME = '/proc/{0}/statm'

    def __init__(self, pgcon, reconnect, pid, dbname, dbver, always_track_pids):
        super(PgstatCollector, self).__init__()
        self.postmaster_pid = pid
        self.pgcon = pgcon
        self.reconnect = reconnect
        self.pids = []
        self.rows_diff = []
        self.rows_diff_output = []
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
            {'out': 'utime', 'in': 13, 'fn': StatCollector.ticks_to_seconds},
            {'out': 'stime', 'in': 14, 'fn': StatCollector.ticks_to_seconds},
            {'out': 'priority', 'in': 17, 'fn': int},
            {'out': 'starttime', 'in': 21, 'fn': long},
            {'out': 'vsize', 'in': 22, 'fn': int},
            {'out': 'rss', 'in': 23, 'fn': int},
            {
                'out': 'delayacct_blkio_ticks',
                'in': 41,
                'fn': long,
                'optional': True,
            },
            {
                'out': 'guest_time',
                'in': 42,
                'fn': StatCollector.ticks_to_seconds,
                'optional': True,
            },
        ]

        self.transform_dict_data = [{'out': 'read_bytes', 'fn': int, 'optional': True}, {'out': 'write_bytes',
                                                                                         'fn': int, 'optional': True}]

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
                'status_fn': self.check_ps_state,
                'warning': 'D',
            },
            {
                'out': 'utime',
                'units': '%',
                'fn': StatCollector.time_diff_to_percent,
                'round': StatCollector.RD,
                'pos': 4,
                'warning': 90,
                'align': COLALIGN.ca_right,
            },
            {
                'out': 'stime',
                'units': '%',
                'fn': StatCollector.time_diff_to_percent,
                'round': StatCollector.RD,
                'pos': 5,
                'warning': 5,
                'critical': 30,
            },
            {
                'out': 'guest',
                'in': 'guest_time',
                'units': '%',
                'fn': StatCollector.time_diff_to_percent,
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
                'fn': StatCollector.bytes_to_mbytes,
                'round': StatCollector.RD,
                'pos': 7,
                'noautohide': True,
            },
            {
                'out': 'write',
                'in': 'write_bytes',
                'units': 'MB/s',
                'fn': StatCollector.bytes_to_mbytes,
                'round': StatCollector.RD,
                'pos': 8,
                'noautohide': True,
            },
            {
                'out': 'uss',
                'in': 'uss',
                'units': 'MB',
                'fn': StatCollector.bytes_to_mbytes,
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

        self.ncurses_custom_fields = {'header': True}
        self.ncurses_custom_fields['prefix'] = None

        self.postinit()

    def get_subprocesses_pid(self):
        ppid = self.postmaster_pid
        result = self.exec_command_with_output('ps -o pid --ppid {0} --noheaders'.format(ppid))
        if result[0] != 0:
            loggers.logger.info("Couldn't determine the pid of subprocesses for {0}".format(ppid))
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
        return (pstype, action)

    @staticmethod
    def _is_auxiliary_process(pstype):
        if pstype == 'backend' or pstype == 'autovacuum':
            return False
        return True

    def set_aux_processes_filter(self, newval):
        self.filter_aux_processes = newval

    def ncurses_filter_row(self, row):
        if self.filter_aux_processes:
            # type is the second column
            return self._is_auxiliary_process(row['type'])
        else:
            return False

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
            loggers.logger.info("failed to query the server: {}".format(e))
            if self.pgcon and not self.pgcon.closed:
                self.pgcon.close()
            self.pgcon = None
            self._do_refresh([])
            return
        loggers.logger.info("new refresh round")
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
                            loggers.logger.error(
                                '{0} content not in the "name: value" form: {1}'.format(fname.format(pid), line))
                            continue
                        else:
                            proc_stat_io_read[x[0]] = int(x[1])
                    raw_result[ftyp] = proc_stat_io_read
            except IOError:
                loggers.logger.warning('Unable to read {0}, process data will be unavailable'.format(fname.format(pid)))
                return None
            finally:
                fp and fp.close()

        # Assume we managed to read the row if we can get its PID
        for cat in 'stat', 'io':
            result.update(self._transform_input(raw_result.get(cat, ({} if cat == 'io' else []))))
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
            loggers.logger.info("calculating memory for process {0}".format(pid))
        except IOError as e:
            loggers.logger.warning(
                'Unable to read {0}: {1}, process memory information will be unavailable'.format(self.format(pid), e))
        finally:
            fp and fp.close()
        if statm and len(statm) >= 3:
            uss = (long(statm[1]) - long(statm[2])) * MEM_PAGE_SIZE
        return uss

    def _get_max_connections(self):
        """ Read max connections from the database """

        cur = self.pgcon.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute('show max_connections')
        result = cur.fetchone()
        cur.close()
        return int(result.get('max_connections', 0))

    def _get_recovery_status(self):
        """ Determine whether the Postgres process is in recovery """

        cur = self.pgcon.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("select case when pg_is_in_recovery() then 'standby' else 'master' end as role")
        result = cur.fetchone()
        cur.close()
        return result.get('role', 'unknown')

    def _read_pg_stat_activity(self):
        """ Read data from pg_stat_activity """

        self.recovery_status = self._get_recovery_status()
        cur = self.pgcon.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        # the pg_stat_activity format has been changed to 9.2, avoiding ambigiuous meanings for some columns.
        # since it makes more sense then the previous layout, we 'cast' the former versions to 9.2
        if self.dbver < 9.2:
            cur.execute("""
                    SELECT datname,
                           procpid as pid,
                           usename,
                           client_addr,
                           client_port,
                           round(extract(epoch from (now() - xact_start))) as age,
                           waiting,
                           string_agg(other.pid::TEXT, ',' ORDER BY other.pid) as locked_by,
                           CASE
                             WHEN current_query = '<IDLE>' THEN 'idle'
                             WHEN current_query = '<IDLE> in transaction' THEN
                                  CASE WHEN xact_start != query_start THEN
                                      'idle in transaction'||' '||CAST(
                                          abs(round(extract(epoch from (now() - query_start)))) AS text
                                      )
                                  ELSE
                                      'idle in transaction'
                                  END
                             WHEN current_query = '<IDLE> in transaction (aborted)' THEN 'idle in transaction (aborted)'
                            ELSE current_query
                           END AS query
                      FROM pg_stat_activity
                      LEFT JOIN pg_locks  this ON (this.pid = procpid and this.granted = 'f')
                      -- acquire the same type of lock that is granted
                      LEFT JOIN pg_locks other ON ((this.locktype = other.locktype AND other.granted = 't')
                                               AND ( ( this.locktype IN ('relation', 'extend')
                                                      AND this.database = other.database
                                                      AND this.relation = other.relation)
                                                     OR (this.locktype ='page'
                                                      AND this.database = other.database
                                                      AND this.relation = other.relation
                                                      AND this.page = other.page)
                                                     OR (this.locktype ='tuple'
                                                      AND this.database = other.database
                                                      AND this.relation = other.relation
                                                      AND this.page = other.page
                                                      AND this.tuple = other.tuple)
                                                     OR (this.locktype ='transactionid'
                                                      AND this.transactionid = other.transactionid)
                                                     OR (this.locktype = 'virtualxid'
                                                      AND this.virtualxid = other.virtualxid)
                                                     OR (this.locktype IN ('object', 'userlock', 'advisory')
                                                      AND this.database = other.database
                                                      AND this.classid = other.classid
                                                      AND this.objid = other.objid
                                                      AND this.objsubid = other.objsubid))
                                                   )
                      WHERE procpid != pg_backend_pid()
                      GROUP BY 1,2,3,4,5,6,7,9
                """)
        elif self.dbver < 9.6:
            cur.execute("""
                    SELECT datname,
                           a.pid as pid,
                           usename,
                           client_addr,
                           client_port,
                           round(extract(epoch from (now() - xact_start))) as age,
                           waiting,
                           string_agg(other.pid::TEXT, ',' ORDER BY other.pid) as locked_by,
                           CASE
                              WHEN state = 'idle in transaction' THEN
                                  CASE WHEN xact_start != state_change THEN
                                      state||' '||CAST( abs(round(extract(epoch from (now() - state_change)))) AS text )
                                  ELSE
                                      state
                                  END
                              WHEN state = 'active' THEN query
                              ELSE state
                              END AS query
                      FROM pg_stat_activity a
                      LEFT JOIN pg_locks  this ON (this.pid = a.pid and this.granted = 'f')
                      -- acquire the same type of lock that is granted
                      LEFT JOIN pg_locks other ON ((this.locktype = other.locktype AND other.granted = 't')
                                               AND ( ( this.locktype IN ('relation', 'extend')
                                                      AND this.database = other.database
                                                      AND this.relation = other.relation)
                                                     OR (this.locktype ='page'
                                                      AND this.database = other.database
                                                      AND this.relation = other.relation
                                                      AND this.page = other.page)
                                                     OR (this.locktype ='tuple'
                                                      AND this.database = other.database
                                                      AND this.relation = other.relation
                                                      AND this.page = other.page
                                                      AND this.tuple = other.tuple)
                                                     OR (this.locktype ='transactionid'
                                                      AND this.transactionid = other.transactionid)
                                                     OR (this.locktype = 'virtualxid'
                                                      AND this.virtualxid = other.virtualxid)
                                                     OR (this.locktype IN ('object', 'userlock', 'advisory')
                                                      AND this.database = other.database
                                                      AND this.classid = other.classid
                                                      AND this.objid = other.objid
                                                      AND this.objsubid = other.objsubid))
                                                   )
                      WHERE a.pid != pg_backend_pid()
                      GROUP BY 1,2,3,4,5,6,7,9
                """)
        else:
            cur.execute("""
                    SELECT datname,
                           a.pid as pid,
                           usename,
                           client_addr,
                           client_port,
                           round(extract(epoch from (now() - xact_start))) as age,
                           CASE WHEN wait_event IS NULL THEN false ELSE true END as waiting,
                           string_agg(other.pid::TEXT, ',' ORDER BY other.pid) as locked_by,
                           CASE
                              WHEN state = 'idle in transaction' THEN
                                  CASE WHEN xact_start != state_change THEN
                                      state||' '||CAST( abs(round(extract(epoch from (now() - state_change)))) AS text )
                                  ELSE
                                      state
                                  END
                              WHEN state = 'active' THEN query
                              ELSE state
                              END AS query
                      FROM pg_stat_activity a
                      LEFT JOIN pg_locks  this ON (this.pid = a.pid and this.granted = 'f')
                      -- acquire the same type of lock that is granted
                      LEFT JOIN pg_locks other ON ((this.locktype = other.locktype AND other.granted = 't')
                                               AND ( ( this.locktype IN ('relation', 'extend')
                                                      AND this.database = other.database
                                                      AND this.relation = other.relation)
                                                     OR (this.locktype ='page'
                                                      AND this.database = other.database
                                                      AND this.relation = other.relation
                                                      AND this.page = other.page)
                                                     OR (this.locktype ='tuple'
                                                      AND this.database = other.database
                                                      AND this.relation = other.relation
                                                      AND this.page = other.page
                                                      AND this.tuple = other.tuple)
                                                     OR (this.locktype ='transactionid'
                                                      AND this.transactionid = other.transactionid)
                                                     OR (this.locktype = 'virtualxid'
                                                      AND this.virtualxid = other.virtualxid)
                                                     OR (this.locktype IN ('object', 'userlock', 'advisory')
                                                      AND this.database = other.database
                                                      AND this.classid = other.classid
                                                      AND this.objid = other.objid
                                                      AND this.objsubid = other.objsubid))
                                                   )
                      WHERE a.pid != pg_backend_pid()
                      GROUP BY 1,2,3,4,5,6,7,9
            """)
        results = cur.fetchall()
        # fill in the number of total connections, including ourselves
        self.total_connections = len(results) + 1
        self.active_connections = 0
        ret = {}
        for r in results:
            # stick multiline queries together
            if r.get('query', None):
                if r['query'] != 'idle':
                    if r['pid'] != self.connection_pid:
                        self.active_connections += 1
                lines = r['query'].splitlines()
                newlines = [re.sub('\s+', ' ', l.strip()) for l in lines]
                r['query'] = ' '.join(newlines)
            ret[r['pid']] = r
        self.pgcon.commit()
        cur.close()
        return ret

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
            return "{dbname} {version} (offline)\n". \
                format(dbname=self.dbname,
                       version=self.server_version)

    @staticmethod
    def process_sort_key(process):
        return process['age'] if process['age'] is not None else maxsize

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
