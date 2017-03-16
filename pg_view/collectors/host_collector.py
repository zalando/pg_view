import os
import socket
from datetime import timedelta
from multiprocessing import cpu_count

from pg_view.collectors.base_collector import StatCollector
from pg_view.loggers import logger
from pg_view.models.outputs import COLSTATUS, COLHEADER


class HostStatCollector(StatCollector):

    """ General system-wide statistics """

    UPTIME_FILE = '/proc/uptime'

    def __init__(self):
        super(HostStatCollector, self).__init__(produce_diffs=False)

        self.transform_list_data = [{'out': 'loadavg', 'infn': self._concat_load_avg}]
        self.transform_uptime_data = [{'out': 'uptime', 'in': 0, 'fn': self._uptime_to_str}]
        self.transform_uname_data = [{'out': 'sysname', 'infn': self._construct_sysname}]

        self.output_transform_data = [
            {
                'out': 'load average',
                'in': 'loadavg',
                'pos': 4,
                'noautohide': True,
                'warning': 5,
                'critical': 20,
                'column_header': COLHEADER.ch_prepend,
                'status_fn': self._load_avg_state,
            },
            {
                'out': 'up',
                'in': 'uptime',
                'pos': 1,
                'noautohide': True,
                'column_header': COLHEADER.ch_prepend,
            },
            {
                'out': 'host',
                'in': 'hostname',
                'pos': 0,
                'noautohide': True,
                'highlight': True,
            },
            {
                'out': 'cores',
                'pos': 2,
                'noautohide': True,
                'column_header': COLHEADER.ch_append,
            },
            {
                'out': 'name',
                'in': 'sysname',
                'pos': 3,
                'noautohide': True,
            },
        ]

        self.ncurses_custom_fields = {'header': False, 'prefix': None, 'prepend_column_headers': False}

        self.postinit()

    def refresh(self):
        raw_result = {}
        raw_result.update(self._read_uptime())
        raw_result.update(self._read_load_average())
        raw_result.update(self._read_hostname())
        raw_result.update(self._read_uname())
        raw_result.update(self._read_cpus())
        self._do_refresh([raw_result])

    def _read_load_average(self):
        return self._transform_list(os.getloadavg())

    def _load_avg_state(self, row, col):
        state = {}
        load_avg_str = row[self.output_column_positions[col['out']]]
        if not load_avg_str:
            return {}
        # load average consists of 3 values.
        load_avg_vals = load_avg_str.split()
        for no, val in enumerate(load_avg_vals):
            if float(val) >= col['critical']:
                state[no] = COLSTATUS.cs_critical
            elif float(val) >= col['warning']:
                state[no] = COLSTATUS.cs_warning
            else:
                state[no] = COLSTATUS.cs_ok
        return state

    @staticmethod
    def _concat_load_avg(colname, row, optional):
        """ concat all load averages into a single string """

        if len(row) >= 3:
            return ' '.join(str(x) for x in row[:3])
        else:
            return ''

    @staticmethod
    def _load_avg_status(row, col, val, bound):
        if val is not None:
            loads = str(val).split()
            if len(loads) != 3:
                logger.error('load average value is not 1min 5min 15 min')
            for x in loads:
                f = float(x)
                if f > bound:
                    return True
        return False

    @staticmethod
    def _read_cpus():
        cpus = 0
        try:
            cpus = cpu_count()
        except:
            logger.error('multiprocessing does not support cpu_count')
        return {'cores': cpus}

    @staticmethod
    def _construct_sysname(attname, row, optional):
        if len(row) < 3:
            return None
        return '{0} {1}'.format(row[0], row[2])

    def _read_uptime(self):
        fp = None
        raw_result = []
        try:
            fp = open(HostStatCollector.UPTIME_FILE, 'rU')
            raw_result = fp.read().split()
        except:
            logger.error('Unable to read uptime from {0}'.format(HostStatCollector.UPTIME_FILE))
        finally:
            fp and fp.close()
        return self._transform_input(raw_result, self.transform_uptime_data)

    @staticmethod
    def _uptime_to_str(uptime):
        return str(timedelta(seconds=int(float(uptime))))

    @staticmethod
    def _read_hostname():
        return {'hostname': socket.gethostname()}

    def _read_uname(self):
        uname_row = os.uname()
        return self._transform_input(uname_row, self.transform_uname_data)

    def output(self, method):
        return super(self.__class__, self).output(method, before_string='Host statistics', after_string='\n')
