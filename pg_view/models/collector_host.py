import socket
from datetime import datetime
from multiprocessing import cpu_count

import os
import psutil

from pg_view.formatters import StatusFormatter
from pg_view.models.collector_base import BaseStatCollector, logger
from pg_view.models.displayers import COLHEADER


class HostStatCollector(BaseStatCollector):
    """ General system-wide statistics """

    def __init__(self):
        super(HostStatCollector, self).__init__(produce_diffs=False)
        self.status_formatter = StatusFormatter(self)

        self.transform_list_data = [
            {'out': 'loadavg', 'infn': self._concat_load_avg}
        ]

        self.transform_uname_data = [
            {'out': 'sysname', 'infn': self._construct_sysname}
        ]

        self.output_transform_data = [
            {
                'out': 'load average',
                'in': 'loadavg',
                'pos': 4,
                'noautohide': True,
                'warning': 5,
                'critical': 20,
                'column_header': COLHEADER.ch_prepend,
                'status_fn': self.status_formatter.load_avg_state,
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
        return raw_result

    def _read_uptime(self):
        uptime = datetime.now().replace(microsecond=0) - datetime.fromtimestamp(psutil.boot_time())
        return {'uptime': str(uptime)}

    def _read_load_average(self):
        return self._transform_list(os.getloadavg())

    def _concat_load_avg(self, colname, row, optional):
        """ concat all load averages into a single string """
        return ' '.join(str(x) for x in row[:3]) if len(row) >= 3 else ''

    @staticmethod
    def _read_hostname():
        return {'hostname': socket.gethostname()}

    def _read_uname(self):
        return self._transform_input(os.uname(), self.transform_uname_data)

    @staticmethod
    def _read_cpus():
        try:
            cpus = cpu_count()
        except NotImplementedError:
            cpus = 0
            logger.error('multiprocessing does not support cpu_count')
        return {'cores': cpus}

    def _construct_sysname(self, attname, row, optional):
        if len(row) < 3:
            return None
        return '{0} {1}'.format(row[0], row[2])

    def output(self, displayer):
        return super(self.__class__, self).output(displayer, before_string='Host statistics', after_string='\n')
