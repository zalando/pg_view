from pg_view.collectors.base_collector import StatCollector
from pg_view.loggers import logger


class MemoryStatCollector(StatCollector):
    """ Collect memory-related statistics """

    MEMORY_STAT_FILE = '/proc/meminfo'

    def __init__(self):
        super(MemoryStatCollector, self).__init__(produce_diffs=False)
        self.transform_dict_data = [
            {'in': 'MemTotal', 'out': 'total', 'fn': int},
            {'in': 'MemFree', 'out': 'free', 'fn': int},
            {
                'in': 'Buffers',
                'out': 'buffers',
                'fn': int,
                'optional': True,
            },
            {'in': 'Cached', 'out': 'cached', 'fn': int},
            {'in': 'Dirty', 'out': 'dirty', 'fn': int},
            {
                'in': 'CommitLimit',
                'out': 'commit_limit',
                'fn': int,
                'optional': True,
            },
            {
                'in': 'Committed_AS',
                'out': 'committed_as',
                'fn': int,
                'optional': True,
            },
            {
                'infn': self.calculate_kb_left_until_limit,
                'out': 'commit_left',
                'fn': int,
                'optional': True,
            },
        ]

        self.output_transform_data = [
            {
                'out': 'total',
                'units': 'MB',
                'fn': self.kb_pretty_print,
                'pos': 0,
                'minw': 6,
            },
            {
                'out': 'free',
                'units': 'MB',
                'fn': self.kb_pretty_print,
                'pos': 1,
                'noautohide': True,
                'minw': 6,
            },
            {
                'out': 'buffers',
                'units': 'MB',
                'fn': self.kb_pretty_print,
                'pos': 2,
                'minw': 6,
            },
            {
                'out': 'cached',
                'units': 'MB',
                'fn': self.kb_pretty_print,
                'pos': 3,
                'minw': 6,
            },
            {
                'out': 'dirty',
                'units': 'MB',
                'fn': self.kb_pretty_print,
                'pos': 4,
                'noautohide': True,
                'minw': 6,
            },
            {
                'out': 'limit',
                'in': 'commit_limit',
                'units': 'MB',
                'fn': self.kb_pretty_print,
                'pos': 5,
                'noautohide': True,
                'minw': 6,
            },
            {
                'out': 'as',
                'in': 'committed_as',
                'units': 'MB',
                'fn': self.kb_pretty_print,
                'pos': 6,
                'minw': 6,
            },
            {
                'out': 'left',
                'in': 'commit_left',
                'units': 'MB',
                'fn': self.kb_pretty_print,
                'pos': 7,
                'noautohide': True,
                'minw': 6,
            },
        ]

        self.ncurses_custom_fields = {'header': False, 'prefix': 'mem: ', 'prepend_column_headers': True}

        self.postinit()

    def refresh(self):
        """ Read statistics from /proc/meminfo """

        memdata = self._read_memory_data()
        raw_result = self._transform_input(memdata)
        self._do_refresh([raw_result])

    def _read_memory_data(self):
        """ Read relevant data from /proc/meminfo. We are interesed in the following fields:
            MemTotal, MemFree, Buffers, Cached, Dirty, CommitLimit, Committed_AS
        """
        result = {}
        try:
            fp = open(MemoryStatCollector.MEMORY_STAT_FILE, 'rU')
            for l in fp:
                vals = l.strip().split()
                if len(vals) >= 2:
                    name, val = vals[:2]
                    # if we have units of measurement different from kB - transform the result
                    if len(vals) == 3 and vals[2] in ('mB', 'gB'):
                        if vals[2] == 'mB':
                            val = val + '0' * 3
                        if vals[2] == 'gB':
                            val = val + '0' * 6
                    if len(str(name)) > 1:
                        result[str(name)[:-1]] = val
                    else:
                        logger.error('name is too short: {0}'.format(str(name)))
                else:
                    logger.error('/proc/meminfo string is not name value: {0}'.format(vals))
        except:
            logger.error('Unable to read /proc/meminfo memory statistics. Check your permissions')
            return result
        finally:
            fp.close()
        return result

    def calculate_kb_left_until_limit(self, colname, row, optional):
        result = (int(row['CommitLimit']) - int(row['Committed_AS']) if row.get('CommitLimit', None) is not None and
                  row.get('Committed_AS', None) is not None else None)
        if result is None and not optional:
            self.warn_non_optional_column(colname)
        return result

    def output(self, method):
        return super(self.__class__, self).output(method, before_string='Memory statistics:', after_string='\n')
