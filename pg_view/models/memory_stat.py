import psutil
from psutil._pslinux import get_procfs_path, open_binary

from pg_view.models.base import StatCollector, warn_non_optional_column, _remap_params


class MemoryStatCollector(StatCollector):
    """ Collect memory-related statistics """

    def __init__(self):
        super(MemoryStatCollector, self).__init__(produce_diffs=False)
        self.transform_dict_data = [
            {'in': 'MemTotal', 'out': 'total', 'fn': int},
            {'in': 'MemFree', 'out': 'free', 'fn': int},
            {'in': 'Buffers', 'out': 'buffers', 'fn': int, 'optional': True},
            {'in': 'Cached', 'out': 'cached', 'fn': int},
            {'in': 'Dirty', 'out': 'dirty', 'fn': int},
            {'in': 'CommitLimit', 'out': 'commit_limit', 'fn': int, 'optional': True},
            {'in': 'Committed_AS', 'out': 'committed_as', 'fn': int, 'optional': True},
            {'infn': self.calculate_kb_left_until_limit, 'out': 'commit_left', 'fn': int, 'optional': True}
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
        memdata = self.read_memory_data()
        raw_result = self._transform_input(memdata)
        self._do_refresh([raw_result])
        return raw_result

    def read_memory_data(self):
        default_memory_mapping = {
            'total': 'MemTotal',
            'free': 'MemFree',
            'buffers': 'Buffers',
            'cached': 'Cached',
            'CommitLimit:': 'CommitLimit',
            'Dirty:': 'Dirty',
            'Committed_AS:': 'Committed_AS',
        }

        memory_stats = psutil.virtual_memory()._asdict()
        memory_st = {k: v / 1024 for k, v in memory_stats.items()}
        if psutil.LINUX:
            refreshed_memory_stats = self.get_missing_memory_stat_from_file()
            memory_st.update(refreshed_memory_stats)
        return _remap_params(memory_st, default_memory_mapping)

    def get_missing_memory_stat_from_file(self):
        missing_data = dict.fromkeys(['Dirty:', 'CommitLimit:', 'Committed_AS:'], 0)
        with open_binary('%s/meminfo' % get_procfs_path()) as f:
            for line in f:
                fields = line.split()
                if fields[0] in missing_data.keys():
                    missing_data[fields[0]] = int(fields[1])
        return missing_data

    def calculate_kb_left_until_limit(self, colname, row, optional):
        memory_left = (int(row['CommitLimit']) - int(row['Committed_AS']) if self._is_commit(row) else None)
        if memory_left is None and not optional:
            warn_non_optional_column(colname)
        return memory_left

    def _is_commit(self, row):
        return row.get('CommitLimit') is not None and row.get('Committed_AS') is not None

    def output(self, method):
        return super(self.__class__, self).output(method, before_string='Memory statistics:', after_string='\n')
