import psutil

from pg_view.models.collector_base import BaseStatCollector, _remap_params
from pg_view.consts import RD


class SystemStatCollector(BaseStatCollector):
    """ Collect global system statistics, i.e. CPU/IO usage, not including memory. """

    def __init__(self):
        super(SystemStatCollector, self).__init__()

        self.transform_list_data = [
            {'out': 'utime', 'in': 0, 'fn': float},
            {'out': 'stime', 'in': 2, 'fn': float},
            {'out': 'idle', 'in': 3, 'fn': float},
            {'out': 'iowait', 'in': 4, 'fn': float},
            {'out': 'irq', 'in': 5, 'fn': float},
            {'out': 'softirq', 'in': 6, 'fn': float, 'optional': True},
            {'out': 'steal', 'in': 7, 'fn': float, 'optional': True},
            {'out': 'guest', 'in': 8, 'fn': float, 'optional': True}
        ]

        self.transform_dict_data = [
            {'out': 'ctxt', 'fn': float},
            {'out': 'cpu'},
            {'out': 'running', 'in': 'procs_running', 'fn': int},
            {'out': 'blocked', 'in': 'procs_blocked', 'fn': int}
        ]

        self.diff_generator_data = [
            {'out': 'utime', 'fn': self._cpu_time_diff},
            {'out': 'stime', 'fn': self._cpu_time_diff},
            {'out': 'idle', 'fn': self._cpu_time_diff},
            {'out': 'iowait', 'fn': self._cpu_time_diff},
            {'out': 'irq', 'fn': self._cpu_time_diff},
            {'out': 'softirq', 'fn': self._cpu_time_diff},
            {'out': 'steal', 'fn': self._cpu_time_diff},
            {'out': 'guest', 'fn': self._cpu_time_diff},
            {'out': 'ctxt'},
            {'out': 'running', 'diff': False},
            {'out': 'blocked', 'diff': False},
        ]

        self.output_transform_data = [
            {
                'out': 'utime',
                'units': '%',
                'fn': self.unit_converter.time_diff_to_percent,
                'round': RD,
                'minw': 5,
                'pos': 0,
                'warning': 50,
                'critial': 90,
            },
            {
                'out': 'stime',
                'units': '%',
                'fn': self.unit_converter.time_diff_to_percent,
                'round': RD,
                'pos': 1,
                'minw': 5,
                'warning': 10,
                'critical': 30,
            },
            {
                'out': 'idle',
                'units': '%',
                'fn': self.unit_converter.time_diff_to_percent,
                'round': RD,
                'pos': 2,
                'minw': 5,
            },
            {
                'out': 'iowait',
                'units': '%',
                'fn': self.unit_converter.time_diff_to_percent,
                'round': RD,
                'pos': 3,
                'minw': 5,
                'warning': 20,
                'critical': 50,
            },
            {
                'out': 'irq',
                'units': '%',
                'fn': self.unit_converter.time_diff_to_percent,
                'round': RD,
            },
            {
                'out': 'soft',
                'in': 'softirq',
                'units': '%',
                'fn': self.unit_converter.time_diff_to_percent,
                'round': RD,
            },
            {
                'out': 'steal',
                'units': '%',
                'fn': self.unit_converter.time_diff_to_percent,
                'round': RD,
            },
            {
                'out': 'guest',
                'units': '%',
                'fn': self.unit_converter.time_diff_to_percent,
                'round': RD,
            },
            {
                'out': 'ctxt',
                'units': '/s',
                'fn': int,
                'pos': 4,
            },
            {
                'out': 'run',
                'in': 'running',
                'pos': 5,
                'minw': 3,
            },
            {
                'out': 'block',
                'in': 'blocked',
                'pos': 6,
                'minw': 3,
                'warning': 1,
                'critial': 5,
            },
        ]

        self.previos_total_cpu_time = 0
        self.current_total_cpu_time = 0
        self.cpu_time_diff = 0
        self.ncurses_custom_fields = {'header': False, 'prefix': 'sys: ', 'prepend_column_headers': True}
        self.postinit()

    def refresh(self):
        cpu_times = self.read_cpu_times()
        cpu_stats = self.read_cpu_stats()
        result = dict(cpu_times, **cpu_stats)

        self._refresh_cpu_time_values(cpu_times)
        self._do_refresh([result])
        return result

    def read_cpu_times(self):
        default_key_mapping = {
            'guest': 'guest',
            'idle': 'idle',
            'iowait': 'iowait',
            'irq': 'irq',
            'softirq': 'softirq',
            'steal': 'steal',
            'system': 'stime',
            'user': 'utime',
        }

        # TODO: Fix it
        cpu_from_psutil_dict = psutil.cpu_times()._asdict()
        cpu_times = {k: v for k, v in cpu_from_psutil_dict.items()}
        return _remap_params(cpu_times, default_key_mapping)

    def _refresh_cpu_time_values(self, cpu_times):
        # calculate the sum of all CPU indicators and store it.
        total_cpu_time = sum(v for v in cpu_times.values() if v is not None)
        # calculate actual differences in cpu time values
        self.previos_total_cpu_time = self.current_total_cpu_time
        self.current_total_cpu_time = total_cpu_time
        self.cpu_time_diff = self.current_total_cpu_time - self.previos_total_cpu_time

    def _cpu_time_diff(self, colname, current, previous):
        if current.get(colname) and previous.get(colname) and self.cpu_time_diff > 0:
            return (current[colname] - previous[colname]) / self.cpu_time_diff
        return None

    def output(self, displayer):
        return super(SystemStatCollector, self).output(
            displayer, before_string='System statistics:', after_string='\n')

    def read_cpu_stats(self):
        # left - from cputils, right - our
        default_key_mapping = {
            'ctx_switches': 'ctxt',
            'procs_running': 'running',
            'procs_blocked': 'blocked',
        }
        cpu_stats = psutil.cpu_stats()._asdict()
        if psutil.LINUX:
            refreshed_cpu_stats = self.get_missing_cpu_stat_from_file()
            cpu_stats.update(refreshed_cpu_stats)
        return _remap_params(cpu_stats, default_key_mapping)

    def get_missing_cpu_stat_from_file(self):
        from psutil._pslinux import open_binary, get_procfs_path
        missing_data = {}
        with open_binary('%s/stat' % get_procfs_path()) as f:
            for line in f:
                name, args, value = line.strip().partition(' ')
                if name.startswith('procs_'):
                    missing_data[name] = int(value)
        return missing_data
