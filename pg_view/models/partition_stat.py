import time
from multiprocessing import Process

import os
import psutil

from pg_view.models.base import StatCollector, COLALIGN, logger, TICK_LENGTH
from pg_view.models.formatters import StatusFormatter

SECTOR_SIZE = 512


class PartitionStatCollector(StatCollector):
    """Collect statistics about PostgreSQL partitions """
    DATA_NAME = 'data'
    XLOG_NAME = 'xlog'

    def __init__(self, dbname, dbversion, work_directory, consumer):
        super(PartitionStatCollector, self).__init__(ticks_per_refresh=1)
        self.dbname = dbname
        self.dbver = dbversion
        self.queue_consumer = consumer
        self.work_directory = work_directory
        self.status_formatter = StatusFormatter(self)

        self.df_list_transformation = [
            {'out': 'dev', 'in': 0, 'fn': self._dereference_dev_name},
            {'out': 'space_total', 'in': 1, 'fn': int},
            {'out': 'space_left', 'in': 2, 'fn': int}
        ]

        self.du_list_transformation = [
            {'out': 'path_size', 'in': 0, 'fn': int},
            {'out': 'path', 'in': 1}
        ]

        self.diff_generator_data = [
            {'out': 'type', 'diff': False},
            {'out': 'dev', 'diff': False},
            {'out': 'path', 'diff': False},
            {'out': 'path_size', 'diff': False},
            {'out': 'space_total', 'diff': False},
            {'out': 'space_left', 'diff': False},
            {'out': 'read', 'in': 'sectors_read'},
            {'out': 'write', 'in': 'sectors_written'},
            {'out': 'path_fill_rate', 'in': 'path_size'},
            {'out': 'time_until_full', 'in': 'space_left', 'fn': self.calculate_time_until_full},
            {'out': 'await'},
        ]

        self.output_transform_data = [
            {'out': 'type', 'pos': 0, 'noautohide': True},
            {'out': 'dev', 'pos': 1, 'noautohide': True},
            {
                'out': 'fill',
                'in': 'path_fill_rate',
                'units': 'MB/s',
                'fn': self.unit_converter.kb_to_mbytes,
                'round': StatCollector.RD,
                'pos': 2,
                'minw': 6,
            },
            {
                'out': 'until_full',
                'in': 'time_until_full',
                'pos': 3,
                'noautohide': True,
                'status_fn': self.status_formatter.time_field_status,
                'fn': self.status_formatter.time_pretty_print,
                'warning': 10800,
                'critical': 3600,
                'hide_if_ok': True,
                'minw': 13,
            },
            {
                'out': 'total',
                'in': 'space_total',
                'fn': self.status_formatter.kb_pretty_print,
                'pos': 4,
                'minw': 5,
                'align': COLALIGN.ca_right,
            },
            {
                'out': 'left',
                'in': 'space_left',
                'fn': self.status_formatter.kb_pretty_print,
                'pos': 5,
                'noautohide': False,
                'minw': 5,
                'align': COLALIGN.ca_right,
            },
            {
                'out': 'read',
                'units': 'MB/s',
                'fn': self.unit_converter.sectors_to_mbytes,
                'round': StatCollector.RD,
                'pos': 6,
                'noautohide': True,
                'minw': 6,
            },
            {
                'out': 'write',
                'units': 'MB/s',
                'fn': self.unit_converter.sectors_to_mbytes,
                'round': StatCollector.RD,
                'pos': 7,
                'noautohide': True,
                'minw': 6,
            },
            {
                'out': 'await',
                'units': 'ms',
                'round': StatCollector.RD,
                'pos': 8,
                'minw': 8,
            },
            {
                'out': 'path_size',
                'fn': self.status_formatter.kb_pretty_print,
                'pos': 9,
                'noautohide': True,
                'align': COLALIGN.ca_right,
            },
            {'out': 'path', 'pos': 10},
        ]
        self.ncurses_custom_fields = {'header': True, 'prefix': None}
        self.postinit()

    @classmethod
    def from_cluster(cls, cluster, consumer):
        return cls(['name'], cluster['ver'], cluster['wd'], consumer)

    def ident(self):
        return '{0} ({1}/{2})'.format(super(PartitionStatCollector, self).ident(), self.dbname, self.dbver)

    def _dereference_dev_name(self, devname):
        return devname.replace('/dev/', '') if devname else None

    def refresh(self):
        result = {}
        du_out = {'data': [], 'xlog': []}
        df_out = {'data': [], 'xlog': []}

        queue_data = self.queue_consumer.fetch(self.work_directory)
        # run df and du in parallel to reduce the I/O waiting time
        if queue_data:
            (du_out, df_out) = queue_data

        for pname in self.DATA_NAME, self.XLOG_NAME:
            result[pname] = self._transform_input(df_out[pname], self.df_list_transformation)

        io_out = self.get_io_data([result[self.DATA_NAME]['dev'], result[self.XLOG_NAME]['dev']])

        for pname in self.DATA_NAME, self.XLOG_NAME:
            if result[pname]['dev'] in io_out:
                result[pname].update(io_out.get(result[pname]['dev']))
            if pname in du_out:
                result[pname].update(self._transform_input(du_out[pname], self.du_list_transformation))
            # set the type manually
            result[pname]['type'] = pname

        new_rows = [result[self.DATA_NAME], result[self.XLOG_NAME]]
        self._do_refresh(new_rows)
        return new_rows

    def calculate_time_until_full(self, colname, prev, cur):
        # both should be expressed in common units, guaranteed by BLOCK_SIZE
        if cur.get('path_size', 0) > 0 and prev.get('path_size', 0) > 0 and cur.get('space_left', 0) > 0:
            if cur['path_size'] < prev['path_size']:
                return cur['space_left'] / (prev['path_size'] - cur['path_size'])
        return None

    def get_io_data(self, pnames):
        io_counters = psutil.disk_io_counters(perdisk=True)
        stats_perdisk = {}
        for disk, stats in io_counters.items():
            stats_perdisk[disk] = {
                'sectors_read': stats.read_bytes / SECTOR_SIZE,
                'sectors_written': stats.write_bytes / SECTOR_SIZE,
                'await': 0
        }
        return stats_perdisk

    def output(self, method):
        return super(self.__class__, self).output(method, before_string='PostgreSQL partitions:', after_string='\n')


class DetachedDiskStatCollector(Process):
    """ This class runs in a separate process and runs du and df """
    BLOCK_SIZE = 1024

    def __init__(self, q, work_directories):
        super(DetachedDiskStatCollector, self).__init__()
        self.work_directories = work_directories
        self.q = q
        self.daemon = True
        self.df_cache = {}

    def run(self):
        while True:
            # wait until the previous data is consumed
            self.q.join()
            result = {}
            self.df_cache = {}
            for work_directory in self.work_directories:
                du_data = self.get_du_data(work_directory)
                df_data = self.get_df_data(work_directory)
                result[work_directory] = [du_data, df_data]
            self.q.put(result)
            time.sleep(TICK_LENGTH)

    def get_du_data(self, work_directory):
        result = {'data': [], 'xlog': []}
        try:
            data_size = self.run_du(work_directory, self.BLOCK_SIZE)
            xlog_size = self.run_du(work_directory + '/pg_xlog/', self.BLOCK_SIZE)
        except Exception as e:
            logger.error('Unable to read free space information for the pg_xlog and data directories for the directory\
             {0}: {1}'.format(work_directory, e))
        else:
            # XXX: why do we pass the block size there?
            result['data'] = str(data_size), work_directory
            result['xlog'] = str(xlog_size), work_directory + '/pg_xlog'
        return result

    @staticmethod
    def run_du(pathname, block_size=BLOCK_SIZE, exclude=None):
        if exclude is None:
            exclude = ['lost+found']
        size = 0
        folders = [pathname]
        root_dev = os.lstat(pathname).st_dev
        while len(folders):
            c = folders.pop()
            for e in os.listdir(c):
                e = os.path.join(c, e)
                try:
                    st = os.lstat(e)
                except os.error:
                    # don't care about files removed while we are trying to read them.
                    continue
                # skip data on different partition
                if st.st_dev != root_dev:
                    continue
                mode = st.st_mode & 0xf000  # S_IFMT
                if mode == 0x4000:  # S_IFDIR
                    if e in exclude:
                        continue
                    folders.append(e)
                    size += st.st_size
                if mode == 0x8000:  # S_IFREG
                    size += st.st_size
        return long(size / block_size)

    def get_df_data(self, work_directory):
        """ Retrive raw data from df (transformations are performed via df_list_transformation) """

        result = {'data': [], 'xlog': []}
        # obtain the device names
        data_dev = self.get_mounted_device(self.get_mount_point(work_directory))
        xlog_dev = self.get_mounted_device(self.get_mount_point(work_directory + '/pg_xlog/'))
        if data_dev not in self.df_cache:
            data_vfs = os.statvfs(work_directory)
            self.df_cache[data_dev] = data_vfs
        else:
            data_vfs = self.df_cache[data_dev]

        if xlog_dev not in self.df_cache:
            xlog_vfs = os.statvfs(work_directory + '/pg_xlog/')
            self.df_cache[xlog_dev] = xlog_vfs
        else:
            xlog_vfs = self.df_cache[xlog_dev]

        data_vfs_blocks = data_vfs.f_bsize / self.BLOCK_SIZE
        result['data'] = (data_dev, data_vfs.f_blocks * data_vfs_blocks, data_vfs.f_bavail * data_vfs_blocks)
        if data_dev != xlog_dev:
            xlog_vfs_blocks = (xlog_vfs.f_bsize / self.BLOCK_SIZE)
            result['xlog'] = (xlog_dev, xlog_vfs.f_blocks * xlog_vfs_blocks, xlog_vfs.f_bavail * xlog_vfs_blocks)
        else:
            result['xlog'] = result['data']
        return result

    @staticmethod
    def get_mounted_device(pathname):
        mounted_devices = [d.device for d in psutil.disk_partitions() if d.mountpoint == pathname]
        if len(mounted_devices) == 1:
            return mounted_devices[0]
        return None

    @staticmethod
    def get_mount_point(pathname):
        """Get the mounlst point of the filesystem containing pathname"""
        pathname = os.path.normcase(os.path.realpath(pathname))
        parent_device = path_device = os.stat(pathname).st_dev
        while parent_device == path_device:
            mount_point = pathname
            pathname = os.path.dirname(pathname)
            if pathname == mount_point:
                break
            parent_device = os.stat(pathname).st_dev
        return mount_point
