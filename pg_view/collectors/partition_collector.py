import glob
import os
import sys
import time
from multiprocessing import Process

from pg_view.collectors.base_collector import StatCollector
from pg_view import consts
from pg_view.loggers import logger
from pg_view.models.outputs import COLALIGN
from pg_view.utils import BLOCK_SIZE

if sys.hexversion >= 0x03000000:
    long = int


class PartitionStatCollector(StatCollector):
    """Collect statistics about PostgreSQL partitions """

    DISK_STAT_FILE = '/proc/diskstats'
    DATA_NAME = 'data'
    XLOG_NAME = 'xlog'
    BLOCK_SIZE = 1024

    def __init__(self, dbname, version, work_directory, consumer):
        super(PartitionStatCollector, self).__init__(ticks_per_refresh=1)
        self.dbname = dbname
        self.version = version
        self.queue_consumer = consumer
        self.work_directory = work_directory
        self.df_list_transformation = [{'out': 'dev', 'in': 0, 'fn': self._dereference_dev_name},
                                       {'out': 'space_total', 'in': 1, 'fn': int},
                                       {'out': 'space_left', 'in': 2, 'fn': int}]
        self.io_list_transformation = [{'out': 'sectors_read', 'in': 5, 'fn': int}, {'out': 'sectors_written', 'in': 9,
                                                                                     'fn': int},
                                       {'out': 'await', 'in': 13, 'fn': int}]
        self.du_list_transformation = [{'out': 'path_size', 'in': 0, 'fn': int}, {'out': 'path', 'in': 1}]

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
                'fn': self.kb_to_mbytes,
                'round': StatCollector.RD,
                'pos': 2,
                'minw': 6,
            },
            {
                'out': 'until_full',
                'in': 'time_until_full',
                'pos': 3,
                'noautohide': True,
                'status_fn': self.time_field_status,
                'fn': StatCollector.time_pretty_print,
                'warning': 10800,
                'critical': 3600,
                'hide_if_ok': True,
                'minw': 13,
            },
            {
                'out': 'total',
                'in': 'space_total',
                'fn': self.kb_pretty_print,
                'pos': 4,
                'minw': 5,
                'align': COLALIGN.ca_right,
            },
            {
                'out': 'left',
                'in': 'space_left',
                'fn': self.kb_pretty_print,
                'pos': 5,
                'noautohide': False,
                'minw': 5,
                'align': COLALIGN.ca_right,
            },
            {
                'out': 'read',
                'units': 'MB/s',
                'fn': self.sectors_to_mbytes,
                'round': StatCollector.RD,
                'pos': 6,
                'noautohide': True,
                'minw': 6,
            },
            {
                'out': 'write',
                'units': 'MB/s',
                'fn': self.sectors_to_mbytes,
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
                'fn': self.kb_pretty_print,
                'pos': 9,
                'noautohide': True,
                'align': COLALIGN.ca_right,
            },
            {'out': 'path', 'pos': 10},
        ]
        self.ncurses_custom_fields = {'header': True,
                                      'prefix': None}
        self.postinit()

    def ident(self):
        return '{0} ({1}/{2})'.format(super(PartitionStatCollector, self).ident(), self.dbname, self.version)

    @staticmethod
    def _dereference_dev_name(devname):
        return devname.replace('/dev/', '') if devname else None

    def refresh(self):
        result = {}
        du_out = {'data': [], 'xlog': []}
        df_out = {'data': [], 'xlog': []}

        queue_data = self.queue_consumer.fetch(self.work_directory)
        # run df and du in parallel to reduce the I/O waiting time
        if queue_data:
            (du_out, df_out) = queue_data

        for pname in PartitionStatCollector.DATA_NAME, PartitionStatCollector.XLOG_NAME:
            result[pname] = self._transform_input(df_out[pname], self.df_list_transformation)

        io_out = self.get_io_data([result[PartitionStatCollector.DATA_NAME]['dev'],
                                   result[PartitionStatCollector.XLOG_NAME]['dev']])

        for pname in PartitionStatCollector.DATA_NAME, PartitionStatCollector.XLOG_NAME:
            if result[pname]['dev'] in io_out:
                result[pname].update(self._transform_input(io_out[result[pname]['dev']], self.io_list_transformation))
            if pname in du_out:
                result[pname].update(self._transform_input(du_out[pname], self.du_list_transformation))
            # set the type manually
            result[pname]['type'] = pname

        self._do_refresh([result[PartitionStatCollector.DATA_NAME], result[PartitionStatCollector.XLOG_NAME]])

    @staticmethod
    def calculate_time_until_full(_, prev, cur):
        # both should be expressed in common units, guaranteed by BLOCK_SIZE
        if (cur.get('path_size', 0) > 0 and
                prev.get('path_size', 0) > 0 and
                cur.get('space_left', 0) > 0 and
                cur['path_size'] < prev['path_size']):
            return cur['space_left'] / (prev['path_size'] - cur['path_size'])
        return None

    @staticmethod
    def get_io_data(pnames):
        """ Retrieve raw data from /proc/diskstat (transformations are perfromed via io_list_transformation)"""
        result = {}
        found = 0  # stop if we found records for all partitions
        total = len(pnames)
        fp = None
        try:
            fp = open(PartitionStatCollector.DISK_STAT_FILE, 'rU')
            for l in fp:
                elements = l.split()
                for pname in pnames:
                    if pname in elements:
                        result[pname] = elements
                        found += 1
                        if found == total:
                            break
                if found == total:
                    break
        except:
            logger.error('Unable to read {0}'.format(PartitionStatCollector.DISK_STAT_FILE))
            result = {}
        finally:
            fp and fp.close()
        return result

    def output(self, method):
        return super(self.__class__, self).output(method, before_string='PostgreSQL partitions:', after_string='\n')


class DetachedDiskStatCollector(Process):
    """ This class runs in a separate process and runs du and df """

    OLD_WAL_SUBDIR = '/pg_xlog/'
    WAL_SUBDIR = '/pg_wal/'

    NEW_WAL_SINCE = 100000

    def __init__(self, q, clusters):
        super(DetachedDiskStatCollector, self).__init__()
        self.q = q
        self.daemon = True
        self.clusters = clusters
        self.df_cache = {}

    @staticmethod
    def wal_directory(version):
        """ Since Postgresql 10.0 wal directory was renamed, so we need to
            choose actual wal directory based on a db_version.
        """
        if version < DetachedDiskStatCollector.NEW_WAL_SINCE:
            return DetachedDiskStatCollector.OLD_WAL_SUBDIR
        else:
            return DetachedDiskStatCollector.WAL_SUBDIR

    def run(self):
        while True:
            # wait until the previous data is consumed
            self.q.join()
            result = {}
            self.df_cache = {}
            for cluster in self.clusters:
                work_directory = cluster['wd']
                wal_directory = self.wal_directory(cluster['version'])
                du_data = self.get_du_data(work_directory, wal_directory)
                df_data = self.get_df_data(work_directory, wal_directory)
                result[work_directory] = [du_data, df_data]
            self.q.put(result)
            time.sleep(consts.TICK_LENGTH)

    def get_du_data(self, work_directory, wal_directory):
        data_size = 0
        xlog_size = 0

        result = {'data': [], 'xlog': []}
        try:
            data_size = self.run_du(work_directory, BLOCK_SIZE)
            xlog_size = self.run_du(work_directory + wal_directory, BLOCK_SIZE)
        except Exception as e:
            logger.error('Unable to read free space information for the pg_xlog and data directories for the directory\
             {0}: {1}'.format(work_directory, e))
        else:
            # XXX: why do we pass the block size there?
            result['data'] = str(data_size), work_directory
            result['xlog'] = str(xlog_size), work_directory + wal_directory
        return result

    @staticmethod
    def run_du(pathname, block_size=BLOCK_SIZE, exclude=None):
        if exclude == None:
            exclude = ["lost+found"]
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

    def get_df_data(self, work_directory, wal_directory):
        """ Retrive raw data from df (transformations are performed via df_list_transformation) """

        result = {'data': [], 'xlog': []}
        # obtain the device names
        data_dev = self.get_mounted_device(self.get_mount_point(work_directory))
        xlog_dev = self.get_mounted_device(self.get_mount_point(work_directory + wal_directory))
        if data_dev not in self.df_cache:
            data_vfs = os.statvfs(work_directory)
            self.df_cache[data_dev] = data_vfs
        else:
            data_vfs = self.df_cache[data_dev]

        if xlog_dev not in self.df_cache:
            xlog_vfs = os.statvfs(work_directory + wal_directory)
            self.df_cache[xlog_dev] = xlog_vfs
        else:
            xlog_vfs = self.df_cache[xlog_dev]

        result['data'] = (data_dev, data_vfs.f_blocks * (data_vfs.f_bsize / BLOCK_SIZE),
                          data_vfs.f_bavail * (data_vfs.f_bsize / BLOCK_SIZE))
        if data_dev != xlog_dev:
            result['xlog'] = (xlog_dev, xlog_vfs.f_blocks * (xlog_vfs.f_bsize / BLOCK_SIZE),
                              xlog_vfs.f_bavail * (xlog_vfs.f_bsize / BLOCK_SIZE))
        else:
            result['xlog'] = result['data']
        return result

    @staticmethod
    def get_mounted_device(pathname):
        """Get the device mounted at pathname"""

        # uses "/proc/mounts"
        raw_dev_name = None
        dev_name = None
        pathname = os.path.normcase(pathname)  # might be unnecessary here
        try:
            with open('/proc/mounts', 'r') as ifp:
                for line in ifp:
                    fields = line.rstrip('\n').split()
                    # note that line above assumes that
                    # no mount points contain whitespace
                    if fields[1] == pathname and (fields[0])[:5] == '/dev/':
                        raw_dev_name = dev_name = fields[0]
                        break
        except EnvironmentError:
            pass
        if raw_dev_name is not None and raw_dev_name[:11] == '/dev/mapper':
            # we have to read the /sys/block/*/*/name and match with the rest of the device
            for fname in glob.glob('/sys/block/*/*/name'):
                try:
                    with open(fname) as f:
                        block_dev_name = f.read().strip()
                except IOError:
                    # ignore those files we couldn't read (lack of permissions)
                    continue
                if raw_dev_name[12:] == block_dev_name:
                    # we found the proper device name, get the 3rd comonent of the path
                    # i.e. /sys/block/dm-0/dm/name
                    components = fname.split('/')
                    if len(components) >= 4:
                        dev_name = components[3]
                    break
        return dev_name

    @staticmethod
    def get_mount_point(pathname):
        """Get the mounlst point of the filesystem containing pathname"""
        mount_point = None
        pathname = os.path.normcase(os.path.realpath(pathname))
        parent_device = path_device = os.stat(pathname).st_dev
        while parent_device == path_device:
            mount_point = pathname
            pathname = os.path.dirname(pathname)
            if pathname == mount_point:
                break
            parent_device = os.stat(pathname).st_dev
        return mount_point
