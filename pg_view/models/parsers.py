import glob
import os
import re
import socket
from collections import namedtuple

from pg_view.loggers import logger
from pg_view.utils import readlines_file, read_file, STAT_FIELD

connection_params = namedtuple('connection_params', ['pid', 'version', 'dbname'])


def get_dbname_from_path(db_path):
    m = re.search(r'/pgsql_(.*?)(/\d+.\d+)?/data/?', db_path)
    return m.group(1) if m else db_path


class ProcWorker(object):
    def get_postmasters_directories(self):
        """ detect all postmasters running and get their pids """
        postmasters = {}
        pg_pids, pg_proc_stat = self._get_postgres_processes()

        # we have a pid -> stat fields map, and an array of all pids.
        # sort pids array by the start time of the process, so that we
        # minimize the number of looks into /proc/../cmdline latter
        # the idea is that processes starting earlier are likely to be
        # parent ones.
        pg_pids.sort(key=lambda pid: pg_proc_stat[pid][STAT_FIELD.st_start_time])
        for pid in pg_pids:
            st = pg_proc_stat[pid]
            ppid = st[STAT_FIELD.st_ppid]
            # if parent is also a postgres process - no way this is a postmaster
            if ppid in pg_pids:
                continue
            link_filename = '/proc/{0}/cwd'.format(pid)
            # now get its data directory in the /proc/[pid]/cmdline
            if not os.access(link_filename, os.R_OK):
                logger.warning(
                    'potential postmaster work directory file {0} is not accessible'.format(link_filename))
                continue
            # now read the actual directory, check this is accessible to us and belongs to PostgreSQL
            # additionally, we check that we haven't seen this directory before, in case the check
            # for a parent pid still produce a postmaster child. Be extra careful to catch all exceptions
            # at this phase, we don't want one bad postmaster to be the reason of tool's failure for the
            # other good ones.
            try:
                pg_dir = os.readlink(link_filename)
            except os.error as e:
                logger.error('unable to readlink {0}: OS reported {1}'.format(link_filename, e))
                continue
            if pg_dir in postmasters:
                continue
            if not os.access(pg_dir, os.R_OK):
                logger.warning(
                    'unable to access the PostgreSQL candidate directory {0}, have to skip it'.format(pg_dir))
                continue

            params = self.get_pg_version_from_file(pid, pg_dir)
            if params:
                postmasters[pg_dir] = params
        return postmasters

    def _get_postgres_processes(self):
        pg_pids = []
        pg_proc_stat = {}
        # get all 'number' directories from /proc/ and sort them
        for f in glob.glob('/proc/[0-9]*/stat'):
            # make sure the particular pid is accessible to us
            if not os.access(f, os.R_OK):
                continue
            try:
                with open(f, 'rU') as fp:
                    stat_fields = fp.read().strip().split()
            except:
                logger.error('failed to read {0}'.format(f))
                continue
            # read PostgreSQL processes. Avoid zombies
            if len(stat_fields) < STAT_FIELD.st_start_time + 1 or stat_fields[STAT_FIELD.st_process_name] not in \
                    ('(postgres)', '(postmaster)') or stat_fields[STAT_FIELD.st_state] == 'Z':
                if stat_fields[STAT_FIELD.st_state] == 'Z':
                    logger.warning('zombie process {0}'.format(f))
                if len(stat_fields) < STAT_FIELD.st_start_time + 1:
                    logger.error('{0} output is too short'.format(f))
                continue
            # convert interesting fields to int
            for no in STAT_FIELD.st_pid, STAT_FIELD.st_ppid, STAT_FIELD.st_start_time:
                stat_fields[no] = int(stat_fields[no])
            pid = stat_fields[STAT_FIELD.st_pid]
            pg_proc_stat[pid] = stat_fields
            pg_pids.append(pid)
        return pg_pids, pg_proc_stat

    def get_pg_version_from_file(self, pid, pg_dir):
        link_filename = '/proc/{0}/cwd'.format(pid)
        # if PG_VERSION file is missing, this is not a postgres directory
        PG_VERSION_FILENAME = '{0}/PG_VERSION'.format(link_filename)
        if not os.access(PG_VERSION_FILENAME, os.R_OK):
            logger.warning('PostgreSQL candidate directory {0} is missing PG_VERSION file, '
                           'have to skip it'.format(pg_dir))
            return None
        try:
            data = read_file(PG_VERSION_FILENAME).strip()
            version = self._version_or_value_error(data)
        except os.error:
            logger.error('unable to read version number from PG_VERSION directory {0}, '
                         'have to skip it'.format(pg_dir))
        except ValueError:
            logger.error("PG_VERSION doesn't contain a valid version number: {0}".format(data))
        else:
            dbname = get_dbname_from_path(pg_dir)
            return connection_params(pid=pid, version=version, dbname=dbname)
        return None

    def _version_or_value_error(self, data):
        if data is not None and len(data) >= 2:
            version = float(data)
        else:
            raise ValueError
        return version

    def detect_with_postmaster_pid(self, work_directory, version):
        # PostgreSQL 9.0 doesn't have enough data
        result = {}
        if version is None or version == 9.0:
            return None
        PID_FILE = '{0}/postmaster.pid'.format(work_directory)

        # try to access the socket directory
        if not os.access(work_directory, os.R_OK | os.X_OK):
            logger.warning(
                'cannot access PostgreSQL cluster directory {0}: permission denied'.format(work_directory))
            return None
        try:
            lines = readlines_file(PID_FILE)
        except os.error as e:
            logger.error('could not read {0}: {1}'.format(PID_FILE, e))
            return None

        if len(lines) < 6:
            logger.error('{0} seems to be truncated, unable to read connection information'.format(PID_FILE))
            return None

        port = lines[3].strip()
        unix_socket_path = lines[4].strip()
        if unix_socket_path != '':
            result['unix'] = [(unix_socket_path, port)]

        tcp_address = lines[5].strip()
        if tcp_address != '':
            if tcp_address == '*':
                tcp_address = '127.0.0.1'
            result['tcp'] = [(tcp_address, port)]
        if not result:
            logger.error('could not acquire a socket postmaster at {0} is listening on'.format(work_directory))
            return None
        return result


class ProcNetParser(object):
    """ Parse /proc/net/{tcp,tcp6,unix} and return the list of address:port
        pairs given the set of socket descriptors belonging to the object.
        The result is grouped by the socket type in a dictionary.
    """
    NET_UNIX_FILENAME = '/proc/net/unix'
    NET_TCP_FILENAME = '/proc/net/tcp'
    NET_TCP6_FILENAME = '/proc/net/tcp6'

    def __init__(self, pid):
        self.pid = pid
        self.sockets = {}
        self.unix_socket_header_len = 0
        # initialize the sockets hash with the contents of unix
        # and tcp sockets. tcp IPv6 is also read if it's present
        for fname in self.NET_UNIX_FILENAME, self.NET_TCP_FILENAME:
            self.read_socket_file(fname)
        if os.access(self.NET_TCP6_FILENAME, os.R_OK):
            self.read_socket_file(ProcNetParser.NET_TCP6_FILENAME)

    @staticmethod
    def _hex_to_int_str(val):
        return str(int(val, 16))

    @staticmethod
    def _hex_to_ip(val):
        newval = format(socket.ntohl(int(val, 16)), '08X')
        return '.'.join([str(int(newval[i: i + 2], 16)) for i in range(0, 8, 2)])

    @staticmethod
    def _hex_to_ipv6(val):
        newval_list = [format(socket.ntohl(int(val[x: x + 8], 16)), '08X') for x in range(0, 32, 8)]
        return ':'.join([':'.join((x[:4], x[4:])) for x in newval_list])

    def fetch_socket_inodes_for_process(self):
        """ read /proc/[pid]/fd and get those that correspond to sockets """
        inodes = []
        fd_dir = '/proc/{0}/fd'.format(self.pid)
        if not os.access(fd_dir, os.R_OK):
            logger.warning("unable to read {0}".format(fd_dir))
        else:
            for link in glob.glob('{0}/*'.format(fd_dir)):
                if not os.access(link, os.F_OK):
                    logger.warning("unable to access link {0}".format(link))
                    continue
                try:
                    target = os.readlink(link)
                except:
                    logger.error('coulnd\'t read link {0}'.format(link))
                else:
                    # socket:[8430]
                    match = re.search(r'socket:\[(\d+)\]', target)
                    if match:
                        inodes.append(int(match.group(1)))
        return inodes

    def match_socket_inodes(self):
        """ return the dictionary with socket types as strings,
            containing addresses (or unix path names) and port
        """
        result = {}
        inodes = self.fetch_socket_inodes_for_process()
        for inode in inodes:
            if inode in self.sockets:
                addr_tuple = self.parse_single_line(inode)
                if addr_tuple is None:
                    continue
                socket_type = addr_tuple[0]
                if socket_type in result:
                    result[socket_type].append(addr_tuple[1:])
                else:
                    result[socket_type] = [addr_tuple[1:]]
        return result

    def read_socket_file(self, filename):
        """ read file content, produce a dict of socket inode -> line """
        socket_type = filename.split('/')[-1]
        try:
            with open(filename) as fp:
                data = fp.readlines()
        except os.error as e:
            logger.error('unable to read from {0}: OS reported {1}'.format(filename, e))
        # remove the header
        header = (data.pop(0)).split()
        if socket_type == 'unix':
            self.unix_socket_header_len = len(header)
        indexes = [i for i, name in enumerate(header) if name.lower() == 'inode']
        if len(indexes) != 1:
            logger.error('attribute \'inode\' in the header of {0} is not unique or missing: {1}'.format(
                filename, header))
        else:
            inode_idx = indexes[0]
            if socket_type != 'unix':
                # for a tcp socket, 2 pairs of fields (tx_queue:rx_queue and tr:tm->when
                # are separated by colons and not spaces)
                inode_idx -= 2
            for line in data:
                fields = line.split()
                inode = int(fields[inode_idx])
                self.sockets[inode] = [socket_type, line]

    def parse_single_line(self, inode):
        """ apply socket-specific parsing rules """
        result = None
        (socket_type, line) = self.sockets[inode]
        if socket_type == 'unix':
            # we are interested in everything in the last field
            # note that it may contain spaces or other separator characters
            fields = line.split(None, self.unix_socket_header_len - 1)
            socket_path = fields[-1]
            # check that it looks like a PostgreSQL socket
            match = re.search(r'(.*?)/\.s\.PGSQL\.(\d+)$', socket_path)
            if match:
                # path - port
                result = (socket_type,) + match.groups(1)
            else:
                logger.warning(
                    'unix socket name is not recognized as belonging to PostgreSQL: {0}'.format(socket_path))
        else:
            address_port = line.split()[1]
            (address_hex, port_hex) = address_port.split(':')
            port = self._hex_to_int_str(port_hex)
            if socket_type == 'tcp6':
                address = self._hex_to_ipv6(address_hex)
            elif socket_type == 'tcp':
                address = self._hex_to_ip(address_hex)
            else:
                logger.error('unrecognized socket type: {0}'.format(socket_type))
            result = (socket_type, address, port)
        return result
