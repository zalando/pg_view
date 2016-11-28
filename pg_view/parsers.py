from collections import defaultdict, namedtuple

import os
import psutil
import re

from pg_view.helpers import readlines_file
from pg_view.models.collector_base import logger


connection_params = namedtuple('connection_params', ['pid', 'version', 'dbname'])


def get_dbname_from_path(db_path):
    m = re.search(r'/pgsql_(.*?)(/\d+.\d+)?/data/?', db_path)
    return m.group(1) if m else db_path


class ProcWorker(object):
    def get_postmasters_directories(self):
        """ detect all postmasters running and get their pids """
        postmasters = {}
        process_candidates = [p for p in psutil.process_iter() if p.name() in ('postgres', 'postmaster')]
        process_candidates_pids = [p.pid for p in process_candidates]

        # Omitting start_time, I assume that lower pid is started earlier
        for proc in process_candidates:
            ppid = proc.ppid()
            # if parent is also a postgres process - no way this is a postmaster
            if ppid in process_candidates_pids:
                continue
            pg_dir = proc.cwd()
            if pg_dir in postmasters:
                continue

            params = self.get_pg_version_from_file(proc, pg_dir)
            if params:
                postmasters[pg_dir] = params
        return postmasters

    def get_pg_version_from_file(self, proc, pg_dir):
        # if PG_VERSION file is missing, this is not a postgres directory
        PG_VERSION_FILENAME = '{0}/PG_VERSION'.format(pg_dir)
        if not os.access(PG_VERSION_FILENAME, os.R_OK):
            logger.warning('PostgreSQL candidate directory {0} is missing PG_VERSION file, '
                           'have to skip it'.format(pg_dir))
            return None
        try:
            fp = open(PG_VERSION_FILENAME, 'rU')
            value = fp.read().strip()
            if value is not None and len(value) >= 3:
                version = float(value)
        except os.error as e:
            logger.error('unable to read version number from PG_VERSION directory {0}, have to skip it'.format(pg_dir))
        except ValueError:
            logger.error("PG_VERSION doesn't contain a valid version number: {0}".format(value))
        else:
            dbname = get_dbname_from_path(pg_dir)
            return connection_params(pid=proc.pid, version=version, dbname=dbname)
        return None

    # TODO: fix by reading it from file
    def _get_version_from_exe(self, proc):
        value = proc.exe().split('/')[-3]
        if value is not None and len(value) >= 3:
            version = float(value)
        return value, version

    def detect_with_postmaster_pid(self, work_directory, version):
        # PostgreSQL 9.0 doesn't have enough data
        result = {}
        if version is None or version == 9.0:
            return None
        PID_FILE = '{0}/postmaster.pid'.format(work_directory)

        # try to access the socket directory
        if not os.access(work_directory, os.R_OK | os.X_OK):
            logger.warning('cannot access PostgreSQL cluster directory {0}: permission denied'.format(work_directory))
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
    ALLOWED_NET_CONNECTIONS = ('unix', 'tcp', 'tcp6')

    def __init__(self, pid):
        self.pid = pid
        self.unix_socket_header_len = 0
        self.sockets = self.get_socket_connections()

    def get_socket_connections(self):
        sockets = {}
        for conn_type in self.ALLOWED_NET_CONNECTIONS:
            sockets[conn_type] = self.get_connections_for_pid(conn_type)
        return sockets

    def get_connections_for_pid(self, conn_type):
        return [c for c in psutil.net_connections(conn_type) if c.pid == self.pid]

    def get_connections_from_sockets(self):
        connections_by_type = defaultdict(list)
        for conn_type, sockets in self.sockets.items():
            for socket in sockets:
                addr_tuple = self._get_connection_by_type(conn_type, socket)
                if addr_tuple:
                    connections_by_type[conn_type].append(addr_tuple)
        return connections_by_type

    def _get_connection_by_type(self, conn_type, sconn):
        if conn_type == 'unix':
            match = re.search(r'(.*?)/\.s\.PGSQL\.(\d+)$', sconn.laddr)
            if match:
                address, port = match.groups(1)
                return address, port
            else:
                logger.warning('unix socket name is not recognized as belonging to PostgreSQL: {0}'.format(sconn))
        elif conn_type in ('tcp', 'tcp6'):
            address, port = sconn.laddr
            return address, port
        return None
