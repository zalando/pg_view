import sys

from pg_view.exceptions import NotConnectedException, NotPidConnectionException, DuplicatedConnectionError

if sys.hexversion >= 0x03000000:
    pass
else:
    pass

import psycopg2

from pg_view.parsers import ProcNetParser, ProcWorker
from pg_view.models.collector_pg import dbversion_as_float
from pg_view.models.collector_base import logger


def read_postmaster_pid(work_directory, dbname):
    """ Parses the postgres directory tree and extracts the pid of the postmaster process """
    fp = None
    try:
        fp = open('{0}/postmaster.pid'.format(work_directory))
        pid = fp.readline().strip()
    except:
        # XXX: do not bail out in case we are collecting data for multiple PostgreSQL clusters
        msg = 'Unable to read postmaster.pid for {name} at {wd}\n HINT: make sure Postgres is running'
        logger.error(msg.format(name=dbname, wd=work_directory))
        return None
    finally:
        if fp is not None:
            fp.close()
    return pid


def make_cluster_desc(name, version, workdir, pid, pgcon, conn):
    """Create cluster descriptor, complete with the reconnect function."""

    def reconnect():
        pgcon = psycopg2.connect(**conn)
        pid = read_postmaster_pid(workdir, name)
        return pgcon, pid

    return {
        'name': name,
        'ver': version,
        'wd': workdir,
        'pid': pid,
        'pgcon': pgcon,
        'reconnect': reconnect
    }


class DBConnection(object):
    def __init__(self, host, port, user='', database=''):
        self.host = host
        self.port = port
        self.user = user
        self.database = database

    def build_connection(self):
        result = {}
        if self.host:
            result['host'] = self.host
        if self.port:
            result['port'] = self.port
        if self.user:
            result['user'] = self.user
        if self.database:
            result['database'] = self.database
        return result


class DBConnectionFinder(object):
    CONN_TYPES = ('unix', 'tcp', 'tcp6')

    def __init__(self, result_work_dir, ppid, dbver, username, dbname):
        self.work_directory = result_work_dir
        self.pid = ppid
        self.version = dbver

        self.username = username
        self.dbname = dbname
        self.proc_worker = ProcWorker()

    def detect_db_connection_arguments(self):
        """ Try to detect database connection arguments from the postmaster.pid
            We do this by first extracting useful information from postmaster.pid,
            next reading the postgresql.conf if necessary and, at last,
        """
        conn_args = self.detect_with_proc_net()
        if not conn_args:
            # if we failed to detect the arguments via the /proc/net/ readings,
            # perhaps we'll get better luck with just peeking into postmaster.pid.
            conn_args = self.proc_worker.detect_with_postmaster_pid(self.work_directory, self.version)
            if not conn_args:
                msg = 'unable to detect connection parameters for the PostgreSQL cluster at {0}'
                logger.error(msg.format(self.work_directory))
                return None
        # try all acquired connection arguments, starting from unix, then tcp, then tcp over ipv6
        result = self.pick_connection_arguments(conn_args)
        if not result:
            logger.error('unable to connect to PostgreSQL cluster at {0} using any of the detected connection '
                         'options: {1}'.format(self.work_directory, conn_args))
            return None
        return result

    def pick_connection_arguments(self, conn_args):
        """ go through all decected connections, picking the first one that actually works """
        result = {}
        for conn_type in self.CONN_TYPES:
            if result:
                break
            for arg in conn_args.get(conn_type, []):
                connection_candidate = DBConnection(*arg, user=self.username, database=self.dbname)
                if self.can_connect_with_connection_arguments(connection_candidate):
                    (result['host'], result['port']) = arg
                    break
        return result

    def can_connect_with_connection_arguments(self, connection):
        """ check that we can connect given the specified arguments """
        conn = connection.build_connection()
        try:
            test_conn = psycopg2.connect(**conn)
            test_conn.close()
        except psycopg2.OperationalError as e:
            logger.error(e)
            return False
        return True

    def detect_with_proc_net(self):
        parser = ProcNetParser(self.pid)
        result = parser.get_connections_from_sockets()
        if not result:
            logger.error('could not detect connection string from /proc/net for postgres process {0}'.format(self.pid))
            return None
        return result


class DBClient(object):
    SHOW_COMMAND = 'SHOW DATA_DIRECTORY'

    def __init__(self, connection):
        self.connection = connection

    def establish_user_defined_connection(self, instance, clusters):
        """ connect the database and get all necessary options like pid and work_directory
            we use port, host and socket_directory, prefering socket over TCP connections
        """
        # establish a new connection
        conn = self.connection.build_connection()
        try:
            pgcon = psycopg2.connect(**conn)
        except Exception as e:
            logger.error('failed to establish connection to {0} via {1}'.format(instance, conn))
            logger.error('PostgreSQL exception: {0}'.format(e))
            raise NotConnectedException

        # get the database version from the pgcon properties
        dbver = dbversion_as_float(pgcon.server_version)
        work_directory = self.execute_query_and_fetchone(pgcon)
        # now, when we have the work directory, acquire the pid of the postmaster.
        pid = read_postmaster_pid(work_directory, instance)

        if pid is None:
            logger.error('failed to read pid of the postmaster on {0}'.format(conn))
            raise NotPidConnectionException

        # check that we don't have the same pid already in the accumulated results.
        # for instance, a user may specify 2 different set of connection options for
        # the same database (one for the unix_socket_directory and another for the host)
        pids = [opt['pid'] for opt in clusters if 'pid' in opt]

        if pid in pids:
            duplicate_instance = [opt['name'] for opt in clusters if 'pid' in opt and opt.get('pid', 0) == pid][0]
            logger.error('duplicate connection options detected  for databases {0} and {1}, '
                         'same pid {2}, skipping {0}'.format(instance, duplicate_instance, pid))
            pgcon.close()
            raise DuplicatedConnectionError

        # now we have all components to create a cluster descriptor
        return make_cluster_desc(
            name=instance, version=dbver, workdir=work_directory, pid=pid, pgcon=pgcon, conn=conn)

    def execute_query_and_fetchone(self, pgcon):
        cur = pgcon.cursor()
        cur.execute(self.SHOW_COMMAND)
        work_directory = cur.fetchone()[0]
        cur.close()
        pgcon.commit()
        return work_directory

    @classmethod
    def from_config(cls, config):
        connection = DBConnection(
            host=config.get('host'),
            port=config.get('port'),
            user=config.get('user'),
            database=config.get('database'),
        )
        return cls(connection)

    @classmethod
    def from_options(cls, options):
        connection = DBConnection(options.host, options.port, options.username, options.dbname)
        return cls(connection)

    @classmethod
    def from_postmasters(cls, result_work_dir, ppid, dbver, options):
        db_finder = DBConnectionFinder(result_work_dir, ppid, dbver, options.username, options.dbname)
        connection_data = db_finder.detect_db_connection_arguments()
        if connection_data is None:
            return None
        connection = DBConnection(
            # connection_data['host'], connection_data['port'], options.username or 'radek', options.dbname or 'atlas')
            connection_data['host'], connection_data['port'], options.username, options.dbname)
        return cls(connection)
