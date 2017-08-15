import psycopg2

from pg_view.exceptions import NotConnectedError, NoPidConnectionError, DuplicatedConnectionError
from pg_view.loggers import logger
from pg_view.models.parsers import ProcNetParser, ProcWorker
from pg_view.utils import dbversion_as_float


def read_postmaster_pid(work_directory, dbname):
    """ Parses the postgres directory tree and extracts the pid of the postmaster process """
    fp = None
    try:
        fp = open('{0}/postmaster.pid'.format(work_directory))
        pid = fp.readline().strip()
    except:
        # XXX: do not bail out in case we are collecting data for multiple PostgreSQL clusters
        logger.error('Unable to read postmaster.pid for {name} at {wd}\n HINT: '
                     'make sure Postgres is running'.format(name=dbname, wd=work_directory))
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


def prepare_connection_params(host, port, user='', database=''):
    result = {}
    if host:
        result['host'] = host
    if port:
        result['port'] = port
    if user:
        result['user'] = user
    if database:
        result['database'] = database
    return result


class DBConnectionFinder(object):
    CONN_TYPES = ('unix', 'tcp', 'tcp6')

    def __init__(self, work_directory, ppid, dbver, username, dbname):
        self.work_directory = work_directory
        self.pid = ppid
        self.version = dbver
        self.username = username
        self.dbname = dbname

    def detect_db_connection_arguments(self):
        """ Try to detect database connection arguments from the postmaster.pid
            We do this by first extracting useful information from postmaster.pid,
            next reading the postgresql.conf if necessary and, at last,
        """
        conn_args = self.detect_with_proc_net()
        if not conn_args:
            # if we failed to detect the arguments via the /proc/net/ readings,
            # perhaps we'll get better luck with just peeking into postmaster.pid.
            conn_args = ProcWorker().detect_with_postmaster_pid(self.work_directory, self.version)
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
                connection_candidate = prepare_connection_params(*arg, user=self.username, database=self.dbname)
                if self.can_connect_with_connection_arguments(connection_candidate):
                    (result['host'], result['port']) = arg
                    break
        return result

    def can_connect_with_connection_arguments(self, connection_params):
        """ check that we can connect given the specified arguments """
        try:
            test_conn = psycopg2.connect(**connection_params)
            test_conn.close()
        except psycopg2.OperationalError as e:
            logger.error(e)
            return False
        return True

    def detect_with_proc_net(self):
        parser = ProcNetParser(self.pid)
        result = parser.match_socket_inodes()
        if not result:
            logger.error(
                'could not detect connection string from /proc/net for postgres process {0}'.format(self.pid))
            return None
        return result


class DBClient(object):
    SHOW_COMMAND = 'SHOW DATA_DIRECTORY'

    def __init__(self, connection_params):
        self.connection_params = connection_params

    def establish_user_defined_connection(self, instance, clusters):
        """ connect the database and get all necessary options like pid and work_directory
            we use port, host and socket_directory, prefering socket over TCP connections
        """
        try:
            pgcon = psycopg2.connect(**self.connection_params)
        except Exception as e:
            logger.error('failed to establish connection to {0} via {1}'.format(instance, self.connection_params))
            logger.error('PostgreSQL exception: {0}'.format(e))
            raise NotConnectedError

        # get the database version from the pgcon properties
        dbver = dbversion_as_float(pgcon.server_version)
        work_directory = self.execute_query_and_fetchone(pgcon)
        # now, when we have the work directory, acquire the pid of the postmaster.
        pid = read_postmaster_pid(work_directory, instance)

        if pid is None:
            logger.error('failed to read pid of the postmaster on {0}'.format(self.connection_params))
            raise NoPidConnectionError

        # check that we don't have the same pid already in the accumulated results.
        # for instance, a user may specify 2 different set of connection options for
        # the same database (one for the unix_socket_directory and another for the host)
        pids = [opt['pid'] for opt in clusters if 'pid' in opt]

        if pid in pids:
            duplicate_instance = self.get_duplicated_instance(clusters, pid)
            logger.error('duplicate connection options detected  for databases {0} and {1}, '
                         'same pid {2}, skipping {0}'.format(instance, duplicate_instance, pid))
            pgcon.close()
            raise DuplicatedConnectionError

        # now we have all components to create a cluster descriptor
        return make_cluster_desc(
            name=instance, version=dbver, workdir=work_directory, pid=pid, pgcon=pgcon, conn=self.connection_params)

    def get_duplicated_instance(self, clusters, pid):
        return [opt['name'] for opt in clusters if 'pid' in opt and opt.get('pid', 0) == pid][0]

    def execute_query_and_fetchone(self, pgcon):
        cur = pgcon.cursor()
        cur.execute(self.SHOW_COMMAND)
        entry = cur.fetchone()[0]
        cur.close()
        pgcon.commit()
        return entry

    @classmethod
    def from_config(cls, config):
        connection_params = prepare_connection_params(
            host=config.get('host'),
            port=config.get('port'),
            user=config.get('user'),
            database=config.get('database'),
        )
        return cls(connection_params)

    @classmethod
    def from_options(cls, options):
        connection_params = prepare_connection_params(options.host, options.port, options.username, options.dbname)
        return cls(connection_params)

    @classmethod
    def from_postmasters(cls, work_directory, ppid, dbver, options):
        db_finder = DBConnectionFinder(work_directory, ppid, dbver, options.username, options.dbname)
        connection_data = db_finder.detect_db_connection_arguments()
        if connection_data is None:
            return None
        connection_params = prepare_connection_params(
            connection_data['host'], connection_data['port'], options.username, options.dbname)
        return cls(connection_params)
