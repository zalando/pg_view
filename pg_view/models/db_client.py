import psycopg2

from pg_view import loggers
from pg_view.collectors.pg_collector import dbversion_as_float
from pg_view.models.parsers import fetch_socket_inodes_for_process, detect_with_postmaster_pid, ProcNetParser


def read_postmaster_pid(work_directory, dbname):
    """ Parses the postgres directory tree and extracts the pid of the postmaster process """

    fp = None
    try:
        fp = open('{0}/postmaster.pid'.format(work_directory))
        pid = fp.readline().strip()
    except:
        # XXX: do not bail out in case we are collecting data for multiple PostgreSQL clusters
        loggers.logger.error('Unable to read postmaster.pid for {name} at {wd}\n HINT: \
            make sure Postgres is running'.format(name=dbname, wd=work_directory))
        return None
    finally:
        if fp is not None:
            fp.close()
    return pid


def build_connection(host, port, user, database):
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


def pick_connection_arguments(conn_args, username, dbname):
    """ go through all decected connections, picking the first one that actually works """
    result = {}
    for conn_type in ('unix', 'tcp', 'tcp6'):
        if len(result) > 0:
            break
        for arg in conn_args.get(conn_type, []):
            if can_connect_with_connection_arguments(*arg, username=username, dbname=dbname):
                (result['host'], result['port']) = arg
                break
    return result


def can_connect_with_connection_arguments(host, port, username, dbname):
    """ check that we can connect given the specified arguments """
    conn = build_connection(host, port, username, dbname)
    try:
        test_conn = psycopg2.connect(**conn)
        test_conn.close()
    except psycopg2.OperationalError:
        return False
    return True


def detect_with_proc_net(pid):
    result = None
    inodes = fetch_socket_inodes_for_process(pid)
    parser = ProcNetParser()
    result = parser.match_socket_inodes(inodes)
    if not result or len(result) == 0:
        loggers.logger.error('could not detect connection string from /proc/net for postgres process {0}'.format(pid))
        return None
    return result


def detect_db_connection_arguments(work_directory, pid, version, username, dbname):
    """
        Try to detect database connection arguments from the postmaster.pid
        We do this by first extracting useful information from postmaster.pid,
        next reading the postgresql.conf if necessary and, at last,
    """
    result = {}
    conn_args = detect_with_proc_net(pid)
    if not conn_args:
        # if we failed to detect the arguments via the /proc/net/ readings,
        # perhaps we'll get better luck with just peeking into postmaster.pid.
        conn_args = detect_with_postmaster_pid(work_directory, version)
        if not conn_args:
            loggers.logger.error('unable to detect connection parameters for the PostgreSQL cluster at {0}'.format(
                         work_directory))
            return None
    # try all acquired connection arguments, starting from unix, then tcp, then tcp over ipv6
    result = pick_connection_arguments(conn_args, username, dbname)
    if len(result) == 0:
        loggers.logger.error('unable to connect to PostgreSQL cluster at {0} using any of '
                             'the detected connection options: {1}'.format(work_directory, conn_args))
        return None
    return result


def establish_user_defined_connection(instance, conn, clusters):
    """ connect the database and get all necessary options like pid and work_directory
        we use port, host and socket_directory, prefering socket over TCP connections
    """
    pgcon = None
    # establish a new connection
    try:
        pgcon = psycopg2.connect(**conn)
    except Exception as e:
        loggers.logger.error('failed to establish connection to {0} via {1}'.format(instance, conn))
        loggers.logger.error('PostgreSQL exception: {0}'.format(e))
        return None
    # get the database version from the pgcon properties
    dbver = dbversion_as_float(pgcon)
    cur = pgcon.cursor()
    cur.execute('show data_directory')
    work_directory = cur.fetchone()[0]
    cur.close()
    pgcon.commit()
    # now, when we have the work directory, acquire the pid of the postmaster.
    pid = read_postmaster_pid(work_directory, instance)
    if pid is None:
        loggers.logger.error('failed to read pid of the postmaster on {0}'.format(conn))
        return None
    # check that we don't have the same pid already in the accumulated results.
    # for instance, a user may specify 2 different set of connection options for
    # the same database (one for the unix_socket_directory and another for the host)
    pids = [opt['pid'] for opt in clusters if 'pid' in opt]
    if pid in pids:
        duplicate_instance = [opt['name'] for opt in clusters if 'pid' in opt and opt.get('pid', 0) == pid][0]
        loggers.logger.error('duplicate connection options detected for databases '
                             '{0} and {1}, same pid {2}, skipping {0}'.format(instance, duplicate_instance, pid))
        pgcon.close()
        return True
    # now we have all components to create a cluster descriptor
    desc = make_cluster_desc(name=instance, version=dbver, workdir=work_directory,
                             pid=pid, pgcon=pgcon, conn=conn)
    clusters.append(desc)
    return True


def make_cluster_desc(name, version, workdir, pid, pgcon, conn):
    """Create cluster descriptor, complete with the reconnect function."""

    def reconnect():
        pgcon = psycopg2.connect(**conn)
        pid = read_postmaster_pid(workdir, name)
        return (pgcon, pid)

    return {
        'name': name,
        'ver': version,
        'wd': workdir,
        'pid': pid,
        'pgcon': pgcon,
        'reconnect': reconnect
    }
