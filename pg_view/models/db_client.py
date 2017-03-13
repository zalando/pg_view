import glob
import os
import re

import psycopg2

from pg_view.loggers import logger
from pg_view.models.parsers import ProcNetParser
from pg_view.utils import STAT_FIELD, dbversion_as_float


def read_postmaster_pid(work_directory, dbname):
    """ Parses the postgres directory tree and extracts the pid of the postmaster process """

    fp = None
    try:
        fp = open('{0}/postmaster.pid'.format(work_directory))
        pid = fp.readline().strip()
    except:
        # XXX: do not bail out in case we are collecting data for multiple PostgreSQL clusters
        logger.error('Unable to read postmaster.pid for {name} at {wd}\n HINT: \
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
        logger.error('could not detect connection string from /proc/net for postgres process {0}'.format(pid))
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
            logger.error('unable to detect connection parameters for the PostgreSQL cluster at {0}'.format(
                work_directory))
            return None
    # try all acquired connection arguments, starting from unix, then tcp, then tcp over ipv6
    result = pick_connection_arguments(conn_args, username, dbname)
    if len(result) == 0:
        logger.error('unable to connect to PostgreSQL cluster at {0} using any of '
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
        logger.error('failed to establish connection to {0} via {1}'.format(instance, conn))
        logger.error('PostgreSQL exception: {0}'.format(e))
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
        logger.error('failed to read pid of the postmaster on {0}'.format(conn))
        return None
    # check that we don't have the same pid already in the accumulated results.
    # for instance, a user may specify 2 different set of connection options for
    # the same database (one for the unix_socket_directory and another for the host)
    pids = [opt['pid'] for opt in clusters if 'pid' in opt]
    if pid in pids:
        duplicate_instance = [opt['name'] for opt in clusters if 'pid' in opt and opt.get('pid', 0) == pid][0]
        logger.error('duplicate connection options detected for databases '
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


def get_postmasters_directories():
    """ detect all postmasters running and get their pids """

    pg_pids = []
    postmasters = {}
    pg_proc_stat = {}
    # get all 'number' directories from /proc/ and sort them
    for f in glob.glob('/proc/[0-9]*/stat'):
        # make sure the particular pid is accessible to us
        if not os.access(f, os.R_OK):
            continue
        stat_fields = []
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
        # if PG_VERSION file is missing, this is not a postgres directory
        PG_VERSION_FILENAME = '{0}/PG_VERSION'.format(link_filename)
        if not os.access(PG_VERSION_FILENAME, os.R_OK):
            logger.warning(
                'PostgreSQL candidate directory {0} is missing PG_VERSION file, have to skip it'.format(pg_dir))
            continue
        try:
            fp = open(PG_VERSION_FILENAME, 'rU')
            val = fp.read().strip()
            if val is not None and len(val) >= 3:
                version = float(val)
        except os.error as e:
            logger.error(
                'unable to read version number from PG_VERSION directory {0}, have to skip it'.format(pg_dir))
            continue
        except ValueError:
            logger.error('PG_VERSION doesn\'t contain a valid version number: {0}'.format(val))
            continue
        else:
            dbname = get_dbname_from_path(pg_dir)
            postmasters[pg_dir] = [pid, version, dbname]
    return postmasters


def fetch_socket_inodes_for_process(pid):
    """ read /proc/[pid]/fd and get those that correspond to sockets """
    inodes = []
    fd_dir = '/proc/{0}/fd'.format(pid)
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


def detect_with_postmaster_pid(work_directory, version):

    # PostgreSQL 9.0 doesn't have enough data
    result = {}
    if version is None or version == 9.0:
        return None
    PID_FILE = '{0}/postmaster.pid'.format(work_directory)
    lines = []

    # try to access the socket directory
    if not os.access(work_directory, os.R_OK | os.X_OK):
        logger.warning(
            'cannot access PostgreSQL cluster directory {0}: permission denied'.format(work_directory))
        return None
    try:
        with open(PID_FILE, 'rU') as fp:
            lines = fp.readlines()
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
    if len(result) == 0:
        logger.error('could not acquire a socket postmaster at {0} is listening on'.format(work_directory))
        return None
    return result


def get_dbname_from_path(db_path):
    '''
    >>> get_dbname_from_path('foo')
    'foo'
    >>> get_dbname_from_path('/pgsql_bar/9.4/data')
    'bar'
    '''
    m = re.search(r'/pgsql_(.*?)(/\d+.\d+)?/data/?', db_path)
    if m:
        dbname = m.group(1)
    else:
        dbname = db_path
    return dbname
