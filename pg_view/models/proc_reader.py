import glob

import os
import re

from pg_view.models.base import enum, logger

STAT_FIELD = enum(st_pid=0, st_process_name=1, st_state=2, st_ppid=3, st_start_time=21)


def get_dbname_from_path(db_path):
    m = re.search(r'/pgsql_(.*?)(/\d+.\d+)?/data/?', db_path)
    return m.group(1) if m else db_path


class ProcWorker(object):
    def get_postmasters_directories(self):
        """ detect all postmasters running and get their pids """

        pg_pids = []
        postmasters = {}
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
                logger.warning('potential postmaster work directory file {0} is not accessible'.format(link_filename))
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
                logger.warning('unable to access the PostgreSQL candidate directory {0}, have to skip it'.format(pg_dir))
                continue
            # if PG_VERSION file is missing, this is not a postgres directory
            PG_VERSION_FILENAME = '{0}/PG_VERSION'.format(link_filename)
            if not os.access(PG_VERSION_FILENAME, os.R_OK):
                logger.warning('PostgreSQL candidate directory {0} is missing PG_VERSION file, have to skip it'.format(
                               pg_dir))
                continue
            try:
                fp = open(PG_VERSION_FILENAME, 'rU')
                val = fp.read().strip()
                if val is not None and len(val) >= 3:
                    version = float(val)
            except os.error as e:
                logger.error('unable to read version number from PG_VERSION directory {0}, have to skip it'.format(pg_dir))
                continue
            except ValueError:
                logger.error('PG_VERSION doesn\'t contain a valid version number: {0}'.format(val))
                continue
            else:
                dbname = get_dbname_from_path(pg_dir)
                postmasters[pg_dir] = [pid, version, dbname]
        return postmasters

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
