#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging
import os
import platform
import sys
import time
import traceback
from multiprocessing import JoinableQueue  # for then number of cpus
from optparse import OptionParser

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))  # noqa
from pg_view.models.consumers import DiskCollectorConsumer
from pg_view.models.parsers import get_postmasters_directories
from pg_view.models.db_client import build_connection, detect_db_connection_arguments, \
    establish_user_defined_connection, make_cluster_desc
from pg_view import loggers
from pg_view import consts
from pg_view.models.outputs import CommonOutput, CursesOutput
from pg_view.utils import get_valid_output_methods, OUTPUT_METHOD, \
    output_method_is_valid, read_configuration, process_single_collector, process_groups
from pg_view.collectors.memory_collector import MemoryStatCollector
from pg_view.collectors.partition_collector import PartitionStatCollector, DetachedDiskStatCollector
from pg_view.collectors.pg_collector import PgstatCollector
from pg_view.collectors.system_collector import SystemStatCollector
from pg_view.collectors.host_collector import HostStatCollector


try:
    import psycopg2
    import psycopg2.extras

    psycopg2_available = True
except ImportError:
    psycopg2_available = False
try:
    import curses

    curses_available = True
except ImportError:
    print('Unable to import ncurses, curses output will be unavailable')
    curses_available = False


def parse_args():
    """parse command-line options"""

    parser = OptionParser(add_help_option=False)
    parser.add_option('-H', '--help', help='show_help', action='help')
    parser.add_option('-v', '--verbose', help='verbose mode', action='store_true', dest='verbose')
    parser.add_option('-i', '--instance', help='name of the instance to monitor', action='store', dest='instance')
    parser.add_option('-t', '--tick', help='tick length (in seconds)',
                      action='store', dest='tick', type='int', default=1)
    parser.add_option('-o', '--output-method', help='send output to the following source', action='store',
                      default=OUTPUT_METHOD.curses, dest='output_method')
    parser.add_option('-V', '--use-version',
                      help='version of the instance to monitor (in case it can\'t be autodetected)',
                      action='store', dest='version', type='float')
    parser.add_option('-l', '--log-file', help='direct log output to the file', action='store',
                      dest='log_file')
    parser.add_option('-R', '--reset-output', help='clear screen after each tick', action='store_true', default=False,
                      dest='clear_screen')
    parser.add_option('-c', '--configuration-file', help='configuration file for PostgreSQL connections',
                      action='store', default='', dest='config_file')
    parser.add_option('-P', '--pid', help='always track a given pid (may be used multiple times)',
                      action='append', type=int, default=[])
    parser.add_option('-U', '--username', help='database user name',
                      action='store', dest='username')
    parser.add_option('-d', '--dbname', help='database name to connect to',
                      action='store', dest='dbname')
    parser.add_option('-h', '--host', help='database connection host '
                                           '(or a directory path for the unix socket connection)',
                      action='store', dest='host')
    parser.add_option('-p', '--port', help='database port number', action='store', dest='port')

    options, args = parser.parse_args()
    return options, args


# setup system constants
output_method = OUTPUT_METHOD.curses
options = None


# execution starts here
def loop(collectors, consumer, groups, output_method):
    if output_method == OUTPUT_METHOD.curses:
        curses.wrapper(do_loop, groups, output_method, collectors, consumer)
    else:
        do_loop(None, groups, output_method, collectors, consumer)


def poll_keys(screen, output):
    c = screen.getch()
    if c == ord('u'):
        consts.display_units = consts.display_units is False
    if c == ord('f'):
        consts.freeze = consts.freeze is False
    if c == ord('s'):
        consts.filter_aux = consts.filter_aux is False
    if c == ord('h'):
        output.toggle_help()
    if c == ord('a'):
        consts.autohide_fields = consts.autohide_fields is False
    if c == ord('t'):
        consts.notrim = consts.notrim is False
    if c == ord('r'):
        consts.realtime = consts.realtime is False
    if c == ord('q'):
        # bail out immediately
        return False
    return True


def do_loop(screen, groups, output_method, collectors, consumer):
    """ Display output (or pass it through to ncurses) """

    output = None
    if output_method == OUTPUT_METHOD.curses:
        if screen is None:
            loggers.logger.error('No parent screen is passed to the curses application')
            sys.exit(1)
        else:
            # initialize the curses output class.
            output = CursesOutput(screen)
            if not output.is_color_supported:
                loggers.logger.error('Curses output requires a terminal that supports color')
                sys.exit(1)
    else:
        output = CommonOutput()
    while 1:
        # process input:
        consumer.consume()
        for st in collectors:
            if output_method == OUTPUT_METHOD.curses:
                if not poll_keys(screen, output):
                    # bail out immediately
                    return
            st.set_units_display(consts.display_units)
            st.set_ignore_autohide(not consts.autohide_fields)
            st.set_notrim(consts.notrim)
            process_single_collector(st)
            if output_method == OUTPUT_METHOD.curses:
                if not poll_keys(screen, output):
                    return

        if output_method == OUTPUT_METHOD.curses:
            process_groups(groups)
        # in the non-curses cases display actually shows the data and refresh
        # clears the screen, so we need to refresh before display to clear the old data.
        if options.clear_screen and output_method != OUTPUT_METHOD.curses:
            output.refresh()
        for st in collectors:
            output.display(st.output(output_method))
        # in the curses case, refresh shows the data queued by display
        if output_method == OUTPUT_METHOD.curses:
            output.refresh()
        if not consts.realtime:
            time.sleep(consts.TICK_LENGTH)


def main():
    global options

    # bail out if we are not running Linux
    if platform.system() != 'Linux':
        print('Non Linux database hosts are not supported at the moment. Can not continue')
        sys.exit(243)

    if not psycopg2_available:
        print('Unable to import psycopg2 module, please, install it (python-psycopg2). Can not continue')
        sys.exit(254)

    options, args = parse_args()
    consts.TICK_LENGTH = options.tick

    output_method = options.output_method

    if not output_method_is_valid(output_method):
        print('Unsupported output method: {0}'.format(output_method))
        print('Valid output methods are: {0}'.format(','.join(get_valid_output_methods())))
        sys.exit(1)

    if output_method == OUTPUT_METHOD.curses and not curses_available:
        print('Curses output is selected, but curses are unavailable, falling back to console output')
        output_method == OUTPUT_METHOD.console

    # set basic logging
    if options.log_file:
        LOG_FILE_NAME = options.log_file

        # truncate the former logs
        with open(LOG_FILE_NAME, 'w'):
            pass
        logging.basicConfig(format='%(levelname)s: %(asctime)-15s %(message)s', filename=LOG_FILE_NAME)
    else:
        logging.basicConfig(format='%(levelname)s: %(asctime)-15s %(message)s')

    log_stderr = loggers.setup_loggers(options)

    user_dbname = options.instance
    user_dbver = options.version
    clusters = []

    # now try to read the configuration file
    config = (read_configuration(options.config_file) if options.config_file else None)
    if config:
        for instance in config:
            if user_dbname and instance != user_dbname:
                continue
            # pass already aquired connections to make sure we only list unique clusters.
            host = config[instance].get('host')
            port = config[instance].get('port')
            conn = build_connection(host, port,
                                    config[instance].get('user'), config[instance].get('dbname'))

            if not establish_user_defined_connection(instance, conn, clusters):
                loggers.logger.error('failed to acquire details about ' +
                                     'the database cluster {0}, the server will be skipped'.format(instance))
    elif options.host:
        port = options.port or "5432"
        # try to connet to the database specified by command-line options
        conn = build_connection(options.host, options.port, options.username, options.dbname)
        instance = options.instance or "default"
        if not establish_user_defined_connection(instance, conn, clusters):
            loggers.logger.error("unable to continue with cluster {0}".format(instance))
    else:
        # do autodetection
        postmasters = get_postmasters_directories()

        # get all PostgreSQL instances
        for result_work_dir, data in postmasters.items():
            (ppid, dbver, dbname) = data
            # if user requested a specific database name and version - don't try to connect to others
            if user_dbname:
                if dbname != user_dbname or not result_work_dir or not ppid:
                    continue
                if user_dbver is not None and dbver != user_dbver:
                    continue
            try:
                conndata = detect_db_connection_arguments(
                    result_work_dir, ppid, dbver, options.username, options.dbname)
                if conndata is None:
                    continue
                host = conndata['host']
                port = conndata['port']
                conn = build_connection(host, port, options.username, options.dbname)
                pgcon = psycopg2.connect(**conn)
            except Exception as e:
                loggers.logger.error('PostgreSQL exception {0}'.format(e))
                pgcon = None
            if pgcon:
                desc = make_cluster_desc(name=dbname, version=dbver, workdir=result_work_dir,
                                         pid=ppid, pgcon=pgcon, conn=conn)
                clusters.append(desc)
    collectors = []
    groups = {}
    try:
        if len(clusters) == 0:
            loggers.logger.error('No suitable PostgreSQL instances detected, exiting...')
            loggers.logger.error('hint: use -v for details, ' +
                                 'or specify connection parameters manually in the configuration file (-c)')
            sys.exit(1)

        # initialize the disks stat collector process and create an exchange queue
        q = JoinableQueue(1)
        work_directories = [cl['wd'] for cl in clusters if 'wd' in cl]
        collector = DetachedDiskStatCollector(q, work_directories)
        collector.start()
        consumer = DiskCollectorConsumer(q)

        collectors.append(HostStatCollector())
        collectors.append(SystemStatCollector())
        collectors.append(MemoryStatCollector())
        for cl in clusters:
            part = PartitionStatCollector(cl['name'], cl['ver'], cl['wd'], consumer)
            pg = PgstatCollector(cl['pgcon'], cl['reconnect'], cl['pid'], cl['name'], cl['ver'], options.pid)
            groupname = cl['wd']
            groups[groupname] = {'pg': pg, 'partitions': part}
            collectors.append(part)
            collectors.append(pg)

        # we don't want to mix diagnostics messages with useful output, so we log the former into a file.
        loggers.logger.removeHandler(log_stderr)
        loop(collectors, consumer, groups, output_method)
        loggers.logger.addHandler(log_stderr)
    except KeyboardInterrupt:
        pass
    except curses.error:
        print(traceback.format_exc())
        if 'SSH_CLIENT' in os.environ and 'SSH_TTY' not in os.environ:
            print('Unable to initialize curses. Make sure you supply -t option (force psedo-tty allocation) to ssh')
    except:
        print(traceback.format_exc())
    finally:
        sys.exit(0)


if __name__ == '__main__':
    main()
