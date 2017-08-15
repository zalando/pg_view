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

from pg_view import consts
from pg_view import flags
from pg_view.collectors.host_collector import HostStatCollector
from pg_view.collectors.memory_collector import MemoryStatCollector
from pg_view.collectors.partition_collector import PartitionStatCollector, DetachedDiskStatCollector, \
    DiskCollectorConsumer
from pg_view.collectors.pg_collector import PgStatCollector
from pg_view.collectors.system_collector import SystemStatCollector
from pg_view.exceptions import NoPidConnectionError, InvalidConnectionParamError, NotConnectedError, \
    DuplicatedConnectionError
from pg_view.loggers import logger, enable_logging_to_stderr, disable_logging_to_stderr
from pg_view.models.db_client import make_cluster_desc, DBClient
from pg_view.models.outputs import CommonOutput, CursesOutput, get_displayer_by_class
from pg_view.models.parsers import ProcWorker
from pg_view.utils import get_valid_output_methods, OUTPUT_METHOD, output_method_is_valid, \
    read_configuration, process_single_collector, process_groups, validate_autodetected_conn_param

try:
    import psycopg2
    import psycopg2.extras
except ImportError:
    print('Unable to import psycopg2 module, please, install it (python-psycopg2). Can not continue')
    sys.exit(254)

try:
    import curses

    curses_available = True
except ImportError:
    print('Unable to import ncurses, curses output will be unavailable')
    curses_available = False

# setup system constants
output_method = OUTPUT_METHOD.curses
options = None


def parse_args():
    """parse command-line options"""

    parser = OptionParser(add_help_option=False)
    parser.add_option('-H', '--help', help='show_help', action='help')
    parser.add_option('-v', '--verbose', help='verbose mode', action='store_true', dest='verbose')
    parser.add_option('-i', '--instance', help='name of the instance to monitor', action='store', dest='instance')
    parser.add_option('-s', '--use-service',
                      help='query the service file for the instance name provided',
                      action='store_true', dest='use_service')
    parser.add_option('-t', '--tick', help='tick length (in seconds)',
                      action='store', dest='tick', type='int', default=1)
    parser.add_option('-o', '--output-method', help='send output to the following source', action='store',
                      default=OUTPUT_METHOD.curses, dest='output_method')
    parser.add_option('-V', '--use-version',
                      help="version of the instance to monitor (in case it can't be autodetected)",
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


# execution starts here
def loop(collectors, consumer, groups, output_method):
    if output_method == OUTPUT_METHOD.curses:
        curses.wrapper(do_loop, groups, output_method, collectors, consumer)
    else:
        do_loop(None, groups, output_method, collectors, consumer)


def poll_keys(screen, output):
    c = screen.getch()
    if c == ord('u'):
        flags.display_units = flags.display_units is False
    if c == ord('f'):
        flags.freeze = flags.freeze is False
    if c == ord('s'):
        flags.filter_aux = flags.filter_aux is False
    if c == ord('h'):
        output.toggle_help()
    if c == ord('a'):
        flags.autohide_fields = flags.autohide_fields is False
    if c == ord('t'):
        flags.notrim = flags.notrim is False
    if c == ord('r'):
        flags.realtime = flags.realtime is False
    if c == ord('q'):
        # bail out immediately
        return False
    return True


def get_output(method, screen):
    if method == OUTPUT_METHOD.curses:
        if screen is None:
            logger.error('No parent screen is passed to the curses application')
            sys.exit(1)
        else:
            # initialize the curses output class.
            output = CursesOutput(screen)
            if not output.is_color_supported:
                logger.error('Curses output requires a terminal that supports color')
                sys.exit(1)
    else:
        output = CommonOutput()
    return output


def do_loop(screen, groups, output_method, collectors, consumer):
    """ Display output (or pass it through to ncurses) """

    output = get_output(output_method, screen)
    while 1:
        # process input:
        consumer.consume()
        for collector in collectors:
            if output_method == OUTPUT_METHOD.curses and not poll_keys(screen, output):
                if not poll_keys(screen, output):
                    # bail out immediately
                    return

            process_single_collector(collector, flags.filter_aux)
            if output_method == OUTPUT_METHOD.curses and not poll_keys(screen, output):
                if not poll_keys(screen, output):
                    return

        if output_method == OUTPUT_METHOD.curses:
            process_groups(groups)
        # in the non-curses cases display actually shows the data and refresh
        # clears the screen, so we need to refresh before display to clear the old data.
        if options.clear_screen and output_method != OUTPUT_METHOD.curses:
            output.refresh()
        for collector in collectors:
            displayer = get_displayer_by_class(
                output_method, collector,
                show_units=flags.display_units,
                ignore_autohide=not flags.autohide_fields,
                notrim=flags.notrim
            )
            formatted_data = collector.output(displayer)
            output.display(formatted_data)
        # in the curses case, refresh shows the data queued by display
        if output_method == OUTPUT_METHOD.curses:
            output.refresh()
        if not flags.realtime:
            time.sleep(consts.TICK_LENGTH)


def main():
    global options

    # bail out if we are not running Linux
    if platform.system() != 'Linux':
        print('Non Linux database hosts are not supported at the moment. Can not continue')
        sys.exit(243)

    options, args = parse_args()
    consts.TICK_LENGTH = options.tick
    output_method = options.output_method

    if not output_method_is_valid(output_method):
        print('Unsupported output method: {0}'.format(output_method))
        print('Valid output methods are: {0}'.format(','.join(get_valid_output_methods())))
        sys.exit(1)

    if output_method == OUTPUT_METHOD.curses and not curses_available:
        print('Curses output is selected, but curses are unavailable, falling back to console output')
        output_method = OUTPUT_METHOD.console

    # set basic logging
    setup_logger(options)

    clusters = []

    config = read_configuration(options.config_file) if options.config_file else None
    dbversion = None
    # configuration file takes priority over the rest of database connection information sources.
    if config:
        for instance in config:
            if options.instance and instance != options.instance:
                continue
            # pass already aquired connections to make sure we only list unique clusters.
            db_client = DBClient.from_config(config[instance])
            try:
                cluster = db_client.establish_user_defined_connection(instance, clusters)
            except (NotConnectedError, NoPidConnectionError):
                logger.error('failed to acquire details about the database cluster {0}, the server '
                             'will be skipped'.format(instance))
            except DuplicatedConnectionError:
                pass
            else:
                clusters.append(cluster)

    elif options.host:
        # connect to the database using the connection string supplied from command-line
        db_client = DBClient.from_options(options)
        instance = options.instance or "default"
        try:
            cluster = db_client.establish_user_defined_connection(instance, clusters)
        except (NotConnectedError, NoPidConnectionError):
            logger.error("unable to continue with cluster {0}".format(instance))
        except DuplicatedConnectionError:
            pass
        else:
            clusters.append(cluster)
    elif options.use_service and options.instance:
        db_client = DBClient({'service': options.instance})
        # connect to the database using the service name
        if not db_client.establish_user_defined_connection(options.instance, clusters):
            logger.error("unable to continue with cluster {0}".format(options.instance))
    else:
        # do autodetection
        postmasters = ProcWorker().get_postmasters_directories()
        # get all PostgreSQL instances
        for result_work_dir, connection_params in postmasters.items():
            (ppid, dbversion, dbname) = connection_params
            try:
                validate_autodetected_conn_param(dbname, dbversion, result_work_dir, connection_params)
            except InvalidConnectionParamError:
                continue

            if options.instance:
                if dbname != options.instance or not result_work_dir or not ppid:
                    continue
                if options.version is not None and dbversion != options.version:
                    continue
            db_client = DBClient.from_postmasters(result_work_dir, ppid, dbversion, options)
            if db_client is None:
                continue
            try:
                pgcon = psycopg2.connect(**db_client.connection_params)
            except Exception as e:
                logger.error('PostgreSQL exception {0}'.format(e))
            else:
                desc = make_cluster_desc(
                    name=dbname,
                    version=dbversion,
                    workdir=result_work_dir,
                    pid=ppid,
                    pgcon=pgcon,
                    conn=db_client.connection_params
                )
                clusters.append(desc)

    collectors = []
    groups = {}
    try:
        if not clusters:
            logger.error('No suitable PostgreSQL instances detected, exiting...')
            logger.error('hint: use -v for details, or specify connection parameters '
                         'manually in the configuration file (-c)')
            sys.exit(1)

        # initialize the disks stat collector process and create an exchange queue
        q = JoinableQueue(1)
        work_directories = [cl['wd'] for cl in clusters if 'wd' in cl]
        dbversion = dbversion or clusters[0]['ver']

        collector = DetachedDiskStatCollector(q, work_directories, dbversion)
        collector.start()
        consumer = DiskCollectorConsumer(q)

        collectors.append(HostStatCollector())
        collectors.append(SystemStatCollector())
        collectors.append(MemoryStatCollector())

        for cluster in clusters:
            partition_collector = PartitionStatCollector.from_cluster(cluster, consumer)
            pg_collector = PgStatCollector.from_cluster(cluster, options.pid)

            groups[cluster['wd']] = {'pg': pg_collector, 'partitions': partition_collector}
            collectors.append(partition_collector)
            collectors.append(pg_collector)

        # we don't want to mix diagnostics messages with useful output, so we log the former into a file.
        disable_logging_to_stderr()
        loop(collectors, consumer, groups, output_method)
        enable_logging_to_stderr()
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


def setup_logger(options):
    logger.setLevel(logging.INFO if options.verbose else logging.ERROR)
    if options.log_file:
        LOG_FILE_NAME = options.log_file
        # truncate the former logs
        with open(LOG_FILE_NAME, 'w'):
            pass
        logging.basicConfig(format='%(levelname)s: %(asctime)-15s %(message)s', filename=LOG_FILE_NAME)
    else:
        logging.basicConfig(format='%(levelname)s: %(asctime)-15s %(message)s')
    enable_logging_to_stderr()


if __name__ == '__main__':
    main()
