import re
import resource
import sys

from pg_view import flags
from pg_view.loggers import logger

if sys.hexversion >= 0x03000000:
    import configparser as ConfigParser
else:
    import ConfigParser


def enum(**enums):
    return type('Enum', (), enums)


STAT_FIELD = enum(st_pid=0, st_process_name=1, st_state=2, st_ppid=3, st_start_time=21)
BLOCK_SIZE = 1024
MEM_PAGE_SIZE = resource.getpagesize()
OUTPUT_METHOD = enum(console='console', json='json', curses='curses')

def get_valid_output_methods():
    result = []
    for key in OUTPUT_METHOD.__dict__.keys():
        if re.match(r'^[a-z][a-z_]+$', key):
            value = OUTPUT_METHOD.__dict__[key]
            result.append(value)
    return result


def output_method_is_valid(method):
    """
    >>> output_method_is_valid('foo')
    False
    >>> output_method_is_valid('curses')
    True
    """
    return method in get_valid_output_methods()


def read_configuration(config_file_name):
    # read PostgreSQL connection options
    config_data = {}
    if not config_file_name:
        return None
    config = ConfigParser.ConfigParser()
    f = config.read(config_file_name)
    if not f:
        logger.error('Configuration file {0} is empty or not found'.format(config_file_name))
        return None
    # get through all defined databases
    for section in config.sections():
        config_data[section] = {}
        for argname in (
                'port',
                'host',
                'user',
                'dbname',
        ):
            try:
                val = config.get(section, argname)
            except ConfigParser.NoOptionError:
                val = None
            # might happen also if the option is there, but the value is not set
            if val is not None:
                config_data[section][argname] = val
    return config_data


def process_single_collector(st):
    """ perform all heavy-lifting for a single collector, i.e. data collection,
        diff calculation, etc. This is meant to be run in a separate thread.
    """
    from pg_view.collectors.pg_collector import PgstatCollector
    if isinstance(st, PgstatCollector):
        st.set_aux_processes_filter(flags.filter_aux)
    st.tick()
    if not flags.freeze:
        if st.needs_refresh():
            st.refresh()
        if st.needs_diffs():
            st.diff()
        else:
            # if the server goes offline, we need to clear diffs here,
            # otherwise rows from the last successful reading will be
            # displayed forever
            st.clear_diffs()


def process_groups(groups):
    for name in groups:
        part = groups[name]['partitions']
        pg = groups[name]['pg']
        part.ncurses_set_prefix(pg.ncurses_produce_prefix())



# The version parsing is shamelessly stolen from https://github.com/zalando/patroni/blob/master/patroni/postgresql.py
def postgres_version_to_int(pg_version):
    """ Convert the server_version to integer

    >>> postgres_version_to_int('9.5.3')
    90503
    >>> postgres_version_to_int('9.3.13')
    90313
    >>> postgres_version_to_int('10.1')
    100001
    >>> postgres_version_to_int('10')
    Traceback (most recent call last):
        ...
    Exception: Invalid PostgreSQL format: X.Y or X.Y.Z is accepted: 10
    >>> postgres_version_to_int('a.b.c')
    Traceback (most recent call last):
        ...
    Exception: Invalid PostgreSQL version: a.b.c
    """
    components = pg_version.split('.')

    result = []
    if len(components) < 2 or len(components) > 3:
        raise Exception("Invalid PostgreSQL format: X.Y or X.Y.Z is accepted: {0}".format(pg_version))
    if len(components) == 2:
        # new style verion numbers, i.e. 10.1 becomes 100001
        components.insert(1, '0')
    try:
        result = [c if int(c) > 10 else '0{0}'.format(c) for c in components]
        result = int(''.join(result))
    except ValueError:
        raise Exception("Invalid PostgreSQL version: {0}".format(pg_version))
    return result

def postgres_major_version_to_int(pg_version):
    """
    >>> postgres_major_version_to_int('10')
    100000
    >>> postgres_major_version_to_int('9.6')
    90600
    """
    return postgres_version_to_int(pg_version + '.0')

