import os
import re
import resource
import subprocess
import sys

from pg_view import consts
from pg_view import flags
from pg_view.exceptions import InvalidConnectionParamError
from pg_view.loggers import logger

if sys.hexversion >= 0x03000000:
    import configparser as ConfigParser
else:
    import ConfigParser


def enum(**enums):
    return type('Enum', (), enums)


BYTES_IN_MB = 1048576
SECTORS_IN_MB = 2048
KB_IN_MB = 1024

STAT_FIELD = enum(st_pid=0, st_process_name=1, st_state=2, st_ppid=3, st_start_time=21)
BLOCK_SIZE = 1024
MEM_PAGE_SIZE = resource.getpagesize()
PAGESIZE = os.sysconf("SC_PAGE_SIZE")
OUTPUT_METHOD = enum(console='console', json='json', curses='curses')


def open_universal(file_path):
    return open(file_path, 'rU')


def read_file(file_path):
    with open_universal(file_path) as f:
        return f.read()


def readlines_file(file_path):
    with open_universal(file_path) as f:
        return f.readlines()


class UnitConverter(object):
    @staticmethod
    def kb_to_mbytes(kb):
        return float(kb) / KB_IN_MB if kb is not None else None

    @staticmethod
    def sectors_to_mbytes(sectors):
        return float(sectors) / SECTORS_IN_MB if sectors is not None else None

    @staticmethod
    def bytes_to_mbytes(bytes_val):
        return float(bytes_val) / BYTES_IN_MB if bytes_val is not None else None

    @staticmethod
    def bytes_to_kb(bytes_val):
        return float(bytes_val) / KB_IN_MB if bytes_val is not None else None

    @staticmethod
    def ticks_to_seconds(tick_value_str):
        return float(tick_value_str) / consts.USER_HZ if tick_value_str is not None else None

    @staticmethod
    def time_diff_to_percent(timediff_val):
        return float(timediff_val) * 100 if timediff_val is not None else None


def get_valid_output_methods():
    result = []
    for key in OUTPUT_METHOD.__dict__.keys():
        if re.match(r'^[a-z][a-z_]+$', key):
            value = OUTPUT_METHOD.__dict__[key]
            result.append(value)
    return result


def output_method_is_valid(method):
    return method in get_valid_output_methods()


def read_configuration(config_file_name):
    # read PostgreSQL connection options
    if not config_file_name:
        return None
    config = ConfigParser.ConfigParser()
    f = config.read(config_file_name)
    if not f:
        logger.error('Configuration file {0} is empty or not found'.format(config_file_name))
        return None

    config_data = {}
    # get through all defined databases
    for section in config.sections():
        config_data[section] = {}
        for argname in ('port', 'host', 'user', 'dbname'):
            try:
                val = config.get(section, argname)
            except ConfigParser.NoOptionError:
                val = None
            # might happen also if the option is there, but the value is not set
            if val is not None:
                config_data[section][argname] = val
    return config_data


def process_single_collector(collector, filter_aux):
    """ perform all heavy-lifting for a single collector, i.e. data collection,
        diff calculation, etc. This is meant to be run in a separate thread.
    """
    from pg_view.collectors.pg_collector import PgStatCollector
    if isinstance(collector, PgStatCollector):
        collector.set_aux_processes_filter(filter_aux)
    collector.tick()
    if not flags.freeze:
        if collector.needs_refresh():
            collector.refresh()
        if collector.needs_diffs():
            collector.diff()
        else:
            # if the server goes offline, we need to clear diffs here,
            # otherwise rows from the last successful reading will be
            # displayed forever
            collector.clear_diffs()


def process_groups(groups):
    for name in groups:
        part = groups[name]['partitions']
        pg = groups[name]['pg']
        part.ncurses_set_prefix(pg.ncurses_produce_prefix())


def validate_autodetected_conn_param(user_dbname, user_dbver, result_work_dir, connection_params):
    if user_dbname:
        if connection_params.dbname != user_dbname or not result_work_dir or not connection_params.pid:
            raise InvalidConnectionParamError
        if user_dbver is not None and user_dbver != connection_params.version:
            raise InvalidConnectionParamError


def exec_command_with_output(cmdline):
    """ Execute comand (including shell ones), return a tuple with error code (1 element) and output (rest) """
    proc = subprocess.Popen(cmdline, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    ret = proc.wait()
    if ret != 0:
        logger.info('The command {cmd} returned a non-zero exit code'.format(cmd=cmdline))
    return ret, proc.stdout.read().strip()


def time_field_to_seconds(val):
    result = 0
    num = 0
    accum_digits = []
    semicolons_no = val.count(':')
    for c in val:
        if c.isdigit():
            accum_digits.append(c)
        else:
            if len(accum_digits) > 0:
                num = int(''.join(accum_digits))
                if c == 'd':
                    num *= 86400
                elif c == ':':
                    num *= 60 ** semicolons_no
                    semicolons_no -= 1
            result += num
            num = 0
            accum_digits = []
    return result


def dbversion_as_float(server_version):
    version_num = server_version
    version_num //= 100
    return float('{0}.{1}'.format(version_num // 100, version_num % 100))
