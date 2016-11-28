import ConfigParser
import subprocess

from pg_view import consts
from pg_view.exceptions import InvalidConnParam

BYTES_IN_MB = 1048576
SECTORS_IN_MB = 2048
KB_IN_MB = 1024


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
    def ticks_to_seconds(tick_value_str):
        return float(tick_value_str) / consts.USER_HZ if tick_value_str is not None else None

    @staticmethod
    def time_diff_to_percent(timediff_val):
        return float(timediff_val) * 100 if timediff_val is not None else None


def open_universal(file_path):
    return open(file_path, 'rU')


def read_file(file_path):
    with open_universal(file_path) as f:
        return f.read()


def readlines_file(file_path):
    with open_universal(file_path) as f:
        return f.readlines()


def process_groups(groups):
    for name in groups:
        part = groups[name]['partitions']
        pg = groups[name]['pg']
        part.ncurses_set_prefix(pg.ncurses_produce_prefix())


def read_configuration(config_file_name):
    # read PostgreSQL connection options
    if not config_file_name:
        return None
    config = ConfigParser.ConfigParser()
    f = config.read(config_file_name)
    if not f:
        from pg_view.models.base import logger
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


def validate_autodetected_conn_param(user_dbname, user_dbver, result_work_dir, connection_params):
    if user_dbname:
        if connection_params.dbname != user_dbname or not result_work_dir or not connection_params.pid:
            raise InvalidConnParam
        if user_dbver is not None and user_dbver != connection_params.version:
            raise InvalidConnParam


def exec_command_with_output(cmdline):
    """ Execute comand (including shell ones), return a tuple with error code (1 element) and output (rest) """
    proc = subprocess.Popen(cmdline, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    ret = proc.wait()
    if ret != 0:
        from pg_view.models.base import logger
        logger.info('The command {cmd} returned a non-zero exit code'.format(cmd=cmdline))
    return ret, proc.stdout.read().strip()


def enum(**enums):
    return type('Enum', (), enums)
