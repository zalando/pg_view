#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import re
import sys
import glob
import logging
from optparse import OptionParser
from operator import itemgetter
from datetime import datetime, timedelta
from numbers import Number
from multiprocessing import Process, JoinableQueue, cpu_count  # for then number of cpus
import platform
import resource
import socket
import subprocess
import time
import traceback
import json
from collections import namedtuple

__appname__ = 'pg_view'
__version__ = '1.4.1'
__author__ = 'Oleksii Kliukin <oleksii.kliukin@zalando.de>'
__license__ = 'Apache 2.0'


if sys.hexversion >= 0x03000000:
    import configparser as ConfigParser
    from queue import Empty
    long = int
    maxsize = sys.maxsize
else:
    import ConfigParser
    from Queue import Empty
    maxsize = sys.maxint

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


# enum emulation

def enum(**enums):
    return type('Enum', (), enums)


class ColumnType(namedtuple('ColumnType', 'value header header_position')):
    __slots__ = ()

    @property
    def length(self):
        return len(self.value) + (0 if not self.header_position else len(self.header) + 1)


COLSTATUS = enum(cs_ok=0, cs_warning=1, cs_critical=2)
COLALIGN = enum(ca_none=0, ca_left=1, ca_center=2, ca_right=3)
COLTYPES = enum(ct_string=0, ct_number=1)
COLHEADER = enum(ch_default=0, ch_prepend=1, ch_append=2)
OUTPUT_METHOD = enum(console='console', json='json', curses='curses')

STAT_FIELD = enum(st_pid=0, st_process_name=1, st_state=2, st_ppid=3, st_start_time=21)
BLOCK_SIZE = 1024
MEM_PAGE_SIZE = resource.getpagesize()

# some global variables for keyboard output
freeze = False
filter_aux = True
autohide_fields = False
display_units = False
notrim = False
realtime = False


# validation functions for OUTPUT_METHOD

def get_valid_output_methods():
    result = []
    for key in OUTPUT_METHOD.__dict__.keys():
        if re.match(r'^[a-z][a-z_]+$', key):
            value = OUTPUT_METHOD.__dict__[key]
            result.append(value)
    return result


def output_method_is_valid(method):
    '''
    >>> output_method_is_valid('foo')
    False
    >>> output_method_is_valid('curses')
    True
    '''
    return method in get_valid_output_methods()


def parse_args():
    '''parse command-line options'''

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
TICK_LENGTH = 1

output_method = OUTPUT_METHOD.curses

options = None

logger = None


class StatCollector(object):

    """ Generic class to store abstract function and data required to collect system statistics,
        produce diffs and emit output rows.
    """

    BYTE_MAP = [('TB', 1073741824), ('GB', 1048576), ('MB', 1024)]
    USER_HZ = os.sysconf(os.sysconf_names['SC_CLK_TCK'])
    RD = 1

    NCURSES_DEFAULTS = {
        'pos': -1,
        'noautohide': False,
        'w': 0,
        'align': COLALIGN.ca_none,
        'column_header': COLHEADER.ch_default,
    }

    NCURSES_CUSTOM_OUTPUT_FIELDS = ['header', 'prefix', 'prepend_column_headers']

    def __init__(self, ticks_per_refresh=1, produce_diffs=True):
        self.rows_prev = []
        self.rows_cur = []
        self.time_diff = 0
        self.rows_diff = []
        self.ticks = 0
        self.ticks_per_refresh = ticks_per_refresh
        self.diff_time = 0
        self._previous_moment = None
        self._current_moment = None
        self.produce_diffs = produce_diffs
        self.show_units = False
        self.ignore_autohide = True
        self.notrim = False

        # transformation data
        self.transform_dict_data = {}  # data to transform a dictionary input to the stat row
        self.transform_list_data = {}  # ditto for the list input

        # diff calculation data
        self.diff_generator_data = {}  # data to produce a diff row out of 2 input ones.
        self.output_transform_data = {}  # data to transform diff output

        self.output_function = {OUTPUT_METHOD.console: self.console_output, OUTPUT_METHOD.json: self.json_output,
                                OUTPUT_METHOD.curses: self.ncurses_output}
        self.cook_function = {OUTPUT_METHOD.curses: self.curses_cook_value}
        self.ncurses_custom_fields = dict.fromkeys(StatCollector.NCURSES_CUSTOM_OUTPUT_FIELDS, None)

    def postinit(self):
        for l in [self.transform_list_data, self.transform_dict_data, self.diff_generator_data,
                  self.output_transform_data]:
            self.validate_list_out(l)
        self.output_column_positions = self._calculate_output_column_positions()

    def set_ignore_autohide(self, new_status):
        self.ignore_autohide = new_status

    def set_notrim(self, val):
        self.notrim = val

    def _calculate_output_column_positions(self):
        result = {}
        for idx, col in enumerate(self.output_transform_data):
            result[col['out']] = idx

        return result

    def enumerate_output_methods(self):
        return self.output_function.keys()

    @staticmethod
    def exec_command_with_output(cmdline):
        """ Execute comand (including shell ones), return a tuple with error code (1 element) and output (rest) """

        proc = subprocess.Popen(cmdline, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
        ret = proc.wait()
        if ret != 0:
            logger.info('The command {cmd} returned a non-zero exit code'.format(cmd=cmdline))
        return ret, proc.stdout.read().strip()

    @staticmethod
    def validate_list_out(l):
        """ If the list element doesn't supply an out column - remove it """

        for col in l:
            if 'out' not in col:
                el = l.pop(l.index(col))
                logger.error('Removed {0} column because it did not specify out value'.format(el))

    @staticmethod
    def ticks_to_seconds(tick_value_str):
        return (float(tick_value_str) / StatCollector.USER_HZ if tick_value_str is not None else None)

    @staticmethod
    def bytes_to_mbytes(bytes_val):
        return (float(bytes_val) / 1048576 if bytes_val is not None else None)

    @staticmethod
    def sectors_to_mbytes(sectors):
        return (float(sectors) / 2048 if sectors is not None else None)

    @staticmethod
    def kb_to_mbytes(kb):
        return (float(kb) / 1024 if kb is not None else None)

    @staticmethod
    def time_diff_to_percent(timediff_val):
        return (float(timediff_val) * 100 if timediff_val is not None else None)

    @staticmethod
    def format_date_from_epoch(epoch_val):
        lt = time.localtime(epoch_val)
        today = time.localtime()
        time_format_str = '%H:%M:%S'
        if lt.tm_year != today.tm_year or lt.tm_mon != today.tm_mon or lt.tm_mday != today.tm_mday:
            # only show minutes and seconds
            time_format_str = '%m-%d %H:%M:%S'
        # show full date
        return time.strftime(time_format_str, time.localtime(epoch_val))

    @staticmethod
    def kb_pretty_print_long(b):
        """ Show kb values in a human readable form. """

        r = []
        for l, n in StatCollector.BYTE_MAP:
            d = b / n
            if d:
                r.append(str(d) + l)
            b = b % n
        return ' '.join(r)

    @staticmethod
    def kb_pretty_print(b):
        """ Show memory size as a float value in the biggest measurement units  """

        r = []
        v = 0
        for l, n in StatCollector.BYTE_MAP:
            if b > n:
                v = round(float(b) / n, 1)
                r.append(str(v) + l)
                break
        if len(r) == 0:
            return '{0}KB'.format(str(b))
        else:
            return ' '.join(r)

    @staticmethod
    def time_interval_pretty_print(start_time, is_delta):
        '''Returns a human readable string that shows a time between now and the timestamp passed as an argument.
        The passed argument can be a timestamp (returned by time.time() call) a datetime object or a timedelta object.
        In case it is a timedelta object, then it is formatted only
        '''

        if isinstance(start_time, Number):
            if is_delta:
                delta = timedelta(seconds=int(time.time() - start_time))
            else:
                delta = timedelta(seconds=start_time)
        elif isinstance(start_time, datetime):
            if is_delta:
                delta = datetime.now() - start_time
            else:
                delta = start_time
        elif isinstance(start_time, timedelta):
            delta = start_time
        else:
            raise ValueError('passed value should be either a number of seconds ' +
                             'from year 1970 or datetime instance of timedelta instance')

        delta = abs(delta)

        secs = delta.seconds
        mins = int(secs / 60)
        secs %= 60
        hrs = int(mins / 60)
        mins %= 60
        hrs %= 24
        result = ''
        if delta.days:
            result += str(delta.days) + 'd'
        if hrs:
            if hrs < 10:
                result += '0'
            result += str(hrs)
            result += ':'
        if mins < 10:
            result += '0'
        result += str(mins)
        result += ':'
        if secs < 10:
            result += '0'
        result += str(secs)
        if not result:
            result = str(int(delta.microseconds / 1000)) + 'ms'
        return result

    @staticmethod
    def time_pretty_print(start_time):
        return StatCollector.time_interval_pretty_print(start_time, False)

    @staticmethod
    def delta_pretty_print(start_time):
        return StatCollector.time_interval_pretty_print(start_time, True)

    @staticmethod
    def sectors_pretty_print(b):
        return StatCollector.kb_pretty_print(b * 2)

    @staticmethod
    def int_lower_than_non_zero(row, col, val, bound):
        return val > 0 and val < bound

    @staticmethod
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

    def time_field_status(self, row, col):
        val = row[self.output_column_positions[col['out']]]
        num = StatCollector.time_field_to_seconds(val)
        if num <= col['critical']:
            return {-1: COLSTATUS.cs_critical}
        elif num <= col['warning']:
            return {-1: COLSTATUS.cs_warning}
        return {-1: COLSTATUS.cs_ok}

    @staticmethod
    def warn_non_optional_column(colname):
        logger.error('Column {0} is not optional, but input row has no value for it'.format(colname))

    def set_units_display(self, status):
        self.show_units = status

    def needs_diffs(self):
        """ whether the collector needs diffs. It might not if it's not interested in them,
            or if it doesn't have data to produce them yet.
        """

        return self.produce_diffs and self.rows_prev and self.rows_cur

    def tick(self):
        self.ticks += 1

    def needs_refresh(self):
        return self.ticks % self.ticks_per_refresh == 0

    def refresh(self):
        self._do_refresh(None)

    def ident(self):
        return str(self.__class__).lower().split('.')[-1].split('statcollector')[0]

    def ncurses_set_prefix(self, new_prefix):
        self.ncurses_custom_fields['prefix'] = new_prefix

    def cook_row(self, row, header, method):
        cooked_vals = []
        if not self.cook_function.get(method):
            return row
        if len(row) != len(header):
            logger.error('Unable to cook row with non-matching number of header and value columns: ' +
                         'row {0} header {1}'.format(row, header))
        cook_fn = self.cook_function[method]
        for no, val in enumerate(row):
            # if might be tempting to just get the column from output_transform_data using
            # the header, but it's wrong: see _produce_output_name for details. This, of
            # course, assumes the number of columns in the output_transform_data is the
            # same as in row: thus, we need to avoid filtering rows in the collector.
            newval = cook_fn(val, header[no], self.output_transform_data[no])
            cooked_vals.append(newval)
        return cooked_vals

    def curses_cook_value(self, attname, raw_val, output_data):
        """ return cooked version of the row, with values transformed. A transformation is
            the same for all columns and depends on the values only.
        """
        val = raw_val
        header = str(attname)
        # change the None output to ''
        if raw_val is None:
            return ColumnType(value='', header='', header_position=None)
        if str(raw_val) == 'True':
            val = 'T'
        elif str(raw_val) == 'False':
            val = 'F'
        if output_data.get('maxw', 0) > 0 and not self.notrim and len(str(val)) > output_data['maxw']:
            # if the value is larger than the maximum allowed width - trim it by removing chars from the middle
            val = self._trim_text_middle(val, output_data['maxw'])
        if self.ncurses_custom_fields.get('prepend_column_headers') or output_data.get('column_header',
           COLHEADER.ch_default) == COLHEADER.ch_prepend:
            header_position = COLHEADER.ch_prepend
        elif output_data.get('column_header', COLHEADER.ch_default) == COLHEADER.ch_append:
            header_position = COLHEADER.ch_append
        else:
            header = ''
            header_position = None
        return ColumnType(value=str(val), header=header, header_position=header_position)

    @staticmethod
    def _trim_text_middle(val, maxw):
        """ Trim data by removing middle characters, so hello world' for 8 will become hel..rld.
            This kind of trimming seems to be better than tail trimming for user and database names.
        """

        half = int((maxw - 2) / 2)
        return val[:half] + '..' + val[-half:]

    def _do_refresh(self, new_rows):
        """ Make a place for new rows and calculate the time diff """

        self.rows_prev = self.rows_cur
        self.rows_cur = new_rows
        self._previous_moment = self._current_moment
        self._current_moment = time.time()
        if self._previous_moment is None:
            self.diff_time = 0
        else:
            self.diff_time = self._current_moment - self._previous_moment

    def _produce_diff_row(self, prev, cur):
        """ produce output columns out of 2 input ones (previous and current). If the value
            doesn't exist in either of the diffed rows - we set the result to None
        """

        # exit early if we don't need any diffs
        if not self.produce_diffs:
            return {}
        result = {}
        for col in self.diff_generator_data:
            # Only process attributes for which out it set.
            attname = col['out']
            incol = (col['in'] if 'in' in col and col['in'] else attname)
            # if diff is False = copy the attribute as is.
            if 'diff' in col and col['diff'] is False:
                result[attname] = (cur[incol] if incol in cur else None)
            elif 'fn' in col:
                # if diff is True and fn is supplied - apply it to the current and previous row.
                result[attname] = (col['fn'](incol, cur, prev) if cur.get(incol, None) is not None and prev.get(incol,
                                   None) is not None else None)
            else:
                # default case - calculate the diff between the current attribute's values of
                # old and new rows and divide it by the time interval passed between measurements.
                result[attname] = ((cur[incol] - prev[incol]) / self.diff_time if cur.get(incol, None) is not None and
                                   prev.get(incol, None) is not None and self.diff_time >= 0 else None)
        return result

    def _produce_output_row(self, row):
        """ produce the output row for the screen, json or the database
            from the diff rows. It consists of renaming columns and rounding
            the result when necessary
        """

        result = {}
        # produce the output row column by column
        for col in self.output_transform_data:
            attname = self._produce_output_name(col)
            val = self._produce_output_value(row, col)
            result[attname] = val
        return result

    def _produce_output_value(self, row, col, method=OUTPUT_METHOD.console):
        # get the input value
        if 'in' in col:
            val = row.get(col['in'], None)
        else:
            val = row.get(col['out'], None)
        # if function is specified - apply it to the input value
        if 'fn' in col and val is not None:
            val = col['fn'](val)
        # if rounding is necessary - round the input value up to specified
        # decimal points
        if 'round' in col and val is not None:
            val = round(val, col['round'])
        return val

    def _produce_output_name(self, col):
        # get the output column name
        attname = col['out']
        # add units to the column name if neccessary
        if 'units' in col and self.show_units:
            attname += ' ' + col['units']
        return attname

    def _calculate_output_status(self, row, col, val, method):
        """ Examine the current status indicators and produce the status
            value for the specific column of the given row
        """

        st = {-1: COLSTATUS.cs_ok}
        # if value is missing - don't bother calculating anything
        if val is None:
            return st
        if 'status_fn' in col:
            st = col['status_fn'](row, col)
            if len(st) == 0:
                st = {-1: COLSTATUS.cs_ok}
        else:
            words = str(val).split()
            for i, word in enumerate(words):
                for st_name, st_status in zip(('critical', 'warning'), (COLSTATUS.cs_critical, COLSTATUS.cs_warning)):
                    if st_name in col:
                        typ = type(col[st_name])
                        if typ == int:
                            typ = float
                        if typ(word) >= col[st_name]:
                            st[i] = st_status
                            break
                if i not in st:
                    st[i] = COLSTATUS.cs_ok
        return st

    def _get_columns_to_hide(self, result_rows, status_rows):
        """ scan the (cooked) rows, do not show columns that are empty """

        to_skip = []
        for col in self.output_transform_data:
            if col.get('pos') == -1:
                continue
            attname = self._produce_output_name(col)
            empty = True
            for r in result_rows:
                if r[attname].value != '':
                    empty = False
                    break
            if empty:
                to_skip.append(attname)
            elif col.get('hide_if_ok', False):
                status_ok = True
                for row in status_rows:
                    if attname in row and row[attname]:
                        for cl in row[attname]:
                            if row[attname][cl] != COLSTATUS.cs_ok:
                                status_ok = False
                                break
                    if not status_ok:
                        break
                if status_ok:
                    to_skip.append(attname)
        return to_skip

    def _transform_input(self, x, custom_transformation_data=None):
        if isinstance(x, list) or isinstance(x, tuple):
            return self._transform_list(x, custom_transformation_data)
        elif isinstance(x, dict):
            return self._transform_dict(x, custom_transformation_data)
        elif isinstance(x, str):
            return self._transform_string(x)
        else:
            raise Exception('transformation of data type {0} is not supported'.format(type(x)))

    # The following 2 functions are almost the same. The only difference is the
    # behavior in case 'in' is not specified: the _dict version assumes the in
    # column is the same as the out one, the list emits the warning and skips
    # the column.
    def _transform_list(self, l, custom_transformation_data=None):
        result = {}
        # choose between the 'embedded' and external transformations
        if custom_transformation_data is not None:
            transformation_data = custom_transformation_data
        else:
            transformation_data = self.transform_list_data
        if transformation_data is not None:
            total = len(l)
            for col in transformation_data:
                # set the output column name
                attname = col['out']
                if 'infn' in col:
                    if len(l) > 0:
                        result[attname] = col['infn'](attname, l, 'optional' in col and col['optional'])
                    else:
                        result[attname] = None
                else:
                    incol = col['in']
                    # get the column from which the value is extracted
                    if incol > total - 1:
                        result[attname] = None
                        # complain on optional columns, but only if the list to transform has any data
                        # we want to catch cases when the data collectors (i.e. df, du) doesn't deliver
                        # the result in the format we ask them to, but, on the other hand, if there is
                        # nothing at all from them - then the problem is elsewhere and there is no need
                        # to bleat here for each missing column.
                        if not col.get('optional', False) and len(l) > 0:
                            self.warn_non_optional_column(incol)
                    else:
                        result[attname] = l[incol]
                # if transformation function is supplied - apply it to the input data.
                if 'fn' in col and result[attname] is not None:
                    result[attname] = col['fn'](result[attname])
            return result
        raise Exception('No data for the list transformation supplied')

    # Most of the functionality is the same as in the dict transforming function above.
    def _transform_dict(self, l, custom_transformation_data=None):
        result = {}
        if custom_transformation_data is not None:
            transformation_data = custom_transformation_data
        else:
            transformation_data = self.transform_dict_data
        if transformation_data:
            for col in transformation_data:
                attname = col['out']
                # if input column name is not supplied - assume it's the same as an output one.
                incol = self._get_input_column_name(col)
                # if infn is supplied - it calculates the column value possbily using other values
                # in the row - we don't use incoming column in this case.
                if 'infn' in col:
                    if len(l) > 0:
                        result[attname] = col['infn'](attname, l, 'optional' in col and col['optional'])
                    else:
                        result[attname] = None
                elif incol not in l:
                    # if the column is marked as optional and it's not present in the output data
                    # set None instead
                    result[attname] = None
                    # see the comment at _transform_list on why we do complain here.
                    if not col.get('optional', False) and len(l) > 0:
                        self.warn_non_optional_column(incol)
                else:
                    result[attname] = l[incol]
                if 'fn' in col and result[attname] is not None:
                    result[attname] = col['fn'](result[attname])
            return result
        raise Exception('No data for the dict transformation supplied')

    def _transform_string(self, d):
        raise Exception('transformation of input type string is not implemented')

    def _output_template_for_console(self):
        return ' '.join(self._output_row_for_console(None, 't'))

    def _output_row_for_console(self, row, typ='v'):
        return self._output_row_generic(row, typ, method=OUTPUT_METHOD.console)

    def _output_row_for_curses(self, row, typ='v'):
        return self._output_row_generic(row, typ, method=OUTPUT_METHOD.curses)

    def _output_row_generic(self, row, typ='v', method=OUTPUT_METHOD.console):
        """ produce a single output row of the type specified by the
            last argument:
            t - template row
            h - header row (only names)
            v - values rows
        """

        vals = []
        # produce the output row column by column
        for i, col in enumerate(self.output_transform_data):
            # get the final attribute name and value
            if typ == 't':
                if 'w' not in col:
                    val = '{{{0}}}'.format(i)
                else:
                    val = '{{{0}:<{1}}}'.format(i, col['w'])
            elif typ == 'h':
                val = self._produce_output_name(col)
            else:
                val = self._produce_output_value(row, col, method)
            # prepare the list for the output
            vals.append(val)
        if 'typ' != 'v':
            return vals
        else:
            return vals

    def console_output(self, rows, before_string=None, after_string=None):
        """ Main entry point for preparing textual console output """

        result = []
        # start by filling-out width of the values
        self._calculate_dynamic_width(rows)

        # now produce output template, headers and actual values
        templ = self._output_template_for_console()
        header = self._output_row_for_console(None, 'h')

        if before_string:
            result.append(before_string)

        result.append(templ.format(*header))

        for r in rows:
            row = self._output_row_for_console(r, 'v')
            result.append(templ.format(*row))

        if after_string:
            result.append(after_string)

        return '\n'.join(result)

    def _calculate_dynamic_width(self, rows, method=OUTPUT_METHOD.console):
        """ Examine values in all rows and get the width dynamically """

        for col in self.output_transform_data:
            minw = col.get('minw', 0)
            attname = self._produce_output_name(col)
            # XXX:  if append_column_header, min width should include the size of the attribut name
            if method == OUTPUT_METHOD.curses and self.ncurses_custom_fields.get('prepend_column_headers'):
                minw += len(attname) + 1
            col['w'] = len(attname)
            # use cooked values
            for row in rows:
                if method == OUTPUT_METHOD.curses and self.ncurses_filter_row(row):
                    continue
                val = self._produce_output_value(row, col, method)
                if self.cook_function.get(method):
                    val = self.cook_function[method](attname, val, col)
                if method == OUTPUT_METHOD.curses:
                    curw = val.length
                else:
                    curw = len(str(val))
                if curw > col['w']:
                    col['w'] = curw
                if minw > 0:
                    col['w'] = max(minw, col['w'])

    def _calculate_statuses_for_row(self, row, method):
        statuses = []
        for num, col in enumerate(self.output_transform_data):
            statuses.append(self._calculate_output_status(row, col, row[num], method))
        return statuses

    def _calculate_column_types(self, rows):
        result = {}
        if len(rows) > 0:
            colnames = rows[0].keys()
            for colname in colnames:
                for r in rows:
                    val = r[colname]
                    if val is None or val == '':
                        continue
                    else:
                        if isinstance(val, Number):
                            result[colname] = COLTYPES.ct_number
                        else:
                            result[colname] = COLTYPES.ct_string
                        break
                else:
                    # if all values are None - we don't care, so use a generic string
                    result[colname] = COLTYPES.ct_string
        return result

    def _get_highlights(self):
        return [col.get('highlight', False) for col in self.output_transform_data]

    @staticmethod
    def _get_input_column_name(col):
        if 'in' in col:
            return col['in']
        else:
            return col['out']

    def json_output(self, rows, before_string=None, after_string=None):
        output = {}
        data = []
        output['type'] = StatCollector.ident(self)
        if self.__dict__.get('dbname') and self.__dict__.get('dbver'):
            output['name'] = '{0}/{1}'.format(self.dbname, self.dbver)
        for r in rows:
            data.append(self._produce_output_row(r))
            output['data'] = data
        return json.dumps(output, indent=4)

    def ncurses_filter_row(self, row):
        return False

    def ncurses_output(self, rows, before_string=None, after_string=None):
        """ for ncurses - we just return data structures. The output code
            is quite complex and deserves a separate class.
        """

        self._calculate_dynamic_width(rows, method=OUTPUT_METHOD.curses)

        raw_result = {}
        for k in StatCollector.NCURSES_DEFAULTS.keys():
            raw_result[k] = []

        for col in self.output_transform_data:
            for opt in StatCollector.NCURSES_DEFAULTS.keys():
                raw_result[opt].append((col[opt] if opt in col else StatCollector.NCURSES_DEFAULTS[opt]))

        result_header = self._output_row_for_curses(None, 'h')
        result_rows = []
        status_rows = []
        values_rows = []

        for r in rows:
            values_row = self._output_row_for_curses(r, 'v')
            if self.ncurses_filter_row(dict(zip(result_header, values_row))):
                continue
            cooked_row = self.cook_row(result_header, values_row, method=OUTPUT_METHOD.curses)
            status_row = self._calculate_statuses_for_row(values_row, method=OUTPUT_METHOD.curses)
            result_rows.append(dict(zip(result_header, cooked_row)))
            status_rows.append(dict(zip(result_header, status_row)))
            values_rows.append(dict(zip(result_header, values_row)))

        types_row = self._calculate_column_types(values_rows)

        result = {}
        result['rows'] = result_rows
        result['statuses'] = status_rows
        result['hide'] = self._get_columns_to_hide(result_rows, status_rows)
        result['highlights'] = dict(zip(result_header, self._get_highlights()))
        result['types'] = types_row
        for x in StatCollector.NCURSES_CUSTOM_OUTPUT_FIELDS:
            result[x] = self.ncurses_custom_fields.get(x, None)
        for k in StatCollector.NCURSES_DEFAULTS.keys():
            if k == 'noautohide' and self.ignore_autohide:
                result[k] = dict.fromkeys(result_header, True)
            else:
                result[k] = dict(zip(result_header, raw_result[k]))
        return {self.ident(): result}

    def output(self, method, before_string=None, after_string=None):
        if method not in self.output_function:
            raise Exception('Output method {0} is not supported'.format(method))
        if self.produce_diffs:
            rows = self.rows_diff
        else:
            rows = self.rows_cur
        return self.output_function[method](rows, before_string, after_string)

    def diff(self):
        self.clear_diffs()
        # empty values for current or prev rows are already covered by the need
        for prev, cur in zip(self.rows_prev, self.rows_cur):
            candidate = self._produce_diff_row(prev, cur)
            if candidate is not None and len(candidate) > 0:
                # produce the actual diff row
                self.rows_diff.append(candidate)

    def clear_diffs(self):
        self.rows_diff = []


class PgstatCollector(StatCollector):

    """ Collect PostgreSQL-related statistics """

    STATM_FILENAME = '/proc/{0}/statm'

    def __init__(self, pgcon, reconnect, pid, dbname, dbver, always_track_pids):
        super(PgstatCollector, self).__init__()
        self.postmaster_pid = pid
        self.pgcon = pgcon
        self.reconnect = reconnect
        self.pids = []
        self.rows_diff = []
        self.rows_diff_output = []
        # figure out our backend pid
        self.connection_pid = pgcon.get_backend_pid()
        self.max_connections = self._get_max_connections()
        self.recovery_status = self._get_recovery_status()
        self.always_track_pids = always_track_pids
        self.dbname = dbname
        self.dbver = dbver
        self.server_version = pgcon.get_parameter_status('server_version')
        self.filter_aux_processes = True
        self.total_connections = 0
        self.active_connections = 0

        self.transform_list_data = [
            {'out': 'pid', 'in': 0, 'fn': int},
            {'out': 'state', 'in': 2},
            {'out': 'utime', 'in': 13, 'fn': StatCollector.ticks_to_seconds},
            {'out': 'stime', 'in': 14, 'fn': StatCollector.ticks_to_seconds},
            {'out': 'priority', 'in': 17, 'fn': int},
            {'out': 'starttime', 'in': 21, 'fn': long},
            {'out': 'vsize', 'in': 22, 'fn': int},
            {'out': 'rss', 'in': 23, 'fn': int},
            {
                'out': 'delayacct_blkio_ticks',
                'in': 41,
                'fn': long,
                'optional': True,
            },
            {
                'out': 'guest_time',
                'in': 42,
                'fn': StatCollector.ticks_to_seconds,
                'optional': True,
            },
        ]

        self.transform_dict_data = [{'out': 'read_bytes', 'fn': int, 'optional': True}, {'out': 'write_bytes',
                                    'fn': int, 'optional': True}]

        self.diff_generator_data = [
            {'out': 'pid', 'diff': False},
            {'out': 'type', 'diff': False},
            {'out': 'state', 'diff': False},
            {'out': 'priority', 'diff': False},
            {'out': 'utime'},
            {'out': 'stime'},
            {'out': 'guest_time'},
            {'out': 'delayacct_blkio_ticks'},
            {'out': 'read_bytes'},
            {'out': 'write_bytes'},
            {'out': 'uss', 'diff': False},
            {'out': 'age', 'diff': False},
            {'out': 'datname', 'diff': False},
            {'out': 'usename', 'diff': False},
            {'out': 'waiting', 'diff': False},
            {'out': 'locked_by', 'diff': False},
            {'out': 'query', 'diff': False},
        ]

        self.output_transform_data = [  # query with age 5 and more will have the age column highlighted
            {
                'out': 'pid',
                'pos': 0,
                'minw': 5,
                'noautohide': True,
            },
            {
                'out': 'lock',
                'in': 'locked_by',
                'pos': 1,
                'minw': 5,
                'noautohide': True,
            },
            {'out': 'type', 'pos': 1},
            {
                'out': 's',
                'in': 'state',
                'pos': 2,
                'status_fn': self.check_ps_state,
                'warning': 'D',
            },
            {
                'out': 'utime',
                'units': '%',
                'fn': StatCollector.time_diff_to_percent,
                'round': StatCollector.RD,
                'pos': 4,
                'warning': 90,
                'align': COLALIGN.ca_right,
            },
            {
                'out': 'stime',
                'units': '%',
                'fn': StatCollector.time_diff_to_percent,
                'round': StatCollector.RD,
                'pos': 5,
                'warning': 5,
                'critical': 30,
            },
            {
                'out': 'guest',
                'in': 'guest_time',
                'units': '%',
                'fn': StatCollector.time_diff_to_percent,
                'round': StatCollector.RD,
                'pos': 6,
            },
            {
                'out': 'delay_blkio',
                'in': 'delayacct_blkio_ticks',
                'units': '/s',
                'round': StatCollector.RD,
            },
            {
                'out': 'read',
                'in': 'read_bytes',
                'units': 'MB/s',
                'fn': StatCollector.bytes_to_mbytes,
                'round': StatCollector.RD,
                'pos': 7,
                'noautohide': True,
            },
            {
                'out': 'write',
                'in': 'write_bytes',
                'units': 'MB/s',
                'fn': StatCollector.bytes_to_mbytes,
                'round': StatCollector.RD,
                'pos': 8,
                'noautohide': True,
            },
            {
                'out': 'uss',
                'in': 'uss',
                'units': 'MB',
                'fn': StatCollector.bytes_to_mbytes,
                'round': StatCollector.RD,
                'pos': 9,
                'noautohide': True
            },
            {
                'out': 'age',
                'in': 'age',
                'noautohide': True,
                'pos': 9,
                'fn': StatCollector.time_pretty_print,
                'status_fn': self.age_status_fn,
                'align': COLALIGN.ca_right,
                'warning': 300,
            },
            {
                'out': 'db',
                'in': 'datname',
                'pos': 10,
                'noautohide': True,
                'maxw': 14,
            },
            {
                'out': 'user',
                'in': 'usename',
                'pos': 11,
                'noautohide': True,
                'maxw': 14,
            },
            {
                'out': 'w',
                'in': 'waiting',
                'pos': -1,
                'hide_if_ok': True,
            },
            {
                'out': 'query',
                'pos': 12,
                'noautohide': True,
                'fn': self.idle_format_fn,
                'warning': 'idle in transaction',
                'critical': 'locked',
                'status_fn': self.query_status_fn,
            },
        ]

        self.ncurses_custom_fields = {'header': True}
        self.ncurses_custom_fields['prefix'] = None

        self.postinit()

    def get_subprocesses_pid(self):
        ppid = self.postmaster_pid
        result = self.exec_command_with_output('ps -o pid --ppid {0} --noheaders'.format(ppid))
        if result[0] != 0:
            logger.info("Couldn't determine the pid of subprocesses for {0}".format(ppid))
            self.pids = []
        self.pids = [int(x) for x in result[1].split()]

    def check_ps_state(self, row, col):
        if row[self.output_column_positions[col['out']]] == col.get('warning', ''):
            return {0: COLSTATUS.cs_warning}
        return {0: COLSTATUS.cs_ok}

    def age_status_fn(self, row, col):
        age_string = row[self.output_column_positions[col['out']]]
        age_seconds = self.time_field_to_seconds(age_string)
        if 'critical' in col and col['critical'] < age_seconds:
            return {-1: COLSTATUS.cs_critical}
        if 'warning' in col and col['warning'] < age_seconds:
            return {-1: COLSTATUS.cs_warning}
        return {-1: COLSTATUS.cs_ok}

    def idle_format_fn(self, text):
        r = re.match(r'idle in transaction (\d+)', text)
        if not r:
            return text
        else:
            if self.dbver >= 9.2:
                return 'idle in transaction for ' + StatCollector.time_pretty_print(int(r.group(1)))
            else:
                return 'idle in transaction ' + StatCollector.time_pretty_print(int(r.group(1))) \
                    + ' since the last query start'

    def query_status_fn(self, row, col):
        if row[self.output_column_positions['w']] is True:
            return {-1: COLSTATUS.cs_critical}
        else:
            val = row[self.output_column_positions[col['out']]]
            if val and val.startswith(col.get('warning', '!')):
                return {-1: COLSTATUS.cs_warning}
        return {-1: COLSTATUS.cs_ok}

    def ident(self):
        return '{0} ({1}/{2})'.format('postgres', self.dbname, self.dbver)

    @staticmethod
    def _get_psinfo(cmdline):
        """ gets PostgreSQL process type from the command-line."""
        pstype = 'unknown'
        action = None
        if cmdline is not None and len(cmdline) > 0:
            # postgres: stats collector process
            m = re.match(r'postgres:\s+(.*)\s+process\s*(.*)$', cmdline)
            if m:
                pstype = m.group(1)
                action = m.group(2)
            else:
                if re.match(r'postgres:.*', cmdline):
                    # assume it's a backend process
                    pstype = 'backend'
        if pstype == 'autovacuum worker':
            pstype = 'autovacuum'
        return (pstype, action)

    @staticmethod
    def _is_auxiliary_process(pstype):
        if pstype == 'backend' or pstype == 'autovacuum':
            return False
        return True

    def set_aux_processes_filter(self, newval):
        self.filter_aux_processes = newval

    def ncurses_filter_row(self, row):
        if self.filter_aux_processes:
            # type is the second column
            return self._is_auxiliary_process(row['type'])
        else:
            return False

    def refresh(self):
        """ Reads data from /proc and PostgreSQL stats """

        result = []
        # fetch up-to-date list of subprocess PIDs
        self.get_subprocesses_pid()
        try:
            if not self.pgcon:
                # if we've lost the connection, try to reconnect and
                # re-initialize all connection invariants
                self.pgcon, self.postmaster_pid = self.reconnect()
                self.connection_pid = self.pgcon.get_backend_pid()
                self.max_connections = self._get_max_connections()
                self.dbver = dbversion_as_float(self.pgcon)
                self.server_version = self.pgcon.get_parameter_status('server_version')
            stat_data = self._read_pg_stat_activity()
        except psycopg2.OperationalError as e:
            logger.info("failed to query the server: {}".format(e))
            if self.pgcon and not self.pgcon.closed:
                self.pgcon.close()
            self.pgcon = None
            self._do_refresh([])
            return
        logger.info("new refresh round")
        for pid in self.pids:
            if pid == self.connection_pid:
                continue
            is_backend = pid in stat_data
            is_active = is_backend and (stat_data[pid]['query'] != 'idle' or pid in self.always_track_pids)
            result_row = {}
            # for each pid, get hash row from /proc/
            proc_data = self._read_proc(pid, is_backend, is_active)
            if proc_data:
                result_row.update(proc_data)
            if stat_data and pid in stat_data:
                # ditto for the pg_stat_activity
                result_row.update(stat_data[pid])
            # result is not empty - add it to the list of current rows
            if result_row:
                result.append(result_row)
        # and refresh the rows with this data
        self._do_refresh(result)

    def _read_proc(self, pid, is_backend, is_active):
        """ see man 5 proc for details (/proc/[pid]/stat) """

        result = {}
        raw_result = {}

        fp = None
        # read raw data from /proc/stat, proc/cmdline and /proc/io
        for ftyp, fname in zip(('stat', 'cmd', 'io',), ('/proc/{0}/stat', '/proc/{0}/cmdline', '/proc/{0}/io')):
            try:
                fp = open(fname.format(pid), 'rU')
                if ftyp == 'stat':
                    raw_result[ftyp] = fp.read().strip().split()
                if ftyp == 'cmd':
                    # large number of trailing \0x00 returned by python
                    raw_result[ftyp] = fp.readline().strip('\x00').strip()
                if ftyp == 'io':
                    proc_stat_io_read = {}
                    for line in fp:
                        x = [e.strip(':') for e in line.split()]
                        if len(x) < 2:
                            logger.error('{0} content not in the "name: value" form: {1}'.format(fname.format(pid),
                                         line))
                            continue
                        else:
                            proc_stat_io_read[x[0]] = int(x[1])
                    raw_result[ftyp] = proc_stat_io_read
            except IOError:
                logger.warning('Unable to read {0}, process data will be unavailable'.format(fname.format(pid)))
                return None
            finally:
                fp and fp.close()

        # Assume we managed to read the row if we can get its PID
        for cat in 'stat', 'io':
            result.update(self._transform_input(raw_result.get(cat, ({} if cat == 'io' else []))))
        # generated columns
        result['cmdline'] = raw_result.get('cmd', None)
        if not is_backend:
            result['type'], action = self._get_psinfo(result['cmdline'])
            if action:
                result['query'] = action
        else:
            result['type'] = 'backend'
        if is_active or not is_backend:
            result['uss'] = self._get_memory_usage(pid)
        return result

    def _get_memory_usage(self, pid):
        """ calculate usage of private memory per process """
        # compute process's own non-shared memory.
        # See http://www.depesz.com/2012/06/09/how-much-ram-is-postgresql-using/ for the explanation of how
        # to measure PostgreSQL process memory usage and the stackexchange answer for details on the unshared counts:
        # http://unix.stackexchange.com/questions/33381/getting-information-about-a-process-memory-usage-from-proc-pid-smaps
        # there is also a good discussion here:
        # http://rhaas.blogspot.de/2012/01/linux-memory-reporting.html
        # we use statm instead of /proc/smaps because of performance considerations. statm is much faster,
        # while providing slightly outdated results.
        uss = 0
        statm = None
        fp = None
        try:
            fp = open(self.STATM_FILENAME.format(pid), 'r')
            statm = fp.read().strip().split()
            logger.info("calculating memory for process {0}".format(pid))
        except IOError as e:
            logger.warning('Unable to read {0}: {1}, process memory information will be unavailable'.format(
                           self.format(pid), e))
        finally:
            fp and fp.close()
        if statm and len(statm) >= 3:
            uss = (long(statm[1]) - long(statm[2])) * MEM_PAGE_SIZE
        return uss

    def _get_max_connections(self):
        """ Read max connections from the database """

        cur = self.pgcon.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute('show max_connections')
        result = cur.fetchone()
        cur.close()
        return int(result.get('max_connections', 0))

    def _get_recovery_status(self):
        """ Determine whether the Postgres process is in recovery """

        cur = self.pgcon.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("select case when pg_is_in_recovery() then 'standby' else 'master' end as role")
        result = cur.fetchone()
        cur.close()
        return result.get('role', 'unknown')

    def _read_pg_stat_activity(self):
        """ Read data from pg_stat_activity """

        self.recovery_status = self._get_recovery_status()
        cur = self.pgcon.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        # the pg_stat_activity format has been changed to 9.2, avoiding ambigiuous meanings for some columns.
        # since it makes more sense then the previous layout, we 'cast' the former versions to 9.2
        if self.dbver < 9.2:
            cur.execute("""
                    SELECT datname,
                           procpid as pid,
                           usename,
                           client_addr,
                           client_port,
                           round(extract(epoch from (now() - xact_start))) as age,
                           waiting,
                           string_agg(other.pid::TEXT, ',' ORDER BY other.pid) as locked_by,
                           CASE
                             WHEN current_query = '<IDLE>' THEN 'idle'
                             WHEN current_query = '<IDLE> in transaction' THEN
                                  CASE WHEN xact_start != query_start THEN
                                      'idle in transaction'||' '||CAST(
                                          abs(round(extract(epoch from (now() - query_start)))) AS text
                                      )
                                  ELSE
                                      'idle in transaction'
                                  END
                             WHEN current_query = '<IDLE> in transaction (aborted)' THEN 'idle in transaction (aborted)'
                            ELSE current_query
                           END AS query
                      FROM pg_stat_activity
                      LEFT JOIN pg_locks  this ON (this.pid = procpid and this.granted = 'f')
                      -- acquire the same type of lock that is granted
                      LEFT JOIN pg_locks other ON ((this.locktype = other.locktype AND other.granted = 't')
                                               AND ( ( this.locktype IN ('relation', 'extend')
                                                      AND this.database = other.database
                                                      AND this.relation = other.relation)
                                                     OR (this.locktype ='page'
                                                      AND this.database = other.database
                                                      AND this.relation = other.relation
                                                      AND this.page = other.page)
                                                     OR (this.locktype ='tuple'
                                                      AND this.database = other.database
                                                      AND this.relation = other.relation
                                                      AND this.page = other.page
                                                      AND this.tuple = other.tuple)
                                                     OR (this.locktype ='transactionid'
                                                      AND this.transactionid = other.transactionid)
                                                     OR (this.locktype = 'virtualxid'
                                                      AND this.virtualxid = other.virtualxid)
                                                     OR (this.locktype IN ('object', 'userlock', 'advisory')
                                                      AND this.database = other.database
                                                      AND this.classid = other.classid
                                                      AND this.objid = other.objid
                                                      AND this.objsubid = other.objsubid))
                                                   )
                      WHERE procpid != pg_backend_pid()
                      GROUP BY 1,2,3,4,5,6,7,9
                """)
        elif self.dbver < 9.6:
            cur.execute("""
                    SELECT datname,
                           a.pid as pid,
                           usename,
                           client_addr,
                           client_port,
                           round(extract(epoch from (now() - xact_start))) as age,
                           waiting,
                           string_agg(other.pid::TEXT, ',' ORDER BY other.pid) as locked_by,
                           CASE
                              WHEN state = 'idle in transaction' THEN
                                  CASE WHEN xact_start != state_change THEN
                                      state||' '||CAST( abs(round(extract(epoch from (now() - state_change)))) AS text )
                                  ELSE
                                      state
                                  END
                              WHEN state = 'active' THEN query
                              ELSE state
                              END AS query
                      FROM pg_stat_activity a
                      LEFT JOIN pg_locks  this ON (this.pid = a.pid and this.granted = 'f')
                      -- acquire the same type of lock that is granted
                      LEFT JOIN pg_locks other ON ((this.locktype = other.locktype AND other.granted = 't')
                                               AND ( ( this.locktype IN ('relation', 'extend')
                                                      AND this.database = other.database
                                                      AND this.relation = other.relation)
                                                     OR (this.locktype ='page'
                                                      AND this.database = other.database
                                                      AND this.relation = other.relation
                                                      AND this.page = other.page)
                                                     OR (this.locktype ='tuple'
                                                      AND this.database = other.database
                                                      AND this.relation = other.relation
                                                      AND this.page = other.page
                                                      AND this.tuple = other.tuple)
                                                     OR (this.locktype ='transactionid'
                                                      AND this.transactionid = other.transactionid)
                                                     OR (this.locktype = 'virtualxid'
                                                      AND this.virtualxid = other.virtualxid)
                                                     OR (this.locktype IN ('object', 'userlock', 'advisory')
                                                      AND this.database = other.database
                                                      AND this.classid = other.classid
                                                      AND this.objid = other.objid
                                                      AND this.objsubid = other.objsubid))
                                                   )
                      WHERE a.pid != pg_backend_pid()
                      GROUP BY 1,2,3,4,5,6,7,9
                """)
        else:
            cur.execute("""
                    SELECT datname,
                           a.pid as pid,
                           usename,
                           client_addr,
                           client_port,
                           round(extract(epoch from (now() - xact_start))) as age,
                           CASE WHEN wait_event IS NULL THEN false ELSE true END as waiting,
                           string_agg(other.pid::TEXT, ',' ORDER BY other.pid) as locked_by,
                           CASE
                              WHEN state = 'idle in transaction' THEN
                                  CASE WHEN xact_start != state_change THEN
                                      state||' '||CAST( abs(round(extract(epoch from (now() - state_change)))) AS text )
                                  ELSE
                                      state
                                  END
                              WHEN state = 'active' THEN query
                              ELSE state
                              END AS query
                      FROM pg_stat_activity a
                      LEFT JOIN pg_locks  this ON (this.pid = a.pid and this.granted = 'f')
                      -- acquire the same type of lock that is granted
                      LEFT JOIN pg_locks other ON ((this.locktype = other.locktype AND other.granted = 't')
                                               AND ( ( this.locktype IN ('relation', 'extend')
                                                      AND this.database = other.database
                                                      AND this.relation = other.relation)
                                                     OR (this.locktype ='page'
                                                      AND this.database = other.database
                                                      AND this.relation = other.relation
                                                      AND this.page = other.page)
                                                     OR (this.locktype ='tuple'
                                                      AND this.database = other.database
                                                      AND this.relation = other.relation
                                                      AND this.page = other.page
                                                      AND this.tuple = other.tuple)
                                                     OR (this.locktype ='transactionid'
                                                      AND this.transactionid = other.transactionid)
                                                     OR (this.locktype = 'virtualxid'
                                                      AND this.virtualxid = other.virtualxid)
                                                     OR (this.locktype IN ('object', 'userlock', 'advisory')
                                                      AND this.database = other.database
                                                      AND this.classid = other.classid
                                                      AND this.objid = other.objid
                                                      AND this.objsubid = other.objsubid))
                                                   )
                      WHERE a.pid != pg_backend_pid()
                      GROUP BY 1,2,3,4,5,6,7,9
            """)
        results = cur.fetchall()
        # fill in the number of total connections, including ourselves
        self.total_connections = len(results) + 1
        self.active_connections = 0
        ret = {}
        for r in results:
            # stick multiline queries together
            if r.get('query', None):
                if r['query'] != 'idle':
                    if r['pid'] != self.connection_pid:
                        self.active_connections += 1
                lines = r['query'].splitlines()
                newlines = [re.sub('\s+', ' ', l.strip()) for l in lines]
                r['query'] = ' '.join(newlines)
            ret[r['pid']] = r
        self.pgcon.commit()
        cur.close()
        return ret

    def ncurses_produce_prefix(self):
        if self.pgcon:
            return "{dbname} {version} {role} connections: {conns} of {max_conns} allocated, {active_conns} active\n".\
                format(dbname=self.dbname,
                       version=self.server_version,
                       role=self.recovery_status,
                       conns=self.total_connections,
                       max_conns=self.max_connections,
                       active_conns=self.active_connections)
        else:
            return "{dbname} {version} (offline)\n".\
                format(dbname=self.dbname,
                       version=self.server_version)

    @staticmethod
    def process_sort_key(process):
        return process['age'] if process['age'] is not None else maxsize

    def diff(self):
        """ we only diff backend processes if new one is not idle and use pid to identify processes """

        self.rows_diff = []
        self.running_diffs = []
        self.blocked_diffs = {}
        for cur in self.rows_cur:
            if 'query' not in cur or cur['query'] != 'idle' or cur['pid'] in self.always_track_pids:
                # look for the previous row corresponding to the current one
                for x in self.rows_prev:
                    if x['pid'] == cur['pid']:
                        prev = x
                        break
                else:
                    continue
                # now we have a previous and a current row - do the diff
                candidate = self._produce_diff_row(prev, cur)
                if candidate is not None and len(candidate) > 0:
                    if candidate['locked_by'] is None:
                        self.running_diffs.append(candidate)
                    else:
                        # when determining the position where to put the blocked process,
                        # only consider the first blocker. This will provide consustent
                        # results for multiple processes blocked by the same set of blockers,
                        # since the list is sorted by pid.
                        block_pid = int(candidate['locked_by'].split(',')[0])
                        if block_pid not in self.blocked_diffs:
                            self.blocked_diffs[block_pid] = [candidate]
                        else:
                            self.blocked_diffs[block_pid].append(candidate)
        # order the result rows by the start time value
        if len(self.blocked_diffs) == 0:
            self.rows_diff = self.running_diffs
            self.rows_diff.sort(key=self.process_sort_key, reverse=True)
        else:
            blocked_temp = []
            # we traverse the tree of blocked processes in a depth-first order, building a list
            # to display the blocked processes near the blockers. The reason we need multiple
            # loops here is because there is no way to quickly fetch the processes blocked
            # by the current one from the plain list of process information rows, that's why
            # we use a dictionary of lists of blocked processes with a blocker pid as a key
            # and effectively build a separate tree for each blocker.
            self.running_diffs.sort(key=self.process_sort_key, reverse=True)
            # sort elements in the blocked lists, so that they still appear in the latest to earliest order
            for key in self.blocked_diffs:
                self.blocked_diffs[key].sort(key=self.process_sort_key)
            for parent_row in self.running_diffs:
                self.rows_diff.append(parent_row)
                # if no processes blocked by this one - just skip to the next row
                if parent_row['pid'] in self.blocked_diffs:
                    blocked_temp.extend(self.blocked_diffs[parent_row['pid']])
                    del self.blocked_diffs[parent_row['pid']]
                    while len(blocked_temp) > 0:
                        # traverse a tree (in DFS order) of all processes blocked by the current one
                        child_row = blocked_temp.pop()
                        self.rows_diff.append(child_row)
                        if child_row['pid'] in self.blocked_diffs:
                            blocked_temp.extend(self.blocked_diffs[child_row['pid']])
                            del self.blocked_diffs[child_row['pid']]

    def output(self, method):
        return super(self.__class__, self).output(method, before_string='PostgreSQL processes:', after_string='\n')


class SystemStatCollector(StatCollector):

    """ Collect global system statistics, i.e. CPU/IO usage, not including memory. """

    PROC_STAT_FILENAME = '/proc/stat'

    def __init__(self):
        super(SystemStatCollector, self).__init__()

        self.transform_list_data = [
            {'out': 'utime', 'in': 0, 'fn': float},
            {'out': 'stime', 'in': 2, 'fn': float},
            {'out': 'idle', 'in': 3, 'fn': float},
            {'out': 'iowait', 'in': 4, 'fn': float},
            {'out': 'irq', 'in': 5, 'fn': float},
            {
                'out': 'softirq',
                'in': 6,
                'fn': float,
                'optional': True,
            },
            {
                'out': 'steal',
                'in': 7,
                'fn': float,
                'optional': True,
            },
            {
                'out': 'guest',
                'in': 8,
                'fn': float,
                'optional': True,
            },
        ]

        self.transform_dict_data = [{'out': 'ctxt', 'fn': float}, {'out': 'cpu'}, {'out': 'running',
                                    'in': 'procs_running', 'fn': int}, {'out': 'blocked', 'in': 'procs_blocked',
                                    'fn': int}]

        self.diff_generator_data = [
            {'out': 'utime', 'fn': self._cpu_time_diff},
            {'out': 'stime', 'fn': self._cpu_time_diff},
            {'out': 'idle', 'fn': self._cpu_time_diff},
            {'out': 'iowait', 'fn': self._cpu_time_diff},
            {'out': 'irq', 'fn': self._cpu_time_diff},
            {'out': 'softirq', 'fn': self._cpu_time_diff},
            {'out': 'steal', 'fn': self._cpu_time_diff},
            {'out': 'guest', 'fn': self._cpu_time_diff},
            {'out': 'ctxt'},
            {'out': 'running', 'diff': False},
            {'out': 'blocked', 'diff': False},
        ]

        self.output_transform_data = [
            {
                'out': 'utime',
                'units': '%',
                'fn': StatCollector.time_diff_to_percent,
                'round': StatCollector.RD,
                'minw': 5,
                'pos': 0,
                'warning': 50,
                'critial': 90,
            },
            {
                'out': 'stime',
                'units': '%',
                'fn': StatCollector.time_diff_to_percent,
                'round': StatCollector.RD,
                'pos': 1,
                'minw': 5,
                'warning': 10,
                'critical': 30,
            },
            {
                'out': 'idle',
                'units': '%',
                'fn': StatCollector.time_diff_to_percent,
                'round': StatCollector.RD,
                'pos': 2,
                'minw': 5,
            },
            {
                'out': 'iowait',
                'units': '%',
                'fn': StatCollector.time_diff_to_percent,
                'round': StatCollector.RD,
                'pos': 3,
                'minw': 5,
                'warning': 20,
                'critical': 50,
            },
            {
                'out': 'irq',
                'units': '%',
                'fn': StatCollector.time_diff_to_percent,
                'round': StatCollector.RD,
            },
            {
                'out': 'soft',
                'in': 'softirq',
                'units': '%',
                'fn': StatCollector.time_diff_to_percent,
                'round': StatCollector.RD,
            },
            {
                'out': 'steal',
                'units': '%',
                'fn': StatCollector.time_diff_to_percent,
                'round': StatCollector.RD,
            },
            {
                'out': 'guest',
                'units': '%',
                'fn': StatCollector.time_diff_to_percent,
                'round': StatCollector.RD,
            },
            {
                'out': 'ctxt',
                'units': '/s',
                'fn': int,
                'pos': 4,
            },
            {
                'out': 'run',
                'in': 'running',
                'pos': 5,
                'minw': 3,
            },
            {
                'out': 'block',
                'in': 'blocked',
                'pos': 6,
                'minw': 3,
                'warning': 1,
                'critial': 5,
            },
        ]

        self.previos_total_cpu_time = 0
        self.current_total_cpu_time = 0
        self.cpu_time_diff = 0
        self.ncurses_custom_fields = {'header': False, 'prefix': 'sys: ', 'prepend_column_headers': True}

        self.postinit()

    def refresh(self):
        """ Read data from global /proc/stat """

        result = {}
        stat_data = self._read_proc_stat()
        cpu_data = self._read_cpu_data(stat_data.get('cpu', []))
        result.update(stat_data)
        result.update(cpu_data)
        self._refresh_cpu_time_values(cpu_data)
        self._do_refresh([result])

    def _refresh_cpu_time_values(self, cpu_data):
        # calculate the sum of all CPU indicators and store it.
        total_cpu_time = sum(v for v in cpu_data.values() if v is not None)
        # calculate actual differences in cpu time values
        self.previos_total_cpu_time = self.current_total_cpu_time
        self.current_total_cpu_time = total_cpu_time
        self.cpu_time_diff = self.current_total_cpu_time - self.previos_total_cpu_time

    def _read_proc_stat(self):
        """ see man 5 proc for details (/proc/stat). We don't parse cpu info here """

        raw_result = {}
        result = {}
        try:
            fp = open(SystemStatCollector.PROC_STAT_FILENAME, 'rU')
            # split /proc/stat into the name - value pairs
            for line in fp:
                elements = line.strip().split()
                if len(elements) > 2:
                    raw_result[elements[0]] = elements[1:]
                elif len(elements) > 1:
                    raw_result[elements[0]] = elements[1]
                # otherwise, the line is probably empty or bogus and should be skipped
            result = self._transform_input(raw_result)
        except IOError:
            logger.error('Unable to read {0}, global data will be unavailable'.format(self.PROC_STAT_FILENAME))
        return result

    def _cpu_time_diff(self, colname, cur, prev):
        if cur.get(colname, None) and prev.get(colname, None) and self.cpu_time_diff > 0:
            return (cur[colname] - prev[colname]) / self.cpu_time_diff
        else:
            return None

    def _read_cpu_data(self, cpu_row):
        """ Parse the cpu row from /proc/stat """

        return self._transform_input(cpu_row)

    def output(self, method):
        return super(SystemStatCollector, self).output(method, before_string='System statistics:', after_string='\n')


class PartitionStatCollector(StatCollector):

    """Collect statistics about PostgreSQL partitions """

    DISK_STAT_FILE = '/proc/diskstats'
    DATA_NAME = 'data'
    XLOG_NAME = 'xlog'
    XLOG_SUBDIR = 'pg_xlog/'
    BLOCK_SIZE = 1024

    def __init__(self, dbname, dbversion, work_directory, consumer):
        super(PartitionStatCollector, self).__init__(ticks_per_refresh=1)
        self.dbname = dbname
        self.dbver = dbversion
        self.queue_consumer = consumer
        self.work_directory = work_directory
        self.df_list_transformation = [{'out': 'dev', 'in': 0, 'fn': self._dereference_dev_name},
                                       {'out': 'space_total', 'in': 1, 'fn': int},
                                       {'out': 'space_left', 'in': 2, 'fn': int}]
        self.io_list_transformation = [{'out': 'sectors_read', 'in': 5, 'fn': int}, {'out': 'sectors_written', 'in': 9,
                                       'fn': int}, {'out': 'await', 'in': 13, 'fn': int}]
        self.du_list_transformation = [{'out': 'path_size', 'in': 0, 'fn': int}, {'out': 'path', 'in': 1}]

        self.diff_generator_data = [
            {'out': 'type', 'diff': False},
            {'out': 'dev', 'diff': False},
            {'out': 'path', 'diff': False},
            {'out': 'path_size', 'diff': False},
            {'out': 'space_total', 'diff': False},
            {'out': 'space_left', 'diff': False},
            {'out': 'read', 'in': 'sectors_read'},
            {'out': 'write', 'in': 'sectors_written'},
            {'out': 'path_fill_rate', 'in': 'path_size'},
            {'out': 'time_until_full', 'in': 'space_left', 'fn': self.calculate_time_until_full},
            {'out': 'await'},
        ]

        self.output_transform_data = [
            {'out': 'type', 'pos': 0, 'noautohide': True},
            {'out': 'dev', 'pos': 1, 'noautohide': True},
            {
                'out': 'fill',
                'in': 'path_fill_rate',
                'units': 'MB/s',
                'fn': self.kb_to_mbytes,
                'round': StatCollector.RD,
                'pos': 2,
                'minw': 6,
            },
            {
                'out': 'until_full',
                'in': 'time_until_full',
                'pos': 3,
                'noautohide': True,
                'status_fn': self.time_field_status,
                'fn': StatCollector.time_pretty_print,
                'warning': 10800,
                'critical': 3600,
                'hide_if_ok': True,
                'minw': 13,
            },
            {
                'out': 'total',
                'in': 'space_total',
                'fn': self.kb_pretty_print,
                'pos': 4,
                'minw': 5,
                'align': COLALIGN.ca_right,
            },
            {
                'out': 'left',
                'in': 'space_left',
                'fn': self.kb_pretty_print,
                'pos': 5,
                'noautohide': False,
                'minw': 5,
                'align': COLALIGN.ca_right,
            },
            {
                'out': 'read',
                'units': 'MB/s',
                'fn': self.sectors_to_mbytes,
                'round': StatCollector.RD,
                'pos': 6,
                'noautohide': True,
                'minw': 6,
            },
            {
                'out': 'write',
                'units': 'MB/s',
                'fn': self.sectors_to_mbytes,
                'round': StatCollector.RD,
                'pos': 7,
                'noautohide': True,
                'minw': 6,
            },
            {
                'out': 'await',
                'units': 'ms',
                'round': StatCollector.RD,
                'pos': 8,
                'minw': 8,
            },
            {
                'out': 'path_size',
                'fn': self.kb_pretty_print,
                'pos': 9,
                'noautohide': True,
                'align': COLALIGN.ca_right,
            },
            {'out': 'path', 'pos': 10},
        ]
        self.ncurses_custom_fields = {'header': True}
        self.ncurses_custom_fields['prefix'] = None
        self.postinit()

    def ident(self):
        return '{0} ({1}/{2})'.format(super(PartitionStatCollector, self).ident(), self.dbname, self.dbver)

    def _dereference_dev_name(self, devname):
        return (devname.replace('/dev/', '') if devname else None)

    def refresh(self):
        result = {}
        du_out = {'data': [], 'xlog': []}
        df_out = {'data': [], 'xlog': []}

        queue_data = self.queue_consumer.fetch(self.work_directory)
        # run df and du in parallel to reduce the I/O waiting time
        if queue_data:
            (du_out, df_out) = queue_data

        for pname in PartitionStatCollector.DATA_NAME, PartitionStatCollector.XLOG_NAME:
            result[pname] = self._transform_input(df_out[pname], self.df_list_transformation)

        io_out = self.get_io_data([result[PartitionStatCollector.DATA_NAME]['dev'],
                                  result[PartitionStatCollector.XLOG_NAME]['dev']])

        for pname in PartitionStatCollector.DATA_NAME, PartitionStatCollector.XLOG_NAME:
            if result[pname]['dev'] in io_out:
                result[pname].update(self._transform_input(io_out[result[pname]['dev']], self.io_list_transformation))
            if pname in du_out:
                result[pname].update(self._transform_input(du_out[pname], self.du_list_transformation))
            # set the type manually
            result[pname]['type'] = pname

        self._do_refresh([result[PartitionStatCollector.DATA_NAME], result[PartitionStatCollector.XLOG_NAME]])

    def calculate_time_until_full(self, colname, prev, cur):
        # both should be expressed in common units, guaranteed by BLOCK_SIZE
        if cur.get('path_size', 0) > 0 and prev.get('path_size', 0) > 0 and cur.get('space_left', 0) > 0:
            if cur['path_size'] < prev['path_size']:
                return cur['space_left'] / (prev['path_size'] - cur['path_size'])
        return None

    def get_io_data(self, pnames):
        """ Retrieve raw data from /proc/diskstat (transformations are perfromed via io_list_transformation)"""

        result = {}
        found = 0  # stop if we found records for all partitions
        total = len(pnames)
        try:
            fp = None
            fp = open(PartitionStatCollector.DISK_STAT_FILE, 'rU')
            for l in fp:
                elements = l.split()
                for pname in pnames:
                    if pname in elements:
                        result[pname] = elements
                        found += 1
                        if found == total:
                            break
                if found == total:
                    break
        except:
            logger.error('Unable to read {0}'.format(PartitionStatCollector.DISK_STAT_FILE))
            result = {}
        finally:
            fp and fp.close()
        return result

    def output(self, method):
        return super(self.__class__, self).output(method, before_string='PostgreSQL partitions:', after_string='\n')


class MemoryStatCollector(StatCollector):

    """ Collect memory-related statistics """

    MEMORY_STAT_FILE = '/proc/meminfo'

    def __init__(self):
        super(MemoryStatCollector, self).__init__(produce_diffs=False)
        self.transform_dict_data = [
            {'in': 'MemTotal', 'out': 'total', 'fn': int},
            {'in': 'MemFree', 'out': 'free', 'fn': int},
            {
                'in': 'Buffers',
                'out': 'buffers',
                'fn': int,
                'optional': True,
            },
            {'in': 'Cached', 'out': 'cached', 'fn': int},
            {'in': 'Dirty', 'out': 'dirty', 'fn': int},
            {
                'in': 'CommitLimit',
                'out': 'commit_limit',
                'fn': int,
                'optional': True,
            },
            {
                'in': 'Committed_AS',
                'out': 'committed_as',
                'fn': int,
                'optional': True,
            },
            {
                'infn': self.calculate_kb_left_until_limit,
                'out': 'commit_left',
                'fn': int,
                'optional': True,
            },
        ]

        self.output_transform_data = [
            {
                'out': 'total',
                'units': 'MB',
                'fn': self.kb_pretty_print,
                'pos': 0,
                'minw': 6,
            },
            {
                'out': 'free',
                'units': 'MB',
                'fn': self.kb_pretty_print,
                'pos': 1,
                'noautohide': True,
                'minw': 6,
            },
            {
                'out': 'buffers',
                'units': 'MB',
                'fn': self.kb_pretty_print,
                'pos': 2,
                'minw': 6,
            },
            {
                'out': 'cached',
                'units': 'MB',
                'fn': self.kb_pretty_print,
                'pos': 3,
                'minw': 6,
            },
            {
                'out': 'dirty',
                'units': 'MB',
                'fn': self.kb_pretty_print,
                'pos': 4,
                'noautohide': True,
                'minw': 6,
            },
            {
                'out': 'limit',
                'in': 'commit_limit',
                'units': 'MB',
                'fn': self.kb_pretty_print,
                'pos': 5,
                'noautohide': True,
                'minw': 6,
            },
            {
                'out': 'as',
                'in': 'committed_as',
                'units': 'MB',
                'fn': self.kb_pretty_print,
                'pos': 6,
                'minw': 6,
            },
            {
                'out': 'left',
                'in': 'commit_left',
                'units': 'MB',
                'fn': self.kb_pretty_print,
                'pos': 7,
                'noautohide': True,
                'minw': 6,
            },
        ]

        self.ncurses_custom_fields = {'header': False, 'prefix': 'mem: ', 'prepend_column_headers': True}

        self.postinit()

    def refresh(self):
        """ Read statistics from /proc/meminfo """

        memdata = self._read_memory_data()
        raw_result = self._transform_input(memdata)
        self._do_refresh([raw_result])

    def _read_memory_data(self):
        """ Read relevant data from /proc/meminfo. We are interesed in the following fields:
            MemTotal, MemFree, Buffers, Cached, Dirty, CommitLimit, Committed_AS
        """

        result = {}
        try:
            fp = open(MemoryStatCollector.MEMORY_STAT_FILE, 'rU')
            for l in fp:
                vals = l.strip().split()
                if len(vals) >= 2:
                    name, val = vals[:2]
                    # if we have units of measurement different from kB - transform the result
                    if len(vals) == 3 and vals[2] in ('mB', 'gB'):
                        if vals[2] == 'mB':
                            val = val + '0' * 3
                        if vals[2] == 'gB':
                            val = val + '0' * 6
                    if len(str(name)) > 1:
                        result[str(name)[:-1]] = val
                    else:
                        logger.error('name is too short: {0}'.format(str(name)))
                else:
                    logger.error('/proc/meminfo string is not name value: {0}'.format(vals))
        except:
            logger.error('Unable to read /proc/meminfo memory statistics. Check your permissions')
            return result
        finally:
            fp.close()
        return result

    def calculate_kb_left_until_limit(self, colname, row, optional):
        result = (int(row['CommitLimit']) - int(row['Committed_AS']) if row.get('CommitLimit', None) is not None and
                  row.get('Committed_AS', None) is not None else None)
        if result is None and not optional:
            self.warn_non_optional_column(colname)
        return result

    def output(self, method):
        return super(self.__class__, self).output(method, before_string='Memory statistics:', after_string='\n')


class HostStatCollector(StatCollector):

    """ General system-wide statistics """

    UPTIME_FILE = '/proc/uptime'

    def __init__(self):
        super(HostStatCollector, self).__init__(produce_diffs=False)

        self.transform_list_data = [{'out': 'loadavg', 'infn': self._concat_load_avg}]
        self.transform_uptime_data = [{'out': 'uptime', 'in': 0, 'fn': self._uptime_to_str}]
        self.transform_uname_data = [{'out': 'sysname', 'infn': self._construct_sysname}]

        self.output_transform_data = [
            {
                'out': 'load average',
                'in': 'loadavg',
                'pos': 4,
                'noautohide': True,
                'warning': 5,
                'critical': 20,
                'column_header': COLHEADER.ch_prepend,
                'status_fn': self._load_avg_state,
            },
            {
                'out': 'up',
                'in': 'uptime',
                'pos': 1,
                'noautohide': True,
                'column_header': COLHEADER.ch_prepend,
            },
            {
                'out': 'host',
                'in': 'hostname',
                'pos': 0,
                'noautohide': True,
                'highlight': True,
            },
            {
                'out': 'cores',
                'pos': 2,
                'noautohide': True,
                'column_header': COLHEADER.ch_append,
            },
            {
                'out': 'name',
                'in': 'sysname',
                'pos': 3,
                'noautohide': True,
            },
        ]

        self.ncurses_custom_fields = {'header': False, 'prefix': None, 'prepend_column_headers': False}

        self.postinit()

    def refresh(self):
        raw_result = {}
        raw_result.update(self._read_uptime())
        raw_result.update(self._read_load_average())
        raw_result.update(self._read_hostname())
        raw_result.update(self._read_uname())
        raw_result.update(self._read_cpus())
        self._do_refresh([raw_result])

    def _read_load_average(self):
        return self._transform_list(os.getloadavg())

    def _load_avg_state(self, row, col):
        state = {}
        load_avg_str = row[self.output_column_positions[col['out']]]
        if not load_avg_str:
            return {}
        # load average consists of 3 values.
        load_avg_vals = load_avg_str.split()
        for no, val in enumerate(load_avg_vals):
            if float(val) >= col['critical']:
                state[no] = COLSTATUS.cs_critical
            elif float(val) >= col['warning']:
                state[no] = COLSTATUS.cs_warning
            else:
                state[no] = COLSTATUS.cs_ok
        return state

    def _concat_load_avg(self, colname, row, optional):
        """ concat all load averages into a single string """

        if len(row) >= 3:
            return ' '.join(str(x) for x in row[:3])
        else:
            return ''

    def _load_avg_status(self, row, col, val, bound):
        if val is not None:
            loads = str(val).split()
            if len(loads) != 3:
                logger.error('load average value is not 1min 5min 15 min')
            for x in loads:
                f = float(x)
                if f > bound:
                    return True
        return False

    @staticmethod
    def _read_cpus():
        cpus = 0
        try:
            cpus = cpu_count()
        except:
            logger.error('multiprocessing does not support cpu_count')
            pass
        return {'cores': cpus}

    def _construct_sysname(self, attname, row, optional):
        if len(row) < 3:
            return None
        return '{0} {1}'.format(row[0], row[2])

    def _read_uptime(self):
        fp = None
        raw_result = []
        try:
            fp = open(HostStatCollector.UPTIME_FILE, 'rU')
            raw_result = fp.read().split()
        except:
            logger.error('Unable to read uptime from {0}'.format(HostStatCollector.UPTIME_FILE))
        finally:
            fp and fp.close()
        return self._transform_input(raw_result, self.transform_uptime_data)

    @staticmethod
    def _uptime_to_str(uptime):
        return str(timedelta(seconds=int(float(uptime))))

    @staticmethod
    def _read_hostname():
        return {'hostname': socket.gethostname()}

    def _read_uname(self):
        uname_row = os.uname()
        return self._transform_input(uname_row, self.transform_uname_data)

    def output(self, method):
        return super(self.__class__, self).output(method, before_string='Host statistics', after_string='\n')


# the following 2 classes share the common 'display' method

class CommonOutput(object):

    """ just a normal console output """

    def __init__(self):
        super(CommonOutput, self)

    def display(self, data):
        print(data)

    def refresh(self):
        os.system('clear')


class CursesOutput(object):

    """ Show ncurses output """

    CLOCK_FORMAT = '%H:%M:%S'

    MIN_ELLIPSIS_FIELD_LENGTH = 10
    MIN_TRUNCATE_FIELD_LENGTH = 50  # do not try to truncate fields lower than this size
    MIN_TRUNCATED_LEAVE = 10  # do not leave the truncated field if it's less than this size

    def __init__(self, screen):
        super(CursesOutput, self)
        self.screen = screen
        self.data = {}
        self.output_order = []
        self.show_help = False
        self.is_color_supported = True

        self._init_display()

    def _init_display(self):
        """ Various ncurses initialization calls """

        if hasattr(curses, 'curs_set'):
            try:
                curses.curs_set(0)  # make the cursor invisible
            except:
                pass
        self.screen.nodelay(1)  # disable delay when waiting for keyboard input

        # initialize colors
        if hasattr(curses, 'use_default_colors'):
            curses.use_default_colors()
            curses.init_pair(1, -1, -1)
            curses.init_pair(2, curses.COLOR_WHITE, curses.COLOR_BLUE)
            curses.init_pair(3, curses.COLOR_WHITE, curses.COLOR_RED)
            curses.init_pair(4, curses.COLOR_WHITE, -1)
            curses.init_pair(5, curses.COLOR_GREEN, -1)
            curses.init_pair(6, curses.COLOR_BLACK, curses.COLOR_WHITE)

            self.COLOR_NORMAL = curses.color_pair(1)
            self.COLOR_WARNING = curses.color_pair(2)
            self.COLOR_CRITICAL = curses.color_pair(3)
            self.COLOR_HIGHLIGHT = curses.color_pair(4)
            self.COLOR_INVERSE_HIGHLIGHT = curses.color_pair(5)
            self.COLOR_MENU = curses.color_pair(2)
            self.COLOR_MENU_SELECTED = curses.color_pair(6)
        else:
            self.is_color_supported = False

    def display(self, data):
        """ just collect the data """

        collector_name = list(data.keys())[0]
        self.data[collector_name] = list(data.values())[0]
        self.output_order.append(collector_name)

    def toggle_help(self):
        self.show_help = self.show_help is False

    def refresh(self):
        """ actual data output goes here """

        self.next_y = 0

        # ncurses doesn't erase the old contents when the screen is refreshed,
        # hence, we need to do it manually here.
        # update screen coordinates
        self.screen.erase()
        self.update_screen_metrics()
        if not self.show_help:
            for collector in self.output_order:
                if self.next_y < self.screen_y - 2:
                    self.show_collector_data(collector)
                else:
                    break
        else:
            self.help()
        # show clock if possible
        self.show_clock()
        self.show_help_bar()
        self.screen.refresh()
        self.output_order = []

    def screen_erase(self):
        self.screen.erase()
        self.screen.refresh()
        pass

    def update_screen_metrics(self):
        self.screen_y, self.screen_x = self.screen.getmaxyx()

    def print_text(self, starty, startx, text, attr=None, trim_middle=False):
        """ output string, truncate it if it doesn't fit, return the new X position"""

        if attr is None:
            attr = self.COLOR_NORMAL
        # bail out if we have hit the screen vertical limit
        if starty > self.screen_y - 1:
            return
        remaining_len = min(self.screen_x - (startx + 1), len(text))
        if remaining_len > 0:
            self.screen.addnstr(starty, startx, text, remaining_len, attr)
            return startx + remaining_len
        else:
            return startx

    def show_help_bar_item(self, key, description, selected, x):
        x = self.print_text(self.screen_y - 1, x, '{0}:'.format(key),
                            ((self.COLOR_MENU_SELECTED if selected else self.COLOR_MENU)) | curses.A_BOLD)
        x = self.print_text(self.screen_y - 1, x, '{0} '.format(description),
                            (self.COLOR_MENU_SELECTED if selected else self.COLOR_MENU))
        return x

    def show_help_bar(self):
        global display_units
        global freeze
        global filter_aux
        global autohide_fields
        global notrim
        global realtime
        # only show help if we have enough screen real estate
        if self.next_y > self.screen_y - 1:
            pass

        menu_items = (
            ('s', 'system', not filter_aux),
            ('f', 'freeze', freeze),
            ('u', 'units', display_units),
            ('a', 'autohide', autohide_fields),
            ('t', 'trimming', notrim),
            ('r', 'realtime', realtime),
            ('h', 'help', self.show_help),
        )

        next_x = 0
        for item in menu_items:
            next_x = self.show_help_bar_item(x=next_x, *item)

        self.print_text(self.screen_y - 1, next_x, 'v{0}'.format(__version__).rjust(self.screen_x - next_x - 1),
                        self.COLOR_MENU | curses.A_BOLD)

    def show_clock(self):
        clock_str_len = len(self.CLOCK_FORMAT)
        clean = True
        for pos in range(0, clock_str_len):
            x = self.screen.inch(0, self.screen_x - clock_str_len - 1 + pos) & 255
            if x != ord(' '):
                clean = False
                break
        if clean:
            clock_str = time.strftime(self.CLOCK_FORMAT, time.localtime())
            self.screen.addnstr(0, self.screen_x - clock_str_len, clock_str, clock_str_len)

    def _status_to_color(self, status, highlight):
        if status == COLSTATUS.cs_critical:
            return self.COLOR_CRITICAL
        if status == COLSTATUS.cs_warning:
            return self.COLOR_WARNING
        if highlight:
            return self.COLOR_HIGHLIGHT | curses.A_BOLD
        return self.COLOR_NORMAL

    def color_text(self, status_map, highlight, text, header, header_position):
        """ for a given header and text - decide on the position and output color """
        result = []
        xcol = 0
        # header_position is either put the header before the value, or after
        # if header_position is empty, no header is present
        if header_position == COLHEADER.ch_prepend:
            xcol = self.color_header(header, xcol, result)
            # the text might be empty, if it was truncated by truncate_column_value
            if text:
                self.color_value(text, xcol, status_map, highlight, result)
        elif header_position == COLHEADER.ch_append:
            xcol = self.color_value(text, xcol, status_map, highlight, result)
            # ditto for the header
            if header:
                self.color_header(header, xcol, result)
        else:
            self.color_value(text, 0, status_map, highlight, result)
        return result

    def color_header(self, header, xcol, result):
        """ add a header outout information"""
        result.append({
            'start': xcol,
            'width': len(header),
            'word': header,
            'color': self.COLOR_NORMAL,
        })
        return xcol + len(header) + 1

    def color_value(self, val, xcol, status_map, highlight, result):
        """ add a text optut information """
        # status format: field_no -> color
        # if the status field contain a single value of -1 - just
        # highlight everything without splitting the text into words
        # get all words from the text and their relative positions
        if len(status_map) == 1 and -1 in status_map:
            color = self._status_to_color(status_map[-1], highlight)
            result.append({
                'start': xcol,
                'word': val,
                'width': len(val),
                'color': color,
            })
            xcol += (len(val) + 1)
        else:
            # XXX: we are calculating the world boundaries again here
            # (first one in calculate_output_status) and using a different method to do so.
            words = list(re.finditer(r'(\S+)', val))
            last_position = xcol
            for no, word in enumerate(words):
                if no in status_map:
                    status = status_map[no]
                    color = self._status_to_color(status, highlight)
                elif -1 in status_map:
                    # -1 is catchall for all fields (i.e for queries)
                    status = status_map[-1]
                    color = self._status_to_color(status, highlight)
                else:
                    color = self.COLOR_NORMAL
                word_len = word.end(0) - word.start(0)
                # convert the relative start to the absolute one
                result.append({
                    'start': xcol + word.start(0),
                    'word': word.group(0),
                    'width': word_len,
                    'color': color,
                })
                last_position = xcol + word.end(0)
            xcol += (last_position + 1)
        return xcol

    def help(self):
        y = 0
        self.print_text(y, 0, '{0} {1} - a monitor for PostgreSQL related system statistics'.format(__appname__,
                        __version__), self.COLOR_NORMAL | curses.A_BOLD)
        y += 1
        self.print_text(y, 0, 'Distributed under the terms of {0} license'.format(__license__))
        y += 2
        self.print_text(y, 0, 'The following hotkeys are supported:')
        y += 1
        x = self.print_text(y, 5, 's: ', self.COLOR_NORMAL | curses.A_BOLD)
        self.print_text(y, x, 'toggle system processes display')
        y += 1
        x = self.print_text(y, 5, 'f: ', self.COLOR_NORMAL | curses.A_BOLD)
        self.print_text(y, x, 'freeze/unfreeze output')
        y += 1
        x = self.print_text(y, 5, 'u: ', self.COLOR_NORMAL | curses.A_BOLD)
        self.print_text(y, x, 'toggle measurement units display (MB, s)')
        y += 1
        x = self.print_text(y, 5, 'a: ', self.COLOR_NORMAL | curses.A_BOLD)
        self.print_text(y, x, 'toggle auto-hiding of non-essential attributes')
        y += 1
        x = self.print_text(y, 5, 't: ', self.COLOR_NORMAL | curses.A_BOLD)
        self.print_text(y, x, 'toggle trimming of attributes in the middle (user and database names)')
        y += 1
        x = self.print_text(y, 5, 'r: ', self.COLOR_NORMAL | curses.A_BOLD)
        self.print_text(y, x, 'update information in real time (may cause additional load)')
        y += 1
        x = self.print_text(y, 5, 'q: ', self.COLOR_NORMAL | curses.A_BOLD)
        self.print_text(y, x, 'exit program')
        y += 2
        self.print_text(y, 0, "Press 'h' to exit this screen")

    def show_collector_data(self, collector, clock=False):
        if collector not in self.data or len(self.data[collector]) <= 0 or\
           len(self.data[collector].get('rows', ())) <= 0 and not self.data[collector]['prefix']:
            return

        rows = self.data[collector]['rows']
        statuses = self.data[collector]['statuses']
        align = self.data[collector]['align']
        header = self.data[collector].get('header', False) or False
        prepend_column_headers = self.data[collector].get('prepend_column_headers', False)
        highlights = self.data[collector]['highlights']
        types = self.data[collector]['types']

        start_x = 1

        prefix_mod = self.display_prefix(collector, header)
        if prefix_mod < 0:
            self.next_y += 1
        else:
            start_x += prefix_mod

        # if the block doesn't fit to screen - just return
        if self.next_y + header + 1 > self.screen_y - 1 or len(rows) == 0:
            return

        # calculate X layout
        layout = self.calculate_fields_position(collector, start_x)

        if header:
            self.display_header(layout, align, types)
            self.next_y += 1

        for i, (row, status) in enumerate(zip(rows, statuses)):
            # if no more rows fit the screen - show '...' instead of the last row that fits
            if self.next_y > self.screen_y - 3 and i != len(rows) - 1:
                for field in layout:
                    self.print_text(self.screen_y - 2, layout[field]['start'], '.' * layout[field]['width'])
                    self.next_y += 1
                break
            self.show_status_of_invisible_fields(layout, status, 0)
            for field in layout:
                # calculate colors and alignment for the data value
                column_alignment = (align.get(field,
                                    COLALIGN.ca_none) if not prepend_column_headers else COLALIGN.ca_left)
                w = layout[field]['width']
                # now check if we need to add ellipsis to indicate that the value has been truncated.
                # we don't do this if the value is less than a certain length or when the column is marked as
                # containing truncated values, but the actual value is not truncated.

                if layout[field].get('truncate', False):
                    # XXX: why do we truncate even when truncate for the column is set to False?
                    header, text = self.truncate_column_value(row[field], w, (w > self.MIN_ELLIPSIS_FIELD_LENGTH))
                else:
                    header, text = row[field].header, row[field].value
                text = self._align_field(text, header, w, column_alignment, types.get(field, COLTYPES.ct_string))
                color_fields = self.color_text(status[field], highlights[field],
                                               text, header, row[field].header_position)
                for f in color_fields:
                    self.screen.addnstr(self.next_y, layout[field]['start'] + f['start'], f['word'], f['width'],
                                        f['color'])
            self.next_y += 1

    def truncate_column_value(self, cv, maxlen, ellipsis=True):
        """ make sure that a pair of header and value fits into the allocated field length """
        value = cv.value
        header = cv.header
        header_position = cv.header_position
        h_len = len(header)
        v_len = len(value)
        maxlen = (maxlen - 3) if ellipsis else maxlen
        if header_position:
            if header_position == COLHEADER.ch_prepend:
                if h_len + 1 >= maxlen:
                    # prepend the header, consider if we have to truncate the header and omit the value altogether
                    header = header[:maxlen] + (' ' if maxlen == h_len + 1 else '') + ('...' if ellipsis else '')
                    value = ''
                else:
                    value = value[:(maxlen - h_len - 1)] + ('...' if ellipsis else '')
            elif header_position == COLHEADER.ch_append:
                if v_len + 1 >= maxlen:
                    # prepend the value, consider if we have to truncate it and omit the header altogether
                    value = value[:maxlen] + (' ' if maxlen == v_len + 1 else '') + ('...' if ellipsis else '')
                    header = ''
                else:
                    header = header[:(maxlen - v_len - 1)] + ('...' if ellipsis else '')
        else:
            # header is set to '' by the collector
            value = value[:maxlen] + ('...' if ellipsis else '')
        return header, value

    def display_prefix(self, collector, header):
        prefix = self.data[collector]['prefix']
        if prefix:
            prefix_len = len(prefix)
            prefix_newline = prefix[-1] == '\n'
            # truncate the prefix if it doesn't fit the screen
            if prefix_len >= self.screen_x and prefix_newline:
                prefix = prefix[:max(self.screen_x - 1, 0)]
            elif prefix_len >= self.screen_x / 5 and not prefix_newline:
                return 0

            color = (self.COLOR_INVERSE_HIGHLIGHT if prefix_newline else self.COLOR_NORMAL)

            self.screen.addnstr(self.next_y, 1, str(prefix), len(str(prefix)), color)
            if prefix_newline:
                return -1
            else:
                return prefix_len
        else:
            return 0

    def display_header(self, layout, align, types):
        for field in layout:
            text = self._align_field(field, '', layout[field]['width'], align.get(field, COLALIGN.ca_none),
                                     types.get(field, COLTYPES.ct_string))
            self.screen.addnstr(self.next_y, layout[field]['start'], text, layout[field]['width'], self.COLOR_NORMAL |
                                curses.A_BOLD)

    def calculate_fields_position(self, collector, xstart):
        width = self.data[collector]['w']
        fields = self._get_fields_sorted_by_position(collector)
        to_hide = self.data[collector]['hide']
        noautohide = self.data[collector]['noautohide']
        candrop = [name for name in fields if name not in to_hide and not noautohide.get(name, False)]
        return self.layout_x(xstart, width, fields, to_hide, candrop)

    def show_status_of_invisible_fields(self, layout, status, xstart):
        """
            Show red/blue bar to the left of the screen representing the most critical
            status of the fields that are now shown.
        """

        status_rest = self._invisible_fields_status(layout, status)
        if status_rest != COLSTATUS.cs_ok:
            color_rest = self._status_to_color(status_rest, False)
            self.screen.addch(self.next_y, 0, ' ', color_rest)

    @staticmethod
    def _align_field(text, header, width, align, typ):
        if align == COLALIGN.ca_none:
            if typ == COLTYPES.ct_number:
                align = COLALIGN.ca_right
            else:
                align = COLALIGN.ca_left
        textlen = len(text) + len(header) + (1 if header and text else 0)
        width_left = width - textlen
        if align == COLALIGN.ca_right:
            return '{0}{1}'.format(' ' * width_left, text)
        if align == COLALIGN.ca_center:
            left_space = width_left / 2
            right_space = width_left - left_space
            return '{0}{1}{2}'.format(' ' * left_space, text, ' ' * right_space)
        return str(text)

    def _get_fields_sorted_by_position(self, collector):
        pos = self.data[collector]['pos']
        sorted_by_pos = sorted(((x, pos[x]) for x in pos if pos[x] != -1), key=itemgetter(1))
        return [f[0] for f in sorted_by_pos]

    def _invisible_fields_status(self, layout, statuses):
        highest_status = COLSTATUS.cs_ok
        invisible = [col for col in statuses if col not in layout]
        for col in invisible:
            for no in statuses[col]:
                if statuses[col][no] > highest_status:
                    highest_status = statuses[col][no]
                    if highest_status == COLSTATUS.cs_critical:
                        return COLSTATUS.cs_critical
        return highest_status

    def layout_x(self, xstart, colwidth, colnames, colhidden, colcandrop):
        """ Figure out width and X start position for each column. Some of the columns
            can be hidden, if they are not important (determined at column defintion) and
            if we don't have enough space for them.
        """

        layout = {}
        # get only the columns that are not hidden
        col_remaining = [name for name in colnames if name not in colhidden]
        # calculate the available screen X dimensions and the width required by all columns
        width_available = self.screen_x - (xstart + 1)
        # we add width of all N fields + N-1 spaces between fields
        width_required = sum(colwidth[name] for name in col_remaining) + len(col_remaining) - 1
        if width_available < width_required and colcandrop and len(colcandrop) > 0:
            for name in colcandrop:
                if name in col_remaining:
                    # remove a column, re-calculate width
                    col_remaining.remove(name)
                    width_required -= colwidth[name] + 1
                    # drop non-essential columns
                    if width_required <= width_available:
                        break
        # we dropped what we can, now show the rest. Track the accumulated width to
        # figure out which columns won't fit.
        x = xstart
        total_remaining = len(col_remaining)
        for idx, name in enumerate(col_remaining):
            w = colwidth[name]
            layout[name] = {'start': x, 'width': w}
            x += w
            if idx != total_remaining - 1:
                x += 1
            # the last possible X position is screen_x - 1, the position of the last character
            # of the current word is layout[name]['start'] + w - 1. The comparison below checks
            # that the field width doesn't exceed the screen boundaries.
            if layout[name]['start'] + w > self.screen_x:
                # if we can't fit even one character - just bail out and don't show the field
                if layout[name]['start'] > self.screen_x - 1:
                    del layout[name]
                else:
                    # truncate it to the length that fits the screen
                    layout[name]['truncate'] = True
                    layout[name]['width'] = self.screen_x - layout[name]['start']
                # oops, we ran across the screen boundary
                # all the columns after this one should be dropped
                break
        return layout


# some utility functions

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


def read_postmaster_pid(work_directory, dbname):
    """ Parses the postgres directory tree and extracts the pid of the postmaster process """

    fp = None
    try:
        fp = open('{0}/postmaster.pid'.format(work_directory))
        pid = fp.readline().strip()
    except:
        # XXX: do not bail out in case we are collecting data for multiple PostgreSQL clusters
        logger.error('Unable to read postmaster.pid for {name} at {wd}\n HINT: \
            make sure Postgres is running'.format(name=dbname,
                     wd=work_directory))
        return None
    finally:
        if fp is not None:
            fp.close()
    return pid


# execution starts here

def loop(collectors, consumer, groups, output_method):
    if output_method == OUTPUT_METHOD.curses:
        curses.wrapper(do_loop, groups, output_method, collectors, consumer)
    else:
        do_loop(None, groups, output_method, collectors, consumer)


def poll_keys(screen, output):
    global display_units
    global freeze
    global filter_aux
    global autohide_fields
    global notrim
    global realtime

    c = screen.getch()
    if c == ord('u'):
        display_units = display_units is False
    if c == ord('f'):
        freeze = freeze is False
    if c == ord('s'):
        filter_aux = filter_aux is False
    if c == ord('h'):
        output.toggle_help()
    if c == ord('a'):
        autohide_fields = autohide_fields is False
    if c == ord('t'):
        notrim = notrim is False
    if c == ord('r'):
        realtime = realtime is False
    if c == ord('q'):
        # bail out immediately
        return False
    return True


def do_loop(screen, groups, output_method, collectors, consumer):
    """ Display output (or pass it through to ncurses) """

    output = None
    global display_units
    global freeze
    global filter_aux
    global autohide_fields
    global notrim
    global realtime

    if output_method == OUTPUT_METHOD.curses:
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
    while 1:
        # process input:
        consumer.consume()
        for st in collectors:
            if output_method == OUTPUT_METHOD.curses:
                if not poll_keys(screen, output):
                    # bail out immediately
                    return
            st.set_units_display(display_units)
            st.set_ignore_autohide(not autohide_fields)
            st.set_notrim(notrim)
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
        if not realtime:
            time.sleep(TICK_LENGTH)


def process_single_collector(st):
    """ perform all heavy-lifting for a single collector, i.e. data collection,
        diff calculation, etc. This is meant to be run in a separate thread.
    """

    if isinstance(st, PgstatCollector):
        st.set_aux_processes_filter(filter_aux)
    st.tick()
    if not freeze:
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


def is_postgres_process(pid):
    # read /proc/stat, check for the PostgreSQL string
    stat_file_name = '/proc/{0}/stat'.format(pid)
    with open(stat_file_name, 'rU') as fp:
        stat_fields = fp.read().strip().split()
        if len(stat_fields) > 3 and stat_fields[1] == '(postgres)':
            result = True
        else:
            result = False
    return result


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


def detect_db_port(socket_dir):
    port = -1
    try:
        x = os.listdir(socket_dir)
    except:
        pass
    else:
        if x:
            # scan files to the presense of a port number
            for f in x:
                m = re.match(r'\.s\.PGSQL\.(\d+)$', f)
                if m:
                    port = int(m.group(1))
                    break
    return port


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


def detect_postgres_version(work_directory):
    """ read the current major version number from pgversion """

    VERSION_FILE = '{0}/PG_VERSION'.format(work_directory)
    fp = None
    version = None
    # get database version first, it will determine how much we can extract from
    # the postmaster.pid
    try:
        fp = open(VERSION_FILE, 'rU')
        val = fp.read().strip()
        if val is not None and len(val) == 3:
            version = float(val)
    except Exception as e:
        # if we failed to read a file - assume version 9.0
        logger.warning('could not read from the PG_VERSION: {0}'.format(e))
        version = None
    finally:
        fp and fp.close()
    return version


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


def pick_connection_arguments(conn_args):
    """ go through all decected connections, picking the first one that actually works """
    result = {}
    for conn_type in ('unix', 'tcp', 'tcp6'):
        if len(result) > 0:
            break
        for arg in conn_args.get(conn_type, []):
            if can_connect_with_connection_arguments(*arg):
                (result['host'], result['port']) = arg
                break
    return result


def can_connect_with_connection_arguments(host, port):
    """ check that we can connect given the specified arguments """
    conn = build_connection(host, port, options.username, options.dbname)
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


def detect_db_connection_arguments(work_directory, pid, version):
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
    result = pick_connection_arguments(conn_args)
    if len(result) == 0:
        logger.error('unable to connect to PostgreSQL cluster ' +
                     'at {0} using any of the detected connection options: {1}'.format(work_directory, conn_args))
        return None
    return result


def dbversion_as_float(pgcon):
    version_num = pgcon.server_version
    version_num /= 100
    return float('{0}.{1}'.format(version_num / 100, version_num % 100))


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
        logger.error('duplicate connection options detected ' +
                     'for databases {0} and {1}, same pid {2}, skipping {0}'.format(instance, duplicate_instance, pid))
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


class ProcNetParser():
    """ Parse /proc/net/{tcp,tcp6,unix} and return the list of address:port
        pairs given the set of socket descriptors belonging to the object.
        The result is grouped by the socket type in a dictionary.
    """
    NET_UNIX_FILENAME = '/proc/net/unix'
    NET_TCP_FILENAME = '/proc/net/tcp'
    NET_TCP6_FILENAME = '/proc/net/tcp6'

    def __init__(self):
        self.reinit()

    def reinit(self):
        self.sockets = {}
        self.unix_socket_header_len = 0
        # initialize the sockets hash with the contents of unix
        # and tcp sockets. tcp IPv6 is also read if it's present
        for fname in (ProcNetParser.NET_UNIX_FILENAME, ProcNetParser.NET_TCP_FILENAME):
            self.read_socket_file(fname)
        if os.access(ProcNetParser.NET_TCP6_FILENAME, os.R_OK):
            self.read_socket_file(ProcNetParser.NET_TCP6_FILENAME)

    @staticmethod
    def _hex_to_int_str(val):
        return str(int(val, 16))

    @staticmethod
    def _hex_to_ip(val):
        newval = format(socket.ntohl(int(val, 16)), '08X')
        return '.'.join([str(int(newval[i: i + 2], 16)) for i in range(0, 8, 2)])

    @staticmethod
    def _hex_to_ipv6(val):
        newval_list = [format(socket.ntohl(int(val[x: x + 8], 16)), '08X') for x in range(0, 32, 8)]
        return ':'.join([':'.join((x[:4], x[4:])) for x in newval_list])

    def match_socket_inodes(self, inodes):
        """ return the dictionary with socket types as strings,
            containing addresses (or unix path names) and port
        """
        result = {}
        for inode in inodes:
            if inode in self.sockets:
                addr_tuple = self.parse_single_line(inode)
                if addr_tuple is None:
                    continue
                socket_type = addr_tuple[0]
                if socket_type in result:
                    result[socket_type].append(addr_tuple[1:])
                else:
                    result[socket_type] = [addr_tuple[1:]]
        return result

    def read_socket_file(self, filename):
        """ read file content, produce a dict of socket inode -> line """
        socket_type = filename.split('/')[-1]
        try:
            with open(filename) as fp:
                data = fp.readlines()
        except os.error as e:
            logger.error('unable to read from {0}: OS reported {1}'.format(filename, e))
        # remove the header
        header = (data.pop(0)).split()
        if socket_type == 'unix':
            self.unix_socket_header_len = len(header)
        indexes = [i for i, name in enumerate(header) if name.lower() == 'inode']
        if len(indexes) != 1:
            logger.error('attribute \'inode\' in the header of {0} is not unique or missing: {1}'.format(
                         filename, header))
        else:
            inode_idx = indexes[0]
            if socket_type != 'unix':
                # for a tcp socket, 2 pairs of fields (tx_queue:rx_queue and tr:tm->when
                # are separated by colons and not spaces)
                inode_idx -= 2
            for line in data:
                fields = line.split()
                inode = int(fields[inode_idx])
                self.sockets[inode] = [socket_type, line]

    def parse_single_line(self, inode):
        """ apply socket-specific parsing rules """
        result = None
        (socket_type, line) = self.sockets[inode]
        if socket_type == 'unix':
            # we are interested in everything in the last field
            # note that it may contain spaces or other separator characters
            fields = line.split(None, self.unix_socket_header_len - 1)
            socket_path = fields[-1]
            # check that it looks like a PostgreSQL socket
            match = re.search(r'(.*?)/\.s\.PGSQL\.(\d+)$', socket_path)
            if match:
                # path - port
                result = (socket_type,) + match.groups(1)
            else:
                logger.warning('unix socket name is not recognized as belonging to PostgreSQL: {0}'.format(socket_path))
        else:
            address_port = line.split()[1]
            (address_hex, port_hex) = address_port.split(':')
            port = self._hex_to_int_str(port_hex)
            if socket_type == 'tcp6':
                address = self._hex_to_ipv6(address_hex)
            elif socket_type == 'tcp':
                address = self._hex_to_ip(address_hex)
            else:
                logger.error('unrecognized socket type: {0}'.format(socket_type))
            result = (socket_type, address, port)
        return result


def main():
    global TICK_LENGTH, logger, options

    # bail out if we are not running Linux
    if platform.system() != 'Linux':
        print('Non Linux database hosts are not supported at the moment. Can not continue')
        sys.exit(243)

    if not psycopg2_available:
        print('Unable to import psycopg2 module, please, install it (python-psycopg2). Can not continue')
        sys.exit(254)

    options, args = parse_args()
    TICK_LENGTH = options.tick

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
    logger = logging.getLogger(__name__)
    logger.setLevel((logging.INFO if options.verbose else logging.ERROR))

    log_stderr = logging.StreamHandler()
    logger.addHandler(log_stderr)

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
                logger.error('failed to acquire details about ' +
                             'the database cluster {0}, the server will be skipped'.format(instance))
    elif options.host:
        port = options.port or "5432"
        # try to connet to the database specified by command-line options
        conn = build_connection(options.host, options.port, options.username, options.dbname)
        instance = options.instance or "default"
        if not establish_user_defined_connection(instance, conn, clusters):
            logger.error("unable to continue with cluster {0}".format(instance))
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
                conndata = detect_db_connection_arguments(result_work_dir, ppid, dbver)
                if conndata is None:
                    continue
                host = conndata['host']
                port = conndata['port']
                conn = build_connection(host, port, options.username, options.dbname)
                pgcon = psycopg2.connect(**conn)
            except Exception as e:
                logger.error('PostgreSQL exception {0}'.format(e))
                pgcon = None
            if pgcon:
                desc = make_cluster_desc(name=dbname, version=dbver, workdir=result_work_dir,
                                         pid=ppid, pgcon=pgcon, conn=conn)
                clusters.append(desc)
    collectors = []
    groups = {}
    try:
        if len(clusters) == 0:
            logger.error('No suitable PostgreSQL instances detected, exiting...')
            logger.error('hint: use -v for details, ' +
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
        logger.removeHandler(log_stderr)
        loop(collectors, consumer, groups, output_method)
        logger.addHandler(log_stderr)
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


class DetachedDiskStatCollector(Process):
    """ This class runs in a separate process and runs du and df """
    def __init__(self, q, work_directories):
        super(DetachedDiskStatCollector, self).__init__()
        self.work_directories = work_directories
        self.q = q
        self.daemon = True
        self.df_cache = {}

    def run(self):
        while True:
            # wait until the previous data is consumed
            self.q.join()
            result = {}
            self.df_cache = {}
            for wd in self.work_directories:
                du_data = self.get_du_data(wd)
                df_data = self.get_df_data(wd)
                result[wd] = [du_data, df_data]
            self.q.put(result)
            time.sleep(TICK_LENGTH)

    def get_du_data(self, wd):
        data_size = 0
        xlog_size = 0

        result = {'data': [], 'xlog': []}
        try:
            data_size = self.run_du(wd, BLOCK_SIZE)
            xlog_size = self.run_du(wd + '/pg_xlog/', BLOCK_SIZE)
        except Exception as e:
            logger.error('Unable to read free space information for the pg_xlog and data directories for the directory\
             {0}: {1}'.format(wd, e))
        else:
            # XXX: why do we pass the block size there?
            result['data'] = str(data_size), wd
            result['xlog'] = str(xlog_size), wd + '/pg_xlog'
        return result

    @staticmethod
    def run_du(pathname, block_size=BLOCK_SIZE, exclude=['lost+found']):
        size = 0
        folders = [pathname]
        root_dev = os.lstat(pathname).st_dev
        while len(folders):
            c = folders.pop()
            for e in os.listdir(c):
                e = os.path.join(c, e)
                try:
                    st = os.lstat(e)
                except os.error:
                    # don't care about files removed while we are trying to read them.
                    continue
                # skip data on different partition
                if st.st_dev != root_dev:
                    continue
                mode = st.st_mode & 0xf000  # S_IFMT
                if mode == 0x4000:  # S_IFDIR
                    if e in exclude:
                        continue
                    folders.append(e)
                    size += st.st_size
                if mode == 0x8000:  # S_IFREG
                    size += st.st_size
        return long(size / block_size)

    def get_df_data(self, work_directory):
        """ Retrive raw data from df (transformations are performed via df_list_transformation) """

        result = {'data': [], 'xlog': []}
        # obtain the device names
        data_dev = self.get_mounted_device(self.get_mount_point(work_directory))
        xlog_dev = self.get_mounted_device(self.get_mount_point(work_directory + '/pg_xlog/'))
        if data_dev not in self.df_cache:
            data_vfs = os.statvfs(work_directory)
            self.df_cache[data_dev] = data_vfs
        else:
            data_vfs = self.df_cache[data_dev]

        if xlog_dev not in self.df_cache:
            xlog_vfs = os.statvfs(work_directory + '/pg_xlog/')
            self.df_cache[xlog_dev] = xlog_vfs
        else:
            xlog_vfs = self.df_cache[xlog_dev]

        result['data'] = (data_dev, data_vfs.f_blocks * (data_vfs.f_bsize / BLOCK_SIZE),
                          data_vfs.f_bavail * (data_vfs.f_bsize / BLOCK_SIZE))
        if data_dev != xlog_dev:
            result['xlog'] = (xlog_dev, xlog_vfs.f_blocks * (xlog_vfs.f_bsize / BLOCK_SIZE),
                              xlog_vfs.f_bavail * (xlog_vfs.f_bsize / BLOCK_SIZE))
        else:
            result['xlog'] = result['data']
        return result

    @staticmethod
    def get_mounted_device(pathname):
        """Get the device mounted at pathname"""

        # uses "/proc/mounts"
        raw_dev_name = None
        dev_name = None
        pathname = os.path.normcase(pathname)  # might be unnecessary here
        try:
            with open('/proc/mounts', 'r') as ifp:
                for line in ifp:
                    fields = line.rstrip('\n').split()
                    # note that line above assumes that
                    # no mount points contain whitespace
                    if fields[1] == pathname and (fields[0])[:5] == '/dev/':
                        raw_dev_name = dev_name = fields[0]
                        break
        except EnvironmentError:
            pass
        if raw_dev_name is not None and raw_dev_name[:11] == '/dev/mapper':
            # we have to read the /sys/block/*/*/name and match with the rest of the device
            for fname in glob.glob('/sys/block/*/*/name'):
                try:
                    with open(fname) as f:
                        block_dev_name = f.read().strip()
                except IOError:
                    # ignore those files we couldn't read (lack of permissions)
                    continue
                if raw_dev_name[12:] == block_dev_name:
                    # we found the proper device name, get the 3rd comonent of the path
                    # i.e. /sys/block/dm-0/dm/name
                    components = fname.split('/')
                    if len(components) >= 4:
                        dev_name = components[3]
                    break
        return dev_name

    @staticmethod
    def get_mount_point(pathname):
        """Get the mounlst point of the filesystem containing pathname"""

        pathname = os.path.normcase(os.path.realpath(pathname))
        parent_device = path_device = os.stat(pathname).st_dev
        while parent_device == path_device:
            mount_point = pathname
            pathname = os.path.dirname(pathname)
            if pathname == mount_point:
                break
            parent_device = os.stat(pathname).st_dev
        return mount_point


class DiskCollectorConsumer(object):
    """ consumes information from the disk collector and provides it for the local
        collector classes running in the same subprocess.
    """
    def __init__(self, q):
        self.result = {}
        self.cached_result = {}
        self.q = q

    def consume(self):
        # if we haven't consumed the previous value
        if len(self.result) != 0:
            return
        try:
            self.result = self.q.get_nowait()
            self.cached_result = self.result.copy()
        except Empty:
            # we are too fast, just do nothing.
            pass
        else:
            self.q.task_done()

    def fetch(self, wd):
        data = None
        if wd in self.result:
            data = self.result[wd]
            del self.result[wd]
        elif wd in self.cached_result:
            data = self.cached_result[wd]
        return data


if __name__ == '__main__':
    main()
