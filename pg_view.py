#!/usr/bin/env python
# -*- coding: utf-8 -*-

__appname__ = 'pg_view'
__version__ = '1.0.1'
__author__ = 'Oleksii Kliukin <oleksii.kliukin@zalando.de>'

import os
import os.path
import re
import stat
import sys
import logging
from optparse import OptionParser
import ConfigParser
from operator import itemgetter
from datetime import datetime, timedelta
from numbers import Number
from multiprocessing import cpu_count  # for then number of cpus
import platform
import socket
import subprocess
import time
import traceback
import json
try:
    import psycopg2 as pg
    import psycopg2.extras
except ImportError:
    print 'Unable to import psycopg2 module, please, install it (python-psycopg2). Can not continue'
    sys.exit(254)
try:
    import curses
    curses_available = True
except ImportError:
    print 'Unable to import ncurses, curses output will be unavailable'
    curses_available = False

# bail out if we are not running Linux
if platform.system() != 'Linux':
    print 'Non Linux database hosts are not supported at the moment. Can not continue'
    sys.exit(243)


# enum emulation

def enum(**enums):
    return type('Enum', (), enums)


COLSTATUS = enum(cs_ok=0, cs_warning=1, cs_critical=2)
COLALIGN = enum(ca_none=0, ca_left=1, ca_center=2, ca_right=3)
COLTYPES = enum(ct_string=0, ct_number=1)
COHEADER = enum(ch_default=0, ch_prepend=1, ch_append=2)
OUTPUT_METHOD = enum(console='console', json='json', curses='curses')

# some global variables for keyboard output
freeze = False
filter_aux = True
autohide_fields = False
display_units = False


# validation functions for OUTPUT_METHOD

def get_valid_output_methods():
    result = []
    for key in OUTPUT_METHOD.__dict__.keys():
        if re.match(r'^[a-z][a-z_]+$', key):
            value = OUTPUT_METHOD.__dict__[key]
            result.append(value)
    return result


def output_method_is_valid(method):
    return method in get_valid_output_methods()


# parse command-line options

parser = OptionParser()
parser.add_option('-v', '--verbose', help='verbose mode', action='store_true', dest='verbose')
parser.add_option('-i', '--instance', help='name of the instance to monitor', action='store', dest='instance')
parser.add_option('-t', '--tick', help='tick length (in seconds)', action='store', dest='tick', type='int', default=1)
parser.add_option('-o', '--output-method', help='send output to the following source', action='store',
                  default=OUTPUT_METHOD.curses, dest='output_method')
parser.add_option('-V', '--use-version', help='version of the instance to monitor (in case it can\'t be autodetected)',
                  action='store', dest='version', type='float')
parser.add_option('-l', '--log-file', help='direct log output to the file', action='store', default='pg_view.log',
                  dest='log_file')
parser.add_option('-R', '--reset-output', help='clear screen after each tick', action='store_true', default=False,
                  dest='clear_screen')
parser.add_option('-c', '--configuration-file', help='configuration file for PostgreSQL connections', action='store',
                  default='', dest='config_file')

options, args = parser.parse_args()

# setup system constants

TICK_LENGTH = options.tick

output_method = options.output_method

if not output_method_is_valid(output_method):
    print 'Unsupported output method: {0}'.format(output_method)
    print 'Valid output methods are: {0}'.format(','.join(get_valid_output_methods()))
    sys.exit(1)

if output_method == OUTPUT_METHOD.curses and not curses_available:
    print 'Curses output is selected, but curses are unavailable, falling back to console output'
    output_method == OUTPUT_METHOD.console

LOG_FILE_NAME = options.log_file

# truncate the former logs
with open(LOG_FILE_NAME, 'w'):
    pass

# set basic logging
logging.basicConfig(format='%(levelname)s: %(asctime)-15s %(message)s', filename=LOG_FILE_NAME)
logger = logging.getLogger(__name__)
logger.setLevel((logging.INFO if options.verbose else logging.ERROR))

log_stderr = logging.StreamHandler()
logger.addHandler(log_stderr)


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
        'column_header': COHEADER.ch_default,
    }

    NCURSES_CUSTOM_OUTPUT_FIELDS = [
        'header',
        'prefix',
        'append_column_headers',
    ]

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
        for l in self.transform_list_data, self.transform_dict_data, self.diff_generator_data, \
            self.output_transform_data:
            self.validate_list_out(l)
        self.output_column_positions = self._calculate_output_column_positions()

    def set_ignore_autohide(self, new_status):
        self.ignore_autohide = new_status

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
            logger.error('The command {cmd} returned a non-zero exit code'.format(cmd=cmdline))
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
            raise ValueError('passed value should be either a number of seconds from year 1970 or datetime instance of timedelta instance'
                             )

        delta = abs(delta)

        secs = delta.seconds
        mins = secs / 60
        secs %= 60
        hrs = mins / 60
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

        return self.produce_diffs and self.rows_prev is not None and self.rows_cur is not None and len(self.rows_prev) \
            > 0 and len(self.rows_cur) > 0

    def tick(self):
        self.ticks += 1

    def needs_refresh(self):
        return self.ticks % self.ticks_per_refresh == 0

    def refresh(self):
        self._do_refresh(None)

    def ident(self):
        return str(self.__class__).lower().split('.')[1].split('statcollector')[0]

    def ncurses_set_prefix(self, new_prefix):
        self.ncurses_custom_fields['prefix'] = new_prefix

    def cook_row(self, row, header, method):
        cooked_vals = []
        if not self.cook_function.get(method):
            return row
        if len(row) != len(header):
            logger.error('Unable to cook row with non-matching number of header and value columns: row {0} header {1}'.format(row,
                         header))
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
        # change the None output to ''
        if raw_val is None:
            val = ''
        elif str(raw_val) == 'True':
            val = 'T'
        elif str(raw_val) == 'False':
            val = 'F'
        if output_data.get('maxw', 0) > 0 and len(str(val)) > output_data['maxw']:
             # if the value is higher than maximum allowed width - trim it byt removing chars from the middle
            val = self._trim_text_middle(val, output_data['maxw'])
        # currently we only support appending values to the header
        if self.ncurses_custom_fields.get('append_column_headers') or output_data.get('column_header',
                COHEADER.ch_default) == COHEADER.ch_prepend:
            val = '{0}|{1}'.format(attname, val)
        elif output_data.get('column_header', COHEADER.ch_default) == COHEADER.ch_append:
            val = '{0}|{1}'.format(val, attname)
        return val

    @staticmethod
    def _trim_text_middle(val, maxw):
        """ Trim data by removing middle characters, so hello world' for 8 will become hel..rld.
            This kind of trimming seems to be better than tail trimming for user and database names.
        """

        half = (maxw - 2) / 2
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
                result[attname] = ((cur[incol] - prev[incol]) / self.diff_time if cur.get(incol, None) is not None
                                   and prev.get(incol, None) is not None and self.diff_time >= 0 else None)
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
                if not i in st:
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
                if r[attname] != '':
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
                        if not ('optional' in col and col['optional']):
                            logger.error('Column {0} is not optional, but input row has no value for it'.format(incol))
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
                    if not ('optional' in col and col['optional']):
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
                if not 'w' in col:
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
            if method == OUTPUT_METHOD.curses and self.ncurses_custom_fields.get('append_column_headers'):
                minw += len(attname) + 1
            col['w'] = len(attname)
            # use cooked values
            for row in rows:
                if method == OUTPUT_METHOD.curses and self.ncurses_filter_row(row):
                    continue
                val = self._produce_output_value(row, col, method)
                if self.cook_function.get(method):
                    val = self.cook_function[method](attname, val, col)
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
        # reset all previous values
        self.rows_diff = []
        # empty values for current or prev rows are already covered by the need
        for prev, cur in zip(self.rows_prev, self.rows_cur):
            candidate = self._produce_diff_row(prev, cur)
            if candidate is not None and len(candidate) > 0:
                # produce the actual diff row
                self.rows_diff.append(candidate)


class PgstatCollector(StatCollector):

    """ Collect PostgreSQL-related statistics """

    def __init__(self, pgcon, pid, dbname, dbver):
        super(PgstatCollector, self).__init__()
        self.postmaster_pid = pid
        self.pgcon = pgcon
        self.pids = []
        self.rows_diff = []
        self.rows_diff_output = []
        # figure out our backend pid
        self.connection_pid = pgcon.get_backend_pid()
        self.dbname = dbname
        self.dbver = dbver
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
            {'out': 'delayacct_blkio_ticks', 'in': 41, 'fn': long},
            {'out': 'guest_time', 'in': 42, 'fn': StatCollector.ticks_to_seconds},
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
            {'out': 'xact_start', 'diff': False},
            {'out': 'datname', 'diff': False},
            {'out': 'usename', 'diff': False},
            {'out': 'waiting', 'diff': False},
            {'out': 'query', 'diff': False},
        ]

        self.output_transform_data = [  # query with age 5 and more will have the age column highlighted
            {
                'out': 'pid',
                'pos': 0,
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
                'out': 'age',
                'in': 'xact_start',
                'noautohide': True,
                'pos': 9,
                'fn': StatCollector.delta_pretty_print,
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
            logger.error("Couldn't determine the pid of subprocesses for {0}".format(ppid))
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
    def _get_pstype(cmdline):
        """ gets PostgreSQL process type from the command-line."""

        pstype = 'unknown'
        if cmdline is not None and len(cmdline) > 0:
            # postgres: stats collector process
            m = re.match(r'postgres:\s+(.*)\s+process', cmdline)
            if m:
                pstype = m.group(1)
            else:
                if re.match(r'postgres:.*', cmdline):
                    # assume it's a backend process
                    pstype = 'backend'
        if pstype == 'autovacuum worker':
            pstype = 'autovacuum'
        return pstype

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
        stat_data = self._read_pg_stat_activity()
        for pid in self.pids:
            if pid == self.connection_pid:
                continue
            result_row = {}
            # for each pid, get hash row from /proc/
            proc_data = self._read_proc(pid)
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

    def _read_proc(self, pid):
        """ see man 5 proc for details (/proc/[pid]/stat) """

        result = {}
        raw_result = {}

        fp = None
        # read raw data from /proc/stat, proc/cmdline and /proc/io
        for ftyp, fname in zip(('stat', 'cmd', 'io'), ('/proc/{0}/stat', '/proc/{0}/cmdline', '/proc/{0}/io')):
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
        result['type'] = self._get_pstype(result['cmdline'])
        return result

    def _read_pg_stat_activity(self):
        """ Read data from pg_stat_activity """

        cur = self.pgcon.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        # the pg_stat_activity format has been changed to 9.2, avoiding ambigiuous meanings for some columns.
        # since it makes more sense then the previous layout, we 'cast' the former versions to 9.2.

        if self.dbver < 9.2:
            cur.execute("""
                SELECT datname,
                       procpid as pid,
                       usename,
                       client_addr,
                       client_port,
                       round(extract(epoch from xact_start)) as xact_start,
                       waiting,
                       CASE
                         WHEN current_query = '<IDLE>' THEN 'idle'
                         WHEN current_query = '<IDLE> in transaction' THEN 'idle in transaction'
                         WHEN current_query = '<IDLE> in transaction (aborted)' THEN 'idle in transaction (aborted)'
                        ELSE current_query
                       END AS query
                  FROM pg_stat_activity WHERE procpid != pg_backend_pid()
                """)
        else:
            cur.execute("""
                SELECT datname,
                       pid,
                       usename,
                       client_addr,
                       client_port,
                       round(extract(epoch from xact_start)) as xact_start,
                       waiting,
                       CASE
                        WHEN state != 'active' THEN state
                       ELSE query
                       END AS query
                  FROM pg_stat_activity WHERE pid != pg_backend_pid()
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
        return "{1} {0} database connections: {2} total, {3} active\n".format(
             self.dbver, self.dbname, self.total_connections, self.active_connections)

    def diff(self):
        """ we only diff backend processes if new one is not idle and use pid to identify processes """

        self.rows_diff = []
        for cur in self.rows_cur:
            if 'query' not in cur or cur['query'] != 'idle':
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
                    self.rows_diff.append(candidate)
        # order the result rows by the start time value
        self.rows_diff.sort(key=itemgetter('xact_start'))

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
        self.ncurses_custom_fields = {
            'header': False,
            'prefix': 'sys: ',
            'append_column_headers': True
        }

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
                elements = line.split()
                if len(elements) > 2:
                    raw_result[elements[0]] = elements[1:]
                else:
                    raw_result[elements[0]] = elements[1]
            result = self._transform_input(raw_result)
        except IOError:
            logger.error('Unable to read {0}, global data will be unavailable', SystemStatCollector.PROC_STAT_FILENAME)
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

    def __init__(self, dbname, dbversion, work_directory):
        super(PartitionStatCollector, self).__init__(ticks_per_refresh=1)
        self.dbname = dbname
        self.dbver = dbversion
        self.work_directory = work_directory
        self.df_list_transformation = [{'out': 'dev', 'in': 0, 'fn': self._dereference_dev_name}, {'out': 'space_total'
                                       , 'in': 1, 'fn': int}, {'out': 'space_left', 'in': 3, 'fn': int}]
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
                'fn': self.sectors_pretty_print,
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
        result = None
        # first check that the name exists
        result = os.path.realpath(devname)
        return (result.replace('/dev/', '') if result else None)

    def refresh(self):
        result = {}

        df_out = self.get_df_data()
        for pname in PartitionStatCollector.DATA_NAME, PartitionStatCollector.XLOG_NAME:
            result[pname] = self._transform_input(df_out[pname], self.df_list_transformation)

        io_out = self.get_io_data([result[PartitionStatCollector.DATA_NAME]['dev'],
                                  result[PartitionStatCollector.XLOG_NAME]['dev']])
        du_out = self.get_du_data()

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

    def get_df_data(self):
        """ Retrive raw data from df (transformations are performed via df_list_transformation) """

        result = {PartitionStatCollector.DATA_NAME: [], PartitionStatCollector.XLOG_NAME: []}
        ret = self.exec_command_with_output('df -PB {0} {1} {1}/pg_xlog/'.format(PartitionStatCollector.BLOCK_SIZE,
                                            self.work_directory))
        if ret[0] != 0 or ret[1] is None:
            logger.error('Unable to read data and xlog partition information for the database {0}'.format(dbname))
        else:
            output = str(ret[1]).splitlines()
            if len(output) > 2:
                result[PartitionStatCollector.DATA_NAME] = output[1].split()
                result[PartitionStatCollector.XLOG_NAME] = output[2].split()
            else:
                logger.error('df output looks truncated'.format(output))
        return result

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
            logger.error('Unable to read {0}', PartitionStatCollector.DISK_STAT_FILE)
            result = {}
        finally:
            fp and fp.close()
        return result

    def get_du_data(self):
        result = {PartitionStatCollector.DATA_NAME: [], PartitionStatCollector.XLOG_NAME: []}
        ret = self.exec_command_with_output('du -B {0} -s {1} {1}/pg_xlog/'.format(PartitionStatCollector.BLOCK_SIZE,
                                            self.work_directory))
        if ret[0] != 0 or ret[1] is None:
            logger.error('Unable to read free space information for the pg_xlog and data directories for the database\
             {0}'.format(self.dbname))
        else:
            output = str(ret[1]).splitlines()
            if len(output) > 1:
                result['data'] = output[0].split()
                result['xlog'] = output[1].split()
            else:
                logger.error('du output looks truncated'.format(output))
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
            {'in': 'Buffers', 'out': 'buffers', 'fn': int},
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

        self.ncurses_custom_fields = {
            'header': False,
            'prefix': 'mem: ',
            'append_column_headers': True,
        }

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
                vals = l.strip().rstrip(' kB').split()
                if len(vals) == 2:
                    name, val = vals
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
        result = (int(row['CommitLimit']) - int(row['Committed_AS']) if row.get('CommitLimit', None) is not None
                  and row.get('Committed_AS', None) is not None else None)
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
                'column_header': COHEADER.ch_prepend,
                'status_fn': self._load_avg_state,
            },
            {
                'out': 'up',
                'in': 'uptime',
                'pos': 1,
                'noautohide': True,
                'column_header': COHEADER.ch_prepend,
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
                'column_header': COHEADER.ch_append,
            },
            {
                'out': 'name',
                'in': 'sysname',
                'pos': 3,
                'noautohide': True,
            },
        ]

        self.ncurses_custom_fields = {
            'header': False,
            'prefix': None,
            'append_column_headers': False
        }

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
        if load_avg_str == None:
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
        print data

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

        curses.curs_set(0)  # make the cursor invisible
        self.screen.nodelay(1)  # disable delay when waiting for keyboard input

        # initialize colors
        if hasattr(curses, 'use_default_colors'):
            curses.use_default_colors()
            curses.init_pair(1, -1, -1)
            curses.init_pair(2, curses.COLOR_WHITE, curses.COLOR_BLUE)
            curses.init_pair(3, curses.COLOR_WHITE, curses.COLOR_RED)
            curses.init_pair(4, -1, curses.COLOR_GREEN)
            curses.init_pair(5, curses.COLOR_GREEN, -1)
            curses.init_pair(6, curses.COLOR_WHITE, curses.COLOR_CYAN)

            self.COLOR_NORMAL = curses.color_pair(1)
            self.COLOR_WARNING = curses.color_pair(2)
            self.COLOR_CRITICAL = curses.color_pair(3)
            self.COLOR_HIGHLIGHT = curses.color_pair(4)
            self.COLOR_INVERSE_HIGHLIGHT = curses.color_pair(5)
            self.COLOR_MENU = curses.color_pair(6)
        else:
            self.is_color_supported = False

    def display(self, data):
        """ just collect the data """

        collector_name = data.keys()[0]
        self.data[collector_name] = data.values()[0]
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

    def show_help_bar(self):
        global display_units
        global freeze
        global filter_aux
        global autohide_fields
        # only show help if we have enough screen real estate
        if self.next_y > self.screen_y - 1:
            pass
        next_x = self.print_text(self.screen_y - 1, 0, '<s>', self.COLOR_NORMAL)
        next_x = self.print_text(self.screen_y - 1, next_x, 'System processes   ',
                                 (self.COLOR_MENU if filter_aux else self.COLOR_CRITICAL))
        next_x = self.print_text(self.screen_y - 1, next_x, '<f>', self.COLOR_NORMAL)
        next_x = self.print_text(self.screen_y - 1, next_x, 'Output freeze   ',
                                 (self.COLOR_MENU if not freeze else self.COLOR_CRITICAL))
        next_x = self.print_text(self.screen_y - 1, next_x, '<u>', self.COLOR_NORMAL)
        next_x = self.print_text(self.screen_y - 1, next_x, 'Measurement units   ',
                                 (self.COLOR_MENU if not display_units else self.COLOR_CRITICAL))
        next_x = self.print_text(self.screen_y - 1, next_x, '<a>', self.COLOR_NORMAL)
        next_x = self.print_text(self.screen_y - 1, next_x, 'Autohide fields   ',
                                 (self.COLOR_MENU if not autohide_fields else self.COLOR_CRITICAL))
        next_x = self.print_text(self.screen_y - 1, next_x, '<h>', self.COLOR_NORMAL)
        next_x = self.print_text(self.screen_y - 1, next_x, 'Help   ',
                                 (self.COLOR_MENU if not self.show_help else self.COLOR_CRITICAL))

    def show_clock(self):
        clock_str_len = len(self.CLOCK_FORMAT)
        clean = True
        for pos in range(0, clock_str_len):
            x = self.screen.inch(0, self.screen_x - clock_str_len - 2 + pos) & 255
            if x != ord(' '):
                clean = False
                break
        if clean:
            clock_str = time.strftime(self.CLOCK_FORMAT, time.localtime())
            self.screen.addnstr(0, self.screen_x - clock_str_len - 1, clock_str, clock_str_len)

    def _status_to_color(self, status, highlight):
        if status == COLSTATUS.cs_critical:
            return self.COLOR_CRITICAL
        if status == COLSTATUS.cs_warning:
            return self.COLOR_WARNING
        if highlight:
            return self.COLOR_HIGHLIGHT
        return self.COLOR_NORMAL

    def color_text(self, status_map, highlight, text):
        result = []
        xcol = 0
        # split the text into the header and the rest
        f = text.split('|')
        if len(f) < 2:
            # header is not there
            values = f[0]
        else:
            # add header with a normal color
            xcol += len(f[0])
            result.append({
                'start': 0,
                'width': xcol,
                'word': f[0],
                'color': self.COLOR_NORMAL,
            })
            values = f[1]
            # add extra space between the header and the values
            xcol += 1
        # status format: field_no -> color
        # if the status field contain a single value of -1 - just
        # highlight everything without splitting the text into words
        # get all words from the text and their relative positions
        if len(status_map) == 1 and -1 in status_map:
            color = self._status_to_color(status_map[-1], highlight)
            result.append({
                'start': xcol,
                'word': values,
                'width': len(values),
                'color': color,
            })
        else:
            words = list(re.finditer(r'(\S+)', values))
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
        return result

    def help(self):
        y = 0
        self.print_text(y, 0, '{0} {1} - a monitor for PostgreSQL related system statistics'.format(__appname__, __version__),
                        self.COLOR_NORMAL | curses.A_BOLD)
        y += 2
        self.print_text(y, 0, 'The following hotkeys are supported:')
        y += 1
        x = self.print_text(y, 5, 's: ', self.COLOR_NORMAL | curses.A_BOLD)
        self.print_text(y, x, 'change system processes display mode')
        y += 1
        x = self.print_text(y, 5, 'f: ', self.COLOR_NORMAL | curses.A_BOLD)
        self.print_text(y, x, 'freeze output')
        y += 1
        x = self.print_text(y, 5, 'u: ', self.COLOR_NORMAL | curses.A_BOLD)
        self.print_text(y, x, 'toggle measurement units (MB, s)')
        y += 1
        x = self.print_text(y, 5, 'w: ', self.COLOR_NORMAL | curses.A_BOLD)
        self.print_text(y, x, 'avoid hiding non-essential attributes')
        y += 2
        self.print_text(y, 0, "Press 'h' to exit this screen")

    def show_collector_data(self, collector, clock=False):

        if collector not in self.data or len(self.data[collector]) <= 0 or len(self.data[collector].get('rows', ())) \
            <= 0 and not self.data[collector]['prefix']:
            return

        rows = self.data[collector]['rows']
        statuses = self.data[collector]['statuses']
        align = self.data[collector]['align']
        header = self.data[collector].get('header', False) or False
        append_column_headers = self.data[collector].get('append_column_headers', False)
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
                                    COLALIGN.ca_none) if not append_column_headers else COLALIGN.ca_left)
                w = layout[field]['width']
                # now check if we need to add ellipsis to indicate that the value has been truncated.
                # we don't do this if the value is less than a certain length or when the column is marked as
                # containing truncated values, but the actual value is not truncated.
                if layout[field].get('truncate', False) and w > self.MIN_ELLIPSIS_FIELD_LENGTH \
                    and w < len(str(row[field])):
                    text = str(row[field])[:w - 3] + '...'
                else:
                    text = str(row[field])[:w]
                text = self._align_field(text, w, column_alignment, types.get(field, COLTYPES.ct_string))
                color_fields = self.color_text(status[field], highlights[field], text)
                for f in color_fields:
                    self.screen.addnstr(self.next_y, layout[field]['start'] + f['start'],
                                        f['word'], f['width'], f['color'])
            self.next_y += 1

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

            color = self.COLOR_INVERSE_HIGHLIGHT if prefix_newline else self.COLOR_NORMAL

            self.screen.addnstr(self.next_y, 1, str(prefix), len(str(prefix)), color)
            if prefix_newline:
                return -1
            else:
                return prefix_len
        else:
            return 0

    def display_header(self, layout, align, types):
        for field in layout:
            text = self._align_field(field, layout[field]['width'], align.get(field, COLALIGN.ca_none),
                                             types.get(field, COLTYPES.ct_string))
            self.screen.addnstr(self.next_y, layout[field]['start'], text,
                                layout[field]['width'], self.COLOR_NORMAL|curses.A_BOLD)

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
    def _align_field(text, width, align, typ):
        if align == COLALIGN.ca_none:
            if typ == COLTYPES.ct_number:
                align = COLALIGN.ca_right
            else:
                align = COLALIGN.ca_left
        textlen = len(text)
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
        col_remaining = [name for name in colnames if not name in colhidden]
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
        for argname in 'port', 'host', 'socket_directory', 'user':
            try:
                val = config.get(section, argname)
            except ConfigParser.NoOptionError:
                val = None
            # might happen also if the option is there, but the value is not set
            if val is not None:
                config_data[section][argname] = val
    return config_data


def read_postmaster_pid(work_directory):
    """ Parses the postgres directory tree and extracts the pid of the postmaster process """

    fp = None
    try:
        fp = open('{0}/postmaster.pid'.format(work_directory))
        pid = fp.readline().strip()
    except:
        # XXX: do not bail out in case we are collecting data for multiple PostgreSQL clusters
        logger.error('Unable to read postmaster.pid for {name}\n HINT: \
            make sure Postgres is running'.format(name=dbname))
        return None
    finally:
        if fp is not None:
            fp.close()
    return pid


# execution starts here

def loop(collectors, groups, output_method):
    if output_method == OUTPUT_METHOD.curses:
        curses.wrapper(do_loop, groups, output_method, collectors)
    else:
        do_loop(None, groups, output_method, collectors)


def do_loop(screen, groups, output_method, collectors):
    """ Display output (or pass it through to ncurses) """

    output = None
    global display_units
    global freeze
    global filter_aux
    global autohide_fields

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
        for st in collectors:
            if output_method == OUTPUT_METHOD.curses:
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
            st.set_units_display(display_units)
            st.set_ignore_autohide(not autohide_fields)
            if isinstance(st, PgstatCollector):
                st.set_aux_processes_filter(filter_aux)
            st.tick()
            if st.needs_refresh() and not freeze:
                st.refresh()
            if st.needs_diffs() and not freeze:
                st.diff()
        if output_method == OUTPUT_METHOD.curses:
            process_groups(groups)
        for st in collectors:
            output.display(st.output(output_method))
        if options.clear_screen or output_method == OUTPUT_METHOD.curses:
            output.refresh()
        time.sleep(TICK_LENGTH)

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


def get_all_postmasters():
    """ detect all postmasters running and get their pids """

    STAT_FIELD_PID = 0
    STAT_FIELD_PROCESS_NAME = 1
    STAT_FIELD_STATE = 2
    STAT_FIELD_PPID = 3
    STAT_FIELD_START_TIME = 21

    all_postgres_pids = []
    postmaster_pids = []
    postmaster_data_dirs = []
    postgres_stat = []
    candidates = []
    # get all 'number' directories from /proc/ and sort them
    for x in os.listdir('/proc'):
        if not re.match(r'\d+', x):
            continue
        st = None
        try:
            st = os.stat('/proc/{0}'.format(x))
        except os.error:
            logger.warn('could not stat /proc/{0}, process has probably terminated'.format(x))
        if st is not None and stat.S_ISDIR(st.st_mode):
            candidates.append(int(x))

    candidates.sort()
    # collect data from /proc/[pid]/stat for all postgres processes
    for pid in candidates:
        stat_file_name = '/proc/{0}/stat'.format(pid)
        stat_fields = []
        # if we can't access the stat - bail out
        if not os.access(stat_file_name, os.R_OK):
            continue
        try:
            with open(stat_file_name, 'rU') as fp:
                stat_fields = fp.read().strip().split()
        except:
            logger.error('failed to read {0}'.format(stat_file_name))
            continue
        # read PostgreSQL processes. Avoid zombies
        if len(stat_fields) < STAT_FIELD_START_TIME + 1 or stat_fields[STAT_FIELD_PROCESS_NAME] != '(postgres)' \
            or stat_fields[STAT_FIELD_STATE] == 'Z':
            if stat_fields[STAT_FIELD_STATE] == 'Z':
                logger.warning('zombie process {0}'.format(pid))
            if len(stat_fields) < STAT_FIELD_START_TIME + 1:
                logger.error('{0} output is too short'.format(stat_file_name))
            continue
        # convert interesting fields to int
        for no in STAT_FIELD_PID, STAT_FIELD_PPID, STAT_FIELD_START_TIME:
            stat_fields[no] = int(stat_fields[no])
        postgres_stat.append(stat_fields)
        all_postgres_pids.append(stat_fields[STAT_FIELD_PID])

    for st in postgres_stat:
        # get the parent pid
        pid = st[STAT_FIELD_PID]
        ppid = st[STAT_FIELD_PPID]
        # if parent is not a postgres process - this is the postmaster
        if ppid not in all_postgres_pids:
            cmd_line_file_name = '/proc/{0}/cmdline'.format(pid)
            # obtain postmaster's work directory
            with open(cmd_line_file_name, 'rU') as fp:
                cmd_line = fp.read()
                if cmd_line:
                    fields = cmd_line.split('\x00')
                    if len(fields) > 2:
                        data_dir = fields[2]
                        postmaster_data_dirs.append(data_dir)
                        postmaster_pids.append(pid)
    return dict(zip(postmaster_data_dirs, postmaster_pids))


def get_dbname_dbversion(db_path):
    # dbname is the last component of the path
    rest, dbname = os.path.split(db_path)
    if dbname == '':  # path ends with /
        dbname = os.path.split(rest)[1]
    if dbname == 'data':  # HACK: assume dbversion/dbname/data format
        rest = os.path.split(rest)[0]
        dbname = os.path.split(rest)[1]
        if dbname == '':
            dbname = 'data'
        else:
            dbname = dbname.replace('pgsql_', '')
    # read the version number
    dbversion = detect_postgres_version(db_path)
    return dbname, dbversion


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
    except Exception, e:
        # if we failed to read a file - assume version 9.0
        logger.warning('could not read from the PG_VERSION: {0}'.format(e))
        version = None
    finally:
        fp and fp.close()
    return version


def detect_db_connection_arguments(work_directory, version):
    """
        Try to detect database connection arguments from the postmaster.pid
        We do this by first extracting useful information from postmaster.pid,
        next reading the postgresql.conf if necessary and, at last,
    """

    args = {}
    PID_FILE = '{0}/postmaster.pid'.format(work_directory)
    fp = None
    # get database version first, it will determine how much we can extract from
    # the postmaster.pid
    if version is None:
        version = 9.0

    # try to access the socket directory
    if not os.access(work_directory, os.R_OK|os.X_OK):
        logger.warning("could not read from PostgreSQL cluster directory {0}: permission denied".format(work_directory))
    else:
        # now when we have version try getting useful information from postmaster.pid
        # we can obtain host, port and socket directory path
        try:
            fp = open(PID_FILE, 'rU')
            lines = fp.readlines()
            for no, l in enumerate(lines):
                if no == 0:
                    args['pid'] = (int(l.strip()) if l else None)
                if no == 1:
                    args['data_dir'] = (l.strip() if l else None)
                if no == 2:
                    if version >= 9.1:
                        args['start_time'] = (l.strip() if l else None)
                    else:
                        args['shmem_key'] = (l.strip().split() if l else None)
                if version >= 9.1:
                    if no == 3:
                        args['port'] = (int(l.strip()) if l else None)
                    if no == 4:
                        args['socket_directory'] = (l.strip() if l else None)
                    if no == 5:
                        args['host'] = (l.strip() if l else None)
                    if no == 6:
                        args['shmem_key'] = (l.strip().split() if l else None)
        except Exception, e:
            logger.warning('could not read pid file: {0}'.format(e))
            fp and fp.close()
        # at the moment, we probably have some of the data, and might need to fill out the rest.
        # check if we do have a port, get if from postgresql.conf if not.
        if version < 9.1:
            conf_file = '{0}/postgresql.conf'.format(work_directory)
            # pre-compile some regular expresisons
            regexes = {}
            regexes['port'] = re.compile('^\s*port\s*=\s*(\d+)\s*$')
            regexes['host'] = re.compile('^\s*host\s*=\s*(\d+)\s*$')
            regexes['socket_directory'] = re.compile('^\s*unix_socket_directory\s*=\s*(\S+)\s*$')

            # try to read parameters from PostgreSQL.conf
            try:
                fp = open(conf_file, 'rU')
                content = fp.readlines()
                # now parse it and get the port, socket directory and listen address
                for l in content:
                    l = l.strip()
                    # ignore lines with comments
                    if re.match('^\s+#', l):
                        continue
                    for keys in regexes:
                        m = re.match(regexes[keys], l)
                        if m:
                            args[keys] = m.group(1).strip("'")
                            break
            except Exception, e:
                logger.warning('could not read configuration file: {0}'.format(e))
            finally:
                if fp is not None:
                    fp.close()
            # now it's time to fill-in defaults
            if args.get('port') is None:
                args['port'] = 5432
            else:
                args['port'] = int(args['port'])
            if args.get('host') is not None:
                if args['host'] == '*':
                    args['host'] = '127.0.0.1'
                else:
                    # get only the first addr
                    args['host'] = args['host'].split(',')[0]
            elif args.get('socket_directory') is None:
                # we don't have the listen address and the socket directory
                # try to guess the socket directory here
                # first, check for the data dir and the data dir upper directory
                for path in work_directory, os.path.split(work_directory)[0]:
                    port = detect_db_port(work_directory)
                    if port != -1:
                        args['port'] = port
                        args['socket_directory'] = path
                        break
    # if we still don't have port and listen_address or unix_socket_directory
    # complain
    if not (args.get('port') and (args.get('host') or args.get('socket_directory'))):
        logger.error('unable to detect connection parameters for the PostgreSQL cluster at {0}'.format(work_directory))
        return None
    return args


def connect_with_connection_arguments(dbname, args):
    """ connect the database and get all necessary options like pid and work_directory
        we use port, host and socket_directory, prefering socket over TCP connections
    """

    pgcon = None
    # sanity check
    if not (args.get('port') and (args.get('host') or args.get('socket_directory'))):
        missing = ('port' if not args.get('port') else 'host or socket_directory')
        logger.error('Not all required connection arguments ({0}) are specified for the database {1}, skipping it'.format(missing,
                  dbname))
        return None

    use_socket = (True if args.get('socket_directory') else False)
    port = args['port']
    host = (args['socket_directory'] if use_socket else args['host'])
    user = args.get('user', 'postgres')
    # establish a new connection
    try:
        pgcon = pg.connect('host={0} port={1} user={2}'.format(host, port, user))
    except Exception, e:
        logger.error('failed to establish connection to {0} on port {1} user {2}'.format(host, port, user))
        logger.error('PostgreSQL exception: {0}'.format(e))
        return None
    # get the database version from the pgcon properties
    version_num = pgcon.server_version
    version_num /= 100
    dbver = float('{0}.{1}'.format(version_num / 100, version_num % 100))
    cur = pgcon.cursor()
    cur.execute('show data_directory')
    work_directory = cur.fetchone()[0]
    cur.close()
    pgcon.commit()
    # now, when we have the work directory, acquire the pid of the postmaster.
    pid = read_postmaster_pid(work_directory)
    if pid is None:
        logger.error('failed to read pid of the postmaster on {0}:{1}'.format(host, port))
        return None
    # now we have all components to create the result
    result = {
        'name': dbname,
        'ver': dbver,
        'wd': work_directory,
        'pid': pid,
        'pgcon': pgcon,
    }
    return result


if __name__ == '__main__':
    user_dbname = options.instance
    user_dbver = options.version
    clusters = []

    # now try to read the configuration file
    config_data = (read_configuration(options.config_file) if options.config_file else None)
    if config_data:
        for dbname in config_data:
            if user_dbname and dbname != user_dbname:
                continue
            conndata = connect_with_connection_arguments(dbname, config_data[dbname])
            if conndata:
                clusters.append(conndata)
            else:
                logger.error('failed to acquire details about the database {0}, the server will be skipped'.format(dbname))
    else:

        # do autodetection
        postmasters = get_all_postmasters()

        # get all PostgreSQL instances
        for result_work_dir, ppid in postmasters.items():
            dbname, dbver = get_dbname_dbversion(result_work_dir)
            # if user requested a specific database name and version - don't try to connect to others
            if user_dbname:
                if dbname != user_dbname or not result_work_dir or not ppid:
                    continue
                if user_dbver is not None and dbver != user_dbver:
                    continue
            try:
                conndata = detect_db_connection_arguments(result_work_dir, dbver)
                if conndata is None:
                    continue
                if conndata.get('socket_directory'):
                    host = conndata['socket_directory']
                else:
                    host = conndata['host']
                port = conndata['port']
                pgcon = pg.connect('host={0} port={1} user=postgres'.format(host, port))
            except Exception, e:
                logger.error('PostgreSQL exception {0}'.format(e))
                pgcon = None
            if pgcon:
                clusters.append({
                    'name': dbname,
                    'ver': dbver,
                    'wd': result_work_dir,
                    'pid': ppid,
                    'pgcon': pgcon,
                })
    try:
        if len(clusters) == 0:
            logger.error('No suitable PostgreSQL instances detected, exiting...')
            logger.error('hint: use -v for details, or specify connection parameters manually in the configuration file (-C)')
            sys.exit(1)

        collectors = []
        groups = {}
        collectors.append(HostStatCollector())
        collectors.append(SystemStatCollector())
        collectors.append(MemoryStatCollector())
        for cl in clusters:
            part = PartitionStatCollector(cl['name'], cl['ver'], cl['wd'])
            pg = PgstatCollector(cl['pgcon'], cl['pid'], cl['name'], cl['ver'])
            groupname = cl['wd']
            groups[groupname] = {'pg': pg, 'partitions': part}
            collectors.append(part)
            collectors.append(pg)

        # we don't want to mix diagnostics messages with useful output, so we log the former into a file.
        logger.removeHandler(log_stderr)
        loop(collectors, groups, output_method)
        logger.addHandler(log_stderr)
    except KeyboardInterrupt:
        print 'Interrupted by user'
        if os.stat(LOG_FILE_NAME)[stat.ST_SIZE] != 0:
            print 'Errors detected, see {0} for warnings and errors output'.format(LOG_FILE_NAME)
    except Exception, e:
        print traceback.format_exc()
    finally:
        for cl in clusters:
            'pgcon' in cl and cl['pgcon'] and cl['pgcon'].close()
        sys.exit(0)
