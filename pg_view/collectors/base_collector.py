import json
import os
import subprocess
import time
from datetime import timedelta, datetime
from numbers import Number

from pg_view.loggers import logger
from pg_view.models.outputs import COLSTATUS, COLALIGN, COLTYPES, COLHEADER, ColumnType
from pg_view.utils import OUTPUT_METHOD


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
        return float(tick_value_str) / StatCollector.USER_HZ if tick_value_str is not None else None

    @staticmethod
    def bytes_to_mbytes(bytes_val):
        return float(bytes_val) / 1048576 if bytes_val is not None else None

    @staticmethod
    def sectors_to_mbytes(sectors):
        return float(sectors) / 2048 if sectors is not None else None

    @staticmethod
    def kb_to_mbytes(kb):
        return float(kb) / 1024 if kb is not None else None

    @staticmethod
    def time_diff_to_percent(timediff_val):
        return float(timediff_val) * 100 if timediff_val is not None else None

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
            b %= n
        return ' '.join(r)

    @staticmethod
    def kb_pretty_print(b):
        """ Show memory size as a float value in the biggest measurement units  """

        r = []
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
        """Returns a human readable string that shows a time between now and the timestamp passed as an argument.
        The passed argument can be a timestamp (returned by time.time() call) a datetime object or a timedelta object.
        In case it is a timedelta object, then it is formatted only
        """

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
        return 0 < val < bound

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
            logger.error(
                'Unable to cook row with non-matching number of header and value columns: ' +
                'row {0} header {1}'.format(row, header)
            )
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
        if self.ncurses_custom_fields.get('prepend_column_headers') or output_data.get(
                'column_header', COLHEADER.ch_default) == COLHEADER.ch_prepend:
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

    @staticmethod
    def _produce_output_value(row, col, method=OUTPUT_METHOD.console):
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

    @staticmethod
    def _calculate_output_status(row, col, val, method):
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

    @staticmethod
    def _transform_string(d):
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

    @staticmethod
    def _calculate_column_types(rows):
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

        result = {'rows': result_rows,
                  'statuses': status_rows,
                  'hide': self._get_columns_to_hide(result_rows, status_rows),
                  'highlights': dict(zip(result_header, self._get_highlights())),
                  'types': types_row}
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
