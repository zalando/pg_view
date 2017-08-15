import json
from abc import ABCMeta
from collections import namedtuple
from numbers import Number

from pg_view.loggers import logger
from pg_view.consts import NCURSES_CUSTOM_OUTPUT_FIELDS
from pg_view.utils import enum

COLALIGN = enum(ca_none=0, ca_left=1, ca_center=2, ca_right=3)
COLSTATUS = enum(cs_ok=0, cs_warning=1, cs_critical=2)
COLTYPES = enum(ct_string=0, ct_number=1)
COLHEADER = enum(ch_default=0, ch_prepend=1, ch_append=2)


class ColumnType(namedtuple('ColumnType', 'value header header_position')):
    __slots__ = ()

    @property
    def length(self):
        return len(self.value) + (0 if not self.header_position else len(self.header) + 1)


class BaseDisplayer(object):
    __metaclass__ = ABCMeta

    def __init__(self, output_transform_data, ident, show_units, ignore_autohide, notrim):
        self.output_transform_data = output_transform_data
        self.ident = ident

        self.show_units = show_units
        self.ignore_autohide = ignore_autohide
        self.notrim = notrim

    def _produce_output_name(self, col):
        # get the output column name
        attname = col['out']
        # add units to the column name if neccessary
        if 'units' in col and self.show_units:
            attname += ' ' + col['units']
        return attname

    @staticmethod
    def _produce_output_value(row, col):
        # get the input value
        if 'in' in col:
            val = row.get(col['in'])
        else:
            val = row.get(col['out'])
        # if function is specified - apply it to the input value
        if 'fn' in col and val is not None:
            val = col['fn'](val)
        # if rounding is necessary - round the input value up to specified
        # decimal points
        if 'round' in col and val is not None:
            val = round(val, col['round'])
        return val

    @classmethod
    def from_collector(cls, collector, show_units, ignore_autohide, notrim):
        return cls(
            collector.output_transform_data,
            collector.ident(),
            show_units=show_units,
            ignore_autohide=ignore_autohide,
            notrim=notrim
        )


class JsonDisplayer(BaseDisplayer):
    def __init__(self, output_transform_data, ident, show_units, ignore_autohide, notrim):
        super(JsonDisplayer, self).__init__(output_transform_data, ident, show_units, ignore_autohide, notrim)

    def display(self, rows, before_string=None, after_string=None):
        output = {}
        data = []
        output['type'] = self.ident
        for row in rows:
            data.append(self._produce_output_row(row))
            output['data'] = data
        return json.dumps(output, indent=4)

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


class BaseStreamDisplayer(BaseDisplayer):
    __metaclass__ = ABCMeta

    def __init__(self, output_transform_data, ident, show_units, ignore_autohide, notrim):
        super(BaseStreamDisplayer, self).__init__(output_transform_data, ident, show_units, ignore_autohide, notrim)

    def _output_row_generic(self, row, typ='v'):
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
                val = self._produce_output_value(row, col)
            # prepare the list for the output
            vals.append(val)
        return vals


class ConsoleDisplayer(BaseStreamDisplayer):
    def _calculate_dynamic_width(self, rows):
        """ Examine values in all rows and get the width dynamically """

        for col in self.output_transform_data:
            minw = col.get('minw', 0)
            attname = self._produce_output_name(col)
            # XXX:  if append_column_header, min width should include the size of the attribut name
            col['w'] = len(attname)
            # use cooked values
            for row in rows:
                val = self._produce_output_value(row, col)
                curw = len(str(val))
                if curw > col['w']:
                    col['w'] = curw
                if minw > 0:
                    col['w'] = max(minw, col['w'])

    def display(self, rows, before_string=None, after_string=None):
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

    def _output_template_for_console(self):
        return ' '.join(self._output_row_for_console(None, 't'))

    def _output_row_for_console(self, row, typ='v'):
        return self._output_row_generic(row, typ)


class CursesDisplayer(BaseStreamDisplayer):
    NCURSES_DEFAULTS = {
        'pos': -1,
        'noautohide': False,
        'w': 0,
        'align': COLALIGN.ca_none,
        'column_header': COLHEADER.ch_default,
    }

    def __init__(self, output_transform_data, ident, ncurses_filter_row, ncurses_custom_fields, show_units,
                 ignore_autohide, notrim):
        self.ncurses_filter_row = ncurses_filter_row
        self.ncurses_custom_fields = ncurses_custom_fields
        super(CursesDisplayer, self).__init__(output_transform_data, ident, show_units, ignore_autohide, notrim)

    def _calculate_dynamic_width(self, rows):
        """ Examine values in all rows and get the width dynamically """

        for col in self.output_transform_data:
            minw = col.get('minw', 0)
            attname = self._produce_output_name(col)
            # XXX:  if append_column_header, min width should include the size of the attribut name
            if self.ncurses_custom_fields.get('prepend_column_headers'):
                minw += len(attname) + 1
            col['w'] = len(attname)
            # use cooked values
            for row in rows:
                if self.ncurses_filter_row(row):
                    continue
                val = self._produce_output_value(row, col)
                val = self.curses_cook_value(attname, val, col)
                curw = val.length
                if curw > col['w']:
                    col['w'] = curw
                if minw > 0:
                    col['w'] = max(minw, col['w'])

    def display(self, rows, before_string=None, after_string=None):
        """ for ncurses - we just return data structures. The output code
            is quite complex and deserves a separate class.
        """

        self._calculate_dynamic_width(rows)

        raw_result = {}
        for k in self.NCURSES_DEFAULTS.keys():
            raw_result[k] = []

        for col in self.output_transform_data:
            for opt in self.NCURSES_DEFAULTS.keys():
                raw_result[opt].append((col[opt] if opt in col else self.NCURSES_DEFAULTS[opt]))

        result_header = self._output_row_for_curses(None, 'h')
        result_rows = []
        status_rows = []
        values_rows = []

        for r in rows:
            values_row = self._output_row_for_curses(r, 'v')
            if self.ncurses_filter_row(dict(zip(result_header, values_row))):
                continue
            cooked_row = self.cook_row(result_header, values_row)
            status_row = self._calculate_statuses_for_row(values_row)
            result_rows.append(dict(zip(result_header, cooked_row)))
            status_rows.append(dict(zip(result_header, status_row)))
            values_rows.append(dict(zip(result_header, values_row)))

        types_row = self._calculate_column_types(values_rows)

        result = {
            'rows': result_rows,
            'statuses': status_rows,
            'hide': self._get_columns_to_hide(result_rows, status_rows),
            'highlights': dict(zip(result_header, self._get_highlights())),
            'types': types_row
        }
        for x in NCURSES_CUSTOM_OUTPUT_FIELDS:
            result[x] = self.ncurses_custom_fields.get(x, None)
        for k in self.NCURSES_DEFAULTS.keys():
            if k == 'noautohide' and self.ignore_autohide:
                result[k] = dict.fromkeys(result_header, True)
            else:
                result[k] = dict(zip(result_header, raw_result[k]))
        return {self.ident: result}

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

    def _output_row_for_curses(self, row, typ='v'):
        return self._output_row_generic(row, typ)

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

    def _calculate_statuses_for_row(self, row):
        statuses = []
        for num, col in enumerate(self.output_transform_data):
            statuses.append(self._calculate_output_status(row, col, row[num]))
        return statuses

    @staticmethod
    def _calculate_output_status(row, col, val):
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

    def _get_highlights(self):
        return [col.get('highlight', False) for col in self.output_transform_data]

    def cook_row(self, row, header):
        cooked_vals = []
        if len(row) != len(header):
            logger.error('Unable to cook row with non-matching number of header and value columns: ' +
                         'row {0} header {1}'.format(row, header))
        for no, val in enumerate(row):
            # if might be tempting to just get the column from output_transform_data using
            # the header, but it's wrong: see _produce_output_name for details. This, of
            # course, assumes the number of columns in the output_transform_data is the
            # same as in row: thus, we need to avoid filtering rows in the collector.
            newval = self.curses_cook_value(val, header[no], self.output_transform_data[no])
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

    @classmethod
    def from_collector(cls, collector, show_units, ignore_autohide, notrim):
        return cls(
            collector.output_transform_data,
            collector.ident(),
            collector.ncurses_filter_row,
            collector.ncurses_custom_fields,
            show_units=show_units,
            ignore_autohide=ignore_autohide,
            notrim=notrim
        )
