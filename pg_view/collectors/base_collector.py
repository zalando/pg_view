import time
from abc import ABCMeta

from pg_view.consts import NCURSES_CUSTOM_OUTPUT_FIELDS
from pg_view.loggers import logger
from pg_view.utils import UnitConverter


def warn_non_optional_column(colname):
    logger.error('Column {0} is not optional, but input row has no value for it'.format(colname))


class BaseStatCollector(object):
    """ Generic class to store abstract function and data required to collect system statistics,
        produce diffs and emit output rows.
    """
    __metaclass__ = ABCMeta

    def __init__(self, ticks_per_refresh=1, produce_diffs=True):
        self.rows_prev = []
        self.rows_cur = []
        self.rows_diff = []
        self.ticks = 0
        self.ticks_per_refresh = ticks_per_refresh
        self.diff_time = 0
        self._previous_moment = None
        self._current_moment = None
        self.produce_diffs = produce_diffs
        self.unit_converter = UnitConverter

        # transformation data
        self.transform_dict_data = {}  # data to transform a dictionary input to the stat row
        self.transform_list_data = {}  # ditto for the list input

        # diff calculation data
        self.diff_generator_data = {}  # data to produce a diff row out of 2 input ones.
        self.output_transform_data = {}  # data to transform diff output

        self.ncurses_custom_fields = dict.fromkeys(NCURSES_CUSTOM_OUTPUT_FIELDS, None)

    def postinit(self):
        for l in [self.transform_list_data, self.transform_dict_data, self.diff_generator_data,
                  self.output_transform_data]:
            self.validate_list_out(l)
        self.output_column_positions = self._calculate_output_column_positions()

    def _calculate_output_column_positions(self):
        result = {}
        for idx, col in enumerate(self.output_transform_data):
            result[col['out']] = idx
        return result

    @staticmethod
    def validate_list_out(l):
        """ If the list element doesn't supply an out column - remove it """

        for col in l:
            if 'out' not in col:
                el = l.pop(l.index(col))
                logger.error('Removed {0} column because it did not specify out value'.format(el))

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
                result[attname] = (col['fn'](incol, cur, prev) if cur.get(incol, None) is not None and prev.get(
                    incol, None) is not None else None)
            else:
                # default case - calculate the diff between the current attribute's values of
                # old and new rows and divide it by the time interval passed between measurements.
                result[attname] = ((cur[incol] - prev[incol]) / self.diff_time if cur.get(incol, None) is not None and
                                   prev.get(incol, None) is not None and self.diff_time >= 0 else None)
        return result

    def _transform_input(self, x, custom_transformation_data=None):
        if isinstance(x, list) or isinstance(x, tuple):
            return self._transform_list(x, custom_transformation_data)
        elif isinstance(x, dict):
            return self._transform_dict(x, custom_transformation_data)
        elif isinstance(x, str):
            raise Exception('transformation of input type string is not implemented')
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
                            warn_non_optional_column(incol)
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
                        warn_non_optional_column(incol)
                else:
                    result[attname] = l[incol]
                if 'fn' in col and result[attname] is not None:
                    result[attname] = col['fn'](result[attname])
            return result
        raise Exception('No data for the dict transformation supplied')

    @staticmethod
    def _get_input_column_name(col):
        return col['in'] if 'in' in col else col['out']

    def ncurses_filter_row(self, row):
        return False

    def output(self, displayer, before_string=None, after_string=None):
        rows = self._get_rows()
        return displayer.display(rows, before_string, after_string)

    def _get_rows(self):
        return self.rows_diff if self.produce_diffs else self.rows_cur

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
