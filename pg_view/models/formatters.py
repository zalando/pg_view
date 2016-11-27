from datetime import timedelta, datetime, time
from numbers import Number

import re

from pg_view.models.base import COLSTATUS


class StatusFormatter(object):
    BYTE_MAP = [('TB', 1073741824), ('GB', 1048576), ('MB', 1024)]

    def __init__(self, collector):
        self.collector = collector

    def idle_format_fn(self, text):
        r = re.match(r'idle in transaction (\d+)', text)
        if not r:
            return text
        formatted_time = self.time_pretty_print(int(r.group(1)))
        if self.collector.dbver >= 9.2:
            return 'idle in transaction for {0}'.format(formatted_time)
        return 'idle in transaction {0} since the last query start'.format(formatted_time)

    def query_status_fn(self, row, col):
        if row[self.collector.output_column_positions['w']] is True:
            return {-1: COLSTATUS.cs_critical}
        else:
            val = row[self.collector.output_column_positions[col['out']]]
            if val and val.startswith(col.get('warning', '!')):
                return {-1: COLSTATUS.cs_warning}
        return {-1: COLSTATUS.cs_ok}

    def age_status_fn(self, row, col):
        age_string = row[self.collector.output_column_positions[col['out']]]
        age_seconds = self.time_field_to_seconds(age_string)
        if 'critical' in col and col['critical'] < age_seconds:
            return {-1: COLSTATUS.cs_critical}
        if 'warning' in col and col['warning'] < age_seconds:
            return {-1: COLSTATUS.cs_warning}
        return {-1: COLSTATUS.cs_ok}

    def check_ps_state(self, row, col):
        if row[self.collector.output_column_positions[col['out']]] == col.get('warning', ''):
            return {0: COLSTATUS.cs_warning}
        return {0: COLSTATUS.cs_ok}

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
        val = row[self.collector.output_column_positions[col['out']]]
        num = self.time_field_to_seconds(val)
        if num <= col['critical']:
            return {-1: COLSTATUS.cs_critical}
        elif num <= col['warning']:
            return {-1: COLSTATUS.cs_warning}
        return {-1: COLSTATUS.cs_ok}

    def time_interval_pretty_print(self, start_time, is_delta):
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

    def time_pretty_print(self, start_time):
        return self.time_interval_pretty_print(start_time, False)

    def kb_pretty_print(self, b):
        """ Show memory size as a float value in the biggest measurement units  """
        r = []
        for l, n in self.BYTE_MAP:
            if b > n:
                v = round(float(b) / n, 1)
                r.append(str(v) + l)
                break
        if len(r) == 0:
            return '{0}KB'.format(str(b))
        else:
            return ' '.join(r)
