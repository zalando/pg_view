#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import absolute_import

import logging
import platform
import resource
import sys
import time
import traceback
from multiprocessing import JoinableQueue  # for then number of cpus
from operator import itemgetter
from optparse import OptionParser

import os
import re

path = os.path.dirname(os.path.dirname(__file__))
sys.path.insert(0, path)

from pg_view.models.base import StatCollector, COLSTATUS, COLALIGN, COLTYPES, COLHEADER, OUTPUT_METHOD, TICK_LENGTH
import pg_view.models.base
from pg_view.models.host_stat import HostStatCollector
from pg_view.models.memory_stat import MemoryStatCollector
from pg_view.models.partition_stat import PartitionStatCollector, DetachedDiskStatCollector
from pg_view.models.pg_stat import PgStatCollector
from pg_view.models.system_stat import SystemStatCollector
from pg_view.validators import get_valid_output_methods, output_method_is_valid
from pg_view.consumers import DiskCollectorConsumer
from pg_view.models.db_client import establish_user_defined_connection, make_cluster_desc, build_connection, \
    read_configuration, detect_db_connection_arguments, get_postmasters_directories
from pg_view.helpers import process_groups

__appname__ = 'pg_view'
__version__ = '1.3.0'
__author__ = 'Oleksii Kliukin <oleksii.kliukin@zalando.de>'
__license__ = 'Apache 2.0'


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

# some global variables for keyboard output
freeze = False
filter_aux = True
autohide_fields = False
display_units = False
notrim = False
realtime = False


def parse_args():
    """parse command-line options"""

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
    CLOCK_FORMAT = '%H:%M:%S'

    MIN_ELLIPSIS_FIELD_LENGTH = 10

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
        self.print_text(y, 0, '{0} {1} - a monitor for PostgreSQL related system statistics'.format(__appname__, __version__),
                        self.COLOR_NORMAL | curses.A_BOLD)
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

    def show_collector_data(self, collector):
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
        for collector in collectors:
            if output_method == OUTPUT_METHOD.curses:
                if not poll_keys(screen, output):
                    # bail out immediately
                    return
            collector.set_units_display(display_units)
            collector.set_ignore_autohide(not autohide_fields)
            collector.set_notrim(notrim)
            process_single_collector(collector, filter_aux)
            if output_method == OUTPUT_METHOD.curses:
                if not poll_keys(screen, output):
                    return

        if output_method == OUTPUT_METHOD.curses:
            process_groups(groups)
        # in the non-curses cases display actually shows the data and refresh
        # clears the screen, so we need to refresh before display to clear the old data.
        if options.clear_screen and output_method != OUTPUT_METHOD.curses:
            output.refresh()
        for collector in collectors:
            output.display(collector.output(output_method))
        # in the curses case, refresh shows the data queued by display
        if output_method == OUTPUT_METHOD.curses:
            output.refresh()
        if not realtime:
            time.sleep(TICK_LENGTH)


def process_single_collector(collector, filter_aux):
    """ perform all heavy-lifting for a single collector, i.e. data collection,
        diff calculation, etc. This is meant to be run in a separate thread.
    """

    if isinstance(collector, PgStatCollector):
        collector.set_aux_processes_filter(filter_aux)
    collector.tick()
    if not freeze:
        if collector.needs_refresh():
            collector.refresh()
        if collector.needs_diffs():
            collector.diff()
        else:
            # if the server goes offline, we need to clear diffs here,
            # otherwise rows from the last successful reading will be
            # displayed forever
            collector.clear_diffs()


def main():
    global logger, options
    # bail out if we are not running Linux
    if platform.system() != 'Linux':
        print('Non Linux database hosts are not supported at the moment. Can not continue')
        sys.exit(243)

    options, args = parse_args()
    pg_view.models.base.TICK_LENGTH = options.tick
    output_method = options.output_method

    if not output_method_is_valid(output_method):
        print('Unsupported output method: {0}'.format(output_method))
        print('Valid output methods are: {0}'.format(','.join(get_valid_output_methods())))
        sys.exit(1)

    if output_method == OUTPUT_METHOD.curses and not curses_available:
        print('Curses output is selected, but curses are unavailable, falling back to console output')
        output_method == OUTPUT_METHOD.console

    log_stderr = setup_logger(options)
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
            conn = build_connection(
                host, port, config[instance].get('user'), config[instance].get('dbname'))

            if not establish_user_defined_connection(instance, conn, clusters):
                pg_view.models.base.logger.error('failed to acquire details about ' +
                    'the database cluster {0}, the server will be skipped'.format(instance))
    elif options.host:
        # try to connet to the database specified by command-line options
        conn = build_connection(options.host, options.port, options.username, options.dbname)
        instance = options.instance or "default"
        if not establish_user_defined_connection(instance, conn, clusters):
            pg_view.models.base.logger.error("unable to continue with cluster {0}".format(instance))
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
                conndata = detect_db_connection_arguments(result_work_dir, ppid, dbver, options.username, options.dbname)
                if conndata is None:
                    continue
                host = conndata['host']
                port = conndata['port']
                conn = build_connection(host, port, options.username, options.dbname)
                pgcon = psycopg2.connect(**conn)
            except Exception as e:
                pg_view.models.base.logger.error('PostgreSQL exception {0}'.format(e))
                pgcon = None
            if pgcon:
                desc = make_cluster_desc(
                    name=dbname, version=dbver, workdir=result_work_dir,
                    pid=ppid, pgcon=pgcon, conn=conn
                )
                clusters.append(desc)

    collectors = []
    groups = {}
    try:
        if len(clusters) == 0:
            pg_view.models.base.logger.error('No suitable PostgreSQL instances detected, exiting...')
            pg_view.models.base.logger.error('hint: use -v for details, ' +
                         'or specify connection parameters manually in the configuration file (-c)')
            sys.exit(1)

        # initialize the disks stat collector process and create an exchange queue
        q = JoinableQueue(1)
        work_directories = [cluster['wd'] for cluster in clusters if 'wd' in cluster]

        collector = DetachedDiskStatCollector(q, work_directories)
        collector.start()
        consumer = DiskCollectorConsumer(q)

        collectors.append(HostStatCollector())
        collectors.append(SystemStatCollector())
        collectors.append(MemoryStatCollector())

        for cluster in clusters:
            part = PartitionStatCollector(cluster['name'], cluster['ver'], cluster['wd'], consumer)
            pg = PgStatCollector(cluster['pgcon'], cluster['reconnect'], cluster['pid'], cluster['name'], cluster['ver'], options.pid)

            groups[cluster['wd']] = {'pg': pg, 'partitions': part}
            collectors.append(part)
            collectors.append(pg)

        # we don't want to mix diagnostics messages with useful output, so we log the former into a file.
        pg_view.models.base.logger.removeHandler(log_stderr)
        loop(collectors, consumer, groups, output_method)
        pg_view.models.base.logger.addHandler(log_stderr)
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
    if options.log_file:
        LOG_FILE_NAME = options.log_file
        # truncate the former logs
        with open(LOG_FILE_NAME, 'w'):
            pass
        logging.basicConfig(format='%(levelname)s: %(asctime)-15s %(message)s', filename=LOG_FILE_NAME)
    else:
        logging.basicConfig(format='%(levelname)s: %(asctime)-15s %(message)s')
    pg_view.models.base.logger.setLevel((logging.INFO if options.verbose else logging.ERROR))
    log_stderr = logging.StreamHandler()
    pg_view.models.base.logger.addHandler(log_stderr)
    return log_stderr


if __name__ == '__main__':
    main()
