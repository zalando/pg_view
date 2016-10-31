#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import absolute_import

import glob
import logging
import platform
import resource
import socket
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

if sys.hexversion >= 0x03000000:
    import configparser as ConfigParser
else:
    import ConfigParser

from pg_view.models.base import StatCollector, COLSTATUS, COLALIGN, COLTYPES, COLHEADER, OUTPUT_METHOD, enum, \
    TICK_LENGTH
import pg_view.models.base
from pg_view.models.host_stat import HostStatCollector
from pg_view.models.memory_stat import MemoryStatCollector
from pg_view.models.partition_stat import PartitionStatCollector, DetachedDiskStatCollector
from pg_view.models.pg_stat import PgStatCollector, dbversion_as_float
from pg_view.models.system_stat import SystemStatCollector
from pg_view.validators import get_valid_output_methods, output_method_is_valid
from pg_view.consumers import DiskCollectorConsumer

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


STAT_FIELD = enum(st_pid=0, st_process_name=1, st_state=2, st_ppid=3, st_start_time=21)
MEM_PAGE_SIZE = resource.getpagesize()

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
        for argname in ('port', 'host', 'user', 'dbname'):
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

    if isinstance(st, PgStatCollector):
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
    """
    >>> get_dbname_from_path('foo')
    'foo'
    >>> get_dbname_from_path('/pgsql_bar/9.4/data')
    'bar'
    """
    m = re.search(r'/pgsql_(.*?)(/\d+.\d+)?/data/?', db_path)
    if m:
        dbname = m.group(1)
    else:
        dbname = db_path
    return dbname


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


def establish_user_defined_connection(instance, conn, clusters):
    """ connect the database and get all necessary options like pid and work_directory
        we use port, host and socket_directory, prefering socket over TCP connections
    """
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
    desc = make_cluster_desc(
        name=instance, version=dbver, workdir=work_directory, pid=pid, pgcon=pgcon, conn=conn)
    clusters.append(desc)
    return True


def make_cluster_desc(name, version, workdir, pid, pgcon, conn):
    """Create cluster descriptor, complete with the reconnect function."""

    def reconnect():
        pgcon = psycopg2.connect(**conn)
        pid = read_postmaster_pid(workdir, name)
        return pgcon, pid

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
        for fname in (self.NET_UNIX_FILENAME, self.NET_TCP_FILENAME):
            self.read_socket_file(fname)
        if os.access(self.NET_TCP6_FILENAME, os.R_OK):
            self.read_socket_file(self.NET_TCP6_FILENAME)

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
                conndata = detect_db_connection_arguments(result_work_dir, ppid, dbver)
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
                desc = make_cluster_desc(name=dbname, version=dbver, workdir=result_work_dir,
                                         pid=ppid, pgcon=pgcon, conn=conn)
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
