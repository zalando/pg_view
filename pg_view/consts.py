import os

USER_HZ = os.sysconf(os.sysconf_names['SC_CLK_TCK'])
NCURSES_CUSTOM_OUTPUT_FIELDS = ['header', 'prefix', 'prepend_column_headers']
TICK_LENGTH = 1
RD = 1
SECTOR_SIZE = 512
