import os

USER_HZ = os.sysconf(os.sysconf_names['SC_CLK_TCK'])
NCURSES_CUSTOM_OUTPUT_FIELDS = ['header', 'prefix', 'prepend_column_headers']
RD = 1
TICK_LENGTH = 1
SECTOR_SIZE = 512
