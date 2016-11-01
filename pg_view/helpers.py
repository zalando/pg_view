from pg_view import consts

BYTES_IN_MB = 1048576
SECTORS_IN_MB = 2048
KB_IN_MB = 1024


class UnitConverter(object):
    @staticmethod
    def kb_to_mbytes(kb):
        return float(kb) / KB_IN_MB if kb is not None else None

    @staticmethod
    def sectors_to_mbytes(sectors):
        return float(sectors) / SECTORS_IN_MB if sectors is not None else None

    @staticmethod
    def bytes_to_mbytes(bytes_val):
        return float(bytes_val) / BYTES_IN_MB if bytes_val is not None else None

    @staticmethod
    def ticks_to_seconds(tick_value_str):
        return float(tick_value_str) / consts.USER_HZ if tick_value_str is not None else None

    @staticmethod
    def time_diff_to_percent(timediff_val):
        return float(timediff_val) * 100 if timediff_val is not None else None


def open_universal(file_path):
    return open(file_path, 'rU')


def read_file(file_path):
    with open_universal(file_path) as f:
        return f.read()


def process_groups(groups):
    for name in groups:
        part = groups[name]['partitions']
        pg = groups[name]['pg']
        part.ncurses_set_prefix(pg.ncurses_produce_prefix())
