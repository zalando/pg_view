from pg_view import consts


class UnitConverter(object):
    @staticmethod
    def kb_to_mbytes(kb):
        return float(kb) / 1024 if kb is not None else None

    @staticmethod
    def sectors_to_mbytes(sectors):
        return float(sectors) / 2048 if sectors is not None else None

    @staticmethod
    def bytes_to_mbytes(bytes_val):
        return float(bytes_val) / 1048576 if bytes_val is not None else None

    @staticmethod
    def ticks_to_seconds(tick_value_str):
        return float(tick_value_str) / consts.USER_HZ if tick_value_str is not None else None

    @staticmethod
    def time_diff_to_percent(timediff_val):
        return float(timediff_val) * 100 if timediff_val is not None else None
