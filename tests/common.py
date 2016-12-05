try:
    from io import StringIO
except ImportError:
    from StringIO import StringIO

import os

TEST_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__)))


class ContextualStringIO(StringIO):
    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()
        return False


class ErrorAfter(object):
    def __init__(self, limit):
        self.limit = limit
        self.calls = 0

    def __call__(self, *args):
        self.calls += 1
        if self.calls > self.limit:
            raise CallableExhaustedError
        return args


class CallableExhaustedError(Exception):
    pass
