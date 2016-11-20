from StringIO import StringIO

import os

TEST_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__)))


class ContextualStringIO(StringIO):
    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()
        return False
