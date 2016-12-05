import sys

if sys.hexversion >= 0x03000000:
    from queue import Empty
else:
    from Queue import Empty


class DiskCollectorConsumer(object):
    """ consumes information from the disk collector and provides it for the local
        collector classes running in the same subprocess.
    """

    def __init__(self, q):
        self.result = {}
        self.cached_result = {}
        self.q = q

    def consume(self):
        # if we haven't consumed the previous value
        if len(self.result) != 0:
            return
        try:
            self.result = self.q.get_nowait()
            self.cached_result = self.result.copy()
        except Empty:
            # we are too fast, just do nothing.
            pass
        else:
            self.q.task_done()

    def fetch(self, work_directory):
        data = None
        if work_directory in self.result:
            data = self.result[work_directory]
            del self.result[work_directory]
        elif work_directory in self.cached_result:
            data = self.cached_result[work_directory]
        return data
