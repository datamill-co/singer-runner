from singer_runner.utils import create_queue

from singer_runner.pipes.base import BasePipe

class MemoryPipe(BasePipe):
    def __init__(self, *args, maxsize=0, **kwargs):
        self.queue = create_queue(maxsize)
        super(MemoryPipe, self).__init__(*args, **kwargs)

    def put(self, raw_singer_message):
        self.queue.put(raw_singer_message)

    def get(self):
        return self.queue.get()
        ## TODO: task_done ??
