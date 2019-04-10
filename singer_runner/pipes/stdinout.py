import sys

from singer_runner.pipes.base import BasePipe

class StdInOutPipe(BasePipe):
    def put(self, raw_singer_message):
        ## TODO: thread safety?
        sys.stdout.write(raw_singer_message.decode('utf-8'))

    def get(self):
        return sys.stdin.readline().encode('utf-8')
