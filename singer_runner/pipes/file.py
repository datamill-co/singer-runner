from smart_open import smart_open

from singer_runner.pipes.base import BasePipe

class FilePipe(BasePipe):
    def __init__(self, filepath, mode, *args, **kwargs):
        self.filepath = filepath
        self.mode = mode
        self.file = smart_open(filepath, mode, **kwargs)

        super(FilePipe, self).__init__(*args, **kwargs)

    def close(self):
        super(FilePipe, self).close()
        self.file.close()

    def put(self, raw_singer_message):
        self.file.write(raw_singer_message + b'\n')

    def get(self):
        return self.file.readline()
