from smart_open import smart_open

from singer_runner.metrics.base import BaseMetricsStorage

class FileMetricsStorage(BaseMetricsStorage):
    def __init__(self, filepath, *args, **kwargs):
        self.filepath = filepath
        self.file = smart_open(filepath, 'w', **kwargs)

        super(FileMetricsStorage, self).__init__(*args, **kwargs)

    def close(self):
        super(FileMetricsStorage, self).close()
        self.file.close()

    def put(self, raw_singer_metric):
        self.file.write(raw_singer_metric + '\n')
