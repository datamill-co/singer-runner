import re
from collections import deque

from singer_runner.pipes.base import BasePipe
from singer_runner.utils import EOF, EMPTYLINE

class GCSPipe(BasePipe):
    OBJECT_REGEX = r'gcs://([^/]+)/(.+)'
    RANGE_REGEX = r'bytes=([0-9]+)\-([0-9]+)'
    DEFAULT_CHUNK_SIZE = 256 * 1024 # 256 KB

    def __init__(self, filepath, mode, *args, chunk_size=None, **kwargs):
        # load internally so these can be extras_require
        from google.cloud import storage
        import requests

        self._storage_client = storage.Client()

        self._filepath = filepath
        self._mode = mode

        if chunk_size:
            if chunk_size % self.DEFAULT_CHUNK_SIZE != 0:
                raise Exception('`chunk_size` must a multiple of 256 KB (256 x 1024 bytes)')
            self._chunk_size = chunk_size
        else:
            self._chunk_size = self.DEFAULT_CHUNK_SIZE

        match = re.match(self.OBJECT_REGEX, filepath)

        if not match:
            raise Exception('Invalid Google Cloud Storage path: {}'.format(filepath))

        bucket_name, blob_name = match.groups()

        self._blob = self._storage_client.bucket(bucket_name).blob(blob_name)
        self._bytes_transmitted = 0
        self._buffer = b''

        if self._mode == 'wb' or self._mode == 'w':
            self._upload_url = self._blob.create_resumable_upload_session()
            self._request_session = requests.Session()
        elif self._mode == 'rb' or self._mode == 'r':
            self._lines = deque()
        else:
            raise Exception('GCSPipe does not support `{}` mode'.format(mode))

        super(GCSPipe, self).__init__(*args, **kwargs)

    def flush(self, force=False):
        while (force and len(self._buffer) > 0) or \
              len(self._buffer) >= self._chunk_size:
            force = False
            chunk = self._buffer[:self._chunk_size]

            start_range = self._bytes_transmitted
            end_range = (start_range + len(chunk)) - 1

            if len(chunk) < self._chunk_size:
                filesize = start_range + len(chunk)
            else:
                filesize = '*'

            response = self._request_session.put(
                self._upload_url,
                data=chunk,
                headers={
                    'Content-Type': 'application/octet-stream',
                    'Content-Range': 'bytes {}-{}/{}'.format(
                        start_range,
                        end_range,
                        filesize)
                })

            if response.status_code >= 400:
                raise Exception('Error uploading chunk to GCS ({}): {}'.format(
                    response.status_code,
                    response.text))

            if response.status_code == 308:
                match = re.match(self.RANGE_REGEX, response.headers['Range'])
                start, end = map(int, match.groups())
                if start != 0 or end != end_range:
                    raise Exception('GCS did not accept chunk: {}-{}'.format(
                        start_range,
                        end_range))
            elif response.status_code in [200, 201]:
                expected_bytes = self._bytes_transmitted + len(chunk)
                data = response.json()
                if int(data['size']) != expected_bytes:
                    raise Exception(
                        'GCS total bytes does not match expected - GCS: {}, local: {}'.format(
                            data['size'],
                            expected_bytes))

            self._bytes_transmitted += self._chunk_size
            self._buffer = self._buffer[self._chunk_size:]

    def close(self):
        super(GCSPipe, self).close()
        self.flush(force=True)

    def put(self, raw_singer_message):
        raw_singer_message += b'\n'

        self._buffer += raw_singer_message

        if len(self._buffer) >= self._chunk_size:
            self.flush()

    def _read_into_line_buffer(self):
        chunk = self._blob.download_as_string(
            start=self._bytes_transmitted,
            end=(self._bytes_transmitted + self._chunk_size) - 1)

        lines = (self._buffer + chunk).split(EMPTYLINE)
        if lines[-1] != EOF:
            self._buffer = lines[-1]
            lines = lines[:-1]
        else:
            self._buffer = b''

        self._lines += lines
        self._bytes_transmitted += len(chunk)

    def get(self):
        if not self._lines:
            self._read_into_line_buffer()

        if self._lines:
            return self._lines.popleft()
