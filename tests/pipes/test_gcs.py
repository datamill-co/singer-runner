import os
import uuid
import json
import random
import string

from singer_runner.pipes import GCSPipe
from singer_runner.utils import EOF

TEST_BUCKET = os.getenv('GCS_TEST_BUCKET')

def test_basic_gcs():
    test_path = 'gcs://{}/test-{}'.format(TEST_BUCKET, uuid.uuid4())

    with GCSPipe(test_path, 'w') as pipe:
        for i in range(0, 10):
            message = json.dumps({
                'type': 'RECORD',
                'value': {
                    'n': i
                }
            })
            pipe.put(message.encode('utf-8'))

    with GCSPipe(test_path, 'r') as pipe:
        i = 0
        raw_message = pipe.get()
        while raw_message != EOF:
            message = json.loads(raw_message.decode('utf-8'))
            assert message['value']['n'] == i
            i += 1
            raw_message = pipe.get()

def random_string(size):
    return ''.join([random.choice(string.ascii_letters + string.digits) for n in range(size)])

def test_large_file_gcs():
    test_path = 'gcs://{}/test-large-{}'.format(TEST_BUCKET, uuid.uuid4())

    with GCSPipe(test_path, 'w', chunk_size=1048576) as pipe:
        for i in range(0, 10000):
            message = json.dumps({
                'type': 'RECORD',
                'value': {
                    'n': i,
                    'data': random_string(2000)
                }
            })
            pipe.put(message.encode('utf-8'))

    with GCSPipe(test_path, 'r', chunk_size=1048576) as pipe:
        i = 0
        raw_message = pipe.get()
        while raw_message != EOF:
            message = json.loads(raw_message.decode('utf-8'))
            assert message['value']['n'] == i
            assert len(message['value']['data']) == 2000
            i += 1
            raw_message = pipe.get()

        assert i == 10000
