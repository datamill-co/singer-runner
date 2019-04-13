import os
import sys
import json
from datetime import datetime
from subprocess import Popen, PIPE

from singer_runner.utils import EOF, EMPTYLINE, TAP, TARGET, run_thread

ON_POSIX = 'posix' in sys.builtin_module_names

SINGER_METRICS_PREFIX = 'INFO METRIC: '
SINGER_METRICS_PREFIX_LN = len(SINGER_METRICS_PREFIX)

def log_helper(stream, logger, metrics_storage):
    for line in iter(stream.readline, b''):
        log_line = line.decode('utf-8')[:-1]

        # detect and process metrics
        if metrics_storage and \
           log_line[:SINGER_METRICS_PREFIX_LN] == SINGER_METRICS_PREFIX:
            metrics_storage.put(log_line[SINGER_METRICS_PREFIX_LN:])

        logger.info(log_line)

def singer_input_helper(stdin, pipe):
    while True:
        raw_singer_message = pipe.get()
        if raw_singer_message in [EOF, EMPTYLINE]:
            stdin.close()
            break
        else:
            stdin.write(raw_singer_message)

def singer_output_helper(stdout, pipe, state_storage):
    for raw_singer_message in iter(stdout.readline, b''):
        raw_singer_message = raw_singer_message[:-1]
        singer_message = json.loads(raw_singer_message)
        if singer_message['type'] == 'STATE' and state_storage:
            state_storage.update_state(singer_message['value'])
        else:
            pipe.put(raw_singer_message)
    pipe.put(EOF) ## TODO: needed?

class SingerProcess:
    def __init__(self,
                 process_logger,
                 command,
                 singer_process_type=TAP,
                 pipe=None,
                 metrics_storage=None,
                 state_storage=None):
        self.started_at = datetime.utcnow()
        self.singer_process_type = singer_process_type
        self.pipe = pipe
        self.metrics_storage = metrics_storage
        self.state_storage = state_storage
        self.threads = []

        process_logger.info(command)

        self.__process_handle = Popen(
            command,
            stdin=PIPE if singer_process_type == TARGET else None,
            stdout=PIPE,
            stderr=PIPE,
            preexec_fn=os.setsid,
            bufsize=1,
            close_fds=ON_POSIX)

        self.logging_thread = run_thread(log_helper,
                                         (self.__process_handle.stderr,
                                          process_logger,
                                          metrics_storage))
        self.threads.append(self.logging_thread)

        if self.singer_process_type == TARGET:
            self.singer_stream_thread = run_thread(singer_input_helper,
                                                   (self.__process_handle.stdin,
                                                    self.pipe))
        else:
            self.singer_stream_thread = run_thread(singer_output_helper,
                                                   (self.__process_handle.stdout,
                                                    self.pipe,
                                                    self.state_storage))
        self.threads.append(self.singer_stream_thread)

    @property
    def returncode(self):
        return self.__process_handle.returncode

    def kill(self):
        self.__process_handle.kill()
        ## TODO: threads?

    def wait(self, process_timeout=None, stream_timeout=5):
        self.__process_handle.wait(timeout=process_timeout)

        for thread in self.threads:
            thread.join(timeout=stream_timeout)
