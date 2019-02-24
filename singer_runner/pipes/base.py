
class BasePipe:
    def put(self, raw_singer_message):
        raise NotImplementedError()

    def get(self):
        raise NotImplementedError()

    def close(self):
        ## TODO: stream_queue EOF?
        pass

    def flush(self):
        pass
