
class BasePipe:
    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        self.close()

    def put(self, raw_singer_message):
        raise NotImplementedError()

    def get(self):
        raise NotImplementedError()

    def close(self):
        pass

    def flush(self):
        pass
