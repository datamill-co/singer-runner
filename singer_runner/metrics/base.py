
class BaseMetricsStorage:
    def put(self, raw_singer_metric):
        raise NotImplementedError()

    def close(self):
        pass

    def flush(self):
        pass
