
class BaseStateStorage:
    def __init__(self, initial_state=None):
        if not initial_state:
            self.state = initial_state

    def update_state(self, state):
        ## TODO: check if state actually changed
        ## TODO: log if state changed?
        ## https://github.com/singer-io/singer-python/blob/master/singer/statediff.py
        self.dump(state)

    def dump(self, state):
        raise NotImplementedError()

    def load(self):
        raise NotImplementedError()

    def close(self):
        pass
