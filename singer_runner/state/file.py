import json

from smart_open import smart_open

from singer_runner.state.base import BaseStateStorage

class FileStateStorage(BaseStateStorage):
    def __init__(self, filepath, *args, **kwargs):
        self.filepath = filepath
        super(FileStateStorage, self).__init__(*args, **kwargs)

    def dump(self, state):
        with smart_open(self.filepath, 'w') as file:
            json.dump(state, file)
        self.state = state

    def load(self):
        try:
            with smart_open(self.filepath, 'r') as file:
                self.state = json.load(file)
        except ValueError:
            self.state = None
        return self.state
