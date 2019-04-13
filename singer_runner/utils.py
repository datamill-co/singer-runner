from queue import PriorityQueue
from threading import Thread

EOF = b''
EMPTYLINE = b'\n'
TAP = 'tap'
TARGET = 'target'

def run_thread(fn, args):
    thread = Thread(target=fn, args=args)
    thread.daemon = True
    thread.start()
    return thread

def create_queue(maxsize=0):
    return PriorityQueue(maxsize)

