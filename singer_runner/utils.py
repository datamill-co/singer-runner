from dataclasses import dataclass, field
from typing import Any
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

@dataclass(order=True)
class PrioritizedItem:
    priority: int
    item: Any=field(compare=False)

def create_queue(maxsize=0):
    return PriorityQueue(maxsize)

def queue_put(queue, item, priority=2, **kwargs):
    queue.put(PrioritizedItem(priority, item=item), **kwargs)

def queue_get(queue, **kwargs):
    prioritized_item = queue.get(**kwargs)
    return prioritized_item.item

def terminate_queue(queue, **kwargs):
    queue_put(queue, EOF, priority=1, **kwargs)
