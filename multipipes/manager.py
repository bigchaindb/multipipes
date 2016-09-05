import os
import sys
import signal
import logging
import threading
import traceback
import multiprocessing as mp


logger = logging.getLogger(__name__)

DEBUG = bool(int(os.environ.get('PYTHONMULTIPIPESDEBUG', 0)))
LAST_ERROR = None


def exception_handler(signum, frame):
    try:
        raise LAST_ERROR
    except:
        print(traceback.format_exc())
    sys.exit(1)

signal.signal(signal.SIGUSR1, exception_handler)


class Manager:
    def __init__(self, events_queue=None):
        self.events_queue = events_queue if events_queue else mp.Queue()
        self.events_thread = threading.Thread(target=self.handle_events,
                                              daemon=True)
        self.running = True
        self.events_thread.start()
        self.workers = {}

        self.mapping = {
            'max_requests': self.handle_max_requests,
            'exit': self.handle_exit,
        }

    def register_worker(self, worker):
        self.workers[worker.uuid] = worker
        worker.events_queue = self.events_queue

    def stop(self):
        self.events_queue.put({'type': 'exit'})

    def send_event(self, event):
        self.events_queue.put(event)

    def handle_events(self):
        while self.running:
            event = self.events_queue.get()
            func = self.mapping[event['type']]
            func(event)

    def handle_max_requests(self, event):
        worker = self.workers[event['uuid']]
        worker.start()

    def handle_exit(self, event):
        self.running = False
