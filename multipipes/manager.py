import os
import sys
import time
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
    def __init__(self):
        self.events_queue = mp.Queue()
        self.events_thread = threading.Thread(target=self.handle_events,
                                              daemon=True)
        self.running = True
        self.events_thread.start()
        self.workers = {}

        self.mapping = {
            'max_requests': self.handle_max_requests,
            'exit': self.handle_exit,
        }

    def register_worker(self, pid, worker):
        self.workers[pid] = worker

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
        worker = self.workers[event['pid']]
        worker.restart()

    def handle_exit(self, event):
        self.running = False


class Manager2:

    def __init__(self, pipeline, events_queue,
                 *, restart_on_error=False,
                 restart_on_kill=False,
                 restart_on_max_requests=True):

        self.events_queue = events_queue
        self.errors = []
        self.pipeline = pipeline
        self.restart_on_error = restart_on_error
        self.restart_on_kill = restart_on_kill
        self.restart_on_max_requests = restart_on_max_requests

        threading.Thread(target=self.handle_events, daemon=True).start()
        # threading.Thread(target=self.check_is_alive, daemon=True).start()

        self.mapping = {
            'exception': self.handle_exception,
            'max_requests': self.handle_max_requests,
            'missing_pid': self.handle_missing_pid
        }

    def handle_events(self):
        while True:
            event = self.events_queue.get()
            func = self.mapping[event['type']]
            func(event['context'])

    def handle_exception(self, exc):
        self.errors.append(exc)

        try:
            raise exc
        except:
            pass
            # logger.exception('Got exception from child proc')

        if DEBUG:
            global LAST_ERROR
            LAST_ERROR = exc
            os.kill(os.getpid(), signal.SIGUSR1)

        if self.restart_on_error:
            logger.info('Restarting pipeline')
            self.pipeline.restart()
            time.sleep(1)

    def handle_max_requests(self, pid):
        # XXX: Awful
        def _find():
            for node in self.pipeline.nodes:
                for i, process in enumerate(node.processes):
                    if process.pid == pid:
                        return node, i

        node, i = _find()
        del node.processes[i]
        node.start_one()

    def handle_missing_pid(self, context):
        pass

    def check_is_alive(self):
        # XXX: there might be a race condition with
        #      `handle_error`
        while True:
            if not self.pipeline.is_alive():
                self.pipeline.restart(hard=True)
            time.sleep(1)

