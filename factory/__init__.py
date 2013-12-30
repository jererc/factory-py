import os
import sys
import time
import traceback
import signal
from multiprocessing import Process, Queue
from datetime import datetime, timedelta
import logging

from pymongo import Connection


DB_NAME = 'factory'
COL_NAME = 'workers'
MANAGER_DELAY = 10  # seconds
WORKER_TIMEOUT = 3600 * 24     # seconds

logger = logging.getLogger(__name__)
db_con = Connection()


class QueueHandler(logging.Handler):
    '''Logging handler which sends events to a multiprocessing queue.
    '''
    def __init__(self, queue):
        logging.Handler.__init__(self)
        self.queue = queue

    def _format_record(self, record):
        record.message = record.msg % record.args if record.args else record.msg
        if record.exc_info:
            dummy = self.format(record)
            record.exc_info = None
        return record

    def emit(self, record):
        try:
            record = self._format_record(record)
            self.queue.put_nowait(record)
        except (KeyboardInterrupt, SystemExit):
            raise
        except:
            self.handleError(record)


class Factory(object):

    def __init__(self, db_name=DB_NAME, collection=COL_NAME):
        self.col = db_con[db_name][collection]
        self.processes = {}
        self.queue = Queue(-1)
        self.logging_handlers = None
        self.manager_delay = MANAGER_DELAY

    def add(self, **worker):
        '''Add a new worker.

        :param target: callable name with module (e.g.: 'math.ceil')
        :param args: callable arguments tuple
        :param kwargs: callable argument dict of keywords arguments
        :param daemon: whether the worker runs forever or not
        :param timeout: timeout in seconds

        :return: worker id
        '''
        res = self.col.find_one(worker)
        if res:
            return res['_id']
        worker['created'] = datetime.utcnow()
        return self.col.insert(worker, safe=True)

    def get(self, **spec):
        return self.col.find_one(spec)

    def remove(self, **spec):
        self.col.remove(spec, safe=True)

    def _set_logging(self):
        '''Set the logging queue handler.
        '''
        root = logging.getLogger()
        for handler in root.handlers:
            root.removeHandler(handler)
        root.addHandler(QueueHandler(self.queue))

    def _listener_process(self):
        '''Handle logging queue records.
        '''
        root = logging.getLogger()

        if self.logging_handlers:
            if not isinstance(self.logging_handlers, (tuple, list)):
                self.logging_handlers = [self.logging_handlers]
            for handler in self.logging_handlers:
                root.addHandler(handler)

        while True:
            try:
                record = self.queue.get()
                if record is None:  # we send this as a sentinel to tell the listener to quit
                    break
                logger = logging.getLogger(record.name)
                logger.handle(record)
            except (KeyboardInterrupt, SystemExit):
                raise
            except:
                traceback.print_exc(file=sys.stderr)

    def _start_listener(self):
        process = Process(target=self._listener_process, name='listener')
        process.start()

    def _get_callable(self, target):
        try:
            module_, func_ = target.rsplit('.', 1)
            module_name = module_.rsplit('.', 1)[-1]
            module = __import__(module_, globals(), locals(), [module_name], -1)
            return getattr(module, func_)
        except ValueError:
            logger.error('no module specified in target "%s"', target)
        except ImportError, e:
            logger.error('failed to import module "%s": %s', module_, str(e))
        except AttributeError:
            logger.error('failed to import "%s" from module "%s"', func_, module_)

    def _worker_process(self, worker):
        self._set_logging()

        callable = self._get_callable(worker['target'])
        if callable:
            args = worker.get('args') or ()
            kwargs = worker.get('kwargs') or {}
            try:
                callable(*args, **kwargs)
            except Exception:
                logger.exception('exception in %s', worker['target'])

    def _start_worker(self, worker):
        proc = Process(target=self._worker_process,
                args=(worker,),
                name=worker.get('name') or worker['target'])
        proc.start()
        self.processes[worker['_id']] = proc

    def _stop_worker(self, worker_id):
        proc = self.processes.pop(worker_id, None)
        if proc:
            try:
                os.setpgid(proc.pid, proc.pid)
            except OSError:
                return
            proc.terminate()

    def _validate_worker(self, worker):
        if worker.get('daemon'):
            return True
        delta = timedelta(seconds=worker.get('timeout', WORKER_TIMEOUT))
        if worker['started'] > datetime.utcnow() - delta:
            return True
        logger.error('worker %s timed out after %s', worker, delta)

    def _sigint_terminate(self, signum, frame):
        os.killpg(os.getpgrp(), 9)

    def run(self):
        '''Start and manage workers.
        '''
        os.setpgrp()

        signal.signal(signal.SIGINT, self._sigint_terminate)
        signal.signal(signal.SIGTERM, self._sigint_terminate)
        signal.signal(signal.SIGTTOU, signal.SIG_IGN)
        # signal.signal(signal.SIGCHLD, signal.SIG_IGN)

        self._start_listener()
        self._set_logging()

        while True:

            for worker in self.col.find():
                if worker['_id'] not in self.processes:
                    if worker.get('daemon') and worker.get('started'):
                        logger.error('daemon worker %s died', worker)
                    self.col.update({'_id': worker['_id']},
                            {'$set': {'started': datetime.utcnow()}},
                            safe=True)
                    self._start_worker(worker)

                elif not self._validate_worker(worker):
                    self._stop_worker(worker['_id'])

            # Reap dead children
            for worker_id, proc in self.processes.items():
                try:
                    os.waitpid(proc.pid, os.WNOHANG)
                except OSError:
                    del self.processes[worker_id]
                    continue

                if not self.col.find_one({'_id': worker_id}):
                    self._stop_worker(worker_id)

            self.col.remove({
                    '_id': {'$nin': self.processes.keys()},
                    'started': {'$exists': True},
                    'daemon': {'$exists': False},
                    }, safe=True)

            time.sleep(self.manager_delay)
