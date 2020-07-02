# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

import logging
import multiprocessing
import os
import pickle
import signal
import sys
from enum import Enum
from logging.handlers import RotatingFileHandler, QueueHandler
from typing import Optional


class LogLevel(Enum):
    CRITICAL = logging.CRITICAL
    FATAL = CRITICAL
    ERROR = logging.ERROR
    WARNING = logging.WARNING
    WARN = WARNING
    INFO = logging.INFO
    DEBUG = logging.DEBUG
    NOTSET = logging.NOTSET


class LogEventQueue(object):
    def __init__(self):
        self._rpipe, self._wpipe = os.pipe2(0)
        self._closed = False

    @staticmethod
    def _int_to_2bytes(x: int) -> bytes:
        if x > 4096:
            print("LogEventQueue: _int_to_2bytes(x): x > 4096", sys.stderr)
            exit(2)
        elif x < 256:
            return bytes([0, x])
        else:
            return x.to_bytes((x.bit_length() + 7) // 8, 'big')

    @staticmethod
    def _int_from_2bytes(xbytes: bytes) -> int:
        x = int.from_bytes(xbytes, 'big')
        if x > 4096:
            print("LogEventQueue: _int_from_2bytes(xbytes): x > 4096", sys.stderr)
            exit(2)
        return x

    def put_nowait(self, record):
        # Convert to blocking
        self.put(record)

    def _read(self, n: int) -> Optional[bytes]:
        nread = 0
        curr = bytes([])
        while nread < n:
            nxt = os.read(self._rpipe, n - nread)
            if len(nxt) == 0:  # EOF on pipe means no more log events
                return None
            curr = curr + nxt
            nread += len(nxt)
        return curr

    def get(self):
        """
        Warning: this will not behave as expected if there is ever more than one
        consumer of the queue. Only intended for usage by the listener process.
        """
        if self._closed:
            return None
        s = self._read(2)
        if s is None:
            return None
        s = self._int_from_2bytes(s)
        if s == 0:
            self._closed = True
            return None
        if s > 4094:  # should never happen with corresponding put
            print("LogEventQueue: msg_bytes > 4094", sys.stderr)
            exit(2)
        p = self._read(s)
        if p is None:  # EOF shouldn't happen between size and pickle
            return None
        return pickle.loads(p)

    def put(self, r: logging.LogRecord):
        if self._closed:
            return

        if r is None:
            os.write(self._wpipe, self._int_to_2bytes(0))
            self._closed = True
            return

        # Need to form a pickle that is <= 4094 bytes (explanation below).
        if len(r.msg) > 3500:
            r.msg = r.msg[:3500] + "...LOG TRUNCATED..."
            r.message = r.msg
        p = pickle.dumps(r)
        while len(p) > 4094:
            if len(r.msg) < 200:
                print("LogEventQueue: r.msg < 200 but len(p) > 4094", sys.stderr)
                exit(2)
            r.msg = r.msg[:-100] + "...LOG TRUNCATED..."
            r.message = r.msg
            p = pickle.dumps(r)

        # POSIX.1-2001 requires for write() bytes less than PIPE_BUF (4096 on Linux)
        # with O_NONBLOCK disabled (must be enabled with fcntl() explicitly which
        # we don't) -- a condition which we satisfy here -- all bytes are written
        # atomically. The write() may block if there is not room for all the
        # bytes to be written immediately. We are okay with blocking write() since
        # this only throttles log producers when consumer (log listener process) is
        # 100% occupied relaying logs it get(), which is a lot of logs..
        os.write(self._wpipe, self._int_to_2bytes(len(p)) + p)  # <= 4096

    def log(self, level: LogLevel, msg):
        """
        A basic facility to write into the logs queue directly to be used in subprocesses
        where state sharing or deadlock may be of concern and to rule out potential
        of logger module internals to be cause of deadlock.
        """
        self.put(logging.LogRecord("batch", int(level.value), "", 0, msg, (()), None))

    # Convenience functions for the common LogLevels.
    def debug(self, msg):
        self.log(LogLevel.DEBUG, msg)

    def info(self, msg):
        self.log(LogLevel.INFO, msg)

    def warning(self, msg):
        self.log(LogLevel.WARNING, msg)

    def error(self, msg):
        self.log(LogLevel.ERROR, msg)

    def critical(self, msg):
        self.log(LogLevel.CRITICAL, msg)


def setup_logging(log_folder, console_log_level, file_log_level):
    """
    Setup a logging configuration that can be safely inherited by a children process tree.
    All logging events will be set up to actually go to a multiprocessing.Queue, and we
    create a single listener process that takes care of pulling events from this queue
    and pushing them to the actual desired destinations. The queue and listener process
    returned by this function should be finalized on main (top-level) process termination
    by invoking:
    ```
      log_queue.put(None)
      log_listener.join()
    ```
    :param console_log_level: log level for console
    :param file_log_level: log level for file
    :param log_folder: if set, the place to log files as well (otherwise no log files)
    :return: pair of the log_queue, log_listener which are provided for proper disposal.
    """
    log_queue = LogEventQueue()
    original_sigint_handler = signal.signal(signal.SIGINT, signal.SIG_IGN)
    log_listener = multiprocessing.Process(
        target=__listener_process_loop, args=(log_queue, log_folder, console_log_level, file_log_level))
    log_listener.name = "LoggingListenerProcess"
    log_listener.start()
    signal.signal(signal.SIGINT, original_sigint_handler)

    # All processes that are not the special LoggingListenerProcess will just see
    # the QueueHandler as their logger.
    h = QueueHandler(log_queue)
    root = logging.getLogger()
    root.addHandler(h)
    root.setLevel(logging.DEBUG)

    return log_queue, log_listener


# Now define a process routine that is only executed by a dedicated process
# which we call the "LoggingListenerProcess" that reads log events from a queue
# and then logs them to the destinations/sinks desired. Because the queue is
# a multiprocessing.Queue, this helps us unify logging with multiple cooperating
# processes only needing to push log events onto the queue.

def __listener_process_loop(queue: LogEventQueue, log_folder, console_log_level, file_log_level):
    __listener_process_configure_logging(log_folder, console_log_level, file_log_level)
    while True:
        record = queue.get()
        if record is None:
            return

        inner_logger = logging.getLogger(record.name)
        inner_logger.handle(record)


def __listener_process_configure_logging(log_folder, console_log_level, file_log_level):
    logger = logging.getLogger("batch")
    logger.setLevel(logging.DEBUG)

    # Create console handler with a higher log level
    console_log = logging.StreamHandler()
    console_log.setLevel(console_log_level)

    # Create formatter and add it to the handler
    formatter = logging.Formatter(
        '%(asctime)s.%(msecs)03d:%(levelname)s:%(processName)s:%(message)s', '%Y-%m-%d %H:%M:%S')
    console_log.setFormatter(formatter)

    # Add the handler to the logger
    logger.addHandler(console_log)

    # Add file based logging as well, if enabled
    if log_folder is not None:
        if not os.path.isdir(log_folder):
            try:
                os.makedirs(log_folder, exist_ok=True)
                logger.info("Log folder {0} created successfully".format(log_folder))
            except OSError as error:
                logger.error("Log folder {0} can not be created: {1}".format(log_folder, error))
                exit(1)

        # Create file handler which logs even debug messages
        file_log = RotatingFileHandler(
            os.path.join(log_folder, "run.log"),
            mode="a",
            maxBytes=50*1024*1024,
            backupCount=20,
            delay=False
        )
        file_log.setLevel(file_log_level)
        file_log.setFormatter(formatter)
        logger.addHandler(file_log)
