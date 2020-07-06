# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.
import multiprocessing
from typing import Callable

import pyinotify
import logging
import os
from .audio import is_valid_audio_file
from .logger import LogEventQueue

logger = logging.getLogger("batch")


class ModifyHandler(pyinotify.ProcessEvent):
    """
    Handler for inotify event for modified files
    """
    def my_init(self, callback, leq: LogEventQueue):
        """
        :param callback: callback when the file under watch is modified
        :param leq: instance of LogEventQueue to be used for logging
        :return: None
        """
        self._callback = callback
        self._leq = leq

    def process_IN_CLOSE_WRITE(self, event):
        """
        Handler function for file modification
        :param event: event to be processed
        :return:
        """
        self._leq.info("Configuration file {0} changed, invoking callback!".format(event.pathname))
        self._callback()


class DirectoryWatchHandler(pyinotify.ProcessEvent):
    """
    Handler for inotify event for modified/added files in a work directory, such as files
    corresponding to work items (audio file, image file, text file, etc..).
    """
    def my_init(self, queue: multiprocessing.Queue, leq: LogEventQueue, predicate: Callable[[str], bool]):
        """
        Keep a handle on the process pool at init time so we can terminate it later if needed
        :param queue: queue onto which new files will be placed if they qualify as work items.
        :param leq: instance of LogEventQueue to be used for logging
        :param predicate: function that qualifies new files as being candidate work items.
        :return: None
        """
        self._queue: multiprocessing.Queue = queue
        self._leq: LogEventQueue = leq
        self._predicate: Callable[[str], bool] = predicate

    def process_IN_CLOSE_WRITE(self, event):
        """
        Handler function for file modification
        :param event: event to be processed
        :return:
        """
        audio_file = event.pathname
        if self._predicate(audio_file):
            self._leq.info("File {0} changed/added, adding it to processing list!".format(audio_file))
            self._queue.put(audio_file)
        else:
            self._leq.warning("{0} is not a regular file or is not a supported work item".format(audio_file))


def notify_file_modified(filename, callback, leq):
    """
    Creates a "modify" file watch on a file, and then executes the callback.
    :param filename: file to add a watch for
    :param callback: invoked when file is modified.
    :param leq: instance of LogEventQueue to be used for logging
    :return: notifier object
    """
    wm = pyinotify.WatchManager()
    notifier = pyinotify.ThreadedNotifier(
        wm,
        ModifyHandler(callback=callback, leq=leq)
    )
    notifier.daemon = True
    notifier.name = "FileModifiedNotifierThread"
    notifier.start()

    full_filename = os.path.abspath(filename)
    assert os.path.isfile(full_filename)

    logger.info("Starting a watch on file: {0}".format(full_filename))
    wm.add_watch(full_filename, pyinotify.IN_CLOSE_WRITE)

    return notifier


def update_work_on_directory_content_change(directory, queue, leq, predicate):
    """
    Creates a "modify" directory watch on an input folder, which dynamically adds work
    :param directory: directory to add a watch for
    :type directory: str
    :param queue: list for audio files dynamically added (out parameter)
    :type queue: multiprocessing.Queue
    :param leq: instance of LogEventQueue to be used for logging
    :type leq: LogEventQueue
    :param predicate: function that qualifies new files as being candidate work items.
    :type predicate: Callable[[str], bool]
    :return: notifier object
    :rtype: pyinotify.ThreadedNotifier
    """
    # NOTE: this only works is /proc/sys/fs/inotify/max_user_watches is sufficiently large. If not
    # you may need to modify this setting on the host, since it cannot be modified inside Docker.
    # https://github.com/guard/listen/wiki/Increasing-the-amount-of-inotify-watchers
    # echo fs.inotify.max_user_watches=524288 | sudo tee -a /etc/sysctl.conf && sudo sysctl -p
    wm = pyinotify.WatchManager()
    notifier = pyinotify.ThreadedNotifier(
        wm,
        DirectoryWatchHandler(queue=queue, leq=leq, predicate=predicate)
    )
    notifier.daemon = True
    notifier.name = "DirectoryContentChangeNotifierThread"
    notifier.start()

    full_directory = os.path.abspath(directory)
    assert os.path.isdir(full_directory)

    logger.info("Starting a watch on directory: {0}".format(full_directory))
    wm.add_watch(full_directory, pyinotify.IN_CLOSE_WRITE)

    return notifier
