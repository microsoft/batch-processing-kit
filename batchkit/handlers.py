# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

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


class AudioWatchHandler(pyinotify.ProcessEvent):
    """
    Handler for inotify event for modified/added audio files
    """
    def my_init(self, audio_queue, leq: LogEventQueue):
        """
        Keep a handle on the process pool at init time so we can terminate it later if needed
        :param audio_queue: queue onto which new audio files will be placed
        :param leq: instance of LogEventQueue to be used for logging
        :return: None
        """
        self._audio_queue = audio_queue
        self._leq = leq

    def process_IN_CLOSE_WRITE(self, event):
        """
        Handler function for file modification
        :param event: event to be processed
        :return:
        """
        audio_file = event.pathname
        if is_valid_audio_file(audio_file):
            self._leq.info("Audio file {0} changed/added, adding it to processing list!".format(audio_file))
            self._audio_queue.put(audio_file)
        else:
            self._leq.warning("{0} is not a file or does not have a supported extension".format(audio_file))


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
    logger.info("Starting a watch on config file {0}".format(full_filename))
    wm.add_watch(full_filename, pyinotify.IN_CLOSE_WRITE)

    return notifier


def update_work_on_audio_change(directory, audio_queue, leq):
    """
    Creates a "modify" directory watch on an input folder, which dynamically adds work
    :param directory: directory to add a watch for
    :param audio_queue: list for audio files dynamically added (out parameter)
    :param leq: instance of LogEventQueue to be used for logging
    :return: notifier object
    """
    # NOTE: this only works is /proc/sys/fs/inotify/max_user_watches is sufficiently large. If not
    # you may need to modify this setting on the host, since it cannot be modified inside Docker.
    # https://github.com/guard/listen/wiki/Increasing-the-amount-of-inotify-watchers
    # echo fs.inotify.max_user_watches=524288 | sudo tee -a /etc/sysctl.conf && sudo sysctl -p
    wm = pyinotify.WatchManager()
    notifier = pyinotify.ThreadedNotifier(
        wm,
        AudioWatchHandler(audio_queue=audio_queue, leq=leq)
    )
    notifier.daemon = True
    notifier.name = "AudioChangeNotifierThread"
    notifier.start()

    full_directory = os.path.abspath(directory)
    assert os.path.isdir(full_directory)

    logger.info("Starting a watch on config file {0}".format(full_directory))
    wm.add_watch(full_directory, pyinotify.IN_CLOSE_WRITE)

    return notifier
