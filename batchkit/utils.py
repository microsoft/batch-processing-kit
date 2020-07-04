# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

import os
import hashlib
import shutil
import signal
import logging
import json
import sys
import tempfile
import multiprocessing
import threading
import traceback
import time
from queue import Empty

import psutil
import requests
import resource
from multiprocessing import pool
from typing import Any, Optional, Set, List, Callable, Iterable
from random import random
from multiprocessing.connection import Connection

from batchkit.audio import is_valid_audio_file

logger = logging.getLogger("batch")


class NoFilesSelectedError(Exception):
    """
    Raised when there are no input files to process
    """
    pass


class InvalidConfigurationError(Exception):
    """
    Raised when configuration is strictly validated
    """
    pass


class EndpointDownError(Exception):
    """
    Raised when we could not reach the endpoint we desire
    """
    pass


class BadRequestError(Exception):
    """
    Raised when an invalid request is made by a consumer.
    """
    pass


class BatchNotFoundException(Exception):
    """
    Raised when a batch id cannot be found in persisted state.
    """


class FailedRecognitionError(Exception):
    """
    Raised when recognizer cannot complete the recognition successfully
    """
    pass


class CancellationTokenException(Exception):
    """
    Raised when recognizer is told to stop a recognition request by
    cancellation token.
    """
    pass


def sha256_checksum(filename, block_size=65536):
    """
    Compute a sha256 checksum of a file
    :param filename: file to compute the checksum for
    :param block_size: size of a chunk for processing
    :return:
    """
    sha256 = hashlib.sha256()
    with open(filename, 'rb') as f:
        for block in iter(lambda: f.read(block_size), b''):
            sha256.update(block)
    return sha256.hexdigest()


def get_input_output_file_names(audio_file, output_folder):
    """
    Compute the base names for audio file and the resulting json file
    :param audio_file: fully qualified path name for the audio file
    :param output_folder: folder where json file will/would be stored.
    :return: audio file basename and fully qualified json file path
    """
    audio_file_basename = os.path.basename(audio_file)
    base, ext = os.path.splitext(audio_file_basename)
    json_file = os.path.join(output_folder, base + "_" + ext[1:] + ".json")
    return audio_file_basename, json_file


def write_json_file_atomic(final_json, json_file, rename_retries=3, log=True):
    json_file = os.path.abspath(json_file)
    # Use same dir for temporary file as target to avoid any
    # cross-device issue.
    target_dir = os.path.dirname(json_file)
    fd, tmp_filename = tempfile.mkstemp(dir=target_dir)
    with os.fdopen(fd, 'w', encoding="utf-8") as outfile:
        outfile.write(json.dumps(final_json, indent=2, sort_keys=True, ensure_ascii=False))
    retries = 0  # of overwrites to path `json_file`
    error = None
    while retries < rename_retries:
        if retries > 0:
            time.sleep(7)
        try:
            _rename_impl(tmp_filename, json_file)
            if log:
                logger.info("write_json_file_atomic():  Atomically wrote file {0}".format(json_file))
            return
        except OSError as err:
            if log:
                logger.warning(
                    "write_json_file_atomic(): Exception while renaming: {0} -> {1}. Details: {2} {3} Errno:{4}".format(
                    tmp_filename, json_file, type(err).__name__, err.strerror, err.errno))
            error = err
            retries += 1
    # Here we were unable to do the overwrite.
    try:
        _unlink_impl(os.path.abspath(tmp_filename))
    except OSError as err:
        if log:
            logger.warning(
                "write_json_file_atomic(): Unable to remove tmp file after unable to rename: {0}".format(tmp_filename))
        raise
    raise error


def tee_to_pipe_decorator(func, pipe: Connection, suppress_reraise=True, pipe_void=True) -> Any:
    """
    Decorate a function so that its return value will be tee'd
    as both return value as well as pickled object on a Connection.
    Additionally, any exception raised by the function is put
    on the pipe in place of the return value, and bubbled up.
    :param func: the function to be tee wrapped
    :param pipe: the send-enabled Connection onto which a pickled
                 copy of the return object (or exception) will be copied.
    :param suppress_reraise: whether to forego throwing an exception to caller
                             after it has already been placed on the pipe (no double raise).
    :param pipe_void: whether to put a None reference on the pipe when the func
                      successfully returns but we detect it has void return type
    :return: the original result.
    """
    def wrapped(*args, **kwargs):
        try:
            ret = func(*args, **kwargs)
            if ret:
                pipe.send(ret)
                return ret
            # There was no exception and the return object is None means
            # the func just returned void normally. In that case,
            # we don't put anything on the pipe unless directed.
            if pipe_void:
                pipe.send(None)
            return
        except Exception as e:
            pipe.send(e)
            if suppress_reraise:
                return
            raise
    return wrapped


class NonDaemonicProcess(multiprocessing.Process):
    @property
    def daemon(self):
        return False

    @daemon.setter
    def daemon(self, value):
        pass


class NonDaemonicContext(type(multiprocessing.get_context())):
    Process = NonDaemonicProcess


class NonDaemonicPool(pool.Pool):
    def __init__(self, *args, **kwargs):
        kwargs['context'] = NonDaemonicContext()
        kwargs['context'].prevent_core = True
        self.target_num_procs = args[0]
        super(NonDaemonicPool, self).__init__(*args, **kwargs)
        self.clean_names()

    def clean_names(self):
        for proc in self._pool:
            proc.name = NonDaemonicPool.sanitize_name(proc.name)

    @staticmethod
    def sanitize_name(name, prefix=""):
        return prefix + "_" + name.replace('NonDaemonic', '')

    # @Override
    def _maintain_pool(self):
        self._join_exited_workers()
        self._processes = self.target_num_procs
        if len(self._pool) < self.target_num_procs:
            self._repopulate_pool()

    def set_min_num_procs(self, num_procs):
        if num_procs > self._processes:
            self.target_num_procs = num_procs


def get_input_files(input_folder: str,
                    predicate: Callable[[str], bool],
                    input_list: Optional[str] = None) -> Set[str]:
    """
    Discover all valid files that can be work items in an input folder.
    :param input_folder: folder where work item files may reside
    :param predicate: function that qualifies whether a file is a valid input work item.
    :param input_list: filepath to a list of files in the `input_folder` to be considered.
                       If None, then all files in `input_folder` are considered.
    :return: set of qualified files. If the input directory or the non-None input_list
             are non-existent paths, then this returns None. Full paths are returned.
    """
    # Do some validation if files are to be limited by `input_list`.
    input_files = set()
    if os.path.isdir(input_folder):
        input_files = {
            f for f in filter_input_files(os.listdir(input_folder), predicate, input_folder)
        }
    else:
        logger.error("Input directory {0} does not exist!".format(input_folder))
        return set()
    file_list: Set[str] = set()
    if input_list is not None:
        if os.path.isfile(input_list):
            with open(os.path.abspath(input_list)) as input_list_file:
                file_list = set(input_list_file.read().splitlines())

            # set comprehension using { }
            file_list = {
                # If the file_list gives the absolute paths
                f for f in filter_input_files(file_list, predicate)
            }.union({
                # If the file_list entries are only with respect to the input_folder
                f for f in filter_input_files(file_list, predicate, input_folder)
            })
        else:
            logger.error("Input list {0} does not exist!".format(input_list))
            return set()
    else:
        file_list = input_files.copy()

    r = input_files & file_list
    if len(r) == 0:
        if input_list is None:
            logger.warning("No candidate work files in input directory {0}".format(input_folder))
        else:
            logger.warning(
                "No candidate work files in input directory {0} matching the input list".format(input_folder)
            )
    return r


def flush_queue_into_set(que) -> Set:
    items = set()
    while True:
        try:
            items.add(que.get_nowait())
        except Empty:
            break
    return items


def move_files(src_dir: str, targ_dir: str, extension: str, exclude: list = [], copy_only: list = [], allow_fail=False):
    exclude = [os.path.abspath(d) for d in exclude]
    copy_only = [os.path.abspath(d) for d in copy_only]
    for f in os.listdir(src_dir):
        _, ext = os.path.splitext(f)
        if ext != extension:
            continue
        src_path = os.path.abspath(os.path.join(src_dir, f))
        if os.path.isfile(src_path) and src_path not in exclude:
            targ_path = os.path.abspath(os.path.join(targ_dir, f))
            try:
                if os.path.isfile(targ_path):
                    _unlink_impl(targ_path)
                if src_path in copy_only:
                    _copyfile_impl(src_path, targ_path)
                else:
                    _move_file_impl(src_path, targ_path)
            except OSError as err:
                if not allow_fail:
                    raise
                else:
                    logger.warning(
                        "move_files(): Exception while moving: "
                        "{0} -> {1}. Details: {2} {3} Errno:{4}".format(
                            src_path, targ_path, type(err).__name__, err.strerror, err.errno))


def write_single_output_json(audio_files, output_folder):
    """
    Azure Batch Speech service returns a single JSON file. This function pieces a single JSON result file
    from individual result JSON files.
    :param audio_files: wave files that were processed
    :param output_folder: output folder where JSON files (input and output) are stored
    :return: None
    """
    output_json = \
        {
            "AudioFileResults": list()
        }

    output_file_json = os.path.abspath(os.path.join(output_folder, "results.json"))

    for audio_file in audio_files:
        _, json_file = get_input_output_file_names(audio_file, output_folder)
        if os.path.isfile(json_file):
            with open(json_file, encoding="utf-8") as jf:
                json_data = json.load(jf)
                json_data = json_data["AudioFileResults"][0]
                output_json["AudioFileResults"].append(json_data)

    with open(output_file_json, 'w', encoding="utf-8") as outfile:
        outfile.write(json.dumps(output_json, indent=2, sort_keys=True, ensure_ascii=False))


def create_dir(path):
    """
    Create or validate that the directory exists.
    :param path: path to the directory.
    """
    # Create or validate that the directory exists
    if not os.path.isdir(path):
        try:
            os.makedirs(path, exist_ok=True, mode=0o777)
            logger.info("Folder {0} created successfully".format(path))
        except OSError as error:
            logger.error("Folder {0} can not be created: {1}".format(path, error))
            raise
    else:
        try:
            os.chmod(path, 0o777)
        except OSError as error:
            logger.error("Folder {0} cannot have perms set to 0o777: {1}".format(path, error))
            raise


def assert_file_exists(path):
    """
    Check whether file exists, and if not, then log and kill this process.
    """
    if not os.path.isfile(path):
        logger.fatal("Could not find file {0}".format(path))
        exit(1)


def assert_sufficient_openfd_rlimit(num_openfd_floor=524288):
    soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
    if soft < num_openfd_floor or hard < num_openfd_floor:
        msg = "Insufficient open file descriptor limit: ulimit -n {} {}".format(soft, hard)
        logger.fatal(msg)
        raise AssertionError(msg)


def kill_children_procs():
    p = psutil.Process()
    children = p.children(recursive=True)
    for p in children:
        try:
            p.send_signal(signal.SIGKILL)
        except psutil.NoSuchProcess as e:
            # Already killed.
            pass
        print("Killed: ", p.pid)


def current_threads_stacktrace(use_logger=True):
    daemons = set()
    threads = threading.enumerate()
    for t in threads:
        if t.isDaemon():
            daemons.add(t.ident)
    code = []
    for threadId, stack in sys._current_frames().items():
        code.append("\n# ThreadID: %s  IsDaemon: %s" % (threadId, threadId in daemons))
        for filename, lineno, name, line in traceback.extract_stack(stack):
            code.append('File: "%s", line %d, in %s' % (filename,
                                                        lineno, name))
            if line:
                code.append("  %s" % (line.strip()))

    for line in code:
        if use_logger:
            logger.debug(line)
        else:
            print(line, file=sys.stderr)


def _rename_impl(src, target):
    __test_gate_oserror()
    os.rename(src, target)


def _unlink_impl(file):
    __test_gate_oserror()
    os.unlink(file)


def _copyfile_impl(src, target):
    __test_gate_oserror()
    shutil.copyfile(src, target)


def _move_file_impl(src, target):
    __test_gate_oserror()
    shutil.move(src, target)


def __test_gate_oserror():
    prob = os.getenv("PROB_IO_OSERROR")
    if prob and random() < float(prob):
        raise OSError("Emulated OSError")


def filter_input_files(file_list: Iterable[str], predicate: Callable[[str], bool], base_dir=None) -> Iterable[str]:
    """
    Check if the input files specified are suitable work files using a user-provided predicate.
    :param file_list: candidate files to check
    :param predicate: function that determines whether each file is a valid input work item.
    :param base_dir: base directory for the file list
    :return: qualified files list (absolute filepaths)
    """
    for f in file_list:
        if base_dir is None:
            file_path = f
        else:
            file_path = os.path.join(base_dir, f)
        file_path = os.path.abspath(file_path)
        if predicate(file_path):
            yield file_path
