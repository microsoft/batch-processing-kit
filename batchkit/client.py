# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.
import copy
import inspect
import json
import time
import traceback
from abc import ABC, abstractmethod
import signal
import os
import multiprocessing
import logging
from argparse import Namespace
from collections import namedtuple
from typing import Optional, List, Set, Callable

from .apiserver import ApiServer
from . import constants
from .batch_config import BatchConfig
from .batch_request import BatchRequest
from .batch_status import BatchStatusProvider, BatchStatusEnum, BatchStatus
from .handlers import update_work_on_directory_content_change
from .logger import setup_logging, LogEventQueue
from .orchestrator import Orchestrator
from .utils import get_input_files, flush_queue_into_set, InvalidConfigurationError, \
    create_dir, assert_file_exists, move_files, assert_sufficient_openfd_rlimit, \
    kill_children_procs, write_single_output_json

Settings = namedtuple("Settings",
                      "input_folder input_list output_folder "           # ONESHOT or DAEMON modes only.
                      "apiserver_port "                                  # APISERVER mode only.
                      "scratch_folder log_folder store_combined_json "
                      "config_file strict_configuration_validation ")

logger = logging.getLogger("batch")

"""
Implements an app for running *BatchRequests in a few different modes.
Built directly on top of ApiServer.
"""


def run(cmd_args: Namespace, batch_type: type):
    """
    Main entry point for the client app to run in ONESHOT, DAEMON, or APISERVER modes.

    :param cmd_args: command line arguments which must be composed of at least the
                     fields in `Settings` namedtuple (depending on mode) as well as:
                     `console_log_level`, `file_log_level`: any of batchkit.logger.LogLevel enum values
                     `run_mode`: 'ONESHOT', 'DAEMON', or 'APISERVER'
                     Other args as necessary to construct the concrete subtype of BatchConfig.

    :param batch_type: a subtype of BatchConfig or BatchRequest for your workload.
                       This is the single mechanism of dependency injection into the framework.
                       The framework will infer the rest of the type injections as long as these requirements are met:
                         - `batch_type` must be in the type hierarchy originating from
                              abstract BatchRequest or BatchConfig.
                         - Your BatchRequest subtype must be in a module named batch_request.py
                         - Your BatchRequest subtype must override all abstract methods of BatchRequest
                         - Your modules must be discoverable from the sys.path (e.g. in an installed package,
                           in the PYTHONPATH, appending to sys.path, etc).

    :return: whether this run succeeded

    Notes:
    - To ensure proper signal handling and control flow, the calling
      thread should be the process's main thread. Where multi-processing
      or multi-threading is required by the consumer, consider using
      inter-process communication to the HTTP endpoint via APISERVER
      run mode (and SIGHUP to this process to shutdown orderly).
    """
    # Ensure sufficient rlimit of open file descriptors since we'll be
    # doing a lot of concurrent writing of results files.
    assert_sufficient_openfd_rlimit()

    # Setup logging early so that all subprocesses forked off in
    # any components have the same configuration.
    log_queue, log_listener = setup_logging(
        cmd_args.log_folder, cmd_args.console_log_level, cmd_args.file_log_level)

    run_mode = cmd_args.run_mode

    settings = Settings(
        # Args only relevant in case of ONESHOT or DAEMON modes.
        input_folder=cmd_args.input_folder,
        input_list=cmd_args.input_list,
        output_folder=cmd_args.output_folder,

        # Args only relevant in case of APISERVER mode.
        apiserver_port=cmd_args.apiserver_port,

        # Following args are relevant in all modes.
        scratch_folder=cmd_args.scratch_folder,
        log_folder=cmd_args.log_folder,
        config_file=cmd_args.configuration_file,
        strict_configuration_validation=cmd_args.strict_configuration_validation,
        store_combined_json=cmd_args.store_combined_json,
    )

    # Determine the type of BatchConfig we need to make from the args.
    config_type: type
    # If concrete BatchConfig subtype was provided.
    if issubclass(batch_type, BatchConfig):
        config_type = batch_type
    # If concrete BatchRequest subtype was provided.
    else:
        assert issubclass(batch_type, BatchRequest)
        config_type = inspect.getfullargspec(getattr(batch_type, "from_config")).annotations['config']

    # Now make the BatchConfig from the args.
    batch_config: config_type = getattr(config_type, "from_args")(cmd_args)

    # Make sure prereqs exist.
    if run_mode != "APISERVER":
        create_dir(cmd_args.output_folder)
    create_dir(cmd_args.scratch_folder)
    assert_file_exists(settings.config_file)
    if settings.input_list:
        assert_file_exists(settings.input_list)

    client = Client.create(run_mode, settings, batch_config, log_queue)

    logger.info("client.py: run(): Running:  {0}  with settings: {1},  and speech config: {2}".format(
        type(client).__name__, settings, batch_config)
    )
    ret = False
    try:
        ret = client.run()
    except Exception as e:
        exception_details = traceback.format_exc()
        logger.error("client.py: run(): Finishing prematurely after catching {0}, \nDetails: {1}".format(
            type(e).__name__, exception_details))

    logger.info("client.py: run(): Finished!")
    log_queue.put(None)
    log_listener.join()

    # There should be no children procs now, but SIGKILL just in case.
    kill_children_procs()

    return ret


class Client(ABC):

    @staticmethod
    def create(client_type, settings: Settings, batch_config: BatchConfig, log_queue: LogEventQueue):
        """
        Factory method for creation of a Client.

        Note: Should be invoked by the main thread of the process
        otherwise signal handlers may not be registered leading to
        disorderly shutdown.
        """
        if client_type == "ONESHOT":
            return OneShotClient(settings, batch_config, log_queue)
        elif client_type == "DAEMON":
            return DaemonClient(settings, batch_config, log_queue)
        elif client_type == "APISERVER":
            return GenericClient(settings, batch_config, log_queue)
        else:
            msg = "Client type {0} unrecognized.".format(client_type)
            logger.error(msg)
            raise InvalidConfigurationError(msg)

    def __init__(self, settings: Settings, batch_config: BatchConfig, log_queue: LogEventQueue):
        super().__init__()
        self._register_signal_handlers()
        self.settings = settings
        self.batch_config = batch_config
        self.log_queue = log_queue

        # Top-level composition of all components.
        self.submission_queue = multiprocessing.Queue()
        self.status_provider = BatchStatusProvider(settings.scratch_folder)

        self.apiserver = ApiServer(
            self.submission_queue,
            self.status_provider,
            self.requires_flask_functional(),
            self.requires_flask_healthprobe(),
            self.settings.apiserver_port,
        )

        self.orchestrator = Orchestrator(
            self.submission_queue,
            self.status_provider,
            self.settings.config_file,
            self.settings.strict_configuration_validation,
            self.settings.log_folder,

            # Take any old run batch dirs found in scratch and the
            # specified out dirs as candidates for cached result locations.
            # Depending on the mode, only one of them may be relevant.
            self.status_provider.batch_dirs() + [self.settings.output_folder],

            # LogEventQueue is a more direct path for logging in concurrent
            # multi-process scenarios.
            log_queue,

            # Client type will determine whether run summary is singleton
            # in nature or reported per-batch.
            self._singleton_run_summary_path(),
        )

    @abstractmethod
    def _singleton_run_summary_path(self) -> Optional[str]:
        """
        Whether the run summary reporting should be done on a
        per-batch basis or per-client-lifetime.
        :returns str: A path to where the singleton run summary
                      is periodically written, else None if
                      the summary is per-batch.
        """
        pass

    def _register_signal_handlers(self):
        """
        Set default behaviors for the major signal types and
        hook signal handler to instance method _handle_signal()
        """
        def __handle_signal(signum, frame):
            self.__class__._handle_signal(self, signum, frame)

        # SIGKILL and SIGSTOP cannot or should not be handled.
        # We will expect SIGHUP and SIGTERM as ways to signal for an orderly termination
        # of the ongoing clients and their work, while a SIGINT (KeyboardInterrupt)
        # is somewhat more violent.
        signal.signal(signal.SIGINT, __handle_signal)  # goes to entire Process Group
        signal.signal(signal.SIGHUP, __handle_signal)
        signal.signal(signal.SIGTERM, __handle_signal)

    @abstractmethod
    def requires_flask_functional(self) -> bool:
        """
        Whether the client is dependent on the ApiServer supporting
        HTTP endpoints in addition to its programmatic endpoints.
        """
        pass

    def requires_flask_healthprobe(self) -> bool:
        """
        Whether a health probe HTTP endpoint should be provided that
        could be used for orchestration systems checking for batch submission readiness.
        """
        pass

    @abstractmethod
    def run(self):
        """
        Entrypoint to run the client.
        """
        pass

    def _handle_signal(self, signum, frame):
        """
        Subtypes should first call super()._handle_signal to take care of
        more violent signals, and can provide implementations for softer signals.
        """
        if signum == signal.SIGINT:
            # Any far downstream children processes unbeknown to us will die (eventually).
            self.orchestrator.request_stop()
            raise KeyboardInterrupt()

    def finish(self):
        logger.info("{0}:  Requesting Orchestrator to stop.".format(type(self).__name__))
        self.orchestrator.request_stop()
        self.orchestrator.join()

    def _do_batch_sync(self, files: List[str]) -> BatchStatus:
        """
        Submit a single batch request, wait for its completion, and move
        results from batch's scratch dir to a particular output dir.
        Synchronously block the thread until these are completed.
        """
        batch_req = BatchRequest.from_config(files, self.batch_config)

        logger.info("{0}:  Submitting {1} with {2} files.".format(
            type(self).__name__, type(batch_req).__name__, len(files)))
        status: BatchStatus = self.apiserver.submit(batch_req)

        while status.status != BatchStatusEnum.done:
            # Check Orchestrator is in healthy state.
            assert self.orchestrator.is_alive()

            logger.info("{0}:  Waiting on batch id: {1}, in status: {2}".format(
                type(self).__name__, status.batch_id, status.status))

            status = self.apiserver.watch(
                status.batch_id,
                target_state=BatchStatusEnum.done,
                timeout=30,
            )

            # Incrementally move interim or final remaining results.
            # Posix move will be done via rename() and preserve the file blocks and inode
            # if the destination is on the same volume as the source. Otherwise the blocks
            # are copied over in the case of cross-device (i.e. scratch dir in container
            # but output dir vol mapped). All results are persisted in the output dir.
            logger.info("{0}:  Moving files (intermediate): {1} -> {2}".format(
                type(self).__name__, status.results_dir, self.settings.output_folder))
            move_files(
                status.results_dir,
                self.settings.output_folder,
                ".json",
                allow_fail=True  # In case of slow NFS sync. Will be retried later.
            )

        # Batch is done. Do a final move that must account for all files.
        logger.info("{0}:  Moving files (final): {1} -> {2}".format(
            type(self).__name__, status.results_dir, self.settings.output_folder))
        # This final move must succeed so we give constant back-off time for NFS sync.
        retries = 0
        while True:
            try:
                move_files(status.results_dir, self.settings.output_folder, ".json", allow_fail=False)
                break
            except OSError as err:
                retries += 1
                if retries == 15:
                    logger.error(
                        "{0}: Could not move all results from "
                        "scratch to output: {1}".format(type(self).__name__, err))
                    break
                logger.warning("{0}:  Sleeping before move_files() retry.".format(type(self).__name__))
                time.sleep(10)  # Generous for NFS sync worth not failing the batch.

        logger.info("{0}: Batch id: {1} is finished.".format(type(self).__name__, status.batch_id))
        return status


class GenericClient(Client):
    """
    Doubles as both a base class from which developers can programmatically build their
    use case on top of the ApiServer as an object for their custom batch application, and
    also as a process host for the ApiServer's HTTP endpoints. Hybrid consumption works too.
    """
    def __init__(self, settings: Settings, batch_config: BatchConfig, log_queue: LogEventQueue):
        super().__init__(settings, batch_config, log_queue)
        self._stop_evt = multiprocessing.Event()

    def _handle_signal(self, signum, frame):
        super()._handle_signal(signum, frame)
        self._stop_evt.set()

    def requires_flask_functional(self):
        return True

    def requires_flask_healthprobe(self):
        return True

    def run(self):
        """
        If running GenericClient for ApiServer's HTTP endpoints, the main thread does
        nothing but wait for a signal to exit. The APIs are freely accessible as long
        as the thread is in here. As you are the one watching batch statuses,
        it shall be assumed any signal means you no longer require service.

        If overriding GenericClient to use ApiServer programmatically, then override
        this method as your entrypoint for consuming ApiServer. It is intended that when
        this method returns, the client is going to exit.

        Note: Should be invoked by a main or daemonic thread.
        """
        self._stop_evt.wait()
        self.finish()
        return True

    def _singleton_run_summary_path(self):
        # Report run_summary per batch, not singleton.
        return None


class DaemonClient(Client):
    def __init__(self, settings: Settings, batch_config: BatchConfig, log_queue: LogEventQueue):
        # Behavior overrides particular to DaemonClient.
        # Combined json result output will be a function of the client Settings, not to be done in the batch itself.
        batch_config = copy.deepcopy(batch_config)
        batch_config.combine_results = False

        super().__init__(settings, batch_config, log_queue)
        self._next_batch_files_que = multiprocessing.Queue()
        self._work_notifier = None
        self._is_success = True
        self._stop_evt = multiprocessing.Event()

    def requires_flask_functional(self):
        return False

    def requires_flask_healthprobe(self):
        return True

    def run(self):
        """
        Run the daemon client. This method does not return unless
        the client process is signaled. The client will keep
        waiting for new files and submit whatever it finds, in addition
        to files that are present in the input directory initially.
        """
        # Function to be used for qualifying files in the input directory as valid work items.
        predicate: Callable[[str], bool] = \
            getattr(BatchRequest.find_type(self.batch_config), "is_valid_input_file")

        # Race condition between reading initial files and hooking for deltas
        # solved by hooking for deltas then reading initial files and de-duping.
        self._work_notifier = update_work_on_directory_content_change(
            self.settings.input_folder, self._next_batch_files_que, self.log_queue, predicate)
        submitted = set()  # all files ever submitted at any time since process started

        input_files: Set[str] = get_input_files(self.settings.input_folder, predicate, None)
        for audio_file in input_files:
            self._next_batch_files_que.put(audio_file)

        # Keep submitting new batch with whatever new files came in, only
        # after the currently running batch finishes.
        while not self._stop_evt.wait(5):

            assert self.orchestrator.is_alive()
            candidates = flush_queue_into_set(self._next_batch_files_que)
            candidates = candidates.difference(submitted)
            if len(candidates) == 0:
                continue

            submitted = submitted.union(candidates)
            self._do_batch_sync(list(candidates))

            if 0 < constants.DAEMON_MODE_MAX_FILES <= len(submitted):
                os.kill(os.getpid(), signal.SIGHUP)

        # No more watching. No more batches to do.
        # Merge all the single-file results into a combined result if requested.
        if self.settings.store_combined_json:
            write_single_output_json(
                submitted,  # Everything submitted across all batches processed
                self.settings.output_folder
            )

        self.finish()
        return self._is_success

    def _handle_signal(self, signum, frame):
        """
        DaemonClient interprets SIGHUP is a directive to stop processing new files,
        and that includes files that may have quite recently shown up. SIGTERM is
        a directive to not only stop listening for new files, but any files
        produced at any time that are not yet finished will be abandoned. The
        reason to use SIGTERM instead of the more violent SIGINT is that you will
        still get a run summary and result files are properly closed.
        """
        super()._handle_signal(signum, frame)
        self._work_notifier.stop()  # Don't be notified of new files.
        self._stop_evt.set()        # Stop waiting for new batches.
        if signum == signal.SIGHUP:
            # We will finish batches already submitted.
            return

        assert signum == signal.SIGTERM
        # Not only will we stop submitting new batches for new files,
        # we also tell the Orchestrator we want it to stop current batch prematurely.
        self.orchestrator.request_stop()  # any batch watcher will finish quickly
        self._is_success = False

    def merge_combined_jsons(self, combined_result_files):
        results = []
        for f in combined_result_files:
            if os.path.isfile(f):
                with open(f, encoding="utf-8") as g:
                    json_data = json.load(g)
                    results.append(json_data)

        output_json_file = os.path.abspath(os.path.join(self.settings.output_folder, "results.json"))
        output_json = {"AudioFileResults": results}
        with open(output_json_file, 'w', encoding="utf-8") as f:
            f.write(json.dumps(output_json, indent=2, sort_keys=True, ensure_ascii=False))

    def _singleton_run_summary_path(self):
        return os.path.abspath(os.path.join(self.settings.output_folder, "run_summary.json"))


class OneShotClient(Client):
    def __init__(self, settings: Settings, batch_config: BatchConfig, log_queue: LogEventQueue):
        # Behavior overrides particular to DaemonClient.
        # Combined json result output will be a function of the client Settings, not to be done in the batch itself.
        batch_config = copy.deepcopy(batch_config)
        batch_config.combine_results = False
        super().__init__(settings, batch_config, log_queue)

    def requires_flask_functional(self):
        return False

    def requires_flask_healthprobe(self):
        return False

    def run(self):
        """
        This method returns once processing of all the input files is finished,
        unless signalled with SIGHUP/SIGTERM, in which case any unfinished files
        are cancelled. The more violent SIGINT also terminates immediately but
        offers no guarantee on result file integrity.
        """
        predicate: Callable[[str], bool] = \
            getattr(BatchRequest.find_type(self.batch_config), "is_valid_input_file")
        files: Set[str] = get_input_files(self.settings.input_folder, predicate, self.settings.input_list)

        if len(files) == 0:
            logger.error("No candidate audio files. Leaving run.")
            self.finish()
            return False

        status: BatchStatus = self._do_batch_sync(list(files))

        # Merge all the single-file results into a combined result if requested.
        if self.settings.store_combined_json:
            write_single_output_json(
                files,
                self.settings.output_folder
            )

        # Clean up scratch directory.
        logger.info("OneShotClient:  Cleaning up scratch directory for single batch: {0}".format(status.batch_id))
        try:
            self.status_provider.rm_batch(status.batch_id)
        except OSError as err:
            logger.warning("OneShotClient:  Unable to clean up scratch directory: {0}".format(err))
        
        self.finish()

        # TODO(andwald): Run summary will be schematized instead of dynamic.
        try:
            with open(self._singleton_run_summary_path(), "r") as run_summ:
                run_summ_json = json.load(run_summ)
                num_failed = run_summ_json["overall_summary"]["file_stats"]["failed"]
                if num_failed == 0:
                    return True
                else:
                    logger.warning("Overall # files failed: {0}".format(num_failed))
                    return False
        except:
            logger.error("Could not read final run_summary at: {0}".format(self._singleton_run_summary_path()))
            return False

    def _handle_signal(self, signum, frame):
        super()._handle_signal(signum, frame)
        # If we're here, we want a graceful stop and we just tell Orchestrator
        # the intention and expect it to prematurely finish the batch(es) it was doing.
        self.orchestrator.request_stop()

    def _singleton_run_summary_path(self):
        return os.path.abspath(os.path.join(self.settings.output_folder, "run_summary.json"))
