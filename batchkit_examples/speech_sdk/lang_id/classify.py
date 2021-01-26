# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

import os
import shutil
import time
import json
import traceback
from threading import Event
import multiprocessing
from multiprocessing import current_process
from functools import wraps
from typing import List, Optional
import copy
import wave
from collections import defaultdict
import grpc

from batchkit.logger import LogEventQueue, LogLevel
from batchkit.utils import sha256_checksum, write_json_file_atomic, \
    EndpointDownError, FailedRecognitionError, tee_to_pipe_decorator, CancellationTokenException, create_dir
from batchkit.constants import RECOGNIZER_SCOPE_RETRIES
from batchkit_examples.speech_sdk.audio import init_gstreamer, convert_audio, WavFileReaderCallback, \
    InvalidAudioFormatError
from .work_item import LangIdWorkItemRequest, LangIdWorkItemResult
from .endpoint_status import LangIdEndpointStatusChecker

from . import pb2
from .pb2 import (
    LIDRequestMessage, AudioConfig, LanguageIdStub, IdentifierConfig_pb2, IdentifierConfig, FinalResultMessage,
    IdentificationCompletedMessage
)


def classify_file_retry(run_classifier_function):
    """
    Retry function for run_classifier
    :param run_classifier_function: run_classifier function wrapped
    :return: wrapped function
    """
    @wraps(run_classifier_function)
    def wrapped(*args, **kwargs):
        """
        This is the wrapped function call, which is executed before run_classifier
        :param args: args to run_classifier
        :param kwargs: kwargs to run_classifier
        :return: same as the wrapped function
        """
        num_retries_left = RECOGNIZER_SCOPE_RETRIES
        result: LangIdWorkItemResult = None

        while num_retries_left > 0:
            result: LangIdWorkItemResult = run_classifier_function(*args, **kwargs)
            if result.passed or \
                    not result.can_retry or \
                    result.error_type == EndpointDownError.__name__:
                break
            num_retries_left = num_retries_left - 1

        result.attempts = RECOGNIZER_SCOPE_RETRIES - num_retries_left + 1
        return result
    return wrapped


@classify_file_retry
def run_classifier(request: LangIdWorkItemRequest, rtf: float,
                   endpoint_config: dict, log_event_queue: LogEventQueue,
                   cancellation_token: multiprocessing.Event):
    """
    Perform continuous language detection on an audio file to break into language segments.
    :param request: request details
    :param rtf: the Real-Time-Factor with which to throttle the audio stream
                when pushing the stream to server.
    :param endpoint_config: about the endpoint to use
    :param log_event_queue: object for enqueueing events to be logged asap.
    :param cancellation_token: Event signalling that the work should be cancelled.
    :return: an instance of LangIdWorkItemResult.
    """
    file_recognizer = FileRecognizer(request, rtf, endpoint_config, log_event_queue)
    return file_recognizer.classify(cancellation_token)


class FileRecognizer:
    """
    Class that does language segmentation on one file at a time and uses a single endpoint.
    """
    def __init__(self, request: LangIdWorkItemRequest,
                 rtf: float, endpoint_config: dict, log_event_queue: LogEventQueue):
        """
        Initialize the object.
        :param endpoint_config: about the endpoint to use
        :param rtf: the Real-Time-Factor with which to throttle the audio stream
            when pushing the stream to server.
        :param log_event_queue: object for enqueueing events to be logged asap.
        """
        self._host = "{0}:{1}".format(endpoint_config["host"], endpoint_config["port"])
        self._log_event_queue = log_event_queue
        self.request = request

        # Set lazily later.
        self._audio_duration: Optional[float] = None
        self._converted_audio_file: Optional[str] = None

    def classify(self, cancellation_token: multiprocessing.Event) -> LangIdWorkItemResult:
        """
        Wrapper that ensures that result is reliably produced or else total accounting for why not.
        :return: LangIdWorkItemResult instance, all details whether passed or failed.
        """
        self._log_event_queue.log(LogLevel.INFO,
                                  "Processing file: {0} on endpoint: {1}, with process: {2}".format(
                                        self.request.filepath,
                                        self._host,
                                        current_process().name))
        passed = False
        cached = False
        audio_duration = 0
        failed_reason = None
        can_retry = True
        error_type = None

        start = time.time()

        try:
            # Try to retrieve a cached result from candidate directories, if there
            audio_duration, cached_wavptrs, start_offset = self.get_cached_result(
                self.request.filepath, self.request.cache_search_dirs)
            completely_cached = audio_duration is not None

            # If we did not pick up a 100% successful cached result, then we need to do a call to the classifier,
            # but if we were partially successful, we can at least pick up from where we left off.
            if not completely_cached:
                audio_duration = self.__recognize_in_subproc(start_offset, cancellation_token)

            if start_offset > 0.0:
                # Partial or total cache hit, but we need to make sure the results exists where they're expected.
                # TODO: Implement this when implement caching.
                #       Iterate through cached_wavptrs and ensure they are located in the output directory
                #       copying any as necessary.
                pass

            passed = True
        except Exception as e:
            exception_details = traceback.format_exc()
            self._log_event_queue.log(LogLevel.DEBUG, "Exception details: {0}".format(exception_details))
            self._log_event_queue.log(LogLevel.WARNING,
                                      "{0} happened, must terminate this language segmentation task."
                                      "Details: {1}".format(type(e).__name__, e))
            failed_reason = str(e)
            can_retry = e.__class__ in [EndpointDownError, FailedRecognitionError,
                                        ChildProcessError, CancellationTokenException, OSError]
            error_type = type(e).__name__

        latency = time.time() - start

        # Generate output
        return LangIdWorkItemResult(
            self.request,
            passed,
            self._host,
            latency,
            1,
            current_process().name,
            can_retry,
            cached,
            audio_duration,
            error_type,
            failed_reason,
        )

    def __recognize_in_subproc(self, start_offset: float, cancellation_token: multiprocessing.Event):
        # Check that this endpoint is actually healthy.
        address, port = self._host.split(":")
        if not LangIdEndpointStatusChecker(self._log_event_queue). \
                check_endpoint(address, int(port), False, False):
            raise EndpointDownError("Target {0} in process {1} failed.".format(self._host, current_process().name))

        # Prepare to fork off the work to a child proc for isolation
        # from non-recoverable faults, and we'll wait.
        # Use pipe to receive pickled exceptions from child.
        parent_conn, child_conn = multiprocessing.Pipe()
        work_proc = multiprocessing.Process(
            target=tee_to_pipe_decorator(FileRecognizer.__classify, child_conn, pipe_void=True),
            args=(self, start_offset, cancellation_token),
            daemon=True,
        )
        work_proc.name = current_process().name + "__LangIdRequestChildProc"
        work_proc.start()
        self._log_event_queue.log(LogLevel.DEBUG,
                                  "Starting FileRecognizer.__classify() in subproc: {0}".format(work_proc.name))

        # We can't do event-driven waitpid() until we know what we're waiting for.
        # > 99.9% of the time the pid be available before a single yield.
        while work_proc.pid is None:
            time.sleep(0)  # yield tick

        _, status = os.waitpid(work_proc.pid, 0)

        self._log_event_queue.log(LogLevel.DEBUG,
                                  "Finished FileRecognizer.__classify() in subproc: {0}".format(work_proc.name))

        if os.WIFSIGNALED(status):
            signum = os.WTERMSIG(status)
            assert not parent_conn.poll()  # sanity check, must be impossible
            child_conn.close()
            parent_conn.close()
            err_msg = "Terminating signum: {0} was received in sdk subproc: {1}" + \
                      "while processing file: {2} on endpoint: {3}, " + \
                      "caught by supervisor: {4}"
            err_msg = err_msg.format(
                            signum,
                            work_proc.name,
                            self.request.filepath,
                            self._host,
                            current_process().name
                      )
            raise FailedRecognitionError(err_msg)
        else:
            assert os.WIFEXITED(status)
            # Determine whether we had an exception or successful return and
            # pass on that outcome either way, as if this occurred in same proc.
            assert parent_conn.poll()  # sanity check prevents deadlock with fast fail
            obj = parent_conn.recv()
            child_conn.close()
            parent_conn.close()
            if isinstance(obj, Exception):
                raise obj
            else:
                # This is the case of regular successful return of the function
                # so we expect a return value of just the audio_duration.
                assert type(obj) == float
                return obj

    def __classify(self, start_offset_secs, cancellation_token) -> float:
        # Fail fast if work item has already been cancelled.
        if cancellation_token.is_set():
            msg = "Canceled {0} on process {1} targeting {2} by cancellation token (before start).".format(
                self.request.filepath, current_process().name, self._host
            )
            self._log_event_queue.log(LogLevel.INFO, msg)
            raise CancellationTokenException(msg)

        start_time = time.time()
        is_success = True

        init_gstreamer()
        self._converted_audio_file, self._audio_duration = convert_audio(self.request.filepath, self._log_event_queue)
        assert self._converted_audio_file is not None

        try:
            # Additional checks beyond the framework's audio validation, because LID model more constrained.
            self._validate_file_format(self._converted_audio_file)

            lang_segments = self._segment(self._converted_audio_file, cancellation_token)

            # Corner case: when there is only a single language segment of language "unknown", the LID
            # backend has absolutely no idea how to even make a homogeneous language estimate. In this case
            # we would rather naively assume some language.
            if len(lang_segments) == 1 and 'unknown' in lang_segments[0][0].lower():
                lang_segments[0][0] = self.request.candidate_languages[0]
        except:
            # While processing we could get a FailedRecognitionError, CancellationTokenException,
            # or other unexpected exceptions. Upstream decides whether to retry, but we need to at least
            # clean up before we bubble up the exception.
            if os.path.abspath(self.request.filepath) != os.path.abspath(self._converted_audio_file):
                os.remove(self._converted_audio_file)
            self._log_event_queue.warning(
                "A language segmentation attempt of file: {0} against endpoint: {1} on process: {2} has failed.".format(
                    self.request.filepath, self._host, current_process().name))
            raise

        end_time = time.time()
        self._log_event_queue.info("Finished language segmentation on file: {0} -- {1}; wall time taken: {2}s".format(
            self.request.filepath,
            "PASS" if is_success else "FAIL",
            end_time - start_time
        ))

        # Write out the language segment files.
        base, _ = os.path.splitext(os.path.basename(self.request.filepath))
        for segno in range(len(lang_segments)):
            segment = lang_segments[segno]
            seg_filepath = os.path.join(
                self.request.output_dir, "{0}.{1}.{2}.seg.json".format(base, segno, segment[0]))
            segment.append(seg_filepath)
            write_json_file_atomic(
                {
                    "file": self.request.filepath,
                    "language": segment[0],
                    "start_offset": segment[1],
                    "end_offset": segment[2]
                },
                seg_filepath,
                log=False,
            )
            self._log_event_queue.info("Atomically wrote file {0}".format(seg_filepath))

        # Summarization file useful for debugging.
        seg_summ_file = os.path.join(self.request.output_dir, base+".lang_segments.json")
        write_json_file_atomic(
            lang_segments,
            seg_summ_file,
            log=False,
        )
        self._log_event_queue.info("Atomically wrote file {0}".format(seg_summ_file))
        return self._audio_duration

    def _segment(self, audio_file: str, cancellation_token: multiprocessing.Event):
        channel = grpc.insecure_channel(self._host)
        stub = LanguageIdStub(channel)
        segments = []

        for resp in stub.Identify(self._generate_messages(audio_file, cancellation_token)):
            if resp.WhichOneof('message') == 'final_result':
                response: FinalResultMessage = resp.final_result
                self._log_event_queue.debug("Segment identified for file {0}: {1}".format(audio_file, response))
                segments.append([
                    response.locale,
                    float(response.start_offset_ms) / 1000.0,
                    float(response.end_offset_ms) / 1000.0
                ])
            elif resp.WhichOneof('message') == 'identification_completed':
                response: IdentificationCompletedMessage = resp.identification_completed
                if response.end_reason == IdentificationCompletedMessage.ERROR:
                    self._log_event_queue.warning(
                        "LID service had internal error while processing file {0}: {1}".format(audio_file, response))
                    # TODO(andwald): Can output results we did get thus far and enable picking up progress in retry.
                    raise FailedRecognitionError("LID service internal error.")
                elif response.end_reason == IdentificationCompletedMessage.AUDIOEND:
                    self._log_event_queue.info(
                        "LID service finished segmenting file {0}: {1}".format(audio_file, response))
                else:
                    self._log_event_queue.info(
                        "LID service returned unexpected end reason for file {0}: {1}".format(audio_file, response))
                    raise FailedRecognitionError("LID service unexpected end reason: {0}.".format(response))

            # Before going to next msg, check for cancellation.
            if cancellation_token.is_set():
                msg = "Canceled during language segmentation on file: {0} on process {1} " \
                      "targeting {2} by cancellation token (in middle).".format(
                        self.request.filepath, current_process().name, self._host)
                self._log_event_queue.info(msg)
                raise CancellationTokenException(msg)

        return segments

    def _validate_file_format(self, audio_file):
        with wave.open(audio_file, 'rb') as fd:
            # Currently only compatible with mono.
            nchan = fd.getnchannels()
            if nchan != 1:
                raise InvalidAudioFormatError("LID currently only compatible with 1-channel audio.")

            # Currently only compatible with 8kHz or 16kHz.
            framerate = fd.getframerate()
            if framerate not in [8000, 16000]:
                raise InvalidAudioFormatError(
                    "LID currently only compatible with 8kHz or 16kHz framerate. Given: {0}".format(framerate))

            # Currently only compatible with 16-bit samples.
            sampwidth = fd.getsampwidth()
            if sampwidth != 2:
                raise InvalidAudioFormatError("LID currently only compatible with 16-bit samples.")

    def _generate_messages(self, audio_file: str, cancellation_token: multiprocessing.Event):
        self._validate_file_format(audio_file)

        # Config message first.
        message = LIDRequestMessage()
        message.config.audio_config.encoding = AudioConfig.PCM
        message.config.audio_config.sample_type = AudioConfig.SAMPLE_S16LE
        message.config.audio_config.channels = AudioConfig.MONO
        message.config.identifier_config.mode = IdentifierConfig_pb2.SEGMENTATION
        message.config.identifier_config.locales.extend(self.request.candidate_languages)
        with wave.open(audio_file, 'rb') as fd:
            # Determine the actual frame rate.
            framerate = fd.getframerate()
            if framerate == 16000:
                message.config.audio_config.sample_rate = AudioConfig.SAMPLE_16KHZ
            else:
                message.config.audio_config.sample_rate = AudioConfig.SAMPLE_8KHZ
            yield message

            # Any number of audio payload messages follow the config message.
            while True:
                # If request has been canceled by framework, we will simply stop producing
                # the request message stream and allow the response reader to throw the CancellationTokenException.
                if cancellation_token.is_set():
                    break
                b = fd.readframes(2048)
                if not b:
                    break
                message.audio_payload = b
                yield message

    def get_cached_result(self, audio_file, dirs: List[str]):
        """
        Check output folder for a matching JSON file. If found, make sure that sha256 hash of the audio file matches
        the information in the JSON file, i.e. that this is the same audio for which we have the result cached.
        :param audio_file: input audio file to check
        :param dirs: directories where result JSON potentially already exists
        :return: (<audio duration, if find a successful existing result file that matches audio hash, otherwise None> ,
                  <result filepath, if find a successful existing result file that matches audio hash, otherwise None> ,
                  <offset seconds into file completed up to successfully, if any, otherwise None>)
        """
        # TODO: Implement this.
        return None, None, 0.0
