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

from batchkit.logger import LogEventQueue, LogLevel
from batchkit.utils import sha256_checksum, write_json_file_atomic, \
    EndpointDownError, FailedRecognitionError, tee_to_pipe_decorator, CancellationTokenException, create_dir
from batchkit.constants import RECOGNIZER_SCOPE_RETRIES
from batchkit_examples.speech_sdk.audio import init_gstreamer, convert_audio, WavFileReaderCallback
from .work_item import LangIdWorkItemRequest, LangIdWorkItemResult
from .endpoint_status import LangIdEndpointStatusChecker

import azure.cognitiveservices.speech as speechsdk


# Time unit for the Speech SDK
TIME_UNIT: float = 10000000.0

LID_URL_BASE = "<base>/speech/languagedetection/cognitiveservices/v1"

# File marked failed after this many successive segment classification failures.
MAX_CONSECUTIVE_LID_REQUEST_FAILURES = 5


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
        self._host = "ws{0}://{1}:{2}".format(
            "s" if endpoint_config["isSecure"] else "",
            endpoint_config["host"],
            endpoint_config["port"]
        )
        self._subscription = endpoint_config.get("subscription")
        self._log_event_queue = log_event_queue

        self.request = request
        self._throttle = str(round(rtf * 100))

        # Set lazily later.
        self._audio_duration: Optional[float] = None
        self._converted_audio_file: Optional[str] = None

        self._lid_request_consecutive_failures = 0
        self._last_lid_request_err: Optional[str] = None

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
        """
        Invoke the Speech SDK in a subprocess to do continuous language segmentation on an audio file.
        This provides per-request process isolation to prevent unexpected terminal faults upstream.
        :param audio_file: original audio file to recognize
        :param json_data: any result from a previous run of this file even if it was a failed transcription
        """
        # Check that this endpoint is actually healthy.
        address, port = self._host.split("//")[1].split(":")
        if not LangIdEndpointStatusChecker(self._log_event_queue). \
                check_endpoint(address, port, False, True):
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
        """
        Use Microsoft Speech SDK to do continuous language segmentation on a single WAV file
        :param cancellation_token multiprocessing.Event: to signal early exit.
        :returns: If all went well, just an int containing the audio duration.
                  The results would already be written as files in that case.
        """
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
            lang_segments = self.__connect_contiguous_segments(
                self.__continuous_lid_rolling_with_voting(cancellation_token))
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

    def __single_segment_lid_request(self,
                                     start_offset: float = 0.0,
                                     end_offset: float = 0.0) -> str:
        """
        :returns: If the language could not be detected but this was due to signal or model insufficiency, the
                  detected_language is returned as 'unknown'. Any error from the LID container or due to the
                  Speech SDK either resolves on a retry or until the max retries are hit in which case a
                  FailedRecognitionError is thrown.
        """
        while self._lid_request_consecutive_failures < MAX_CONSECUTIVE_LID_REQUEST_FAILURES:

            if start_offset == 0 and end_offset == 0:
                audio_config = speechsdk.audio.AudioConfig(filename=self._converted_audio_file)
            else:
                callback = WavFileReaderCallback(
                    filename=self._converted_audio_file,
                    log_event_queue=self._log_event_queue,
                    start_offset=start_offset,  # in seconds
                    end_offset=end_offset,
                )
                stream = speechsdk.audio.PullAudioInputStream(callback, callback.audio_format())
                audio_config = speechsdk.audio.AudioConfig(stream=stream)

            # Constrain the language candidate set in the endpoint url.
            endpoint = self.__lid_url_for_candidate_langs()

            sdk_config = speechsdk.SpeechConfig(endpoint=endpoint, subscription=self._subscription)
            sdk_config.enable_dictation()
            sdk_config.set_property_by_name('Auto-Detect-Source-Language-Only', 'true')

            # Throttle to provided RTF. Throttle from the beginning.
            sdk_config.set_property_by_name("SPEECH-AudioThrottleAsPercentageOfRealTime", self._throttle)
            sdk_config.set_property_by_name("SPEECH-TransmitLengthBeforThrottleMs", "0")

            # Important - we need to have the phraseDetection mode set to None for all language-detection-only requests
            sdk_config.set_service_property(
                name='speechcontext-phraseDetection.Mode',
                value='None',
                channel=speechsdk.ServicePropertyChannel.UriQueryParameter,
            )
            sdk_config.set_service_property(
                name='speechcontext-languageId.OnUnknown.Action',
                value='None',
                channel=speechsdk.ServicePropertyChannel.UriQueryParameter,
            )
            # As opposed to PrioritizeLatency, since this is a batch (non-real-time) application.
            sdk_config.set_service_property(
                name='speechcontext-languageId.Priority',
                value='PrioritizeAccuracy',
                channel=speechsdk.ServicePropertyChannel.UriQueryParameter,
            )

            # Add logs for debugging.
            audio_file_basename = os.path.basename(self.request.filepath)
            if self.request.log_dir is not None:
                base, ext = os.path.splitext(audio_file_basename)
                log_filename = "{0}.{1}.sdk.langid.log".format(os.path.join(base + "_" + ext[1:]),
                                                               current_process().name)
                sdk_config.set_property(
                    speechsdk.PropertyId.Speech_LogFilename,
                    os.path.join(self.request.log_dir, log_filename))
                sdk_config.set_property_by_name("SPEECH-FileLogSizeMB", "10")

            recognizer = speechsdk.SpeechRecognizer(
                speech_config=sdk_config,
                audio_config=audio_config
            )
            result = recognizer.recognize_once()

            # On successful classification.
            if result.reason == speechsdk.ResultReason.RecognizedSpeech:
                detected_lang = result.properties.get(
                    speechsdk.PropertyId.SpeechServiceConnection_AutoDetectSourceLanguageResult, "unknown").lower()
                self._log_event_queue.debug(
                    "SEGMENT CLASSIFIED: File: {0}  start_offset: {1}  end_offset: {2}  "
                    "  Detected Lang: {3}".format(self._converted_audio_file, start_offset, end_offset, detected_lang))
                self._lid_request_consecutive_failures = 0
                self._last_lid_request_err = None
                return detected_lang

            # Unexpected cancellation by the server (or less likely, by the sdk).
            elif result.reason == speechsdk.ResultReason.Canceled:
                cancellation_details = result.cancellation_details
                self._log_event_queue.error(
                    "SEGMENT CANCELED UNEXPECTEDLY by server: File: {0}  start_offset: {1}  end_offset: {2}  "
                    "  Endpoint: {3}".format(self._converted_audio_file, start_offset, end_offset, self._host))
                if cancellation_details.reason == speechsdk.CancellationReason.Error:
                    self._log_event_queue.error(
                        "Cancellation Reason was an Error: {0}".format(cancellation_details.error_details))
                self._lid_request_consecutive_failures += 1
                self._last_lid_request_err = cancellation_details.error_details

            # Else something else went wrong that we are unfamiliar with.
            else:
                err_str = "{0} {1}".format(result.reason, result.__repr__())
                self._log_event_queue.error(
                    "SEGMENT RESULT HAS UNEXPECTED REASON: File: {0}  start_offset: {1}  end_offset: {2}  "
                    "Endpoint: {3}  Reason code: {4}  Result object: {5}".format(
                        self._converted_audio_file, start_offset, end_offset, self._host, result.reason, result.__repr__()))
                self._lid_request_consecutive_failures += 1
                self._last_lid_request_err = err_str

        # Retries hit the max. This is expected to be a very rare path, so we throw.
        raise FailedRecognitionError("Too many max consecutive LID request failures ({0}). Last error: {1}".format(
            MAX_CONSECUTIVE_LID_REQUEST_FAILURES, self._last_lid_request_err))

    def __continuous_lid_rolling(self, cancellation_token: multiprocessing.Event):
        results = []
        # Constants. These might be too difficult to explain to users but consider making these user-settable params
        # so that users can make their own trade-off between language cross-over precision and compute intensity.
        window = 6.0  # should be an integer multiple of window
        stride = 3.0  # should be an integer multiple of trans_stride
        trans_stride = 1.0  # stride when there is language crossover ambiguity, should not be a multiple of 1.0
        time = 0.0
        last_detected = None
        last_detailed = False
        while time + window < self._audio_duration:

            # Check for cancellation reasonably often..
            if cancellation_token.is_set():
                msg = "Canceled during language segmentation on file: {0} on process {1} " \
                      "targeting {2} by cancellation token (in middle).".format(
                        self.request.filepath, current_process().name, self._host
                )
                self._log_event_queue.info(msg)
                raise CancellationTokenException(msg)

            # Classify language for this window.
            detected = self.__single_segment_lid_request(start_offset=time, end_offset=time + window)
            results.append([detected, time, time + window])

            # Detection of language cross-over..
            if last_detected and detected and last_detected != detected:
                # Additional detailing from last time to this time.
                if not last_detailed:
                    time_ = time - stride + trans_stride
                    while time_ < time:
                        detected_ = self.__single_segment_lid_request(start_offset=time_, end_offset=time_ + window)
                        results.append([detected_, time_, time_ + window])
                        time_ += trans_stride
                # Additional detailing past this time.
                time_ = time + trans_stride
                while time_ + window < self._audio_duration and time_ < time + stride:
                    detected_ = self.__single_segment_lid_request(start_offset=time_, end_offset=time_ + window)
                    results.append([detected_, time_, time_ + window])
                    time_ += trans_stride
                last_detailed = True
            # No language cross-over..
            else:
                # Thus no need for detailing around +/- this window's time.
                last_detailed = False

            last_detected = detected
            time += stride
        return results

    def __continuous_lid_rolling_with_voting(self, cancellation_token: multiprocessing.Event):
        results = self.__continuous_lid_rolling(cancellation_token)
        # These are the votes for a given second t representing [t, t+1.0)
        votes_by_time = defaultdict(lambda: defaultdict(int))
        # This is the max vote for a given second t representing [t, t+1.0)
        maxvote_by_time = defaultdict(lambda: "unknown")
        results_voted = []
        max_time_voted = 0  # representing [time, time+1.0) for which we have any vote
        # Do voting. Make a vote for each 1.0 second of audio.
        for result in results:
            lang = result[0]
            start = int(round(result[1]))
            end = int(round(result[2])) - 1  # inclusive
            max_time_voted = max(max_time_voted, end)
            for t in range(start, end + 1, 1):
                votes_by_time[t][lang] += 1

        # Get the max votes at every second.
        first_known_lang = "unknown"
        for t in range(0, max_time_voted + 1):
            max_lang = "unknown"
            max_votes = 0
            for lang, num_votes in votes_by_time[t].items():
                if lang != "unknown" and num_votes > max_votes:
                    max_lang = lang
                    max_votes = num_votes
            maxvote_by_time[t] = max_lang
            if first_known_lang == "unknown":
                first_known_lang = max_lang

        # Finally, put together contiguous sequences and remove 'unknown'. The default policy for 'unknown' vote
        # is to re-use the last known maxvote in time. If the audio starts unknown, then we use the
        # first known language seen.
        last_known_lang = first_known_lang
        current_start = 0
        last_sec_incl = int(self._audio_duration)
        for t in range(1, last_sec_incl + 1):
            current_lang = maxvote_by_time[t]
            last_lang = maxvote_by_time[t - 1]
            if current_lang != last_lang:
                results_voted.append([last_lang if last_lang != 'unknown' else last_known_lang, current_start, t])
                current_start = t
            if current_lang != "unknown":
                last_known_lang = current_lang
        # Final language segment
        last_lang = maxvote_by_time[last_sec_incl]
        results_voted.append(
            [last_lang if last_lang != 'unknown' else last_known_lang, current_start, last_sec_incl + 1])
        return results_voted

    def __connect_contiguous_segments(self, segments):
        if len(segments) == 0:
            return segments
        segments = copy.deepcopy(segments)
        sorted(segments, key=lambda seg: seg[1], reverse=False)
        on_idx = 0
        for next_seg in segments[1:]:
            if segments[on_idx][0] == next_seg[0] and segments[on_idx][2] == next_seg[1]:
                segments[on_idx][2] = next_seg[2]
            else:
                on_idx += 1
                segments[on_idx] = next_seg
        # on_idx is the last one inclusive
        if on_idx < len(segments) - 1:
            del segments[on_idx + 1:]
        return segments

    def __lid_url_for_candidate_langs(self) -> str:
        url = LID_URL_BASE.replace("<base>", self._host) + '?'
        for lang in self.request.candidate_languages:
            url += "speechcontext-languageId.Languages={0}&".format(lang)
            if lang != self.request.candidate_languages[-1]:
                url += '&'
        return url

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
