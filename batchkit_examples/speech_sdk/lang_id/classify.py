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
    EndpointDownError, FailedRecognitionError, tee_to_pipe_decorator, CancellationTokenException
from batchkit.constants import RECOGNIZER_SCOPE_RETRIES
from batchkit_examples.speech_sdk.audio import init_gstreamer, convert_audio, WavFileReaderCallback
from .work_item import LangIdWorkItemRequest, LangIdWorkItemResult
from .endpoint_status import LangIdEndpointStatusChecker

import azure.cognitiveservices.speech as speechsdk


# Time unit for the Speech SDK
TIME_UNIT: float = 10000000.0
LID_URL_BASE = "<base>/speech/languagedetection/cognitiveservices/v1"


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
    return file_recognizer.recognize(cancellation_token)


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
                audio_duration = self.__recognize_in_subproc(self.request.filepath, start_offset, cancellation_token)

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

    def __recognize_in_subproc(self, audio_file, json_data, cancellation_token: multiprocessing.Event):
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
            target=tee_to_pipe_decorator(FileRecognizer.__recognize, child_conn, pipe_void=True),
            args=(self, audio_file, json_data, cancellation_token),
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
                            audio_file,
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

    def __classify(self, audio_file, start_offset_secs, cancellation_token) -> float:
        """
        Use Microsoft Speech SDK to do continuous language segmentation on a single WAV file
        :param audio_file: original audio file to recognize
        :param json_data: any result from a previous run of this file even if it was a failed transcription
        :param cancellation_token multiprocessing.Event: to signal early exit.
        :returns: If all went well, just an int containing the audio duration.
                  The results would already be written as files in that case.
        """
        # Fail fast if work item has already been cancelled.
        if cancellation_token.is_set():
            msg = "Canceled {0} on process {1} targeting {2} by cancellation token (before start).".format(
                audio_file, current_process().name, self._host
            )
            self._log_event_queue.log(LogLevel.INFO, msg)
            raise CancellationTokenException(msg)

        is_success = True
        failing_seg_error_details = None
        failing_seg_cancel_msg = None

        init_gstreamer()
        converted_audio_file, audio_duration = convert_audio(audio_file, self._log_event_queue)
        assert audio_duration is not None


        def __single_segment_lid_request(start_offset: float = 0, end_offset: float = 0) -> str:
            if start_offset == 0 and end_offset == 0:
                audio_config = speechsdk.audio.AudioConfig(filename=self._converted_audio_file)
            else:
                callback = LidWavFileReaderCallback(
                    filename=self._converted_audio_file,
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
            audio_file_basename = os.path.basename(audio_file)
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
                return "unknown"

            # Else something else went wrong that we are unfamiliar with.
            self._log_event_queue.error(
                "SEGMENT RESULT HAS UNEXPECTED REASON: File: {0}  start_offset: {1}  end_offset: {2}  "
                "Endpoint: {3}  Reason code: {4}  Result object: {5}".format(
                    self._converted_audio_file, start_offset, end_offset, self._host, result.reason, result.__repr__()))
            return "unknown"













        # If fail during the sequence of segments, do something like:
        # self._log_event_queue.error("ERROR during language segment classification on file:
        # {0}, start_offset: {1}, end_offset: {2}, details: {3}".format(audio_file, error_details))

        # During the sequence, have to check if the cancellation token gets toggled.
        if cancellation_token.is_set():
            cancel_msg = "Canceled during language segmentation on file: {0} on process {1} " \
                         "targeting {2} by cancellation token (in middle).".format(
                            audio_file, current_process().name, self._host)
            self._log_event_queue.info(cancel_msg)



        # After each language segment, log:
        self._log_event_queue.debug(
            "RECOGNIZED language for file: {0} start_offset: {1} end_offset: {2} "
            "sent by endpoint: {3} handled by process: {4}".format(
                audio_file_basename, start_offset, end_offset, self._host, current_process().name
            )
        )


        start_time = time.time()


        # Wait for the done event to be set, but check if it's because cancellation was triggered.
        if cancellation_token.is_set():
            raise CancellationTokenException(cancel_msg)
        end_time = time.time()



        self._log_event_queue.log(LogLevel.INFO, "Finished recognizing {0} -- {1}; recognition time: {2}s".format(
            audio_file_basename,
            "PASS" if is_success else "FAIL",
            end_time - start_time
        ))

        # AudioFileHash and AudioLengthInSeconds are added to support idempotent cached runs
        final_json = \
            {
                "AudioFileName": audio_file_basename,
                "AudioFileUrl": audio_file,
                "AudioFileHash": sha256_checksum(audio_file),
                "AudioLengthInSeconds": audio_duration,
                "TranscriptionStatus": "Succeeded" if is_success else "Failed",
                "SegmentResults": segment_results,
                "CombinedResults": combined_results,
                "LastProcessedOffsetInSeconds": last_processed_offset_secs,
            }

        # Emplace the segment results and combined result into the `final_json`.
        # This also has the side effect of fixing the offsets if start_offset_secs was not zero.
        self.populate_json_results(final_json, json_result_list, start_offset_secs)

        # Write json output irrespective of whether the recognition was successful; also make it atomic
        # in case this work item has a duplicate.
        write_json_file_atomic(
            {"AudioFileResults": [final_json]},
            json_file,
            log=False
        )
        self._log_event_queue.log(LogLevel.INFO, "Atomically wrote file {0}".format(json_file))

        if not is_success:
            self._log_event_queue.log(LogLevel.WARNING,
                                      "Target {0} on process {1} failed 1 time.".format(
                                          self._host, current_process().name))
            raise FailedRecognitionError(error_details)

        if os.path.abspath(audio_file) != os.path.abspath(converted_audio_file):
            os.remove(converted_audio_file)

        return audio_duration

    def __cont_lid_rolling(self):
        results = []
        # Constants. These might be too difficult to explain to users but consider making these user-settable params
        # so that users can make their own trade-off between language cross-over precision and compute intensity.
        window = 6.0  # should be an integer multiple of window
        stride = 3.0  # should be an integer multiple of trans_stride
        trans_stride = 1.0  # stride when there is language crossover ambiguity, should not be a multiple of 1.0
        time = 0.0
        last_detected = None
        last_detailed = False
        while time + window < lang_segments[-1][2]:
            detected = __single_segment_lid_request(example_file, start_offset=time, end_offset=time + window)
            results.append([detected, time, time + window])

            # Detection of language cross-over..
            if last_detected and detected and last_detected != detected:
                # Additional detailing from last time to this time.
                if not last_detailed:
                    time_ = time - stride + trans_stride
                    while time_ < time:
                        detected_ = __single_segment_lid_request(example_file, start_offset=time_,
                                                                 end_offset=time_ + window)
                        results.append([detected_, time_, time_ + window])
                        time_ += trans_stride
                # Additional detailing past this time.
                time_ = time + trans_stride
                while time_ + window < lang_segments[-1][2] and time_ < time + stride:
                    detected_ = __single_segment_lid_request(example_file, start_offset=time_,
                                                             end_offset=time_ + window)
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
