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
from typing import List

from batchkit.logger import LogEventQueue, LogLevel
from batchkit.utils import sha256_checksum, get_input_output_file_names, write_json_file_atomic, \
    EndpointDownError, FailedRecognitionError, tee_to_pipe_decorator, CancellationTokenException
from batchkit.audio import init_gstreamer, convert_audio
from batchkit.constants import RECOGNIZER_SCOPE_RETRIES
from .work_item import SpeechSDKWorkItemRequest, SpeechSDKWorkItemResult
from .audio import WavFileReaderCallback
from .endpoint_status import SpeechSDKEndpointStatusChecker


# Time unit for the Speech SDK
TIME_UNIT: float = 10000000.0


# Dependency Injection permitting mocked behavior of the SDK.
# If desirable, this must be a function that imports the module.
speechsdk_provider = None


def recognize_file_retry(run_recognizer_function):
    """
    Retry function for run_recognizer
    :param run_recognizer_function: run recognizer function wrapped
    :return: wrapped function
    """
    @wraps(run_recognizer_function)
    def wrapped(*args, **kwargs):
        """
        This is the wrapped function call, which is executed before run_recognizer
        :param args: args to run_recognizer
        :param kwargs: kwargs to run_recognizer
        :return: same as the wrapped function
        """
        num_retries_left = RECOGNIZER_SCOPE_RETRIES
        result: SpeechSDKWorkItemResult = None

        while num_retries_left > 0:
            result: SpeechSDKWorkItemResult = run_recognizer_function(*args, **kwargs)
            if result.passed or \
                    not result.can_retry or \
                    result.error_type == EndpointDownError.__name__:
                break
            num_retries_left = num_retries_left - 1

        result.attempts = RECOGNIZER_SCOPE_RETRIES - num_retries_left + 1
        return result
    return wrapped


@recognize_file_retry
def run_recognizer(request: SpeechSDKWorkItemRequest, rtf: float,
                   endpoint_config: dict, log_event_queue: LogEventQueue,
                   cancellation_token: multiprocessing.Event):
    """
    Perform continuous speech recognition from audio to text.
    :param request: request details
    :param rtf: the Real-Time-Factor with which to throttle the audio stream
                when pushing the stream to server.
    :param endpoint_config: about the endpoint to use
    :param log_event_queue: object for enqueueing events to be logged asap.
    :param cancellation_token: Event signalling that the work should be cancelled.
    :return: an instance of SpeechSDKWorkItemResult.
    """

    file_recognizer = FileRecognizer(request, rtf, endpoint_config, log_event_queue)
    return file_recognizer.recognize(cancellation_token)


class FileRecognizer:
    """
    Class that does speech recognition on one file at a time and uses a single endpoint.
    """
    def __init__(self, request: SpeechSDKWorkItemRequest,
                 rtf: float, endpoint_config: dict, log_event_queue: LogEventQueue):
        """
        Initialize the file recognizer
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
        self._language = endpoint_config["language"]
        self._log_event_queue = log_event_queue

        assert(request.language == self._language)
        self.request = request
        self._filepath = request.filepath
        self._nbest = request.nbest
        self._diarization = request.diarization
        self._profanity = request.profanity
        self._enable_sentiment = request.enable_sentiment
        self._output_folder = request.output_folder
        self._cache_search_dirs = list(request.cache_search_dirs)
        self._log_folder = request.log_dir
        self._allow_resume = request.allow_resume
        self._throttle = str(round(rtf * 100))

    def recognize(self, cancellation_token: multiprocessing.Event) -> SpeechSDKWorkItemResult:
        """
        Recognize wrapper that ensures that result dictionary is ultimately generated
        :return: SpeechSDKWorkItemResult instance, all details whether passed or failed.
        """
        self._log_event_queue.log(LogLevel.INFO,
                                  "Processing file: {0} on endpoint: {1}, with process: {2}".format(
                                        self._filepath,
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
            audio_duration, cached_filepath, json_data = self.get_cached_result(self._filepath, self._cache_search_dirs)
            cached = audio_duration is not None

            # If we did not pick up a cached result, then we need to call recognizer
            if not cached:
                audio_duration = self.__recognize_in_subproc(self._filepath, json_data, cancellation_token)
            else:
                # It is cached, but we need to make sure the result exists where we need it.
                _, target_filepath = get_input_output_file_names(self._filepath, self._output_folder)
                if target_filepath != cached_filepath:
                    shutil.copyfile(cached_filepath, target_filepath)

            passed = True
        except Exception as e:
            exception_details = traceback.format_exc()
            self._log_event_queue.log(LogLevel.DEBUG, "Exception details: {0}".format(exception_details))
            self._log_event_queue.log(LogLevel.WARNING,
                                      "{0} happened, must terminate the recognition."
                                      "Details: {1}".format(type(e).__name__, e))
            failed_reason = str(e)
            can_retry = e.__class__ in [EndpointDownError, FailedRecognitionError,
                                        ChildProcessError, CancellationTokenException, OSError]
            error_type = type(e).__name__

        latency = time.time() - start

        # Generate output
        return SpeechSDKWorkItemResult(
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
        Invoke the Speech SDK in a subprocess to recognize a single WAV file.
        This provides per-request process isolation to prevent unexpected terminal faults upstream.
        :param audio_file: original audio file to recognize
        :param json_data: any result from a previous run of this file even if it was a failed transcription
        """
        # Check that this endpoint is actually healthy.
        address, port = self._host.split("//")[1].split(":")
        if not SpeechSDKEndpointStatusChecker(self._log_event_queue). \
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
        work_proc.name = current_process().name + "__SDKRequestChildProc"
        work_proc.start()
        self._log_event_queue.log(LogLevel.DEBUG,
                                  "Starting FileRecognizer.__recognize() in subproc: {0}".format(work_proc.name))

        # We can't do event-driven waitpid() until we know what we're waiting for.
        # > 99.9% of the time the pid be available before a single yield.
        while work_proc.pid is None:
            time.sleep(0)  # yield tick

        _, status = os.waitpid(work_proc.pid, 0)

        self._log_event_queue.log(LogLevel.DEBUG,
                                  "Finished FileRecognizer.__recognize() in subproc: {0}".format(work_proc.name))

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

    def __recognize(self, audio_file, json_data, cancellation_token) -> float:
        """
        Use Microsoft Speech SDK to recognize a single WAV file
        :param audio_file: original audio file to recognize
        :param json_data: any result from a previous run of this file even if it was a failed transcription
        :param cancellation_token multiprocessing.Event: to signal early exit.
        :returns: If all went well, just an int containing the audio duration.
                  The json result would already be written to file in that case.
        """
        # Fail fast if work item has already been cancelled.
        if cancellation_token.is_set():
            msg = "Canceled {0} on process {1} targeting {2} by cancellation token (before start).".format(
                audio_file, current_process().name, self._host
            )
            self._log_event_queue.log(LogLevel.INFO, msg)
            raise CancellationTokenException(msg)

        json_result_list = list()
        done_event = Event()
        is_success = True
        error_details = None
        cancel_msg = None

        init_gstreamer()
        converted_audio_file, audio_duration = convert_audio(audio_file)
        assert audio_duration is not None

        if self._allow_resume and json_data is not None:
            start_offset_secs = json_data.get("LastProcessedOffsetInSeconds", 0.0)
            segment_results = json_data.get("SegmentResults", list())
            combined_results = json_data.get("CombinedResults", list())
        else:
            start_offset_secs = 0.0
            segment_results = list()
            combined_results = list()
        last_processed_offset_secs: float = start_offset_secs

        # @TODO: Confining module import to here limits terminal process fault risk to only the process
        # in which this method executes, at the tradeoff of measured 10-100 ms cpu. This lazy load of the
        # speech sdk means global static state of the library is totally confined. This can be removed
        # out of request scope if term faults are never caused by sdk.
        global speechsdk_provider
        if speechsdk_provider:
            speechsdk = speechsdk_provider()
        else:
            import azure.cognitiveservices.speech as speechsdk

        speech_config = speechsdk.SpeechConfig(host=self._host, subscription=self._subscription,
                                               speech_recognition_language=self._language)

        speech_config.request_word_level_timestamps()
        speech_config.set_service_property(
            name='speechcontext-PhraseOutput.Format',
            value='Detailed',
            channel=speechsdk.ServicePropertyChannel.UriQueryParameter
        )

        if self._enable_sentiment:
            speech_config.set_service_property(
                name='speechcontext-PhraseOutput.Detailed.Options',
                value='["Sentiment","WordTimings"]',
                channel=speechsdk.ServicePropertyChannel.UriQueryParameter
            )
            speech_config.set_service_property(
                name='speechcontext-phraseDetection.sentimentanalysis.enabled',
                value='true',
                channel=speechsdk.ServicePropertyChannel.UriQueryParameter
            )

            speech_config.set_service_property(
                name='speechcontext-phraseDetection.sentimentAnalysis.modelversion',
                value='latest',
                channel=speechsdk.ServicePropertyChannel.UriQueryParameter
            )
        else:
            speech_config.set_service_property(
                name='speechcontext-PhraseOutput.Detailed.Options',
                value='["WordTimings"]',
                channel=speechsdk.ServicePropertyChannel.UriQueryParameter
            )

        # Set up diarization with the SDK.
        if self._diarization != "None":
            speech_config.set_service_property(
                name='speechcontext-phraseDetection.speakerDiarization.mode',
                value=self._diarization,
                channel=speechsdk.ServicePropertyChannel.UriQueryParameter
            )

        # Set up profanity masking setting with the SDK.
        if self._profanity == "Removed":
            speech_config.set_profanity(speechsdk.ProfanityOption.Removed)
        elif self._profanity == "Raw":
            speech_config.set_profanity(speechsdk.ProfanityOption.Raw)
        else:
            speech_config.set_profanity(speechsdk.ProfanityOption.Masked)

        # Throttle to provided RTF. Throttle from the beginning.
        speech_config.set_property_by_name("SPEECH-AudioThrottleAsPercentageOfRealTime", self._throttle)
        speech_config.set_property_by_name("SPEECH-TransmitLengthBeforThrottleMs", "0")

        # Make the buffers larger than default.
        speech_config.set_property_by_name("SPEECH-MaxBufferSizeSeconds", "1800")

        if self._log_folder is not None:
            audio_file_basename = os.path.basename(audio_file)
            base, ext = os.path.splitext(audio_file_basename)
            log_filename = "{0}.{1}.sdk.log".format(os.path.join(base + "_" + ext[1:]), current_process().name)
            output_file_log = os.path.join(self._log_folder, log_filename)
            speech_config.set_property(speechsdk.PropertyId.Speech_LogFilename, output_file_log)
            speech_config.set_property_by_name("SPEECH-FileLogSizeMB", "10")

        if self._allow_resume and start_offset_secs > 0.0:
            self._log_event_queue.info("RESUMING file {0} from offset {1} seconds".format(
                audio_file_basename, start_offset_secs))
            callback = WavFileReaderCallback(
                filename=converted_audio_file,
                offset=start_offset_secs,  # in seconds
                log_event_queue=self._log_event_queue
            )
            stream = speechsdk.audio.PullAudioInputStream(callback, callback.audio_format())
            audio_config = speechsdk.audio.AudioConfig(stream=stream)
        else:
            audio_config = speechsdk.audio.AudioConfig(filename=converted_audio_file)

        speech_recognizer = speechsdk.SpeechRecognizer(speech_config=speech_config, audio_config=audio_config)

        def stop_continuous(evt):
            """
            callback that stops continuous recognition upon receiving an event 'evt'.
            :param evt: event listened to stop speech recognizing
            """
            nonlocal done_event, is_success, audio_file, error_details
            speech_recognizer.stop_continuous_recognition()
            done_event.set()
            if evt.result.reason == speechsdk.ResultReason.Canceled and \
                    evt.cancellation_details.reason == speechsdk.CancellationReason.Error:
                # WORKAROUND: For a known bug with erroneous InternalServerError returned by SRFrontEnd
                # only in sentiment mode at end of file:
                if self._enable_sentiment and last_processed_offset_secs >= audio_duration - 60:
                    # work-around taken. do not mark as failed.
                    pass
                else:
                    is_success = False
                    error_details = str(evt.cancellation_details)
                    self._log_event_queue.log(LogLevel.DEBUG,
                                              "Error transcribing {0}, details: {1}".format(audio_file, error_details))

        def audio_recognized(evt):
            """
            callback that catches the recognized result of audio from an event 'evt'.
            :param evt: event listened to catch recognition result.
            """
            nonlocal json_result_list, cancel_msg, cancellation_token, last_processed_offset_secs
            if evt.result.reason == speechsdk.ResultReason.RecognizedSpeech:
                # Append non empty ones to a list of jsons, we can have multiple results for longer audio
                if evt.result.json:
                    self._log_event_queue.log(
                        LogLevel.INFO,
                        "RECOGNIZED event for file: {0} sent by endpoint: {1} handled by process: {2}".format(
                            audio_file_basename, self._host, current_process().name
                        )
                    )
                    segment_json = json.loads(evt.result.json)
                    self._log_event_queue.log(LogLevel.DEBUG,
                                              "RECOGNIZED: {0}".format(segment_json))
                    json_result_list.append(segment_json)
                    last_processed_offset_secs = start_offset_secs + \
                        (float(segment_json["Offset"] + segment_json["Duration"]) / TIME_UNIT)

            # Early cancellation by the cancellation_token.
            if cancellation_token.is_set():
                cancel_msg = "Canceled {0} on process {1} targeting {2} by cancellation token (in middle).".format(
                    audio_file, current_process().name, self._host
                )
                self._log_event_queue.log(LogLevel.INFO, cancel_msg)
                done_event.set()

        # Connect callbacks to the events fired by the speech recognizer
        # Catch recognized result of audio file
        speech_recognizer.recognized.connect(audio_recognized)

        # Stop continuous recognition on canceled event, which we are guaranteed to get on a file
        speech_recognizer.canceled.connect(stop_continuous)
        speech_recognizer.session_stopped.connect(stop_continuous)

        # Recognizer recognizing...
        start_time = time.time()
        speech_recognizer.start_continuous_recognition()

        # Wait for the done event to be set, but check if it's because cancellation was triggered.
        done_event.wait()
        if cancellation_token.is_set():
            raise CancellationTokenException(cancel_msg)
        end_time = time.time()

        audio_file_basename, json_file = get_input_output_file_names(audio_file, self._output_folder)

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

    def populate_json_results(self, final_json, json_result_list, start_offset_secs: float):
        # If there were any results coming back (it was not all silence), put them together
        def fix_offsets(json_entry):
            """
            Correct offset with respect to actual start_offset_secs, and
            add offset and duration in seconds.
            """
            start_offset = int(start_offset_secs * TIME_UNIT)
            if "Offset" in json_entry:
                json_entry["Offset"] += start_offset
                json_entry["OffsetInSeconds"] = json_entry["Offset"] / TIME_UNIT
            if "Duration" in json_entry:
                json_entry["DurationInSeconds"] = json_entry["Duration"] / TIME_UNIT

        if len(json_result_list) > 0:
            lexical = list()
            itn = list()
            masked_itn = list()
            display = list()
            for json_result in json_result_list:
                json_result["ChannelNumber"] = None
                if "NBest" in json_result:
                    nbest_list = json_result["NBest"]
                    # Reduce the NBest list to the number specified on cmd line (default is 1)
                    json_result["NBest"] = nbest_list[0:self._nbest]

                    top_result = nbest_list[0]
                    lexical.append(top_result["Lexical"])
                    itn.append(top_result["ITN"])
                    masked_itn.append(top_result["MaskedITN"])
                    display.append(top_result["Display"])
                    if "Words" in top_result:
                        for word in top_result["Words"]:
                            fix_offsets(word)

                fix_offsets(json_result)
                final_json["SegmentResults"].append(json_result)

            # Also put together the combined results
            final_json["CombinedResults"].append({
                "ChannelNumber": None,
                "Lexical": " ".join(lexical),
                "ITN": " ".join(itn),
                "MaskedITN": " ".join(masked_itn),
                "Display": " ".join(display),
            })
        else:
            # If there were no segments recognized, show the combined result
            # fields but with empty strings.
            final_json["CombinedResults"].append({
                "ChannelNumber": None,
                "Lexical": "",
                "ITN": "",
                "MaskedITN": "",
                "Display": "",
            })

    def get_cached_result(self, audio_file, dirs: List[str]):
        """
        Check output folder for a matching JSON file. If found, make sure that sha256 hash of the audio file matches
        the information in the JSON file, i.e. that this is the same audio for which we have the result cached.
        :param audio_file: input audio file to check
        :param dirs: directories where result JSON potentially already exists
        :return: (<audio duration, if find a successful existing result file that matches audio hash, otherwise None> ,
                  <result filepath, if find a successful existing result file that matches audio hash, otherwise None> ,
                  <previously existing audio result json w/ matching hash, partial or success, if any, otherwise None>)
        """
        for d in dirs:
            audio_file_basename, json_file = get_input_output_file_names(audio_file, d)
            if os.path.isfile(json_file):
                with open(json_file, encoding="utf-8") as jf:
                    json_data = json.load(jf)
                    if "AudioFileResults" in json_data and len(json_data["AudioFileResults"]) == 1 and \
                            "AudioFileHash" in json_data["AudioFileResults"][0]:
                        current_hash = sha256_checksum(audio_file)
                        json_data = json_data["AudioFileResults"][0]
                        if current_hash == json_data["AudioFileHash"]:
                            if "TranscriptionStatus" in json_data \
                              and json_data["TranscriptionStatus"] == "Succeeded":
                                self._log_event_queue.log(
                                    LogLevel.INFO,
                                    "Found cached result for {0} in {1} with valid hash {2}".format(
                                        audio_file_basename,
                                        json_file,
                                        current_hash))
                                return json_data["AudioLengthInSeconds"], json_file, json_data
                            else:
                                return None, None, json_data
                        else:
                            self._log_event_queue.log(
                                LogLevel.WARNING,
                                "Caching Unusable: Found result for {0} in {1} with MISMATCHED hash {2} != {3}".format(
                                    audio_file_basename,
                                    json_file,
                                    current_hash,
                                    json_data["AudioFileHash"]
                                )
                            )
        return None, None, None
