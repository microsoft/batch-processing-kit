import time
from argparse import Namespace
from collections import namedtuple
from ctypes import cdll
import sys
import os
import json
from multiprocessing import Event
from typing import Tuple
import yaml
import tempfile
from random import random
from functools import partial
import shutil
from threading import Thread

from batchkit.client import run
import batchkit.orchestrator
import batchkit.utils as utils
import batchkit.endpoint_manager
import batchkit_examples.speech_sdk.recognize
import batchkit_examples.speech_sdk.run_summarizer
import batchkit_examples.speech_sdk.endpoint_status
import batchkit_examples.speech_sdk.audio
from batchkit_examples.speech_sdk.batch_config import SpeechSDKBatchConfig
from batchkit_examples.speech_sdk.parser import parse_cmdline


NUM_AUDIO_FILES = 5000

# Failure probabilities to do with the SDK.
PROB_SEGV = 0.05
PROB_RAISE_FAILED_RECO = 0.05
PROB_ENDPOINT_DOWN = 0.05
PROB_ERROR_EVENT = 0.05

# What fraction of files will take non-negligible time to transcribe
# (for testing intermediate cancellation tokens).
LONG_LIVED_TRANSCRIPTION_FRAC = 0.05
LONG_LIVED_TRANSCRIPTION_TIME = 3  # seconds

# What fraction of files will ultimately deadlock, i.e. emulate Speech SDK malfunctions
# and there is never any callback.
DEADLOCKED_TRANSCRIPTION_FRAC = 0.02
DEADLOCKED_TRANSCRIPTION_TIMEOUT = LONG_LIVED_TRANSCRIPTION_TIME + 3  # Used to override SPEECHSDK_RESULT_TIMEOUT

# Failure probability to emulate an unreliable filesystem (e.g. SMB share)
# for ops other than reads/writes like rename, unlink, copy.
PROB_IO_OSERROR = 0.05

# How often to partially change the endpoint config in seconds.
HOT_CONFIG_TURNOVER = 5

# Hot configuration changes are for the configuration file here.
ENDPOINTS_CONFIG_FILE_INITIAL = 'tests/configs/testmockedendpoints.yaml'
ENDPOINTS_CONFIG_FILE_LIVE = 'testmockedendpoints.yaml'

# How often to write a new audio file when testing for daemon mode.
NEW_AUDIO_FILE_PERIOD = 0.01


# Re-use the same audio wav many times.
test_root = os.path.dirname(os.path.realpath(__file__))
sample = os.path.join(
    test_root,
    'resources/whatstheweatherlike.wav'
)
audio_duration = batchkit_examples.speech_sdk.audio.check_audio_file(sample)

# Read some json that we can produce for each segment final result (recognized event).
sample_segment = os.path.join(
    test_root,
    'resources/whatstheweatherlike.json'
)
with open(sample_segment, 'r') as f:
    sample_segment_json = json.dumps(
        json.load(f)['AudioFileResults'][0]['SegmentResults'][0]
    )

# For getting a segv.
test_root = os.path.dirname(os.path.realpath(__file__))
libblah_path = os.path.join(test_root, 'resources/libsegv.so')


def check_server(*args, **kwargs):
    return True


check_audio_file_original = batchkit_examples.speech_sdk.audio.check_audio_file
convert_audio_original = batchkit_examples.speech_sdk.audio.convert_audio


def check_audio_file(*args) -> float:
    return audio_duration


def convert_audio(audio_file: str, *args) -> Tuple[str, float]:
    return audio_file, audio_duration


def contrived_audio_files_oneshot(num_audio_files, basepath, *args):
    return set([os.path.join(basepath, 'whatstheweatherlike_{0}.wav'.format(i))
                for i in range(num_audio_files)])


# First third of files will be present before the run.
def contrived_audio_files_daemon_init(num_audio_files, basepath, *args):
    s = set()
    superset = contrived_audio_files_oneshot(num_audio_files, basepath)
    target_size = int(0.333*len(superset))
    for cnt in range(target_size):
        s.add(superset.pop())
    return s


# Other two thirds are dropped as new audio files during the run.
def contrived_audio_files_daemon_incremental(num_audio_files, basepath, *args):
    return contrived_audio_files_oneshot(num_audio_files, basepath).difference(
        contrived_audio_files_daemon_init(num_audio_files, basepath))


def speechsdk_provider():

    import azure.cognitiveservices.speech as speechsdk

    def maybe_segv():
        if random() < PROB_SEGV:
            lib = cdll.LoadLibrary(libblah_path)
            lib.foo()

    def maybe_raise():
        if random() < PROB_RAISE_FAILED_RECO:
            raise utils.FailedRecognitionError("raised in mock")
        if random() < PROB_ENDPOINT_DOWN:
            raise utils.EndpointDownError("raised in mock")

    class EventCallable(object):
        def __init__(self):
            self._cobs = []  # Array of callables (w/ event details)
        def connect(self, fn):
            self._cobs.append(fn)
        def on_event(self, event):
            for fn in self._cobs:
                maybe_segv()
                maybe_raise()
                fn(event)

    Result = namedtuple("Result", ["reason", "json"])
    class Event(object):
        def __init__(self):
            self.result = Result("no_reason", "no_json")
            self.cancellation_details = Result("no_reason", "no_json")

        @staticmethod
        def RecognizedEvent():
            evt = Event()
            evt.result = Result(
                speechsdk.ResultReason.RecognizedSpeech,
                sample_segment_json
            )
            return evt

        @staticmethod
        def SessionStoppedEvent():
            evt = Event()
            evt.result = Result(
                speechsdk.ResultReason.Canceled,
                ""
            )
            evt.cancellation_details = Result(
                speechsdk.CancellationReason.EndOfStream,
                "reached EOS and all is well"
            )
            return evt

        @staticmethod
        def SessionErrorEvent():
            evt = Event()
            evt.result = Result(
                speechsdk.ResultReason.Canceled,
                ""
            )
            evt.cancellation_details = Result(
                speechsdk.CancellationReason.Error,
                "error details"
            )
            return evt

    class SpeechRecognizer(object):
        def __init__(self, *args, **kwargs):
            self.recognized = EventCallable()
            self.canceled = EventCallable()
            self.session_stopped = EventCallable()

        def stop_continuous_recognition(self):
            pass

        def start_continuous_recognition(self):
            maybe_segv()
            maybe_raise()
            if random() < PROB_ERROR_EVENT:
                # By some chance we cancel due to error
                self.canceled.on_event(Event.SessionErrorEvent())
                self.session_stopped.on_event(Event.SessionErrorEvent())
            else:
                # We have a successful outcome and just one utterance.
                if random() < LONG_LIVED_TRANSCRIPTION_FRAC:
                    time.sleep(LONG_LIVED_TRANSCRIPTION_TIME)
                if random() < DEADLOCKED_TRANSCRIPTION_FRAC:
                    # Emulate that the speech sdk library never invokes the callbacks
                    # ever again for this session.
                    return
                self.recognized.on_event(Event.RecognizedEvent())
                maybe_segv()
                maybe_raise()
                self.session_stopped.on_event(Event.RecognizedEvent())

    speechsdk.SpeechRecognizer = SpeechRecognizer
    return speechsdk


# We'll get digest from the name since we're picking them.
def sha256checksum(audiofile):
    res = 0
    for i in audiofile.split():
        if i.isdigit():
            res *= 10
            res += int(i)
    return res


# Used to simulate live modifications of the endpoints config.
def modify_config(orig_config_path, live_config_path, cancellation: Event):
    # Original (starting) config.
    with open(orig_config_path) as f:
        orig_config = yaml.load(f, Loader=yaml.FullLoader)
    while True:
        live_config = {}
        # Randomly remove half of the endpoints.
        for ep_k, ep_v in orig_config.items():
            if random() < 0.50:
                live_config[ep_k] = ep_v
        with open(live_config_path, 'w') as o:
            yaml.dump(live_config, o)
        if cancellation.wait(HOT_CONFIG_TURNOVER):
            return


class UnstableSDKTestCase(object):
    def test_e2e_unstable_mocked_sdk(self, daemon_mode: bool = False):

        os.environ['PROB_IO_OSERROR'] = str(PROB_IO_OSERROR)

        batchkit_examples.speech_sdk.recognize.sha256_checksum = sha256checksum
        batchkit_examples.speech_sdk.recognize.speechsdk_provider = speechsdk_provider
        batchkit_examples.speech_sdk.recognize.SpeechSDKEndpointStatusChecker.check_endpoint = check_server
        batchkit_examples.speech_sdk.endpoint_status.SpeechSDKEndpointStatusChecker.check_endpoint = check_server
        batchkit_examples.speech_sdk.recognize.SPEECHSDK_RESULT_TIMEOUT = DEADLOCKED_TRANSCRIPTION_TIMEOUT

        # We only cause transient errors so eventually all files pass.
        batchkit_examples.speech_sdk.recognize.RECOGNIZER_SCOPE_RETRIES = 2
        batchkit.orchestrator.ORCHESTRATOR_SCOPE_MAX_RETRIES = 200

        with tempfile.TemporaryDirectory() as tempdir_out:
            with tempfile.TemporaryDirectory() as tempdir_log:
                with tempfile.TemporaryDirectory() as tempdir_scratch:
                    with tempfile.TemporaryDirectory() as tempdir_in:

                        # In testing daemon mode, we use real files for both the initial and incremental files.
                        # In testing oneshot mode, we just fake the existence of these files.
                        if not daemon_mode:
                            _contrived_audio_files_oneshot_fn = partial(contrived_audio_files_oneshot, NUM_AUDIO_FILES,
                                                                        tempdir_in)
                            batchkit.client.get_input_files = _contrived_audio_files_oneshot_fn
                            batchkit_examples.speech_sdk.audio.check_audio_file = check_audio_file
                            batchkit_examples.speech_sdk.recognize.convert_audio = convert_audio
                        else:
                            _contrived_audio_files_daemon_init = contrived_audio_files_daemon_init(
                                NUM_AUDIO_FILES, tempdir_in)
                            _contrived_audio_files_daemon_incremental = contrived_audio_files_daemon_incremental(
                                NUM_AUDIO_FILES, tempdir_in)

                            # Set a maximum number of files that can ever be submitted by DaemonClient
                            # over all batches collectively, otherwise DaemonClient would never return from run().
                            batchkit.client.constants.DAEMON_MODE_MAX_FILES = NUM_AUDIO_FILES

                            # Actual implementations.
                            batchkit.client.get_input_files = utils.get_input_files
                            batchkit_examples.speech_sdk.audio.check_audio_file = check_audio_file_original
                            batchkit_examples.speech_sdk.recognize.convert_audio = convert_audio_original

                        # Reflect on batch-client src root directory.
                        root = os.path.join(os.path.dirname(os.path.realpath(__file__)), '..')
                        sys.path.append(root)

                        # Where to find the original endpoints config and where the live one is placed
                        # during runtime.
                        start_config_path = os.path.join(root, ENDPOINTS_CONFIG_FILE_INITIAL)
                        live_config_path = os.path.join(tempdir_out, ENDPOINTS_CONFIG_FILE_LIVE)

                        # Write initial endpoints config.
                        shutil.copyfile(start_config_path, live_config_path)

                        # Create a background thread that will make small modifications
                        # to the config periodically to simulate consumer changes.
                        cancellation = Event()
                        Thread(
                            target=modify_config,
                            args=(start_config_path, live_config_path, cancellation),
                            daemon=True
                        ).start()

                        if daemon_mode:
                            audio_file = os.path.join(root, "tests/resources/whatstheweatherlike.wav")
                            for a in _contrived_audio_files_daemon_init:
                                shutil.copyfile(audio_file, a)

                        args = [
                            '-config', live_config_path,
                            '-output_folder', tempdir_out,
                            '-input_folder', tempdir_in,
                            '-scratch_folder', tempdir_scratch,
                            '-log_folder', tempdir_log,
                            '-console_log_level', 'INFO',  # DEBUG
                            '-file_log_level', 'DEBUG',
                            '-nbest', '1',
                            '-m', 'DAEMON' if daemon_mode else 'ONESHOT',
                            '-profanity', 'Raw',
                            '--store-combined-json',
                            '--poll'  # watcher also applied; tests works with both at same time.
                        ]

                        if daemon_mode:
                            audio_file = os.path.join(root, "tests/resources/whatstheweatherlike.wav")
                            def drop_file_emulation():
                                for contrived in _contrived_audio_files_daemon_incremental:
                                    shutil.copyfile(audio_file, contrived)
                                    time.sleep(NEW_AUDIO_FILE_PERIOD)
                            file_drop_thread = Thread(
                                target=drop_file_emulation,
                                args=(),
                                daemon=False
                            )
                            file_drop_thread.start()

                        parsed_args: Namespace = parse_cmdline(args)
                        parsed_args.language = 'en-US'
                        run(parsed_args, SpeechSDKBatchConfig)
                        cancellation.set()

                        if daemon_mode:
                            file_drop_thread.join()
                            all_contrived = _contrived_audio_files_daemon_init.union(
                                _contrived_audio_files_daemon_incremental)
                        else:
                            all_contrived = _contrived_audio_files_oneshot_fn()

                        # Check all the results are there according to run_summary.json
                        files_done = set()
                        run_summ_json = os.path.join(tempdir_out, 'run_summary.json')
                        with open(run_summ_json, 'r') as r:
                            res = json.load(r)
                            for p in res['processed_files']:
                                assert p['passed']
                                files_done.add(p['filepath'])
                        assert files_done == all_contrived

                        # Check individual file results are in the output dir as anticipated.
                        for r in all_contrived:
                            single_result_json = os.path.basename(r).replace(".wav", "_wav.json")
                            single_result_json = os.path.join(tempdir_out, single_result_json)
                            assert os.path.isfile(single_result_json)

                        # Check they are all in the combined result.
                        combined_json = os.path.join(tempdir_out, 'results.json')
                        with open(combined_json, 'r') as c:
                            res = json.load(c)
                            res = res['AudioFileResults']
                            assert len(res) == len(all_contrived)
                            audio_file_urls = {single['AudioFileUrl'] for single in res}
                            assert audio_file_urls == all_contrived

        print("Test passed!")
        return True
