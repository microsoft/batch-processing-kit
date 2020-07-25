# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.
import gi
import os
import tempfile
import wave
import azure.cognitiveservices.speech as speechsdk

from batchkit.logger import LogEventQueue, LogLevel

gi.require_version('Gst', '1.0')
# noinspection PyUnresolvedReferences
from gi.repository import GObject, Gst

native_extensions = [".wav"]
conversion_extensions = [".mp3", ".flac", ".ogg", ".opus", ".alaw", ".mulaw"]


class InvalidAudioFormatError(Exception):
    """
    Raised when we could not process the audio format properly
    """
    pass


def init_gstreamer():
    """
    Initialize GStreamer objects necessary for conversion
    :return: None
    """
    GObject.threads_init()
    Gst.init(None)


def gstreamer_convert(input_file, output_file, leq: LogEventQueue, audio_format=None):
    """
    Convert audio from input codec to wave file
    :param input_file: input file in some other audio format
    :param output_file: file where wave format will be stored
    :param leq: for logging
    :param audio_format: audio file format
    :return: whether conversion succeeded or not
    """
    pipeline_str_template = (
        'filesrc location="{0}" '
        '! {1} '
        '! audioconvert '
        '! audioresample '
        '! audio/x-raw,format=S16LE,rate=16000,channels=1 '
        '! wavenc '
        '! filesink location="{2}"'
    )

    if audio_format is None:
        audio_format = os.path.splitext(input_file)[1].lower()

    parse_decode_str = get_parser_decoder(audio_format, leq)
    if parse_decode_str is None:
        raise InvalidAudioFormatError("Invalid source audio format {0} for file {1}".format(
            audio_format[1:], input_file))

    pipeline_str = pipeline_str_template.format(input_file, parse_decode_str, output_file)
    leq.info("Converting audio file {0} to {1} using following gstreamer pipeline:\n \'gst-launch-1.0 {2}\'".format(
        input_file, output_file, pipeline_str))
    pipeline = Gst.parse_launch(pipeline_str)
    pipeline.set_state(Gst.State.PLAYING)
    bus = pipeline.get_bus()
    message = bus.timed_pop_filtered(Gst.CLOCK_TIME_NONE, Gst.MessageType.ERROR | Gst.MessageType.EOS)
    pipeline.set_state(Gst.State.NULL)

    if message.type != Gst.MessageType.EOS:
        raise InvalidAudioFormatError(
            "Unable to convert the file {0} to wav using source audio format {1}".format(
                input_file, audio_format[1:]))


def get_parser_decoder(audio_format, leq: LogEventQueue):
    """
    Get the parsing, decoding and conversion GStreamer pipeline string for a given
    audio file (based on its extension)
    :param audio_format: format of the file to convert
    :param leq: for logging
    :return: pipeline string
    """
    if audio_format == ".flac":
        return "flacparse ! flacdec"
    elif audio_format == ".mulaw":
        return "wavparse ! mulawdec"
    elif audio_format == ".alaw":
        return "wavparse ! alawdec"
    elif audio_format == ".opus":
        return "oggdemux ! opusdec"
    elif audio_format == ".ogg":
        return "oggdemux ! vorbisdec"
    elif audio_format == ".mp3":
        return "mpegaudioparse ! mpg123audiodec"
    else:
        leq.error("Unsupported audio conversion source format found {0}".format(audio_format))
        return None


def convert_audio(audio_file, leq: LogEventQueue):
    """
    Convert audio file if necessary
    :param audio_file: file to process
    :param leq: for logging
    :return:
    """
    ext = os.path.splitext(audio_file)[1].lower()

    # Patch the extension for the formats embedded in the wav container
    if ext == ".wav":
        try:
            wave.open(audio_file)
        except Exception as e:
            if str(e) == "unknown format: 7":
                ext = ".mulaw"
            elif str(e) == "unknown format: 6":
                ext = ".alaw"

    # Now convert the audio, if necessary
    if ext in conversion_extensions:
        _, converted_file = tempfile.mkstemp()
        gstreamer_convert(audio_file, converted_file, leq, ext)
    elif ext == ".wav":
        converted_file = audio_file
    else:
        raise InvalidAudioFormatError("Invalid source audio format {0} for file {1}".format(
            ext[1:], audio_file))

    audio_duration = check_audio_file(converted_file)
    return converted_file, audio_duration


def check_audio_file(audio_file):
    """
    Check if the audio file contents and format match the needs of the speech service. Currently we only support
    16 KHz, 16 bit, MONO, PCM audio format. All others will be rejected.
    :param audio_file: file to check
    :return: audio duration, if file matches the format expected, otherwise None
    """
    # Verify that all wave files are in the right format
    try:
        with wave.open(audio_file) as my_wave:
            frame_rate = my_wave.getframerate()
            if frame_rate >= 8000 and my_wave.getnchannels() in [1, 2] \
                    and my_wave.getsampwidth() == 2 and my_wave.getcomptype() == 'NONE':
                audio_duration = my_wave.getnframes() / frame_rate

                return audio_duration
            else:
                raise InvalidAudioFormatError(
                    "File {0} is not in the right format, it must be: Mono/Stereo, 16bit, PCM, 8KHz or above. "
                    "Found: ChannelCount={1}, SampleWidth={2}, CompType={3}, FrameRate={4}. Ignoring input!".format(
                        audio_file,
                        my_wave.getnchannels(),
                        my_wave.getsampwidth(),
                        my_wave.getcomptype(),
                        frame_rate
                    )
                )
    except Exception as e:
        raise InvalidAudioFormatError("Invalid wave file {0}, reason: {1} :{2}".format(audio_file, type(e).__name__, e))


# @TODO: We should be doing this based on file headers and not based on extensions
def is_valid_audio_file(file_path):
    """
    Checks if this file qualifies as an audio file we should process
    :param file_path: file to check
    :return: True or False
    """
    supported_extensions = native_extensions + conversion_extensions
    return os.path.isfile(file_path) and os.path.splitext(file_path)[1].lower() in supported_extensions


class WavFileReaderCallback(speechsdk.audio.PullAudioInputStreamCallback):
    """
    Class that implements the Pull Audio Stream interface to recognize speech from an audio file
    """
    def __init__(self, filename: str, offset: float, log_event_queue: LogEventQueue):
        super().__init__()
        self.filename = filename
        self._file_h = wave.open(filename, mode=None)
        self._log_event_queue = log_event_queue
        self.sample_width = self._file_h.getsampwidth()
        self.frame_rate = self._file_h.getframerate()
        self.num_channels = self._file_h.getnchannels()
        self.compression_type = self._file_h.getcomptype()
        self.num_frames = self._file_h.getnframes()
        assert self.compression_type == 'NONE'

        if offset > 0.0:
            self._file_h.setpos(self.wave_position_from_offset(offset))

    def read(self, buffer: memoryview) -> int:
        """
        Read callback function
        """
        size = buffer.nbytes
        frames = self._file_h.readframes(size // self.sample_width)

        buffer[:len(frames)] = frames

        return len(frames)

    def close(self):
        """
        Close callback function
        """
        self._file_h.close()

    def audio_format(self):
        return speechsdk.audio.AudioStreamFormat(
            samples_per_second=self.frame_rate,
            bits_per_sample=self.sample_width*8,
            channels=self.num_channels
        )

    def wave_position_from_offset(self, offset: float) -> int:
        wave_units_per_second = self.frame_rate
        start_frame = int(offset * wave_units_per_second)
        self._log_event_queue.log(
            LogLevel.INFO, "Restarting {0} from frame {1} out of {2} frames".format(
                self.filename, start_frame, self.num_frames))
        return start_frame
