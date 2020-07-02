# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

import wave
import azure.cognitiveservices.speech as speechsdk

from batchkit.logger import LogEventQueue, LogLevel


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
