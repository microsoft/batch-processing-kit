from enum import Enum

class MultilanguageBatchStage(Enum):
    STARTING = 0
    LANG_SEGMENT = 1
    PER_LANG_STT = 2
    STITCHING = 3
    DONE = 4
