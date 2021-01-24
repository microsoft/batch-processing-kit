from batchkit.work_item import WorkItemRequest, WorkItemResult
from batchkit.utils import kill_children_procs, NonDaemonicPool
from multiprocessing import Barrier, Value, Event
import ctypes


count_remaining = Value(ctypes.c_int32, 0)
done_evt = Event()


def process_entrypoint():
    print("proc created.")


def work_item_entrypoint():
    print("entering.")
    from . import pb2
    from .pb2 import (
        LIDRequestMessage, AudioConfig, LanguageIdStub, IdentifierConfig_pb2, IdentifierConfig, FinalResultMessage
    )
    print("finished.")


def pool_error_callback(exception: BaseException):
    msg = "EndpointManager failure in a WorkItemRequest: {0}\n{1}".format(
        type(exception).__name__, exception.__traceback__)
    print("fatal: ", msg)
    kill_children_procs()
    exit(1)


def pool_callback(self, result: WorkItemResult):
    with count_remaining.get_lock():
        count_remaining.value -= 1
        if count_remaining.value == 0:
            done_evt.set()
    print("pool_callback success")


# Main entry point
if __name__ == '__main__':
    pool_size = 1
    num_items = 1
    proc_pool: NonDaemonicPool = NonDaemonicPool(pool_size, process_entrypoint)
    count_remaining.value = num_items

    for itemno in range(num_items):
        proc_pool.apply_async(
            work_item_entrypoint,
            [],
            callback=pool_callback,
            error_callback=pool_error_callback
        )

    done_evt.wait()
    print("Done.")


