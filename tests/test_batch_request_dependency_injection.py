import logging
import sys
import multiprocessing
from multiprocessing import RLock
from argparse import Namespace
from typing import List, Dict
from unittest import TestCase

from batchkit.batch_config import BatchConfig
from batchkit.batch_request import BatchRequest
from batchkit.logger import LogEventQueue
from batchkit.run_summarizer import BatchRunSummarizer
from batchkit.work_item import WorkItemRequest, WorkItemResult
from batchkit.work_item_processor import WorkItemProcessor, StubWorkItemProcessor

logger = logging.getLogger("test_dependency_injection")
# logger.level = logging.DEBUG
logger.level = logging.INFO
log_stream_handler = logging.StreamHandler(sys.stdout)


class TBatchConfig(BatchConfig):
    def __init__(self, some_int: int, some_bool: bool):
        super().__init__()
        self.some_int = some_int
        self.some_bool = some_bool

    @staticmethod
    def from_args(args: Namespace):
        return TBatchConfig(
            args.some_int,
            args.some_bool
        )


class TWorkItemRequest(WorkItemRequest):
    def __init__(self, filepath, output_dir, some_int):
        super().__init__(filepath)
        self.output_dir = output_dir
        self.some_int = some_int

    def process_impl(self, endpoint_config: dict, rtf: float,
                     log_event_queue: LogEventQueue, cancellation_token: multiprocessing.Event,
                     global_workitem_lock: RLock):
        log_event_queue.info("Did the work")


class TBatchRunSummarizer(BatchRunSummarizer):
    def __init__(self):
        super().__init__()

    def run_summary(self, snap_work_results: Dict[str, WorkItemResult], snap_file_queue_size: int,
                    snap_num_running: int, start_time: float, num_endpoints: int, log_conclusion_msg: bool) -> dict:
        pass


class TBatchRequest(BatchRequest):
    def __init__(self, files: List[str], some_int: int, some_bool: bool):
        super().__init__(files)
        self.some_int = some_int
        self.some_bool = some_bool

    @staticmethod
    def is_valid_input_file(file: str) -> bool:
        return True

    def get_batch_run_summarizer(self) -> BatchRunSummarizer:
        return TBatchRunSummarizer()

    @staticmethod
    def from_json(json: dict):
        assert "files" in json
        assert "some_int" in json
        assert "some_bool" in json
        return TBatchRequest(json['files'], json['some_int'], json['some_bool'])

    @staticmethod
    def from_config(files: List[str], config: TBatchConfig):
        return TBatchRequest(files, config.some_int, config.some_bool)

    def make_work_items(self, output_dir: str,
                        cache_search_dirs: List[str],
                        log_dir: str) -> List[TWorkItemRequest]:
        return [TWorkItemRequest(f, output_dir, self.some_int) for f in self.files]

    @staticmethod
    def get_work_item_processor() -> WorkItemProcessor:
        return StubWorkItemProcessor()


class DependencyInjectionTestCase(TestCase):
    def test_batch_request_finds_subtypes(self):
        assert TBatchRequest in BatchRequest.find_subtypes()

    def test_batch_request_from_json(self):
        req: BatchRequest = BatchRequest.from_json({
            "type": "TBatchRequest",
            "files": ["/a/b/c", "/d/e/f"],
            "some_int": 1,
            "some_bool": True,
        })
        assert issubclass(type(req), BatchRequest)
        assert type(req) == TBatchRequest

        work: List[WorkItemRequest] = req.make_work_items(
            "/output/dir",
            ["/output/dir", "/another/cache/search/dir"],
            "/log/dir"
        )

        assert len(work) == 2
        work_item_files = set([w.filepath for w in work])
        assert work_item_files == {"/a/b/c", "/d/e/f"}

    def test_batch_request_from_config(self):
        files = ["/a/b/c", "/d/e/f", "/g/h/i"]
        config: BatchConfig = TBatchConfig(2, False)
        req: BatchRequest = BatchRequest.from_config(files, config)
        assert issubclass(type(req), BatchRequest)
        assert type(req) == TBatchRequest

        work: List[WorkItemRequest] = req.make_work_items(
            "/output/dir",
            ["/output/dir", "/another/cache/search/dir"],
            "/log/dir"
        )

        assert len(work) == 3
        work_item_files = set([w.filepath for w in work])
        assert work_item_files == {"/a/b/c", "/d/e/f", "/g/h/i"}
        for w in work:
            assert type(w) == TWorkItemRequest
            assert w.output_dir == "/output/dir"
            assert w.some_int == 2

