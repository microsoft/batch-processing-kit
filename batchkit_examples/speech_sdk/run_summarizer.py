# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

import json
import time
from typing import Dict
import logging

from batchkit.constants import ORCHESTRATOR_SCOPE_MAX_RETRIES
from batchkit.run_summarizer import BatchRunSummarizer
from .work_item import SpeechSDKWorkItemResult


logger = logging.getLogger("batch")


class SpeechSDKBatchRunSummarizer(BatchRunSummarizer):
    def __init__(self):
        super().__init__()

    def run_summary(self,
                    snap_work_results: Dict[str, SpeechSDKWorkItemResult],
                    snap_file_queue_size: int,
                    snap_num_running: int,
                    start_time: float,
                    num_endpoints: int,
                    log_conclusion_msg: bool) -> dict:

        work_results_list = list(snap_work_results.values())

        # Compute some metrics about the run
        total_audio = sum(
            item.audio_duration for item in work_results_list if
            item.passed and not item.cached and item.audio_duration is not None)
        num_passed = sum(item.passed for item in work_results_list)
        num_cached = sum(item.cached for item in work_results_list)
        # Files that have failed and are not being retried:
        failed_files = [(item.endpoint, item.filepath) for item in work_results_list if
                        not item.passed and
                        (item.attempts - 1 >= ORCHESTRATOR_SCOPE_MAX_RETRIES or not item.can_retry)]
        # Files that have failed at least once but are being retried:
        in_retry_files = [(item.endpoint, item.filepath) for item in work_results_list if
                          not item.passed and
                          item.can_retry and
                          item.attempts - 1 < ORCHESTRATOR_SCOPE_MAX_RETRIES]
        num_failed_terminal = len(failed_files)
        num_in_retry = len(in_retry_files)
        elapsed = time.time() - start_time
        decode_ratio = total_audio / elapsed
        num_in_que = snap_file_queue_size
        num_total = num_passed + num_failed_terminal + num_in_que + snap_num_running
        total_retries = 0
        work_results_list = [w.to_dict() for w in work_results_list]
        for w in work_results_list:
            w['retries'] = w['attempts'] - 1
            total_retries += w['retries']
            del w['attempts']

        final_json = {
            "overall_summary": {
                "audio_stats": {
                    "time_elapsed_seconds": elapsed,
                    "total_decoded_seconds": total_audio,
                    "num_endpoints": num_endpoints,
                    "decode_ratio": decode_ratio,
                },
                "file_stats": {
                    "passed": num_passed,
                    "failed": num_failed_terminal,
                    "in_retry": num_in_retry,
                    "queued": num_in_que,
                    "running": snap_num_running,
                    "cached": num_cached,
                    "total": num_total
                }
            },
            "processed_files": work_results_list
        }

        if log_conclusion_msg:
            logger.info("Output list:")
            for item in work_results_list:
                logger.info("Result for: {0}: {1}".format(item['filepath'], json.dumps(item, indent=2, sort_keys=True)))
            logger.info("Decoded {0:.2f} seconds in {1:.2f} seconds on {2} decoders for a ratio of {3:.2f}".format(
                total_audio,
                elapsed,
                num_endpoints,
                decode_ratio))
            logger.info("Processed {0}/{1} files, {2} passed, {3} fetched from cache and {4} failed.".format(
                len(work_results_list),
                num_total,
                num_passed,
                num_cached,
                num_failed_terminal))
            logger.info("Total retries: {0}".format(total_retries))
            if num_failed_terminal > 0:
                logger.info("\nFailed recognitions ({0}):\n".format(num_failed_terminal))
                logger.info(" {:<30}{}".format("END POINT", "AUDIO FILE"))
                for end_point, audio_file in failed_files:
                    logger.info(" {:<30}{}".format(end_point, audio_file))

        return final_json