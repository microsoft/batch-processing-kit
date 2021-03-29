import json
from threading import Thread
import multiprocessing
import os
from typing import List, Optional, Dict, Set
import traceback

from batchkit.utils import write_json_file_atomic
from batchkit_examples.speech_sdk.multilanguage.stage import MultilanguageBatchStage

class UnifiedRunSummarizer(Thread):
    """
    Writes the unified run summary for a one-shot multi-language STT batch.
    After instantiated, it should be updated with set_stage() accordingly.
    Once start() is called, it will automatically write updated run summaries
    periodically. Call finish_autoloop() to stop automatic run summaries and
    make the Thread joinable. Even after finish_autoloop() is invoked,
    you can still invoke iterate() on the current thread. Example:

        ```
        urs = UnifiedRunSummarized(..)
        urs.start()
        ...
        urs.set_stage(MultiLanguageBatchStage.PER_LANG_STT)
        ...
        urs.set_stage(MultiLanguageBatchStage.STITCHING)
        ...
        urs.set_stage(MultiLanguageBatchStage.DONE)
        ...
        urs.finish_autoloop()
        urs.join()
        urs.iterate()  # Final unified run summary.
        ```
    """

    INTERVAL: int = 20  # seconds

    def __init__(self, lid_input, lid_output, stt_input, stt_output_base, output, langs):
        super().__init__(name="UnifiedRunSummarizerThread", daemon=False)
        self.stage: MultilanguageBatchStage = MultilanguageBatchStage.STARTING
        self.done_evt = multiprocessing.Event()
        self.lid_input: str = lid_input
        self.lid_output: str = lid_output
        self.stt_input: str = stt_input
        self.stt_output_base: str = stt_output_base
        self.output: str = output
        self.langs: List[str] = langs
        self._cached_meta: Dict = {}

    def finish_autoloop(self):
        """
        This Thread becomes joinable (via join()) immediately or within one iterate() from the time
        this is called. Thereafter,
        """
        self.done_evt.set()

    def run(self):
        self.iterate()
        while not self.done_evt.is_set():
            # Sleep for interval, or earlier if done_evt triggered.
            if self.done_evt.wait(UnifiedRunSummarizer.INTERVAL):
                break
            self.iterate()


    def set_stage(self, new_stage: MultilanguageBatchStage):
        self.stage = new_stage

    def _read_json_or_empty(self, path):
        if not os.path.exists(path):
            return {}
        with open(path, encoding="utf-8") as f:
            try:
                return json.load(f)
            except:
                return {}

    def _lid_run_summ(self) -> Dict:
        path = os.path.join(self.lid_output, "run_summary.json")
        return self._read_json_or_empty(path)

    def _stt_run_summ(self, lang) -> Dict:
        path = os.path.join(self.stt_output_base, lang, "run_summary.json")
        return self._read_json_or_empty(path)

    def _iterate_starting(self) -> Dict:
        return {
            "stage": MultilanguageBatchStage.STARTING.name,
            "running_language": None,
            "stage_file_stats": None,
            "processed_files": None,
        }

    def _iterate_lid_running(self) -> Dict:
        rs = self._lid_run_summ()
        return {
            "stage": MultilanguageBatchStage.LANG_SEGMENT.name,
            "running_language": "lid",
            "stage_file_stats": rs.get("overall_summary", {}).get("file_stats", {}),
            "processed_files": rs.get("processed_files", []),
        }

    def _iterate_stt_running_or_done(self) -> Dict:
        stage = self.stage  # snapshot; can change.
        rs = self._lid_run_summ()
        lid_processed_files = rs.get("processed_files", None)
        if not lid_processed_files:
            return {"unified_run_summarizer_error": "lid_run_summary_missing"}
        processed_files = {}  # filepath -> {status meta}. Number of keys is total number of files.
        files_failed_lid: Set = set()  # filepaths of all files known to have failed lid
        files_failed_stt: Set = set()  # filepaths of all files known to have failed at least one stt segment
        for f in lid_processed_files:
            proc_file = {
                "filepath": f["filepath"],
                "audio_duration": f["audio_duration"],
                "passed_language_segmentation": f["passed"],
                "passed_all_stt": "running" if f["passed"] else False,
                "errors": [],
                "unknown_defaulted_lang": False,  # note: subject to change
            }
            if not f["passed"]:
                proc_file["errors"].append(
                    {
                        "lang": "lid",
                        "file": f["filepath"],
                        "error_type": f["error_type"],
                        "failed_reason": f["failed_reason"],
                    }
                )
                files_failed_lid.add(f["filepath"])
            processed_files[f["filepath"]] = proc_file

        # Same order in which processing is done.
        latest_lang = "<pending>"
        latest_lang_file_stats = {}
        for lang in self.langs:
            rs = self._stt_run_summ(lang)
            file_stats = rs.get("overall_summary", {}).get("file_stats", {})
            if file_stats != {}:
                latest_lang = lang
                latest_lang_file_stats = file_stats
            stt_processed_files = rs.get("processed_files", [])
            if len(stt_processed_files) > 0:
                for f in stt_processed_files:
                    assert (f["filepath"].endswith(".seg.json"))
                    if f["filepath"] not in self._cached_meta:
                        with open(f["filepath"], encoding="utf-8") as m:
                            self._cached_meta[f["filepath"]] = json.load(m)
                    meta = self._cached_meta[f["filepath"]]
                    actual_audio_file = meta["file"]
                    start_offset_secs = float(meta["start_offset"])
                    end_offset_secs = float(meta["end_offset"])
                    unknown_defaulted_lang = meta["defaulted_lang_on_unknown"]  # would apply to entire original file
                    if unknown_defaulted_lang:
                        processed_files[actual_audio_file]["unknown_defaulted_lang"] = True
                    if not f["passed"]:
                        files_failed_stt.add(f["filepath"])
                        processed_files[actual_audio_file]["passed_all_stt"] = False
                        processed_files[actual_audio_file]["errors"].append({
                            "lang": lang,
                            "file": f["filepath"],
                            "error_type": f["error_type"],
                            "failed_reason": f["failed_reason"]
                        })

        total_files = len(processed_files)
        failed_files = len(files_failed_lid.union(files_failed_stt))
        succeeded_files = total_files - failed_files

        # If no errors for a file in any segment, it passed all stt.
        if stage.value >= MultilanguageBatchStage.STITCHING.value:  # Must be done all languages
            for k, v in processed_files.items():
                if len(processed_files[k]["errors"]) == 0:
                    processed_files[k]["passed_all_stt"] = True

        if stage == MultilanguageBatchStage.PER_LANG_STT:
            stage_str = MultilanguageBatchStage.PER_LANG_STT.name + "_" + latest_lang
            running_language = latest_lang
            stage_file_stats = latest_lang_file_stats
        elif stage == MultilanguageBatchStage.STITCHING:
            stage_str = MultilanguageBatchStage.STITCHING
            running_language = None
            stage_file_stats = {}
        else:
            stage_str = stage.name
            running_language = None
            stage_file_stats = {
                "total": total_files,
                "passed": succeeded_files,
                "failed": failed_files,
            }

        return {
            "stage": stage_str,
            "running_language": running_language,
            "stage_file_stats": stage_file_stats,
            "processed_files": processed_files,
        }

    def iterate(self):
        try:
            run_summ: {}
            if self.stage == MultilanguageBatchStage.STARTING:
                run_summ = self._iterate_starting()
            elif self.stage == MultilanguageBatchStage.LANG_SEGMENT:
                run_summ = self._iterate_lid_running()
            else:
                run_summ = self._iterate_stt_running_or_done()

            write_json_file_atomic(
                run_summ,
                os.path.join(self.output, "run_summary.json"),
                log=False)
        except Exception as e:
            stack = traceback.format_exc()
            print("UnifiedRunSummarizer Exception: {0}, {1}, {2}\n".format(type(e).__name__, e.__repr__(), stack))
