# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

import argparse
from argparse import Namespace
import tempfile


def check_positive(value):
    """
    Ensure an argument is positive integer
    :param value: argument to check
    :return: integer cast of the passed in value
    """
    int_value = int(value)
    if int_value <= 0:
        raise argparse.ArgumentTypeError("{0} is not a positive int value".format(value))
    return int_value


def create_parser():
    parser = argparse.ArgumentParser(
        description='Run a Speech SDK batch client for batch transcription of audio files',
        add_help=True,
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument(
        '-config', '--configuration-file',
        default="/usr/local/batch/input/config.yaml",
        help='configuration file holding the information about endpoints, ports and concurrency'
    )
    parser.add_argument(
        '-output_folder', '--output-folder',
        default="/usr/local/batch/output",
        help='Folder to store transcriptions and logs. Use with --run-mode ONESHOT or DAEMON.'
    )
    parser.add_argument(
        '-input_folder', '--input-folder',
        default="/usr/local/batch/input",
        help="Folder where audio files are stored. Use with --run-mode ONESHOT or DAEMON."
    )
    parser.add_argument(
        '-log_folder', '--log-folder',
        default=None,
        help="Folder where logs are stored. If not provided, logs will not be written to file."
    )
    parser.add_argument(
        '-console_log_level', '--console-log-level',
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
        default='INFO', help="Set the console logging level"
    )
    parser.add_argument(
        '-file_log_level', '--file-log-level',
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
        default='INFO', help="Set the file logging level"
    )
    parser.add_argument(
        '-nbest', '--nbest',
        default=1, type=check_positive,
        help="How many maximum results to consider per recognition"
    )
    parser.add_argument(
        '-combined_json', '--store-combined-json',
        default=False, action='store_true',
        help="whether to also produce a combined JSON result for the entire run"
    )
    parser.add_argument(
        '-input_list', '--input-list',
        help="File containing list of audio files to process. If not provided all files in "
             "the input folder are considered. Use with --run-mode ONESHOT only."
    )
    parser.add_argument(
        '-m', '--run-mode',
        default='ONESHOT', choices=['ONESHOT', 'DAEMON', 'APISERVER'],
        help="whether to run in a daemon mode listening to more changes in the input folder"
    )
    parser.add_argument(
        '-scratch_folder', '--scratch-folder',
        required=False,
        help="[Optional] Scratch folder will be created if it doesn't"
             "exist and is cleaned on exit. If unspecified, a temporary"
             "directory is used under /tmp"
    )
    parser.add_argument(
        '-diarization', '--diarization-mode',
        default='None', choices=['None', 'Identity', 'Anonymous'],
        help="diarization mode selection"
    )
    parser.add_argument(
        '-language', '--language', nargs='+', default="en-US",
        help="Space-separated list of candidate languages for transcription.\n"
             "   Example:  --language en-US  \n"
             "   Example:  --language en-US fr-FR de-DE  \n"
             "If exactly one language is provided, no language segmentation is performed and you do not need "
             "Language Identification (LID) endpoints in your config. With two or more languages, you must have at "
             "least one LID endpoint listed in your config available for multi-language segmentation to run first. "
             "Each LID endpoint should be marked with language 'lid' in the endpoint config."
    )
    parser.add_argument(
        '-strict_config', '--strict-configuration-validation',
        default=False, action='store_true',
        help="whether to fail an invalid configuration file"
    )
    parser.add_argument(
        '-profanity', '--profanity-mode',
        default='Masked', choices=['Masked', 'Raw', 'Removed'],
        help="how to handle profanity in the response"
    )
    parser.add_argument(
        '-sentiment', '--enable-sentiment', default=False, action='store_true',
        help="Enable sentiment analysis"
    )
    parser.add_argument(
        '-resume', '--allow-resume', default=False, action='store_true',
        help="whether to allow resuming from a failed transcription (WARNING: results may differ)"
    )
    parser.add_argument(
        '-port', '--apiserver_port', default=5000, type=check_positive,
        help="Port for listening when using APISERVER mode"
    )
    parser.add_argument(
        '-poll', '--poll', default=False, action='store_true',
        help="In DAEMON mode, toggle to periodically poll the input directory for new files instead of relying "
             "only on the Posix Watches facility. This is needed to make DAEMON mode work with filesystems that "
             "do not support Posix Watches, for example CIFS mount. Polling is an increased burden on the filesystem. "
             "Applies to --run-mode DAEMON only."
    )
    parser.add_argument(
        '-max_segment_length', '--max-segment-length',
        default=3600, type=check_positive,
        help="[Applies when multiple --language given only]."
             "Cap the maximum audio segment length produced during language segmentation. "
             "Longer segments will be broken up into smaller ones. Unit: positive integer seconds."
    )
    parser.add_argument(
        '-debug_loop_interval', '--debug-loop-interval',
        default=0, type=check_positive,
        help="Interval in seconds to re-log debug information about the batchkit's orchestration components. "
             "Useful for debugging. The default value of 0 means this information is not logged. "
    )
    return parser


def parse_cmdline(args=None) -> Namespace:
    """
    Create a command line parser for the batch client, and parse arguments
    :param args: arguments to parse
    :return: parsed command line arguments
    """
    parser = create_parser()
    args: Namespace = parser.parse_args(args=args)

    if args.input_list is not None and args.run_mode != 'ONESHOT':
        parser.error("argument -input_list/--input-list: not allowed if the run mode is not ONESHOT")

    if args.scratch_folder is None:
        args.scratch_folder = tempfile.mkdtemp()

    if isinstance(args.language, list) and len(args.language) > 1 and args.run_mode != 'ONESHOT':
        parser.error("Multi-language speech-batch-kit can only be used in ONESHOT mode in this version.")

    if isinstance(args.language, str):
        args.language = [args.language]

    return args
