# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

ORCHESTRATOR_SCOPE_MAX_RETRIES = 9    # Includes all retries. How # before orchestrator won't reschedule.
RUN_SUMMARY_LOOP_INTERVAL = 30        # How often the run summary is recomputed and written.
DEBUG_LOOP_INTERVAL = 30              # Applies only when debug thread enabled.

# The number of times we will retry a SpeechSDKWorkItem as
# long as its previous reasons for failure appear to be transient issues
# that we can probably resolve using the same endpoint again.
RECOGNIZER_SCOPE_RETRIES = 2

# Upper bound on the number of files that may ever be processed in daemon mode.
# Once this number of files processed is met or exceeded as of the last batch finished,
# the daemon mode client will exit. The value 0 means no limit.
DAEMON_MODE_MAX_FILES = 0
