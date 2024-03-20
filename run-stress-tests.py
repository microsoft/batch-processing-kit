#!/usr/bin/env python3

# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

import sys
import os
import cProfile

from tests.test_e2e_stress_mock_speechsdk import UnstableSDKTestCase

do_profile = False
sys.path.append(os.path.dirname(os.path.realpath(__file__)))


# Main entry point
if __name__ == '__main__':
    if do_profile:
        # One-shot mode.
        cProfile.run(
            'UnstableSDKTestCase().test_e2e_unstable_mocked_sdk(daemon_mode=False)',
            '/tmp/batchcli_entrythread_profile_oneshot'
        )
        # Daemon mode.
        cProfile.run(
            'UnstableSDKTestCase().test_e2e_unstable_mocked_sdk(daemon_mode=True)',
            '/tmp/batchcli_entrythread_profile_daemon'
        )
    else:
        # One-shot mode.
        UnstableSDKTestCase().test_e2e_unstable_mocked_sdk(daemon_mode=False)
        # Daemon mode.
        UnstableSDKTestCase().test_e2e_unstable_mocked_sdk(daemon_mode=True)
