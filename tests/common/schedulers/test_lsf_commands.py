# Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License"). You may not use this file except in compliance
# with the License. A copy of the License is located at
#
# http://aws.amazon.com/apache2.0/
#
# or in the "LICENSE.txt" file accompanying this file. This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES
# OR CONDITIONS OF ANY KIND, express or implied. See the License for the specific language governing permissions and
# limitations under the License.
import subprocess
from unittest.mock import call
import json

import pytest
from assertpy import assert_that
from common.schedulers.lsf_commands import (
    get_compute_nodes_info,
    get_jobs_info,
    get_pending_jobs_info,
)

from tests.common import read_text


@pytest.mark.parametrize(
    "bhosts_mocked_response, expected_output",
    [
        (
            "bhosts_output.json",
            {
                "COMMAND":"bhosts",
                "HOSTS":1,
                "RECORDS":[
                    {
                    "HOST_NAME":"ip-10-30-9-105",
                    "STATUS":"closed_Full",
                    "MAX":"4",
                    "NJOBS":"4",
                    "RUN":"4",
                    "SSUSP":"0",
                    "USUSP":"0",
                    "RSV":"0"
                    }
                ]
            }
        ),
        ("bhosts_empty.json", {}),
        ("bhosts_error.json", {}),
    ],
    ids=["mixed_output", "empty_output", "errored_output"],
)
def test_get_compute_nodes_info(bhosts_mocked_response, expected_output, mocker, test_datadir):
    pbsnodes_output = read_text(test_datadir / bhosts_mocked_response)
    mock = mocker.patch(
        "common.schedulers.lsf_commands.check_command_output", return_value=pbsnodes_output, autospec=True
    )
    
    nodes = get_compute_nodes_info()
    
    mock.assert_called_with('bhosts -o "HOST_NAME STATUS MAX NJOBS RUN SSUSP USUSP RSV" -json', raise_on_error=False)
    assert_that(nodes).is_equal_to(expected_output)


@pytest.mark.parametrize(
    "bjobs_mocked_response, filter_by_states, filter_by_exec_hosts, expected_output",
    [
        (
            "bjobs_output.json",
            None,
            None,
            {
                "COMMAND":"bjobs",
                "JOBS":6,
                "RECORDS":[
                    {
                    "JOBID":"217",
                    "QUEUE":"receiver",
                    "STAT":"RUN",
                    "JOB_NAME":"sleep 1h"
                    },
                    {
                    "JOBID":"218",
                    "QUEUE":"receiver",
                    "STAT":"RUN",
                    "JOB_NAME":"sleep 1h"
                    },
                    {
                    "JOBID":"219",
                    "QUEUE":"receiver",
                    "STAT":"RUN",
                    "JOB_NAME":"sleep 1h"
                    },
                    {
                    "JOBID":"220",
                    "QUEUE":"receiver",
                    "STAT":"RUN",
                    "JOB_NAME":"sleep 1h"
                    },
                    {
                    "JOBID":"221",
                    "QUEUE":"receiver",
                    "STAT":"PEND",
                    "JOB_NAME":"sleep 1h"
                    },
                    {
                    "JOBID":"222",
                    "QUEUE":"receiver",
                    "STAT":"PEND",
                    "JOB_NAME":"sleep 1h"
                    }
                ]
            },
        ),
        ("bjobs_empty.json", None, None, []),
        ("bjobs_error.json", None, None, [])
    ],
    ids=[
        "mixed_output",
        "empty_output",
        "error_output"
    ],
)
def test_get_jobs_info(bjobs_mocked_response, filter_by_states, filter_by_exec_hosts, expected_output, mocker, test_datadir):
    qstat_output = read_text(test_datadir / bjobs_mocked_response)
    mock = mocker.patch(
        "common.schedulers.lsf_commands.check_command_output", return_value=qstat_output, autospec=True
    )

    jobs = get_jobs_info(filter_by_states, filter_by_exec_hosts)

    mock.assert_called_with('bjobs -o "JOBID QUEUE STAT JOB_NAME" -json', raise_on_error=False)
    assert_that(jobs).is_equal_to(expected_output)

@pytest.mark.parametrize(
    "bjobs_mocked_response, max_slots_filter, skip_if_state, expected_output",
    [
        (
            "bjobs_output.json",
            None,
            None,
            {
                "COMMAND":"bjobs",
                "JOBS":2,
                "RECORDS":[
                    {
                    "JOBID":"221",
                    "QUEUE":"receiver",
                    "STAT":"PEND",
                    "JOB_NAME":"sleep 1h"
                    },
                    {
                    "JOBID":"222",
                    "QUEUE":"receiver",
                    "STAT":"PEND",
                    "JOB_NAME":"sleep 1h"
                    }
                ]
            },
        ),
        ("bjobs_empty.json", None, None, []),
        ("bjobs_error.json", None, None, [])
    ],
    ids=[
        "mixed_output",
        "empty_output",
        "error_output"
    ],
)
def test_get_pending_jobs_info(bjobs_mocked_response, max_slots_filter, skip_if_state, expected_output, mocker, test_datadir):
    qstat_output = read_text(test_datadir / bjobs_mocked_response)
    mock = mocker.patch(
        "common.schedulers.lsf_commands.check_command_output", return_value=qstat_output, autospec=True
    )

    jobs = get_pending_jobs_info(max_slots_filter, skip_if_state)

    mock.assert_called_with('bjobs -p -o "JOBID QUEUE STAT JOB_NAME" -json', raise_on_error=False)
    assert_that(jobs).is_equal_to(expected_output)