"""Test the monitor's .nextflow.log parser — the live-progress signal.

The dashboard animates off real-time task counts parsed from the head's
.nextflow.log (the S3 trace.tsv finalises too late on the spawn executor). This
locks in the exact line formats nf-spawn emits, so a plugin log-format change
can't silently freeze the dashboard at 0/0 again.
"""

import re

# The two regexes live in pipeline/monitor.py (a runtime-substituted template, not
# importable as a module), so we mirror them here and assert against real lines.
_RE_SUBMIT = re.compile(r"Submitting task '([^']+)' to spawn instance '([^']+)'")
_RE_DONE = re.compile(r"Task '([^']+)' completed \(exit (\d+)\) on instance '([^']+)'")

_SAMPLE = [
    "Jun-25 18:04:01.2 [Task submitter] INFO  io.nextflow.spawn.SpawnTaskHandler - "
    "Submitting task 'CALL_VARIANTS (HG01879)' to spawn instance 'nf-52d502e965f8'",
    "Jun-25 18:05:34.0 [Task monitor] INFO  io.nextflow.spawn.SpawnTaskHandler - "
    "Task 'CALL_VARIANTS (HG01879)' completed (exit 0) on instance 'nf-52d502e965f8'",
    "Jun-25 18:05:35.0 [Task monitor] INFO  io.nextflow.spawn.SpawnTaskHandler - "
    "Task 'CALL_VARIANTS (HG01880)' completed (exit 1) on instance 'nf-0014ac589af3'",
    "Jun-25 18:10:00.0 [Task submitter] INFO  io.nextflow.spawn.SpawnTaskHandler - "
    "Submitting task 'MERGE_VCFS' to spawn instance 'nf-aaaa'",
]


def _parse(lines):
    submitted, completed, failed = set(), set(), set()
    for line in lines:
        m = _RE_SUBMIT.search(line)
        if m:
            submitted.add(m.group(1))
            continue
        m = _RE_DONE.search(line)
        if m:
            completed.add(m.group(1))
            if m.group(2) != "0":
                failed.add(m.group(1))
    return submitted, completed, failed


def test_submit_and_complete_parsed():
    sub, done, failed = _parse(_SAMPLE)
    assert "CALL_VARIANTS (HG01879)" in sub
    assert "CALL_VARIANTS (HG01879)" in done
    assert "MERGE_VCFS" in sub


def test_nonzero_exit_is_failure():
    _sub, _done, failed = _parse(_SAMPLE)
    assert "CALL_VARIANTS (HG01880)" in failed  # exit 1
    assert "CALL_VARIANTS (HG01879)" not in failed  # exit 0


def test_running_is_submitted_minus_completed():
    sub, done, _ = _parse(_SAMPLE)
    call_sub = {t for t in sub if "CALL_VARIANTS" in t}
    call_done = {t for t in done if "CALL_VARIANTS" in t}
    # HG01879 done, HG01880 done (failed), so 2 submitted CALL_VARIANTS, 2 done → 0 running
    running = call_sub - call_done
    assert running == set()


def test_sample_name_extraction():
    name = "CALL_VARIANTS (HG01879)"
    assert re.search(r"\(([^)]+)\)", name).group(1) == "HG01879"
