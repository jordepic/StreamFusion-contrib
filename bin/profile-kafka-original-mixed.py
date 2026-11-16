#!/usr/bin/env python3
"""Observe the original Kafka class while its original neighboring classes execute."""

import argparse
import json
import os
from pathlib import Path
import re
import shutil
import signal
import subprocess
import time
import xml.etree.ElementTree as ET


TARGET = "tech.streamfusion.CanonicalKafkaPipelineFailureJobTest"
COHORT = (
    TARGET,
    "tech.streamfusion.CanonicalOrdinaryRegularJoinJobTest",
    "tech.streamfusion.CanonicalManagedUdfTimestampKeyJobTest",
    "tech.streamfusion.CanonicalGlobalHiddenExtremaJobTest",
)


def report(path):
    try:
        suite = ET.parse(path).getroot()
    except (FileNotFoundError, ET.ParseError):
        return None
    return dict(name=suite.get("name"), **{
        key: int(suite.get(key, "0")) for key in ("tests", "failures", "errors", "skipped")})


def stop_group(process):
    try:
        os.killpg(process.pid, signal.SIGTERM)
    except ProcessLookupError:
        pass
    deadline = time.monotonic() + 2
    while time.monotonic() < deadline:
        process.poll()
        try:
            os.killpg(process.pid, 0)
        except ProcessLookupError:
            break
        time.sleep(0.1)
    try:
        os.killpg(process.pid, signal.SIGKILL)
    except ProcessLookupError:
        pass
    process.wait()


def run(root, reports, output, command, timeout):
    target = reports / ("TEST-" + TARGET + ".xml")
    if target.exists():
        raise ValueError("Refusing stale Kafka XML; use a fresh reports directory")
    output.mkdir(parents=True, exist_ok=False)
    log = output / "maven.log"
    started = time.monotonic()
    observed = {}
    result = {"command": command, "target": TARGET,
              "neighbor_policy": "Run unchanged neighbors only until the Kafka class completes; no neighbor coverage claim"}
    with log.open("w") as stream:
        process = subprocess.Popen(command, cwd=root, stdout=stream,
                                   stderr=subprocess.STDOUT, start_new_session=True)
        result["owned_process_group"] = process.pid
        try:
            while True:
                elapsed = time.monotonic() - started
                text = re.sub(r"\x1b\[[0-?]*[ -/]*[@-~]", "", log.read_text(errors="replace"))
                for name in COHORT:
                    if name not in observed and "Running " + name in text:
                        observed[name] = elapsed
                        print("Original cohort started: " + name, flush=True)
                completed = report(target)
                if completed is not None:
                    result["kafka_report"] = completed
                    result["maven_active_at_target_completion"] = process.poll() is None
                    result["completed_reports_before_stop"] = []
                    retained = output / "completed-reports"
                    retained.mkdir()
                    for path in reports.glob("TEST-*.xml"):
                        value = report(path)
                        if value is not None:
                            result["completed_reports_before_stop"].append(value)
                            shutil.copyfile(path, retained / path.name)
                    break
                if process.poll() is not None:
                    result["diagnostic_error"] = "Maven exited before producing complete Kafka XML"
                    break
                if elapsed >= timeout:
                    result["diagnostic_error"] = "Kafka did not complete within the bounded diagnostic"
                    break
                time.sleep(0.5)
        finally:
            result["cohort_class_start_observations_seconds"] = observed
            result["seconds_until_target_or_error"] = time.monotonic() - started
            stop_group(process)
            result["maven_exit_after_owned_group_cleanup"] = process.returncode
    completed = result.get("kafka_report")
    result["cohort_start_spread_seconds"] = max(observed.values()) - min(observed.values()) if observed else None
    valid = (completed is not None and completed["name"] == TARGET
             and completed["tests"] == 4 and completed["skipped"] == 0
             and set(observed) == set(COHORT)
             and result["cohort_start_spread_seconds"] <= 5
             and "diagnostic_error" not in result)
    passed = valid and all(not value["failures"] and not value["errors"]
                           for value in result.get("completed_reports_before_stop", []))
    result["target_and_mixed_cohort_valid"] = valid
    result["target_passed"] = passed
    result["scope"] = "Four original Kafka providers; incomplete neighbor remainders deliberately stopped, not passed or qualified"
    (output / "outcome.json").write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps({key: result.get(key) for key in
                      ("target_passed", "kafka_report", "diagnostic_error", "seconds_until_target_or_error")}), flush=True)
    return 0 if passed else 1


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path.cwd())
    parser.add_argument("--reports", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--timeout", type=int, default=900)
    parser.add_argument("command", nargs=argparse.REMAINDER)
    args = parser.parse_args()
    command = args.command[1:] if args.command[:1] == ["--"] else args.command
    if not command:
        parser.error("Pass the Maven command after --")
    def interrupted(signum, frame):
        raise KeyboardInterrupt
    signal.signal(signal.SIGTERM, interrupted)
    try:
        return run(args.root.resolve(), args.reports.resolve(), args.output.resolve(), command, args.timeout)
    except KeyboardInterrupt:
        return 130


if __name__ == "__main__":
    raise SystemExit(main())
