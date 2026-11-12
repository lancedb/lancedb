# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors
"""Retain command output and actual numeric status without masking failure."""

import json
import subprocess
import sys
import time
from pathlib import Path

out = Path(sys.argv[1])
command = sys.argv[2:]
record = {"command": command, "started": time.time()}
with out.with_suffix(".log").open("w") as log:
    process = subprocess.Popen(
        command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT
    )
    record["pid"] = process.pid
    out.with_suffix(".json").write_text(json.dumps(record, indent=2) + "\n")
    for line in iter(process.stdout.readline, b""):
        log.write(line.decode(errors="replace"))
        log.flush()
        sys.stdout.buffer.write(line)
        sys.stdout.buffer.flush()
    record["exitcode"] = process.wait()
record["finished"] = time.time()
out.with_suffix(".json").write_text(json.dumps(record, indent=2) + "\n")
raise SystemExit(record["exitcode"])
