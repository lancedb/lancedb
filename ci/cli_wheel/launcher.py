"""Console entry point for the bundled LanceDB CLI."""

import os
import platform
import sys
from pathlib import Path


def main():
    if sys.platform == "linux" and platform.machine() == "x86_64":
        # manylinux tags describe glibc and architecture, not CPU extensions.
        with open("/proc/cpuinfo") as cpuinfo:
            flags = next(
                (line.split() for line in cpuinfo if line.startswith("flags")), []
            )
        if not {"avx2", "fma", "f16c"}.issubset(flags):
            sys.exit("The LanceDB Linux x86-64 binary requires AVX2, FMA, and F16C.")

    executable = Path(__file__).with_name("lancedb")
    os.execv(executable, [str(executable), *sys.argv[1:]])
