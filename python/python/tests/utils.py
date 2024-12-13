# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors
import pytest


def exception_output(e_info: pytest.ExceptionInfo):
    import traceback

    # skip traceback part, since it's not worth checking in tests
    lines = traceback.format_exception_only(e_info.type, e_info.value)
    return "".join(lines).strip()
