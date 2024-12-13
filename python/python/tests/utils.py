# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors
import pytest


def exception_output(e_info: pytest.ExceptionInfo):
    import traceback

    # skip traceback part, since it's not worth checking in tests
    e_info.value.__traceback__ = None
    lines = traceback.format_exception(e_info.value)
    return "".join(lines).strip()
