# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors
import pytest
from sklearn.metrics import average_precision_score


def exception_output(e_info: pytest.ExceptionInfo):
    import traceback

    # skip traceback part, since it's not worth checking in tests
    lines = traceback.format_exception_only(e_info.type, e_info.value)
    return "".join(lines).strip()


def compute_recall_at_k(retrieved_labels, true_label, k):
    top_k = retrieved_labels[:k]
    return 1 if true_label in top_k else 0


def compute_average_precision(retrieved_labels, true_label):
    y_true = [1 if lbl == true_label else 0 for lbl in retrieved_labels]
    y_score = [1.0 / (i + 1) for i in range(len(y_true))]  # Higher rank = higher score
    return average_precision_score(y_true, y_score)
