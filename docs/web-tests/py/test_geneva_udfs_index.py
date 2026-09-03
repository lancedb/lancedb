# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

from unittest.mock import MagicMock, create_autospec


def test_registration_udf(monkeypatch):
    # Verifies that Table.add_columns exists with the expected signature.
    # If this fails, update the Registration row in docs/geneva/udfs/index.mdx.
    import geneva
    mock_table = create_autospec(geneva.table.Table, instance=True)
    my_udf = MagicMock()

    # --8<-- [start:registration_udf]
    mock_table.add_columns({"col": my_udf})
    # --8<-- [end:registration_udf]

    mock_table.add_columns.assert_called_once()


def test_registration_scalar_udtf(monkeypatch):
    # Verifies that Connection.create_udtf_view accepts a chunker.
    # If this fails, update the Registration row in docs/geneva/udfs/index.mdx.
    import geneva
    mock_db = create_autospec(geneva.db.Connection, instance=True)
    monkeypatch.setattr("geneva.connect", MagicMock(return_value=mock_db))
    my_source = MagicMock()
    my_chunker = MagicMock()

    # --8<-- [start:registration_scalar_udtf]
    db = geneva.connect("/data/mydb")
    db.create_udtf_view("my_view", source=my_source, udtf=my_chunker)
    # --8<-- [end:registration_scalar_udtf]

    mock_db.create_udtf_view.assert_called_once()


def test_registration_udtf(monkeypatch):
    # Verifies that Connection.create_udtf_view exists with the expected signature.
    # If this fails, update the Registration row in docs/geneva/udfs/index.mdx.
    import geneva
    mock_db = create_autospec(geneva.db.Connection, instance=True)
    monkeypatch.setattr("geneva.connect", MagicMock(return_value=mock_db))
    my_source = MagicMock()
    my_udtf = MagicMock()

    # --8<-- [start:registration_udtf]
    db = geneva.connect("/data/mydb")
    db.create_udtf_view("my_view", source=my_source, udtf=my_udtf)
    # --8<-- [end:registration_udtf]

    mock_db.create_udtf_view.assert_called_once()
