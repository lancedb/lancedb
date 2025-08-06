import pytest
import pyarrow as pa

torch = pytest.importorskip("torch")


def test_table_dataloader(mem_db):
    table = mem_db.create_table("test_table", pa.table({"a": range(1000)}))
    dataloader = torch.utils.data.DataLoader(table, batch_size=10)
    for batch in dataloader:
        assert batch.num_rows == 10
