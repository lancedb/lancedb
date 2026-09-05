

def test_from_tables_split_without_permutation_table(mem_db):
    """A split requested without a permutation table raises a descriptive error."""
    tbl = mem_db.create_table(
        "split_no_perm_table", pa.table({"x": range(10), "y": range(10)})
    )
    with pytest.raises(ValueError) as exc_info:
        Permutation.from_tables(tbl, None, split="train")

    assert str(exc_info.value) == (
        "Cannot create a permutation on split `train` because no permutation table is provided"
    )
