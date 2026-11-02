#!/usr/bin/env python3
"""Validate the local Function and BlobReader examples from ENT-3561."""

from lancedb import BlobReader, FunctionApplication, FunctionVersion, UdfDefinition, udf


@udf
def normalize(text: str) -> str:
    return text.strip().lower()


@udf
class Scale:
    def __init__(self, factor: float):
        self.factor = factor
        self.closed = False

    def __call__(self, value: float) -> float:
        return value * self.factor

    def close(self) -> None:
        self.closed = True
        self.factor = 0.0


def main() -> None:
    assert isinstance(normalize, UdfDefinition)
    assert normalize(" Hello ") == "hello"
    assert not isinstance(normalize(" Hello "), FunctionApplication)

    scaler = Scale(factor=2.0)
    assert type(scaler).__name__ == "Scale"
    assert scaler(3.0) == 6.0
    scaler.close()
    assert scaler.closed

    assert FunctionVersion is not UdfDefinition

    payload = b"0123456789"

    def read_range(offset: int, length: int) -> bytes:
        return payload[offset : offset + length]

    with BlobReader(read_range, len(payload)) as reader:
        assert reader.read_range(6, 4) == b"6789"
        assert reader.tell() == 0
        assert reader.read(3) == b"012"
        assert reader.tell() == 3
        assert reader.read_range(8, 10) == b"89"
        assert reader.tell() == 3

    print("ENT-3561 local Function and BlobReader examples passed")


if __name__ == "__main__":
    main()
