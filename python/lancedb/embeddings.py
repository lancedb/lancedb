#  Copyright 2023 LanceDB Developers
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

import math
from retry import retry
from typing import Callable, Union

from lance.vector import vec_to_table
import numpy as np
import pandas as pd
import pyarrow as pa


def with_embeddings(
    func: Callable,
    data: Union[pa.Table, pd.DataFrame],
    column: str = "text",
    wrap_api: bool = True,
    show_progress: bool = False,
    batch_size: int = 1000,
):
    func = EmbeddingFunction(func)
    if wrap_api:
        func = func.retry().rate_limit()
    func = func.batch_size(batch_size)
    if show_progress:
        func = func.show_progress()
    if isinstance(data, pd.DataFrame):
        data = pa.Table.from_pandas(data, preserve_index=False)
    embeddings = func(data[column].to_numpy())
    table = vec_to_table(np.array(embeddings))
    return data.append_column("vector", table["vector"])


class EmbeddingFunction:
    def __init__(self, func: Callable):
        self.func = func
        self.rate_limiter_kwargs = {}
        self.retry_kwargs = {}
        self._batch_size = None
        self._progress = False

    def __call__(self, text):
        # Get the embedding with retry
        if len(self.retry_kwargs) > 0:

            @retry(**self.retry_kwargs)
            def embed_func(c):
                return self.func(c.tolist())

        else:

            def embed_func(c):
                return self.func(c.tolist())

        if len(self.rate_limiter_kwargs) > 0:
            import ratelimiter

            max_calls = self.rate_limiter_kwargs["max_calls"]
            limiter = ratelimiter.RateLimiter(
                max_calls, period=self.rate_limiter_kwargs["period"]
            )
            embed_func = limiter(embed_func)
        batches = self.to_batches(text)
        embeds = [emb for c in batches for emb in embed_func(c)]
        return embeds

    def __repr__(self):
        return f"EmbeddingFunction(func={self.func})"

    def rate_limit(self, max_calls=0.9, period=1.0):
        import sys

        v = int(sys.version_info.minor)
        if v >= 11:
            raise ValueError("rate limit only support up to 3.10")
        self.rate_limiter_kwargs = dict(max_calls=max_calls, period=period)
        return self

    def retry(self, tries=10, delay=1, max_delay=30, backoff=3, jitter=1):
        self.retry_kwargs = dict(
            tries=tries,
            delay=delay,
            max_delay=max_delay,
            backoff=backoff,
            jitter=jitter,
        )
        return self

    def batch_size(self, batch_size):
        self._batch_size = batch_size
        return self

    def show_progress(self):
        self._progress = True
        return self

    def to_batches(self, arr):
        length = len(arr)

        def _chunker(arr):
            for start_i in range(0, len(arr), self._batch_size):
                yield arr[start_i : start_i + self._batch_size]

        if self._progress:
            from tqdm.auto import tqdm

            yield from tqdm(_chunker(arr), total=math.ceil(length / self._batch_size))
        else:
            yield from _chunker(arr)
