# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

import json
from typing import List, Optional

import numpy as np
from huggingface_hub import snapshot_download
from pydantic import BaseModel
from transformers import BertTokenizer

try:
    import mlx.core as mx
    import mlx.nn as nn
except ImportError:
    raise ImportError("You need to install MLX to use this model use - pip install mlx")


def average_pool(last_hidden_state: mx.array, attention_mask: mx.array) -> mx.array:
    last_hidden = mx.multiply(last_hidden_state, attention_mask[..., None])
    return last_hidden.sum(axis=1) / attention_mask.sum(axis=1)[..., None]


class ModelConfig(BaseModel):
    dim: int = 1024
    num_attention_heads: int = 16
    num_hidden_layers: int = 24
    vocab_size: int = 30522
    attention_probs_dropout_prob: float = 0.1
    hidden_dropout_prob: float = 0.1
    layer_norm_eps: float = 1e-12
    max_position_embeddings: int = 512


class TransformerEncoderLayer(nn.Module):
    """
    A transformer encoder layer with (the original BERT) post-normalization.
    """

    def __init__(
        self,
        dims: int,
        num_heads: int,
        mlp_dims: Optional[int] = None,
        layer_norm_eps: float = 1e-12,
    ):
        super().__init__()
        mlp_dims = mlp_dims or dims * 4
        self.attention = nn.MultiHeadAttention(dims, num_heads, bias=True)
        self.ln1 = nn.LayerNorm(dims, eps=layer_norm_eps)
        self.ln2 = nn.LayerNorm(dims, eps=layer_norm_eps)
        self.linear1 = nn.Linear(dims, mlp_dims)
        self.linear2 = nn.Linear(mlp_dims, dims)
        self.gelu = nn.GELU()

    def __call__(self, x, mask):
        attention_out = self.attention(x, x, x, mask)
        add_and_norm = self.ln1(x + attention_out)

        ff = self.linear1(add_and_norm)
        ff_gelu = self.gelu(ff)
        ff_out = self.linear2(ff_gelu)
        x = self.ln2(ff_out + add_and_norm)

        return x


class TransformerEncoder(nn.Module):
    def __init__(
        self, num_layers: int, dims: int, num_heads: int, mlp_dims: Optional[int] = None
    ):
        super().__init__()
        self.layers = [
            TransformerEncoderLayer(dims, num_heads, mlp_dims)
            for i in range(num_layers)
        ]

    def __call__(self, x, mask):
        for layer in self.layers:
            x = layer(x, mask)

        return x


class BertEmbeddings(nn.Module):
    def __init__(self, config: ModelConfig):
        self.word_embeddings = nn.Embedding(config.vocab_size, config.dim)
        self.token_type_embeddings = nn.Embedding(2, config.dim)
        self.position_embeddings = nn.Embedding(
            config.max_position_embeddings, config.dim
        )
        self.norm = nn.LayerNorm(config.dim, eps=config.layer_norm_eps)

    def __call__(self, input_ids: mx.array, token_type_ids: mx.array) -> mx.array:
        words = self.word_embeddings(input_ids)
        position = self.position_embeddings(
            mx.broadcast_to(mx.arange(input_ids.shape[1]), input_ids.shape)
        )
        token_types = self.token_type_embeddings(token_type_ids)

        embeddings = position + words + token_types
        return self.norm(embeddings)


class Bert(nn.Module):
    def __init__(self, config: ModelConfig):
        self.embeddings = BertEmbeddings(config)
        self.encoder = TransformerEncoder(
            num_layers=config.num_hidden_layers,
            dims=config.dim,
            num_heads=config.num_attention_heads,
        )
        self.pooler = nn.Linear(config.dim, config.dim)

    def __call__(
        self,
        input_ids: mx.array,
        token_type_ids: mx.array,
        attention_mask: mx.array = None,
    ) -> tuple[mx.array, mx.array]:
        x = self.embeddings(input_ids, token_type_ids)

        if attention_mask is not None:
            # convert 0's to -infs, 1's to 0's, and make it broadcastable
            attention_mask = mx.log(attention_mask)
            attention_mask = mx.expand_dims(attention_mask, (1, 2))

        y = self.encoder(x, attention_mask)
        return y, mx.tanh(self.pooler(y[:, 0]))


class Model:
    def __init__(self) -> None:
        # get converted embedding model
        model_path = snapshot_download(repo_id="vegaluisjose/mlx-rag")
        with open(f"{model_path}/config.json") as f:
            model_config = ModelConfig(**json.load(f))
        self.dims = model_config.dim
        self.model = Bert(model_config)
        self.model.load_weights(f"{model_path}/model.npz")
        self.tokenizer = BertTokenizer.from_pretrained("thenlper/gte-large")
        self.embeddings = []

    def run(self, input_text: List[str]) -> mx.array:
        tokens = self.tokenizer(input_text, return_tensors="np", padding=True)
        tokens = {key: mx.array(v) for key, v in tokens.items()}

        last_hidden_state, _ = self.model(**tokens)

        embeddings = average_pool(
            last_hidden_state, tokens["attention_mask"].astype(mx.float32)
        )
        self.embeddings = (
            embeddings / mx.linalg.norm(embeddings, ord=2, axis=1)[..., None]
        )

        return np.array(embeddings.astype(mx.float32))
