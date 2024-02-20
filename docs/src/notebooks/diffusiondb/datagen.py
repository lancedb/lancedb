#!/usr/bin/env python
#
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

"""Dataset hf://poloclub/diffusiondb
"""

import io
from argparse import ArgumentParser
from multiprocessing import Pool

import lance
import pyarrow as pa
from datasets import load_dataset
from transformers import CLIPModel, CLIPProcessor, CLIPTokenizerFast


MODEL_ID = "openai/clip-vit-base-patch32"

device = "cuda"

tokenizer = CLIPTokenizerFast.from_pretrained(MODEL_ID)
model = CLIPModel.from_pretrained("openai/clip-vit-base-patch32").to(device)
processor = CLIPProcessor.from_pretrained("openai/clip-vit-base-patch32")

schema = pa.schema(
    [
        pa.field("prompt", pa.string()),
        pa.field("seed", pa.uint32()),
        pa.field("step", pa.uint16()),
        pa.field("cfg", pa.float32()),
        pa.field("sampler", pa.string()),
        pa.field("width", pa.uint16()),
        pa.field("height", pa.uint16()),
        pa.field("timestamp", pa.timestamp("s")),
        pa.field("image_nsfw", pa.float32()),
        pa.field("prompt_nsfw", pa.float32()),
        pa.field("vector", pa.list_(pa.float32(), 512)),
        pa.field("image", pa.binary()),
    ]
)


def pil_to_bytes(img) -> list[bytes]:
    buf = io.BytesIO()
    img.save(buf, format="PNG")
    return buf.getvalue()


def generate_clip_embeddings(batch) -> pa.RecordBatch:
    image = processor(text=None, images=batch["image"], return_tensors="pt")[
        "pixel_values"
    ].to(device)
    img_emb = model.get_image_features(image)
    batch["vector"] = img_emb.cpu().tolist()

    with Pool() as p:
        batch["image_bytes"] = p.map(pil_to_bytes, batch["image"])
    return batch


def datagen(args):
    """Generate DiffusionDB dataset, and use CLIP model to generate image embeddings."""
    dataset = load_dataset("poloclub/diffusiondb", args.subset)
    data = []
    for b in dataset.map(
        generate_clip_embeddings, batched=True, batch_size=256, remove_columns=["image"]
    )["train"]:
        b["image"] = b["image_bytes"]
        del b["image_bytes"]
        data.append(b)
    tbl = pa.Table.from_pylist(data, schema=schema)
    return tbl


def main():
    parser = ArgumentParser()
    parser.add_argument(
        "-o", "--output", metavar="DIR", help="Output lance directory", required=True
    )
    parser.add_argument(
        "-s",
        "--subset",
        choices=["2m_all", "2m_first_10k", "2m_first_100k"],
        default="2m_first_10k",
        help="subset of the hg dataset",
    )

    args = parser.parse_args()

    batches = datagen(args)
    lance.write_dataset(batches, args.output)


if __name__ == "__main__":
    main()
