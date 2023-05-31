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
import lancedb
import pyarrow as pa
from datasets import load_dataset
from PIL import Image
from transformers import CLIPModel, CLIPProcessor, CLIPTokenizerFast

MODEL_ID = "openai/clip-vit-base-patch32"

device = "cuda"

tokenizer = CLIPTokenizerFast.from_pretrained(MODEL_ID)
model = CLIPModel.from_pretrained("openai/clip-vit-base-patch32").to(device)
processor = CLIPProcessor.from_pretrained("openai/clip-vit-base-patch32")

schema = pa.schema(
    [
        pa.field("image", pa.binary()),
        pa.field("prompt", pa.string()),
        pa.field("seed", pa.uint32()),
        pa.field("step", pa.uint16()),
        pa.field("cfg", pa.float32()),
        pa.field("sampler", pa.string()),
        pa.field("width", pa.uint16()),
        pa.field("height", pa.uint16()),
        pa.field("timestamp", pa.string()),
        pa.field("image_nsfw", pa.float32()),
        pa.field("prompt_nsfw", pa.float32()),
        pa.field("vector", pa.list_(pa.float32(), 512)),
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
        batch["image"] = p.map(pil_to_bytes, batch["image"])
    print(batch)
    return batch


def datagen(args):
    dataset = load_dataset("poloclub/diffusiondb", args.subset)
    print(dir(dataset))
    for b in dataset.map(generate_clip_embeddings, batched=True, batch_size=512):
        yield b
        break


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

    reader = pa.RecordBatchReader.from_batches(schema, datagen(args))
    list(datagen(args))
    # lance.write_dataset(reader, "./diffusiondb.lance")


if __name__ == "__main__":
    main()
