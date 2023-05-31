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

from argparse import ArgumentParser

from datasets import load_dataset
from transformers import CLIPProcessor, CLIPModel, CLIPTokenizerFast

MODEL_ID = "openai/clip-vit-base-patch32"

device = "cuda"

tokenizer = CLIPTokenizerFast.from_pretrained(MODEL_ID)
model = CLIPModel.from_pretrained("openai/clip-vit-base-patch32").to(device)
processor = CLIPProcessor.from_pretrained("openai/clip-vit-base-patch32")


def generate_clip_embeddings(batch):
    # print(batches)
    # print(batch.keys())
    image = processor(text=None, images=batch["image"], return_tensors='pt')["pixel_values"].to(device)
    print(image.shape)
    img_emb = model.get_image_features(image)
    print(img_emb.shape)
    return batch


def datagen(args):
    dataset = load_dataset("poloclub/diffusiondb", args.subset)

    for b in dataset.map(generate_clip_embeddings, batched=True, batch_size=512):
        print(b)
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
    datagen(args)


if __name__ == "__main__":
    main()
