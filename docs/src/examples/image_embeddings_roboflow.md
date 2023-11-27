# How to Load Image Embeddings into LanceDB

With the rise of Large Multimodal Models (LMMs) such as [GPT-4 Vision](https://blog.roboflow.com/gpt-4-vision/), the need for storing image embeddings is growing. The most effective way to store text and image embeddings is in a vector database such as LanceDB. Vector databases are a special kind of data store that enables efficient search over stored embeddings. 

[CLIP](https://blog.roboflow.com/openai-clip/), a multimodal model developed by OpenAI, is commonly used to calculate image embeddings. These embeddings can then be used with a vector database to build a semantic search engine that you can query using images or text. For example, you could use LanceDB and CLIP embeddings to build a search engine for a database of folders.

In this guide, we are going to show you how to use Roboflow Inference to load image embeddings into LanceDB. Without further ado, let’s get started!

## Step #1: Install Roboflow Inference

[Roboflow Inference](https://inference.roboflow.com) enables you to run state-of-the-art computer vision models with minimal configuration. Inference supports a range of models, from fine-tuned object detection, classification, and segmentation models to foundation models like CLIP. We will use Inference to calculate CLIP image embeddings.

Inference provides a HTTP API through which you can run vision models.

Inference powers the Roboflow hosted API, and is available as an open source utility. In this guide, we are going to run Inference locally, which enables you to calculate CLIP embeddings on your own hardware. We will also show you how to use the hosted Roboflow CLIP API, which is ideal if you need to scale and do not want to manage a system for calculating embeddings.

To get started, first install the Inference CLI:

```
pip install inference-cli
```

Next, install Docker. Refer to the official Docker installation instructions for your operating system to get Docker set up. Once Docker is ready, you can start Inference using the following command:

```
inference server start
```

An Inference server will start running at ‘http://localhost:9001’.

## Step #2: Set Up a LanceDB Vector Database

Now that we have Inference running, we can set up a LanceDB vector database. You can run LanceDB in JavaScript and Python. For this guide, we will use the Python API. But, you can take the HTTP requests we make below and change them to JavaScript if required.

For this guide, we are going to search the [COCO 128 dataset](https://universe.roboflow.com/team-roboflow/coco-128), which contains a wide range of objects. The variability in objects present in this dataset makes it a good dataset to demonstrate the capabilities of vector search. If you want to use this dataset, you can download [COCO 128 from Roboflow Universe](https://universe.roboflow.com/team-roboflow/coco-128). With that said, you can search whatever folder of images you want.

Once you have a dataset ready, install LanceDB with the following command:

```
pip install lancedb
```

We also need to install a specific commit of `tantivy`, a dependency of the LanceDB full text search engine we will use later in this guide:

```
pip install tantivy@git+https://github.com/quickwit-oss/tantivy-py#164adc87e1a033117001cf70e38c82a53014d985
```

Create a new Python file and add the following code:

```python
import cv2
import supervision as sv
import requests

import lancedb

db = lancedb.connect("./embeddings")

IMAGE_DIR = "images/"
API_KEY = os.environ.get("ROBOFLOW_API_KEY")
SERVER_URL = "http://localhost:9001"

results = []

for i, image in enumerate(os.listdir(IMAGE_DIR)):
    infer_clip_payload = {
        #Images can be provided as urls or as base64 encoded strings
        "image": {
            "type": "base64",
            "value": base64.b64encode(open(IMAGE_DIR + image, "rb").read()).decode("utf-8"),
        },
    }

    res = requests.post(
        f"{SERVER_URL}/clip/embed_image?api_key={API_KEY}",
        json=infer_clip_payload,
    )

    embeddings = res.json()['embeddings']

    print("Calculated embedding for image: ", image)

    image = {"vector": embeddings[0], "name": os.path.join(IMAGE_DIR, image)}

    results.append(image)

tbl = db.create_table("images", data=results)

tbl.create_fts_index("name")
```

To use the code above, you will need a Roboflow API key. [Learn how to retrieve a Roboflow API key](https://docs.roboflow.com/api-reference/authentication#retrieve-an-api-key). Run the following command to set up your API key in your environment:

```
export ROBOFLOW_API_KEY=""
```

Replace the `IMAGE_DIR` value with the folder in which you are storing the images for which you want to calculate embeddings. If you want to use the Roboflow CLIP API to calculate embeddings, replace the `SERVER_URL` value with `https://infer.roboflow.com`.

Run the script above to create a new LanceDB database. This database will be stored on your local machine. The database will be called `embeddings` and the table will be called `images`.

The script above calculates all embeddings for a folder then creates a new table. To add additional images, use the following code:

```python
def make_batches():
    for i in range(5):
        yield [
                {"vector": [3.1, 4.1], "name": "image1.png"},
                {"vector": [5.9, 26.5], "name": "image2.png"}
            ]

tbl = db.open_table("images")
tbl.add(make_batches())
```

Replacing the `make_batches()` function with code to load embeddings for images.

## Step #3: Run a Search Query

We are now ready to run a search query. To run a search query, we need a text embedding that represents a text query. We can use this embedding to search our LanceDB database for an entry.

Let’s calculate a text embedding for the query “cat”, then run a search query:

```python
infer_clip_payload = {
    "text": "cat",
}

res = requests.post(
    f"{SERVER_URL}/clip/embed_text?api_key={API_KEY}",
    json=infer_clip_payload,
)

embeddings = res.json()['embeddings']

df = tbl.search(embeddings[0]).limit(3).to_list()

print("Results:")

for i in df:
    print(i["name"])
```

This code will search for the three images most closely related to the prompt “cat”. The names of the most similar three images will be printed to the console. Here are the three top results:

```
dataset/images/train/000000000650_jpg.rf.1b74ba165c5a3513a3211d4a80b69e1c.jpg
dataset/images/train/000000000138_jpg.rf.af439ef1c55dd8a4e4b142d186b9c957.jpg
dataset/images/train/000000000165_jpg.rf.eae14d5509bf0c9ceccddbb53a5f0c66.jpg
```

Let’s open the top image:

![Cat](https://media.roboflow.com/cat_lancedb.jpg)

The top image was a cat. Our search was successful.

## Conclusion

LanceDB is a vector database that you can use to store and efficiently search your image embeddings. You can use Roboflow Inference, a scalable computer vision inference server, to calculate CLIP embeddings that you can store in LanceDB.

You can use Inference and LanceDB together to build a range of applications with image embeddings, from a media search engine to a retrieval-augmented generation pipeline for use with LMMs.

To learn more about Inference and its capabilities, refer to the Inference documentation.