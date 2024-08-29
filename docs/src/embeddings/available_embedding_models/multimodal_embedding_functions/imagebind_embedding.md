# Imagebind embeddings
We have support for [imagebind](https://github.com/facebookresearch/ImageBind) model embeddings. You can download our version of the packaged model via - `pip install imagebind-packaged==0.1.2`.

This function is registered as `imagebind` and supports Audio, Video and Text modalities(extending to Thermal,Depth,IMU data):

| Parameter | Type | Default Value | Description |
|---|---|---|---|
| `name` | `str` | `"imagebind_huge"` | Name of the model. |
| `device` | `str` | `"cpu"` | The device to run the model on. Can be `"cpu"` or `"gpu"`. |
| `normalize` | `bool` | `False` | set to `True` to normalize your inputs before model ingestion. |

Below is an example demonstrating how the API works:

```python
import lancedb
from lancedb.pydantic import LanceModel, Vector
from lancedb.embeddings import get_registry

db = lancedb.connect(tmp_path)
func = get_registry.get("imagebind").create()

class ImageBindModel(LanceModel):
    text: str
    image_uri: str = func.SourceField()
    audio_path: str
    vector: Vector(func.ndims()) = func.VectorField()

# add locally accessible image paths
text_list=["A dog.", "A car", "A bird"]
image_paths=[".assets/dog_image.jpg", ".assets/car_image.jpg", ".assets/bird_image.jpg"]
audio_paths=[".assets/dog_audio.wav", ".assets/car_audio.wav", ".assets/bird_audio.wav"]

# Load data
inputs = [
    {"text": a, "audio_path": b, "image_uri": c}
    for a, b, c in zip(text_list, audio_paths, image_paths)
]

#create table and add data
table = db.create_table("img_bind", schema=ImageBindModel)
table.add(inputs)
```

Now, we can search using any modality:

#### image search
```python
query_image = "./assets/dog_image2.jpg" #download an image and enter that path here
actual = table.search(query_image).limit(1).to_pydantic(ImageBindModel)[0]
print(actual.text == "dog")
```
#### audio search

```python
query_audio = "./assets/car_audio2.wav" #download an audio clip and enter path here
actual = table.search(query_audio).limit(1).to_pydantic(ImageBindModel)[0]
print(actual.text == "car")
```
#### Text search
You can add any input query and fetch the result as follows:
```python
query = "an animal which flies and tweets" 
actual = table.search(query).limit(1).to_pydantic(ImageBindModel)[0]
print(actual.text == "bird")
```

If you have any questions about the embeddings API, supported models, or see a relevant model missing, please raise an issue [on GitHub](https://github.com/lancedb/lancedb/issues).
