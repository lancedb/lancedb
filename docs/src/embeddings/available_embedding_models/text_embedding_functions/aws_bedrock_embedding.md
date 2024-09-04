# AWS Bedrock Text Embedding Functions

AWS Bedrock supports multiple base models for generating text embeddings. You need to setup the AWS credentials to use this embedding function.
You can do so by using `awscli` and also add your session_token:
```shell
aws configure
aws configure set aws_session_token "<your_session_token>"
```
to ensure that the credentials are set up correctly, you can run the following command:
```shell
aws sts get-caller-identity
```

Supported Embedding modelIDs are:
* `amazon.titan-embed-text-v1`
* `cohere.embed-english-v3`
* `cohere.embed-multilingual-v3`

Supported parameters (to be passed in `create` method) are:

| Parameter | Type | Default Value | Description |
|---|---|---|---|
| **name** | str | "amazon.titan-embed-text-v1" | The model ID of the bedrock model to use. Supported base models for Text Embeddings: amazon.titan-embed-text-v1, cohere.embed-english-v3, cohere.embed-multilingual-v3 |
| **region** | str | "us-east-1" | Optional name of the AWS Region in which the service should be called (e.g., "us-east-1"). |
| **profile_name** | str | None | Optional name of the AWS profile to use for calling the Bedrock service. If not specified, the default profile will be used. |
| **assumed_role** | str | None | Optional ARN of an AWS IAM role to assume for calling the Bedrock service. If not specified, the current active credentials will be used. |
| **role_session_name** | str | "lancedb-embeddings" | Optional name of the AWS IAM role session to use for calling the Bedrock service. If not specified, a "lancedb-embeddings" name will be used. |
| **runtime** | bool | True | Optional choice of getting different client to perform operations with the Amazon Bedrock service. |
| **max_retries** | int | 7 | Optional number of retries to perform when a request fails. |

Usage Example:

```python
import lancedb
from lancedb.pydantic import LanceModel, Vector
from lancedb.embeddings import get_registry
import pandas as pd

model = get_registry().get("bedrock-text").create()

class TextModel(LanceModel):
    text: str = model.SourceField()
    vector: Vector(model.ndims()) = model.VectorField()

df = pd.DataFrame({"text": ["hello world", "goodbye world"]})
db = lancedb.connect("tmp_path")
tbl = db.create_table("test", schema=TextModel, mode="overwrite")

tbl.add(df)
rs = tbl.search("hello").limit(1).to_pandas()
```
