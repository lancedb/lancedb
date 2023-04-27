# Serverless LanceDB

## Store your data on S3 and use Lambda to compute embeddings and retrieve queries in production easily.

<img id="splash" width="400" alt="s3-lambda" src="https://user-images.githubusercontent.com/917119/234653050-305a1e90-9305-40ab-b014-c823172a948c.png">

This is a great option if you're wanting to scale with your use case and save effort and costs of maintenance.

Let's walk through how to get a simple Lambda function that queries the SIFT dataset on S3.

Before we start, you'll need to ensure you create a secure account access to AWS. We recommend using user policies, as this way AWS can share credentials securely without you having to pass around environment variables into Lambda.

We'll also use a container to ship our Lambda code. This is a good option for Lambda as you don't have the space limits that you would otherwise by building a package yourself.

First, let's create a new `Dockerfile` using the AWS python container base:

```docker
FROM public.ecr.aws/lambda/python:3.10

RUN pip3 install --upgrade pip
RUN pip3 install --no-cache-dir -U numpy --target "${LAMBDA_TASK_ROOT}"
RUN pip3 install --no-cache-dir -U pylance --target "${LAMBDA_TASK_ROOT}"

COPY app.py ${LAMBDA_TASK_ROOT}

CMD [ "app.handler" ]
```

Now let's make a simple Lambda function that queries the SIFT dataset, and allows the user to enter a vector and change the nearest neighbour parameter in `app.py`.

```python    
import time
import json

import numpy as np
import lance
from lance.vector import vec_to_table

s3_dataset = lance.dataset("s3://eto-public/datasets/sift/vec_data.lance")

def handler(event, context):
    status_code = 200
    num_k = 10

    if event['query_vector'] is None:
        status_code = 404
        return {
            "statusCode": status_code,
            "headers": {
                "Content-Type": "application/json"
            },
            "body": json.dumps({
                "Error ": "No vector to query was issued"
            })
        }
    
    # Shape of SIFT is (128,1M), d=float32
    query_vector = np.array(event['query_vector'], dtype=np.float32)
    
    if event['num_k'] is not None:
        num_k = event['num_k']
    
    if event['debug'] is not None:
        rs = s3_dataset.to_table(nearest={"column": "vector", "k": num_k, "q": query_vector})
    else:
        rs = s3_dataset.to_table(nearest={"column": "vector", "k": num_k, "q": query_vector})

    return {
        "statusCode": status_code,
        "headers": {
            "Content-Type": "application/json"
        },
        "body": rs.to_pandas().to_json()
    }
