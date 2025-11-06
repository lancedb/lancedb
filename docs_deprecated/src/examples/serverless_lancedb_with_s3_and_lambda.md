# Serverless LanceDB

## Store your data on S3 and use Lambda to compute embeddings and retrieve queries in production easily.

<img id="splash" width="400" alt="s3-lambda" src="https://user-images.githubusercontent.com/917119/234653050-305a1e90-9305-40ab-b014-c823172a948c.png">

This is a great option if you're wanting to scale with your use case and save effort and costs of maintenance.

Let's walk through how to get a simple Lambda function that queries the SIFT dataset on S3.

Before we start, you'll need to ensure you create a secure account access to AWS. We recommend using user policies, as this way AWS can share credentials securely without you having to pass around environment variables into Lambda.

We'll also use a container to ship our Lambda code. This is a good option for Lambda as you don't have the space limits that you would otherwise by building a package yourself.

# Initial setup: creating a LanceDB Table and storing it remotely on S3

We'll use the SIFT vector dataset as an example. To make it easier, we've already made a Lance-format SIFT dataset publicly available, which we can access and use to populate our LanceDB Table. 

To do this, download the Lance files locally first from:

```
s3://eto-public/datasets/sift/vec_data.lance
```

Then, we can write a quick Python script to populate our LanceDB Table:

```python
import lance
sift_dataset = lance.dataset("/path/to/local/vec_data.lance")
df = sift_dataset.to_table().to_pandas()

import lancedb
db = lancedb.connect(".")
table = db.create_table("vector_example", df)
```

Once we've created our Table, we are free to move this data over to S3 so we can remotely host it.

# Building our Lambda app: a simple event handler for vector search

Now that we've got a remotely hosted LanceDB Table, we'll want to be able to query it from Lambda. To do so, let's create a new `Dockerfile` using the AWS python container base:

```docker
FROM public.ecr.aws/lambda/python:3.10

RUN pip3 install --upgrade pip
RUN pip3 install --no-cache-dir -U numpy --target "${LAMBDA_TASK_ROOT}"
RUN pip3 install --no-cache-dir -U lancedb --target "${LAMBDA_TASK_ROOT}"

COPY app.py ${LAMBDA_TASK_ROOT}

CMD [ "app.handler" ]
```

Now let's make a simple Lambda function that queries the SIFT dataset in `app.py`.

```python    
import json
import numpy as np
import lancedb

db = lancedb.connect("s3://eto-public/tables")
table = db.open_table("vector_example")

def handler(event, context):
    status_code = 200

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

    rs = table.search(query_vector).limit(2).to_list()

    return {
        "statusCode": status_code,
        "headers": {
            "Content-Type": "application/json"
        },
        "body": json.dumps(rs)
    }
``` 

# Deploying the container to ECR

The next step is to build and push the container to ECR, where it can then be used to create a new Lambda function. 

It's best to follow the official AWS documentation for how to do this, which you can view here:

```
https://docs.aws.amazon.com/lambda/latest/dg/images-create.html#images-upload
```

# Final step: setting up your Lambda function

Once the container is pushed, you can create a Lambda function by selecting the container. 
