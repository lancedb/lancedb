# OCI Generative AI Embeddings

[Oracle Cloud Infrastructure (OCI) Generative AI](https://docs.oracle.com/en-us/iaas/Content/generative-ai/home.htm) hosts Cohere embedding models behind an OCI-native, IAM-authenticated API.

Using the `oci-genai` embedding function requires the OCI Python SDK, which can be installed using `pip install oci`. No API key is stored in LanceDB: requests are signed by the SDK with one of the standard OCI authentication methods, so a compartment OCID and an identity that is allowed to `use generative-ai-family` in that compartment are all that is needed.

Supported models are:

- `cohere.embed-v4.0` (1536 dims by default; `output_dimensions` can request 256, 512, 1024 or 1536)
- `cohere.embed-english-v3.0`, `cohere.embed-multilingual-v3.0` (1024 dims)
- `cohere.embed-english-light-v3.0`, `cohere.embed-multilingual-light-v3.0` (384 dims)
- Any dedicated AI cluster endpoint OCID (`ocid1.generativeaiendpoint...`) hosting an embedding model

Cohere embedding models are asymmetric: rows of the source column are embedded with `input_type="SEARCH_DOCUMENT"` and search queries with `input_type="SEARCH_QUERY"`. Inputs are sent in batches of at most 96 texts (the service limit), and the vector size is discovered with a single probe request unless `output_dimensions` is set.

Supported parameters (to be passed in `create` method) are:

| Parameter | Type | Default Value | Description |
|---|---|--------|---------|
| `name` | `str` | `"cohere.embed-v4.0"` | Model ID of an on-demand embedding model, or a dedicated AI cluster endpoint OCID. |
| `compartment_id` | `str` | `None` | OCID of the compartment the requests are authorized in and billed to. Required by the service; falls back to the `OCI_COMPARTMENT_ID` environment variable. |
| `region` | `str` | `"us-chicago-1"` | Region used to derive the inference endpoint `https://inference.generativeai.<region>.oci.oraclecloud.com`. |
| `service_endpoint` | `str` | `None` | Full inference endpoint. Overrides `region`; use it for realms with a different endpoint template. |
| `auth_type` | `str` | `"API_KEY"` | `API_KEY`, `SECURITY_TOKEN`, `INSTANCE_PRINCIPAL` or `RESOURCE_PRINCIPAL` (see below). |
| `auth_profile` | `str` | `"DEFAULT"` | Profile name in the OCI config file (`API_KEY` and `SECURITY_TOKEN` only). |
| `auth_file_location` | `str` | `"~/.oci/config"` | Path of the OCI config file (`API_KEY` and `SECURITY_TOKEN` only). |
| `truncate` | `str` | `"END"` | What to do with inputs longer than the model context: `NONE` (fail), `START` or `END` (drop tokens from that side). |
| `source_input_type` | `str` | `"SEARCH_DOCUMENT"` | `input_type` used when embedding the source column. |
| `query_input_type` | `str` | `"SEARCH_QUERY"` | `input_type` used when embedding queries. |
| `output_dimensions` | `int` | `None` | Embedding size for models that support it (`cohere.embed-v4.0`: 256, 512, 1024, 1536). |

Authentication options:

| `auth_type` | Identity used |
|---|---|
| `API_KEY` | User API key from profile `auth_profile` in `auth_file_location` (the setup produced by `oci setup config`). |
| `SECURITY_TOKEN` | Short-lived session token from the same profile, as written by `oci session authenticate`. |
| `INSTANCE_PRINCIPAL` | The Compute instance the code runs on; needs a dynamic group policy. |
| `RESOURCE_PRINCIPAL` | The OCI resource the code runs in (OKE workload identity, Functions, Data Science, ...). |

Usage Example:

```python
    import lancedb
    from lancedb.pydantic import LanceModel, Vector
    from lancedb.embeddings import get_registry

    oci_genai = get_registry().get("oci-genai").create(
        name="cohere.embed-v4.0",
        compartment_id="ocid1.compartment.oc1..<your-compartment>",
        auth_type="API_KEY",
        auth_profile="DEFAULT",
        region="us-chicago-1",
    )

    class TextModel(LanceModel):
        text: str = oci_genai.SourceField()
        vector: Vector(oci_genai.ndims()) = oci_genai.VectorField()

    data = [{"text": "hello world"},
            {"text": "goodbye world"}]

    db = lancedb.connect("~/.lancedb")
    tbl = db.create_table("test", schema=TextModel, mode="overwrite")

    tbl.add(data)
    rs = tbl.search("hello").limit(1).to_pandas()
```

The compartment OCID can also be supplied at runtime instead of being stored in the table metadata, either through the `OCI_COMPARTMENT_ID` environment variable or with a registry variable:

```python
    registry = get_registry()
    registry.set_var("oci_compartment", "ocid1.compartment.oc1..<your-compartment>")
    oci_genai = registry.get("oci-genai").create(compartment_id="$var:oci_compartment")
```
