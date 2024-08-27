# Sentence transformers
Allows you to set parameters when registering a `sentence-transformers` object.

!!! info
    Sentence transformer embeddings are normalized by default. It is recommended to use normalized embeddings for similarity search.

| Parameter | Type | Default Value | Description |
|---|---|---|---|
| `name` | `str` | `all-MiniLM-L6-v2` | The name of the model |
| `device` | `str` | `cpu` | The device to run the model on (can be `cpu` or `gpu`) |
| `normalize` | `bool` | `True` | Whether to normalize the input text before feeding it to the model |
| `trust_remote_code` | `bool` | `False` | Whether to trust and execute remote code from the model's Huggingface repository |


??? "Check out available sentence-transformer models here!"
    ```markdown
    - sentence-transformers/all-MiniLM-L12-v2
    - sentence-transformers/paraphrase-mpnet-base-v2
    - sentence-transformers/gtr-t5-base
    - sentence-transformers/LaBSE
    - sentence-transformers/all-MiniLM-L6-v2
    - sentence-transformers/bert-base-nli-max-tokens
    - sentence-transformers/bert-base-nli-mean-tokens
    - sentence-transformers/bert-base-nli-stsb-mean-tokens
    - sentence-transformers/bert-base-wikipedia-sections-mean-tokens
    - sentence-transformers/bert-large-nli-cls-token
    - sentence-transformers/bert-large-nli-max-tokens
    - sentence-transformers/bert-large-nli-mean-tokens
    - sentence-transformers/bert-large-nli-stsb-mean-tokens
    - sentence-transformers/distilbert-base-nli-max-tokens
    - sentence-transformers/distilbert-base-nli-mean-tokens
    - sentence-transformers/distilbert-base-nli-stsb-mean-tokens
    - sentence-transformers/distilroberta-base-msmarco-v1
    - sentence-transformers/distilroberta-base-msmarco-v2
    - sentence-transformers/nli-bert-base-cls-pooling
    - sentence-transformers/nli-bert-base-max-pooling
    - sentence-transformers/nli-bert-base
    - sentence-transformers/nli-bert-large-cls-pooling
    - sentence-transformers/nli-bert-large-max-pooling
    - sentence-transformers/nli-bert-large
    - sentence-transformers/nli-distilbert-base-max-pooling
    - sentence-transformers/nli-distilbert-base
    - sentence-transformers/nli-roberta-base
    - sentence-transformers/nli-roberta-large
    - sentence-transformers/roberta-base-nli-mean-tokens
    - sentence-transformers/roberta-base-nli-stsb-mean-tokens
    - sentence-transformers/roberta-large-nli-mean-tokens
    - sentence-transformers/roberta-large-nli-stsb-mean-tokens
    - sentence-transformers/stsb-bert-base
    - sentence-transformers/stsb-bert-large
    - sentence-transformers/stsb-distilbert-base
    - sentence-transformers/stsb-roberta-base
    - sentence-transformers/stsb-roberta-large
    - sentence-transformers/xlm-r-100langs-bert-base-nli-mean-tokens
    - sentence-transformers/xlm-r-100langs-bert-base-nli-stsb-mean-tokens
    - sentence-transformers/xlm-r-base-en-ko-nli-ststb
    - sentence-transformers/xlm-r-bert-base-nli-mean-tokens
    - sentence-transformers/xlm-r-bert-base-nli-stsb-mean-tokens
    - sentence-transformers/xlm-r-large-en-ko-nli-ststb
    - sentence-transformers/bert-base-nli-cls-token
    - sentence-transformers/all-distilroberta-v1
    - sentence-transformers/multi-qa-MiniLM-L6-dot-v1
    - sentence-transformers/multi-qa-distilbert-cos-v1
    - sentence-transformers/multi-qa-distilbert-dot-v1
    - sentence-transformers/multi-qa-mpnet-base-cos-v1
    - sentence-transformers/multi-qa-mpnet-base-dot-v1
    - sentence-transformers/nli-distilroberta-base-v2
    - sentence-transformers/all-MiniLM-L6-v1
    - sentence-transformers/all-mpnet-base-v1
    - sentence-transformers/all-mpnet-base-v2
    - sentence-transformers/all-roberta-large-v1
    - sentence-transformers/allenai-specter
    - sentence-transformers/average_word_embeddings_glove.6B.300d
    - sentence-transformers/average_word_embeddings_glove.840B.300d
    - sentence-transformers/average_word_embeddings_komninos
    - sentence-transformers/average_word_embeddings_levy_dependency
    - sentence-transformers/clip-ViT-B-32-multilingual-v1
    - sentence-transformers/clip-ViT-B-32
    - sentence-transformers/distilbert-base-nli-stsb-quora-ranking
    - sentence-transformers/distilbert-multilingual-nli-stsb-quora-ranking
    - sentence-transformers/distilroberta-base-paraphrase-v1
    - sentence-transformers/distiluse-base-multilingual-cased-v1
    - sentence-transformers/distiluse-base-multilingual-cased-v2
    - sentence-transformers/distiluse-base-multilingual-cased
    - sentence-transformers/facebook-dpr-ctx_encoder-multiset-base
    - sentence-transformers/facebook-dpr-ctx_encoder-single-nq-base
    - sentence-transformers/facebook-dpr-question_encoder-multiset-base
    - sentence-transformers/facebook-dpr-question_encoder-single-nq-base
    - sentence-transformers/gtr-t5-large
    - sentence-transformers/gtr-t5-xl
    - sentence-transformers/gtr-t5-xxl
    - sentence-transformers/msmarco-MiniLM-L-12-v3
    - sentence-transformers/msmarco-MiniLM-L-6-v3
    - sentence-transformers/msmarco-MiniLM-L12-cos-v5
    - sentence-transformers/msmarco-MiniLM-L6-cos-v5
    - sentence-transformers/msmarco-bert-base-dot-v5
    - sentence-transformers/msmarco-bert-co-condensor
    - sentence-transformers/msmarco-distilbert-base-dot-prod-v3
    - sentence-transformers/msmarco-distilbert-base-tas-b
    - sentence-transformers/msmarco-distilbert-base-v2
    - sentence-transformers/msmarco-distilbert-base-v3
    - sentence-transformers/msmarco-distilbert-base-v4
    - sentence-transformers/msmarco-distilbert-cos-v5
    - sentence-transformers/msmarco-distilbert-dot-v5
    - sentence-transformers/msmarco-distilbert-multilingual-en-de-v2-tmp-lng-aligned
    - sentence-transformers/msmarco-distilbert-multilingual-en-de-v2-tmp-trained-scratch
    - sentence-transformers/msmarco-distilroberta-base-v2
    - sentence-transformers/msmarco-roberta-base-ance-firstp
    - sentence-transformers/msmarco-roberta-base-v2
    - sentence-transformers/msmarco-roberta-base-v3
    - sentence-transformers/multi-qa-MiniLM-L6-cos-v1
    - sentence-transformers/nli-mpnet-base-v2
    - sentence-transformers/nli-roberta-base-v2
    - sentence-transformers/nq-distilbert-base-v1
    - sentence-transformers/paraphrase-MiniLM-L12-v2
    - sentence-transformers/paraphrase-MiniLM-L3-v2
    - sentence-transformers/paraphrase-MiniLM-L6-v2
    - sentence-transformers/paraphrase-TinyBERT-L6-v2
    - sentence-transformers/paraphrase-albert-base-v2
    - sentence-transformers/paraphrase-albert-small-v2
    - sentence-transformers/paraphrase-distilroberta-base-v1
    - sentence-transformers/paraphrase-distilroberta-base-v2
    - sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2
    - sentence-transformers/paraphrase-multilingual-mpnet-base-v2
    - sentence-transformers/paraphrase-xlm-r-multilingual-v1
    - sentence-transformers/quora-distilbert-base
    - sentence-transformers/quora-distilbert-multilingual
    - sentence-transformers/sentence-t5-base
    - sentence-transformers/sentence-t5-large
    - sentence-transformers/sentence-t5-xxl
    - sentence-transformers/sentence-t5-xl
    - sentence-transformers/stsb-distilroberta-base-v2
    - sentence-transformers/stsb-mpnet-base-v2
    - sentence-transformers/stsb-roberta-base-v2
    - sentence-transformers/stsb-xlm-r-multilingual
    - sentence-transformers/xlm-r-distilroberta-base-paraphrase-v1
    - sentence-transformers/clip-ViT-L-14
    - sentence-transformers/clip-ViT-B-16
    - sentence-transformers/use-cmlm-multilingual
    - sentence-transformers/all-MiniLM-L12-v1
    ```

!!! info
    You can also load many other model architectures from the library. For example models from sources such as BAAI, nomic, salesforce research, etc.
    See this HF hub page for all [supported models](https://huggingface.co/models?library=sentence-transformers).

!!! note "BAAI Embeddings example"
    Here is an example that uses BAAI embedding model from the HuggingFace Hub [supported models](https://huggingface.co/models?library=sentence-transformers)
    ```python
    import lancedb
    from lancedb.pydantic import LanceModel, Vector
    from lancedb.embeddings import get_registry

    db = lancedb.connect("/tmp/db")
    model = get_registry().get("sentence-transformers").create(name="BAAI/bge-small-en-v1.5", device="cpu")

    class Words(LanceModel):
        text: str = model.SourceField()
        vector: Vector(model.ndims()) = model.VectorField()

    table = db.create_table("words", schema=Words)
    table.add(
        [
            {"text": "hello world"},
            {"text": "goodbye world"}
        ]
    )

    query = "greetings"
    actual = table.search(query).limit(1).to_pydantic(Words)[0]
    print(actual.text)
    ```
Visit sentence-transformers [HuggingFace HUB](https://huggingface.co/sentence-transformers) page for more information on the available models.

