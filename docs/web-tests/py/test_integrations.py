import os

import pytest


def require_env(var_name: str) -> str:
    """Skip the test unless the required environment variable is provided."""
    value = os.environ.get(var_name)
    if not value:
        pytest.skip(f"Set {var_name} to run this integration snippet")
    return value


def require_flag(flag_name: str) -> None:
    """Skip unless a generic feature flag is set to a truthy value."""
    value = os.environ.get(flag_name, "").lower()
    if value not in {"1", "true", "yes", "on"}:
        pytest.skip(f"Enable {flag_name} to run this integration snippet")


def test_embedding_openai_basic() -> None:
    require_env("OPENAI_API_KEY")

    # --8<-- [start:embedding_openai_basic]
    import tempfile
    from pathlib import Path

    import lancedb
    from lancedb.embeddings import get_registry
    from lancedb.pydantic import LanceModel, Vector

    db_path = Path(tempfile.mkdtemp()) / "openai-embeddings"
    db = lancedb.connect(str(db_path))
    func = get_registry().get("openai").create(name="text-embedding-ada-002")

    class Words(LanceModel):
        text: str = func.SourceField()
        vector: Vector(func.ndims()) = func.VectorField()

    table = db.create_table("words", schema=Words, mode="overwrite")
    table.add(
        [
            {"text": "hello world"},
            {"text": "goodbye world"},
        ]
    )

    query = "greetings"
    actual = table.search(query).limit(1).to_pydantic(Words)[0]
    print(actual.text)
    # --8<-- [end:embedding_openai_basic]


def test_embedding_aws_usage() -> None:
    require_flag("RUN_AWS_BEDROCK_SNIPPETS")

    # --8<-- [start:embedding_aws_usage]
    import tempfile
    from pathlib import Path

    import lancedb
    import pandas as pd
    from lancedb.embeddings import get_registry
    from lancedb.pydantic import LanceModel, Vector

    model = get_registry().get("bedrock-text").create()

    class TextModel(LanceModel):
        text: str = model.SourceField()
        vector: Vector(model.ndims()) = model.VectorField()

    df = pd.DataFrame({"text": ["hello world", "goodbye world"]})
    db = lancedb.connect(str(Path(tempfile.mkdtemp()) / "bedrock-demo"))
    tbl = db.create_table("test", schema=TextModel, mode="overwrite")

    tbl.add(df)
    rs = tbl.search("hello").limit(1).to_pandas()
    print(rs.head())
    # --8<-- [end:embedding_aws_usage]


def test_embedding_cohere_usage() -> None:
    require_env("COHERE_API_KEY")

    # --8<-- [start:embedding_cohere_usage]
    import tempfile
    from pathlib import Path

    import lancedb
    from lancedb.embeddings import EmbeddingFunctionRegistry
    from lancedb.pydantic import LanceModel, Vector

    cohere = (
        EmbeddingFunctionRegistry.get_instance()
        .get("cohere")
        .create(name="embed-multilingual-v2.0")
    )

    class TextModel(LanceModel):
        text: str = cohere.SourceField()
        vector: Vector(cohere.ndims()) = cohere.VectorField()

    data = [{"text": "hello world"}, {"text": "goodbye world"}]

    db = lancedb.connect(str(Path(tempfile.mkdtemp()) / "cohere-demo"))
    tbl = db.create_table("test", schema=TextModel, mode="overwrite")
    tbl.add(data)
    # --8<-- [end:embedding_cohere_usage]


def test_embedding_gemini_usage() -> None:
    require_flag("RUN_GEMINI_SNIPPETS")

    # --8<-- [start:embedding_gemini_usage]
    import tempfile
    from pathlib import Path

    import lancedb
    import pandas as pd
    from lancedb.embeddings import get_registry
    from lancedb.pydantic import LanceModel, Vector

    model = get_registry().get("gemini-text").create()

    class TextModel(LanceModel):
        text: str = model.SourceField()
        vector: Vector(model.ndims()) = model.VectorField()

    df = pd.DataFrame({"text": ["hello world", "goodbye world"]})
    db = lancedb.connect(str(Path(tempfile.mkdtemp()) / "gemini-demo"))
    tbl = db.create_table("test", schema=TextModel, mode="overwrite")

    tbl.add(df)
    rs = tbl.search("hello").limit(1).to_pandas()
    print(rs.head())
    # --8<-- [end:embedding_gemini_usage]


def test_embedding_huggingface_usage() -> None:
    require_flag("RUN_HUGGINGFACE_SNIPPETS")

    # --8<-- [start:embedding_huggingface_usage]
    import tempfile
    from pathlib import Path

    import lancedb
    import pandas as pd
    from lancedb.embeddings import get_registry
    from lancedb.pydantic import LanceModel, Vector

    db = lancedb.connect(str(Path(tempfile.mkdtemp()) / "huggingface-demo"))
    model = get_registry().get("huggingface").create(name="facebook/bart-base")

    class Words(LanceModel):
        text: str = model.SourceField()
        vector: Vector(model.ndims()) = model.VectorField()

    df = pd.DataFrame({"text": ["hi hello sayonara", "goodbye world"]})
    table = db.create_table("greets", schema=Words)
    table.add(df)
    query = "old greeting"
    actual = table.search(query).limit(1).to_pydantic(Words)[0]
    print(actual.text)
    # --8<-- [end:embedding_huggingface_usage]


def test_frameworks_lerobot_lancedb_usage() -> None:
    require_flag("RUN_LEROBOT_LANCEDB_SNIPPETS")

    # --8<-- [start:frameworks_lerobot_lancedb_image_dataset]
    from lerobot_lancedb import LeRobotLanceDataset

    dataset = LeRobotLanceDataset(
        repo_id="your-org/your-lerobot-lance-images",
        delta_timestamps={
            "observation.images.front": [-0.2, -0.1, 0.0],
        },
        return_uint8=True,
    )

    sample = dataset[0]
    print(sample["observation.state"].shape)
    print(sample["action"].shape)
    # --8<-- [end:frameworks_lerobot_lancedb_image_dataset]

    # --8<-- [start:frameworks_lerobot_lancedb_video_dataset]
    from lerobot_lancedb import LeRobotLanceVideoDataset

    video_dataset = LeRobotLanceVideoDataset(
        repo_id="lance-format/lerobot-pusht-lance",
        delta_timestamps={
            "observation.images.image": [-0.2, -0.1, 0.0],
        },
        return_uint8=True,
    )

    video_sample = video_dataset[0]
    print(video_sample["observation.images.image"].shape)
    # --8<-- [end:frameworks_lerobot_lancedb_video_dataset]

    # --8<-- [start:frameworks_lerobot_open_lance_tables]
    import lancedb

    db = lancedb.connect("hf://datasets/lance-format/lerobot-pusht-lance/data")
    frames = db.open_table("frames")
    episodes = db.open_table("episodes")
    videos = db.open_table("videos")

    print(len(frames), len(episodes), len(videos))
    print(frames.schema)
    # --8<-- [end:frameworks_lerobot_open_lance_tables]

    # --8<-- [start:frameworks_lerobot_filter_frames]
    frame_rows = (
        frames.search()
        .where("episode_index = 0 AND frame_index < 10", prefilter=True)
        .select(["episode_index", "frame_index", "timestamp", "action"])
        .limit(10)
        .to_list()
    )

    for row in frame_rows:
        print(row["episode_index"], row["frame_index"], row["timestamp"])
    # --8<-- [end:frameworks_lerobot_filter_frames]


def test_frameworks_stable_worldmodel_usage() -> None:
    require_flag("RUN_STABLE_WORLDMODEL_SNIPPETS")

    # --8<-- [start:frameworks_stable_worldmodel_collect_lance]
    import stable_worldmodel as swm

    world = swm.World("swm/PushT-v1", num_envs=8)
    world.set_policy(your_expert_policy)
    world.collect("data/pusht_demo.lance", episodes=100, seed=0)
    # --8<-- [end:frameworks_stable_worldmodel_collect_lance]

    # --8<-- [start:frameworks_stable_worldmodel_load_lance]
    dataset = swm.data.load_dataset("data/pusht_demo.lance", num_steps=16)

    batch = dataset[0]
    print(batch.keys())
    # --8<-- [end:frameworks_stable_worldmodel_load_lance]

    # --8<-- [start:frameworks_stable_worldmodel_convert]
    swm.data.convert(
        "data/pusht_demo.lance",
        "data/pusht_video",
        dest_format="video",
        fps=30,
    )
    # --8<-- [end:frameworks_stable_worldmodel_convert]

    # --8<-- [start:frameworks_stable_worldmodel_evaluate]
    from stable_worldmodel.policy import PlanConfig, WorldModelPolicy
    from stable_worldmodel.solver import CEMSolver

    solver = CEMSolver(model=world_model, num_samples=300)
    policy = WorldModelPolicy(solver=solver, config=PlanConfig(horizon=10))

    world.set_policy(policy)
    results = world.evaluate(episodes=50)
    print(f"Success Rate: {results['success_rate']:.1f}%")
    # --8<-- [end:frameworks_stable_worldmodel_evaluate]


def test_embedding_ibm_usage() -> None:
    require_flag("RUN_IBM_WATSONX_SNIPPETS")

    # --8<-- [start:embedding_ibm_usage]
    import os
    import tempfile
    from pathlib import Path

    import lancedb
    from lancedb.embeddings import EmbeddingFunctionRegistry
    from lancedb.pydantic import LanceModel, Vector

    watsonx_embed = (
        EmbeddingFunctionRegistry.get_instance()
        .get("watsonx")
        .create(
            name="ibm/slate-125m-english-rtrvr",
            api_key=os.environ.get("WATSONX_API_KEY"),
            project_id=os.environ.get("WATSONX_PROJECT_ID"),
        )
    )

    class TextModel(LanceModel):
        text: str = watsonx_embed.SourceField()
        vector: Vector(watsonx_embed.ndims()) = watsonx_embed.VectorField()

    data = [
        {"text": "hello world"},
        {"text": "goodbye world"},
    ]

    db = lancedb.connect(str(Path(tempfile.mkdtemp()) / "watsonx-demo"))
    tbl = db.create_table("watsonx_test", schema=TextModel, mode="overwrite")
    tbl.add(data)

    rs = tbl.search("hello").limit(1).to_pandas()
    print(rs.head())
    # --8<-- [end:embedding_ibm_usage]


def test_embedding_imagebind_examples() -> None:
    require_flag("RUN_IMAGEBIND_SNIPPETS")
    pytest.importorskip("imagebind")

    # --8<-- [start:embedding_imagebind_setup]
    import lancedb
    from lancedb.embeddings import get_registry
    from lancedb.pydantic import LanceModel, Vector

    db = lancedb.connect("/tmp/imagebind-db")
    func = get_registry().get("imagebind").create()

    class ImageBindModel(LanceModel):
        text: str
        image_uri: str = func.SourceField()
        audio_path: str
        vector: Vector(func.ndims()) = func.VectorField()

    text_list = ["A dog.", "A car", "A bird"]
    image_paths = [
        "./assets/dog_image.jpg",
        "./assets/car_image.jpg",
        "./assets/bird_image.jpg",
    ]
    audio_paths = [
        "./assets/dog_audio.wav",
        "./assets/car_audio.wav",
        "./assets/bird_audio.wav",
    ]

    inputs = [
        {"text": a, "audio_path": b, "image_uri": c}
        for a, b, c in zip(text_list, audio_paths, image_paths)
    ]

    table = db.create_table("img_bind", schema=ImageBindModel)
    table.add(inputs)
    # --8<-- [end:embedding_imagebind_setup]

    # --8<-- [start:embedding_imagebind_image_search]
    query_image = "./assets/dog_image2.jpg"
    actual = table.search(query_image).limit(1).to_pydantic(ImageBindModel)[0]
    print(actual.text == "dog")
    # --8<-- [end:embedding_imagebind_image_search]

    # --8<-- [start:embedding_imagebind_audio_search]
    query_audio = "./assets/car_audio2.wav"
    actual = table.search(query_audio).limit(1).to_pydantic(ImageBindModel)[0]
    print(actual.text == "car")
    # --8<-- [end:embedding_imagebind_audio_search]

    # --8<-- [start:embedding_imagebind_text_search]
    query = "an animal which flies and tweets"
    actual = table.search(query).limit(1).to_pydantic(ImageBindModel)[0]
    print(actual.text == "bird")
    # --8<-- [end:embedding_imagebind_text_search]


def test_embedding_colpali_examples() -> None:
    require_flag("RUN_COLPALI_SNIPPETS")
    pytest.importorskip("colpali_engine")

    # --8<-- [start:embedding_colpali_setup]
    import tempfile
    from pathlib import Path

    import lancedb
    import pandas as pd
    import requests
    from lancedb.embeddings import get_registry
    from lancedb.pydantic import LanceModel, MultiVector

    db = lancedb.connect(str(Path(tempfile.mkdtemp()) / "colpali-demo"))
    func = get_registry().get("colpali").create()

    class Images(LanceModel):
        label: str
        image_uri: str = func.SourceField()
        image_bytes: bytes = func.SourceField()
        vector: MultiVector(func.ndims()) = func.VectorField()
        vec_from_bytes: MultiVector(func.ndims()) = func.VectorField()

    table = db.create_table("images", schema=Images)
    labels = ["cat", "dog", "horse"]
    uris = [
        "http://farm1.staticflickr.com/53/167798175_7c7845bbbd_z.jpg",
        "http://farm9.staticflickr.com/8387/8602747737_2e5c2a45d4_z.jpg",
        "http://farm9.staticflickr.com/8216/8434969557_d37882c42d_z.jpg",
    ]
    image_bytes = [requests.get(uri).content for uri in uris]
    table.add(
        pd.DataFrame({"label": labels, "image_uri": uris, "image_bytes": image_bytes})
    )
    # --8<-- [end:embedding_colpali_setup]

    # --8<-- [start:embedding_colpali_text_search]
    actual = (
        table.search("a furry pet", vector_column_name="vector")
        .limit(1)
        .to_pydantic(Images)[0]
    )
    print(actual.label)
    # --8<-- [end:embedding_colpali_text_search]


def test_embedding_instructor_usage() -> None:
    require_flag("RUN_INSTRUCTOR_SNIPPETS")

    # --8<-- [start:embedding_instructor_usage]
    import tempfile
    from pathlib import Path

    import lancedb
    from lancedb.embeddings import get_registry
    from lancedb.pydantic import LanceModel, Vector

    instructor = (
        get_registry()
        .get("instructor")
        .create(
            source_instruction="represent the document for retrieval",
            query_instruction="represent the document for retrieving the most similar documents",
        )
    )

    class Schema(LanceModel):
        vector: Vector(instructor.ndims()) = instructor.VectorField()
        text: str = instructor.SourceField()

    db = lancedb.connect(str(Path(tempfile.mkdtemp()) / "instructor-demo"))
    tbl = db.create_table("test", schema=Schema, mode="overwrite")

    texts = [
        {
            "text": "Capitalism has been dominant in the Western world since the end of feudalism."
        },
        {
            "text": "The disparate impact theory is especially controversial under the Fair Housing Act."
        },
        {
            "text": "Disparate impact in United States labor law refers to practices in employment."
        },
    ]

    tbl.add(texts)
    # --8<-- [end:embedding_instructor_usage]


def test_embedding_jina_text() -> None:
    require_env("JINA_API_KEY")

    # --8<-- [start:embedding_jina_text]
    import os
    import tempfile
    from pathlib import Path

    import lancedb
    from lancedb.embeddings import EmbeddingFunctionRegistry
    from lancedb.pydantic import LanceModel, Vector

    os.environ["JINA_API_KEY"] = os.environ["JINA_API_KEY"]

    jina_embed = (
        EmbeddingFunctionRegistry.get_instance()
        .get("jina")
        .create(name="jina-embeddings-v2-base-en")
    )

    class TextModel(LanceModel):
        text: str = jina_embed.SourceField()
        vector: Vector(jina_embed.ndims()) = jina_embed.VectorField()

    data = [{"text": "hello world"}, {"text": "goodbye world"}]

    db = lancedb.connect(str(Path(tempfile.mkdtemp()) / "jina-text"))
    tbl = db.create_table("test", schema=TextModel, mode="overwrite")

    tbl.add(data)
    # --8<-- [end:embedding_jina_text]


def test_embedding_jina_multimodal() -> None:
    require_flag("RUN_JINA_MULTIMODAL_SNIPPETS")

    # --8<-- [start:embedding_jina_multimodal]
    import os
    import tempfile
    from pathlib import Path

    import lancedb
    import pandas as pd
    import requests
    from lancedb.embeddings import get_registry
    from lancedb.pydantic import LanceModel, Vector

    os.environ["JINA_API_KEY"] = os.environ.get("JINA_API_KEY", "jina_*")

    db = lancedb.connect(str(Path(tempfile.mkdtemp()) / "jina-images"))
    func = get_registry().get("jina").create()

    class Images(LanceModel):
        label: str
        image_uri: str = func.SourceField()
        image_bytes: bytes = func.SourceField()
        vector: Vector(func.ndims()) = func.VectorField()
        vec_from_bytes: Vector(func.ndims()) = func.VectorField()

    table = db.create_table("images", schema=Images)
    labels = ["cat", "cat", "dog", "dog", "horse", "horse"]
    uris = [
        "http://farm1.staticflickr.com/53/167798175_7c7845bbbd_z.jpg",
        "http://farm1.staticflickr.com/134/332220238_da527d8140_z.jpg",
        "http://farm9.staticflickr.com/8387/8602747737_2e5c2a45d4_z.jpg",
        "http://farm5.staticflickr.com/4092/5017326486_1f46057f5f_z.jpg",
        "http://farm9.staticflickr.com/8216/8434969557_d37882c42d_z.jpg",
        "http://farm6.staticflickr.com/5142/5835678453_4f3a4edb45_z.jpg",
    ]
    image_bytes = [requests.get(uri).content for uri in uris]
    table.add(
        pd.DataFrame({"label": labels, "image_uri": uris, "image_bytes": image_bytes})
    )
    # --8<-- [end:embedding_jina_multimodal]


def test_embedding_ollama_usage() -> None:
    require_flag("RUN_OLLAMA_SNIPPETS")

    # --8<-- [start:embedding_ollama_usage]
    import tempfile
    from pathlib import Path

    import lancedb
    from lancedb.embeddings import get_registry
    from lancedb.pydantic import LanceModel, Vector

    db = lancedb.connect(str(Path(tempfile.mkdtemp()) / "ollama-demo"))
    func = get_registry().get("ollama").create(name="nomic-embed-text")

    class Words(LanceModel):
        text: str = func.SourceField()
        vector: Vector(func.ndims()) = func.VectorField()

    table = db.create_table("words", schema=Words, mode="overwrite")
    table.add(
        [
            {"text": "hello world"},
            {"text": "goodbye world"},
        ]
    )

    query = "greetings"
    actual = table.search(query).limit(1).to_pydantic(Words)[0]
    print(actual.text)
    # --8<-- [end:embedding_ollama_usage]


def test_embedding_openclip_examples() -> None:
    require_flag("RUN_OPENCLIP_SNIPPETS")

    # --8<-- [start:embedding_openclip_setup]
    import tempfile
    from pathlib import Path

    import lancedb
    import pandas as pd
    import requests
    from lancedb.embeddings import get_registry
    from lancedb.pydantic import LanceModel, Vector

    db = lancedb.connect(str(Path(tempfile.mkdtemp()) / "openclip-demo"))
    func = get_registry().get("open-clip").create()

    class Images(LanceModel):
        label: str
        image_uri: str = func.SourceField()
        image_bytes: bytes = func.SourceField()
        vector: Vector(func.ndims()) = func.VectorField()
        vec_from_bytes: Vector(func.ndims()) = func.VectorField()

    table = db.create_table("images", schema=Images)
    labels = ["cat", "cat", "dog", "dog", "horse", "horse"]
    uris = [
        "http://farm1.staticflickr.com/53/167798175_7c7845bbbd_z.jpg",
        "http://farm1.staticflickr.com/134/332220238_da527d8140_z.jpg",
        "http://farm9.staticflickr.com/8387/8602747737_2e5c2a45d4_z.jpg",
        "http://farm5.staticflickr.com/4092/5017326486_1f46057f5f_z.jpg",
        "http://farm9.staticflickr.com/8216/8434969557_d37882c42d_z.jpg",
        "http://farm6.staticflickr.com/5142/5835678453_4f3a4edb45_z.jpg",
    ]
    image_bytes = [requests.get(uri).content for uri in uris]
    table.add(
        pd.DataFrame({"label": labels, "image_uri": uris, "image_bytes": image_bytes})
    )
    # --8<-- [end:embedding_openclip_setup]

    # --8<-- [start:embedding_openclip_text_search]
    actual = table.search("man's best friend").limit(1).to_pydantic(Images)[0]
    print(actual.label)

    frombytes = (
        table.search("man's best friend", vector_column_name="vec_from_bytes")
        .limit(1)
        .to_pydantic(Images)[0]
    )
    print(frombytes.label)
    # --8<-- [end:embedding_openclip_text_search]

    # --8<-- [start:embedding_openclip_image_search]
    import io

    from PIL import Image

    query_image_uri = "http://farm1.staticflickr.com/200/467715466_ed4a31801f_z.jpg"
    image_bytes = requests.get(query_image_uri).content
    query_image = Image.open(io.BytesIO(image_bytes))
    actual = table.search(query_image).limit(1).to_pydantic(Images)[0]
    print(actual.label == "dog")

    other = (
        table.search(query_image, vector_column_name="vec_from_bytes")
        .limit(1)
        .to_pydantic(Images)[0]
    )
    print(other.label)
    # --8<-- [end:embedding_openclip_image_search]


def test_embedding_sentence_transformers_baai() -> None:
    require_flag("RUN_SENTENCE_TRANSFORMERS_SNIPPETS")

    # --8<-- [start:embedding_sentence_transformers_baai]
    import tempfile
    from pathlib import Path

    import lancedb
    from lancedb.embeddings import get_registry
    from lancedb.pydantic import LanceModel, Vector

    db = lancedb.connect(str(Path(tempfile.mkdtemp()) / "sentence-transformers"))
    model = (
        get_registry()
        .get("sentence-transformers")
        .create(name="BAAI/bge-small-en-v1.5", device="cpu")
    )

    class Words(LanceModel):
        text: str = model.SourceField()
        vector: Vector(model.ndims()) = model.VectorField()

    table = db.create_table("words", schema=Words)
    table.add(
        [
            {"text": "hello world"},
            {"text": "goodbye world"},
        ]
    )

    query = "greetings"
    actual = table.search(query).limit(1).to_pydantic(Words)[0]
    print(actual.text)
    # --8<-- [end:embedding_sentence_transformers_baai]


def test_embedding_voyageai_usage() -> None:
    require_env("VOYAGE_API_KEY")

    # --8<-- [start:embedding_voyageai_usage]
    import tempfile
    from pathlib import Path

    import lancedb
    from lancedb.embeddings import EmbeddingFunctionRegistry
    from lancedb.pydantic import LanceModel, Vector

    voyageai = (
        EmbeddingFunctionRegistry.get_instance().get("voyageai").create(name="voyage-3")
    )

    class TextModel(LanceModel):
        text: str = voyageai.SourceField()
        vector: Vector(voyageai.ndims()) = voyageai.VectorField()

    data = [{"text": "hello world"}, {"text": "goodbye world"}]

    db = lancedb.connect(str(Path(tempfile.mkdtemp()) / "voyageai-demo"))
    tbl = db.create_table("test", schema=TextModel, mode="overwrite")

    tbl.add(data)
    # --8<-- [end:embedding_voyageai_usage]


def test_embedding_voyageai_multimodal() -> None:
    require_env("VOYAGE_API_KEY")

    # --8<-- [start:embedding_voyageai_multimodal]
    import tempfile
    from pathlib import Path

    import lancedb
    from lancedb.embeddings import EmbeddingFunctionRegistry
    from lancedb.pydantic import LanceModel, Vector

    # Create multimodal embedding function with custom dimension
    voyageai = (
        EmbeddingFunctionRegistry.get_instance()
        .get("voyageai")
        .create(name="voyage-multimodal-3.5", output_dimension=512)
    )

    class ImageModel(LanceModel):
        image_uri: str = voyageai.SourceField()
        vector: Vector(voyageai.ndims()) = voyageai.VectorField()

    db = lancedb.connect(str(Path(tempfile.mkdtemp()) / "voyageai-multimodal"))
    tbl = db.create_table("images", schema=ImageModel, mode="overwrite")

    # Add images using URLs
    tbl.add(
        [
            {"image_uri": "https://upload.wikimedia.org/wikipedia/commons/thumb/4/47/PNG_transparency_demonstration_1.png/300px-PNG_transparency_demonstration_1.png"},
        ]
    )

    # Search with text query
    results = tbl.search("dice").limit(1).to_list()
    print(results)
    # --8<-- [end:embedding_voyageai_multimodal]


# Reranking integrations


def test_reranking_answerdotai_usage() -> None:
    require_flag("RUN_RERANKER_SNIPPETS")

    # --8<-- [start:reranking_answerdotai_usage]
    import lancedb
    from lancedb.embeddings import get_registry
    from lancedb.pydantic import LanceModel, Vector
    from lancedb.rerankers import AnswerdotaiRerankers

    embedder = get_registry().get("sentence-transformers").create()
    db = lancedb.connect("~/.lancedb")

    class Schema(LanceModel):
        text: str = embedder.SourceField()
        vector: Vector(embedder.ndims()) = embedder.VectorField()

    data = [
        {"text": "hello world"},
        {"text": "goodbye world"},
    ]
    tbl = db.create_table("test", schema=Schema, mode="overwrite")
    tbl.add(data)
    reranker = AnswerdotaiRerankers()

    # Run vector search with a reranker
    result = tbl.search("hello").rerank(reranker=reranker).to_list()

    # Run FTS search with a reranker
    result = tbl.search("hello", query_type="fts").rerank(reranker=reranker).to_list()

    # Run hybrid search with a reranker
    tbl.create_fts_index("text", replace=True)
    result = (
        tbl.search("hello", query_type="hybrid").rerank(reranker=reranker).to_list()
    )
    # --8<-- [end:reranking_answerdotai_usage]


def test_reranking_cohere_usage() -> None:
    require_flag("RUN_RERANKER_SNIPPETS")
    os.environ["COHERE_API_KEY"] = require_env("COHERE_API_KEY")

    # --8<-- [start:reranking_cohere_usage]
    import os

    import lancedb
    from lancedb.embeddings import get_registry
    from lancedb.pydantic import LanceModel, Vector
    from lancedb.rerankers import CohereReranker

    embedder = get_registry().get("sentence-transformers").create()
    db = lancedb.connect("~/.lancedb")

    class Schema(LanceModel):
        text: str = embedder.SourceField()
        vector: Vector(embedder.ndims()) = embedder.VectorField()

    data = [
        {"text": "hello world"},
        {"text": "goodbye world"},
    ]
    tbl = db.create_table("test", schema=Schema, mode="overwrite")
    tbl.add(data)
    reranker = CohereReranker(api_key=os.environ["COHERE_API_KEY"])

    # Run vector search with a reranker
    result = tbl.search("hello").rerank(reranker=reranker).to_list()

    # Run FTS search with a reranker
    result = tbl.search("hello", query_type="fts").rerank(reranker=reranker).to_list()

    # Run hybrid search with a reranker
    tbl.create_fts_index("text", replace=True)
    result = (
        tbl.search("hello", query_type="hybrid").rerank(reranker=reranker).to_list()
    )
    # --8<-- [end:reranking_cohere_usage]


def test_reranking_colbert_usage() -> None:
    require_flag("RUN_RERANKER_SNIPPETS")

    # --8<-- [start:reranking_colbert_usage]
    import lancedb
    from lancedb.embeddings import get_registry
    from lancedb.pydantic import LanceModel, Vector
    from lancedb.rerankers import ColbertReranker

    embedder = get_registry().get("sentence-transformers").create()
    db = lancedb.connect("~/.lancedb")

    class Schema(LanceModel):
        text: str = embedder.SourceField()
        vector: Vector(embedder.ndims()) = embedder.VectorField()

    data = [
        {"text": "hello world"},
        {"text": "goodbye world"},
    ]
    tbl = db.create_table("test", schema=Schema, mode="overwrite")
    tbl.add(data)
    reranker = ColbertReranker()

    # Run vector search with a reranker
    result = tbl.search("hello").rerank(reranker=reranker).to_list()

    # Run FTS search with a reranker
    result = tbl.search("hello", query_type="fts").rerank(reranker=reranker).to_list()

    # Run hybrid search with a reranker
    tbl.create_fts_index("text", replace=True)
    result = (
        tbl.search("hello", query_type="hybrid").rerank(reranker=reranker).to_list()
    )
    # --8<-- [end:reranking_colbert_usage]


def test_reranking_cross_encoder_usage() -> None:
    require_flag("RUN_RERANKER_SNIPPETS")

    # --8<-- [start:reranking_cross_encoder_usage]
    import lancedb
    from lancedb.embeddings import get_registry
    from lancedb.pydantic import LanceModel, Vector
    from lancedb.rerankers import CrossEncoderReranker

    embedder = get_registry().get("sentence-transformers").create()
    db = lancedb.connect("~/.lancedb")

    class Schema(LanceModel):
        text: str = embedder.SourceField()
        vector: Vector(embedder.ndims()) = embedder.VectorField()

    data = [
        {"text": "hello world"},
        {"text": "goodbye world"},
    ]
    tbl = db.create_table("test", schema=Schema, mode="overwrite")
    tbl.add(data)
    reranker = CrossEncoderReranker()

    # Run vector search with a reranker
    result = tbl.search("hello").rerank(reranker=reranker).to_list()

    # Run FTS search with a reranker
    result = tbl.search("hello", query_type="fts").rerank(reranker=reranker).to_list()

    # Run hybrid search with a reranker
    tbl.create_fts_index("text", replace=True)
    result = (
        tbl.search("hello", query_type="hybrid").rerank(reranker=reranker).to_list()
    )
    # --8<-- [end:reranking_cross_encoder_usage]


def test_reranking_jina_usage() -> None:
    require_flag("RUN_RERANKER_SNIPPETS")
    os.environ["JINA_API_KEY"] = require_env("JINA_API_KEY")

    # --8<-- [start:reranking_jina_usage]
    import os

    import lancedb
    from lancedb.embeddings import get_registry
    from lancedb.pydantic import LanceModel, Vector
    from lancedb.rerankers import JinaReranker

    embedder = get_registry().get("jina").create()
    db = lancedb.connect("~/.lancedb")

    class Schema(LanceModel):
        text: str = embedder.SourceField()
        vector: Vector(embedder.ndims()) = embedder.VectorField()

    data = [
        {"text": "hello world"},
        {"text": "goodbye world"},
    ]
    tbl = db.create_table("test", schema=Schema, mode="overwrite")
    tbl.add(data)
    reranker = JinaReranker(api_key=os.environ["JINA_API_KEY"])

    # Run vector search with a reranker
    result = tbl.search("hello").rerank(reranker=reranker).to_list()

    # Run FTS search with a reranker
    result = tbl.search("hello", query_type="fts").rerank(reranker=reranker).to_list()

    # Run hybrid search with a reranker
    tbl.create_fts_index("text", replace=True)
    result = (
        tbl.search("hello", query_type="hybrid").rerank(reranker=reranker).to_list()
    )
    # --8<-- [end:reranking_jina_usage]


def test_reranking_linear_combination_usage() -> None:
    require_flag("RUN_RERANKER_SNIPPETS")

    # --8<-- [start:reranking_linear_combination_usage]
    import lancedb
    from lancedb.embeddings import get_registry
    from lancedb.pydantic import LanceModel, Vector
    from lancedb.rerankers import LinearCombinationReranker

    embedder = get_registry().get("sentence-transformers").create()
    db = lancedb.connect("~/.lancedb")

    class Schema(LanceModel):
        text: str = embedder.SourceField()
        vector: Vector(embedder.ndims()) = embedder.VectorField()

    data = [
        {"text": "hello world"},
        {"text": "goodbye world"},
    ]
    tbl = db.create_table("test", schema=Schema, mode="overwrite")
    tbl.add(data)
    reranker = LinearCombinationReranker()

    # Run hybrid search with a reranker
    tbl.create_fts_index("text", replace=True)
    result = (
        tbl.search("hello", query_type="hybrid").rerank(reranker=reranker).to_list()
    )
    # --8<-- [end:reranking_linear_combination_usage]


def test_reranking_openai_usage() -> None:
    require_flag("RUN_RERANKER_SNIPPETS")
    os.environ["OPENAI_API_KEY"] = require_env("OPENAI_API_KEY")

    # --8<-- [start:reranking_openai_usage]
    import lancedb
    from lancedb.embeddings import get_registry
    from lancedb.pydantic import LanceModel, Vector
    from lancedb.rerankers import OpenaiReranker

    embedder = get_registry().get("sentence-transformers").create()
    db = lancedb.connect("~/.lancedb")

    class Schema(LanceModel):
        text: str = embedder.SourceField()
        vector: Vector(embedder.ndims()) = embedder.VectorField()

    data = [
        {"text": "hello world"},
        {"text": "goodbye world"},
    ]
    tbl = db.create_table("test", schema=Schema, mode="overwrite")
    tbl.add(data)
    reranker = OpenaiReranker()

    # Run vector search with a reranker
    result = tbl.search("hello").rerank(reranker=reranker).to_list()

    # Run FTS search with a reranker
    result = tbl.search("hello", query_type="fts").rerank(reranker=reranker).to_list()

    # Run hybrid search with a reranker
    tbl.create_fts_index("text", replace=True)
    result = (
        tbl.search("hello", query_type="hybrid").rerank(reranker=reranker).to_list()
    )
    # --8<-- [end:reranking_openai_usage]


def test_reranking_rrf_usage() -> None:
    require_flag("RUN_RERANKER_SNIPPETS")

    # --8<-- [start:reranking_rrf_usage]
    import lancedb
    from lancedb.embeddings import get_registry
    from lancedb.pydantic import LanceModel, Vector
    from lancedb.rerankers import RRFReranker

    embedder = get_registry().get("sentence-transformers").create()
    db = lancedb.connect("~/.lancedb")

    class Schema(LanceModel):
        text: str = embedder.SourceField()
        vector: Vector(embedder.ndims()) = embedder.VectorField()

    data = [
        {"text": "hello world"},
        {"text": "goodbye world"},
    ]
    tbl = db.create_table("test", schema=Schema, mode="overwrite")
    tbl.add(data)
    reranker = RRFReranker()

    # Run hybrid search with a reranker
    tbl.create_fts_index("text", replace=True)
    result = (
        tbl.search("hello", query_type="hybrid").rerank(reranker=reranker).to_list()
    )
    # --8<-- [end:reranking_rrf_usage]


def test_reranking_mrr_usage() -> None:
    require_flag("RUN_RERANKER_SNIPPETS")

    # --8<-- [start:reranking_mrr_usage]
    import lancedb
    from lancedb.embeddings import get_registry
    from lancedb.pydantic import LanceModel, Vector
    from lancedb.rerankers import MRRReranker

    embedder = get_registry().get("sentence-transformers").create()
    db = lancedb.connect("~/.lancedb")

    class Schema(LanceModel):
        text: str = embedder.SourceField()
        vector: Vector(embedder.ndims()) = embedder.VectorField()

    data = [
        {"text": "hello world"},
        {"text": "goodbye world"},
    ]
    tbl = db.create_table("test", schema=Schema, mode="overwrite")
    tbl.add(data)
    reranker = MRRReranker(weight_vector=0.7, weight_fts=0.3)

    # Run hybrid search with a reranker
    tbl.create_fts_index("text", replace=True)
    result = (
        tbl.search("hello", query_type="hybrid").rerank(reranker=reranker).to_list()
    )

    # Run multivector search across multiple vector columns
    rs1 = tbl.search("hello").limit(10).with_row_id(True).to_arrow()
    rs2 = tbl.search("greeting").limit(10).with_row_id(True).to_arrow()
    combined = MRRReranker().rerank_multivector([rs1, rs2])
    # --8<-- [end:reranking_mrr_usage]


def test_reranking_voyageai_usage() -> None:
    require_flag("RUN_RERANKER_SNIPPETS")
    os.environ["VOYAGE_API_KEY"] = require_env("VOYAGE_API_KEY")

    # --8<-- [start:reranking_voyageai_usage]
    import os

    import lancedb
    from lancedb.embeddings import get_registry
    from lancedb.pydantic import LanceModel, Vector
    from lancedb.rerankers import VoyageAIReranker

    embedder = get_registry().get("sentence-transformers").create()
    db = lancedb.connect("~/.lancedb")

    class Schema(LanceModel):
        text: str = embedder.SourceField()
        vector: Vector(embedder.ndims()) = embedder.VectorField()

    data = [
        {"text": "hello world"},
        {"text": "goodbye world"},
    ]
    tbl = db.create_table("test", schema=Schema, mode="overwrite")
    tbl.add(data)
    reranker = VoyageAIReranker(model_name="rerank-2")

    # Run vector search with a reranker
    result = tbl.search("hello").rerank(reranker=reranker).to_list()

    # Run FTS search with a reranker
    result = tbl.search("hello", query_type="fts").rerank(reranker=reranker).to_list()

    # Run hybrid search with a reranker
    tbl.create_fts_index("text", replace=True)
    result = (
        tbl.search("hello", query_type="hybrid").rerank(reranker=reranker).to_list()
    )
    # --8<-- [end:reranking_voyageai_usage]


# Framework integrations


def test_frameworks_langchain_examples() -> None:
    require_flag("RUN_LANGCHAIN_SNIPPETS")
    pytest.importorskip("langchain")
    pytest.importorskip("langchain_openai")
    pytest.importorskip("langchain_text_splitters")

    # --8<-- [start:frameworks_langchain_quick_start]
    import os

    from langchain.document_loaders import TextLoader
    from langchain.vectorstores import LanceDB
    from langchain_openai import OpenAIEmbeddings
    from langchain_text_splitters import CharacterTextSplitter

    os.environ["OPENAI_API_KEY"] = "sk-..."

    loader = TextLoader(
        "../../modules/state_of_the_union.txt"
    )  # Replace with your data path
    documents = loader.load()

    documents = CharacterTextSplitter().split_documents(documents)
    embeddings = OpenAIEmbeddings()

    docsearch = LanceDB.from_documents(documents, embeddings)
    query = "What did the president say about Ketanji Brown Jackson"
    docs = docsearch.similarity_search(query)
    print(docs[0].page_content)
    # --8<-- [end:frameworks_langchain_quick_start]

    # --8<-- [start:frameworks_langchain_vector_store_config]
    db_url = "db://lang_test"  # url of db you created
    api_key = "xxxxx"  # your API key
    region = "us-east-1-dev"  # your selected region

    vector_store = LanceDB(
        uri=db_url,
        api_key=api_key,  # (dont include for local API)
        region=region,  # (dont include for local API)
        embedding=embeddings,
        table_name="langchain_test",  # Optional
    )
    # --8<-- [end:frameworks_langchain_vector_store_config]

    # --8<-- [start:frameworks_langchain_add_texts]
    vector_store.add_texts(texts=["test_123"], metadatas=[{"source": "wiki"}])

    # Additionaly, to explore the table you can load it into a df or save it in a csv file:

    tbl = vector_store.get_table()
    print("tbl:", tbl)
    pd_df = tbl.to_pandas()
    pd_df.to_csv("docsearch.csv", index=False)

    # you can also create a new vector store object using an older connection object:
    vector_store = LanceDB(connection=tbl, embedding=embeddings)
    # --8<-- [end:frameworks_langchain_add_texts]

    # --8<-- [start:frameworks_langchain_create_index]
    # for creating vector index
    vector_store.create_index(vector_col="vector", metric="cosine")

    # for creating scalar index(for non-vector columns)
    vector_store.create_index(col_name="text")
    # --8<-- [end:frameworks_langchain_create_index]

    # --8<-- [start:frameworks_langchain_similarity_search]
    docs = docsearch.similarity_search(query)
    print(docs[0].page_content)
    # --8<-- [end:frameworks_langchain_similarity_search]

    # --8<-- [start:frameworks_langchain_similarity_search_by_vector]
    docs = docsearch.similarity_search_by_vector(query)
    print(docs[0].page_content)
    # --8<-- [end:frameworks_langchain_similarity_search_by_vector]

    # --8<-- [start:frameworks_langchain_similarity_search_with_scores]
    docs = docsearch.similarity_search_with_relevance_scores(query)
    print("relevance score - ", docs[0][1])
    print("text- ", docs[0][0].page_content[:1000])
    # --8<-- [end:frameworks_langchain_similarity_search_with_scores]

    # --8<-- [start:frameworks_langchain_similarity_search_by_vector_with_scores]
    query_embedding = embeddings.embed_query("text")
    docs = docsearch.similarity_search_by_vector_with_relevance_scores(query_embedding)
    print("relevance score - ", docs[0][1])
    print("text- ", docs[0][0].page_content[:1000])
    # --8<-- [end:frameworks_langchain_similarity_search_by_vector_with_scores]

    # --8<-- [start:frameworks_langchain_max_marginal_relevance]
    result = docsearch.max_marginal_relevance_search(query="text")
    result_texts = [doc.page_content for doc in result]
    print(result_texts)

    # search by vector :
    result = docsearch.max_marginal_relevance_search_by_vector(
        embeddings.embed_query("text")
    )
    result_texts = [doc.page_content for doc in result]
    print(result_texts)
    # --8<-- [end:frameworks_langchain_max_marginal_relevance]

    # --8<-- [start:frameworks_langchain_add_images]
    image_uris = ["./assets/image-1.png", "./assets/image-2.png"]
    vector_store.add_images(uris=image_uris)
    # here image_uris are local fs paths to the images.
    # --8<-- [end:frameworks_langchain_add_images]


def test_frameworks_llamaindex_examples() -> None:
    require_flag("RUN_LLAMAINDEX_SNIPPETS")
    pytest.importorskip("llama_index")
    pytest.importorskip("llama_index.vector_stores.lancedb")

    # --8<-- [start:frameworks_llamaindex_quick_start]
    import logging
    import sys
    import textwrap

    import openai

    # Uncomment to see debug logs
    # logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)
    # logging.getLogger().addHandler(logging.StreamHandler(stream=sys.stdout))
    from llama_index.core import (
        Document,
        SimpleDirectoryReader,
        StorageContext,
        VectorStoreIndex,
    )
    from llama_index.vector_stores.lancedb import LanceDBVectorStore

    openai.api_key = "sk-..."

    documents = SimpleDirectoryReader("./data/your-data-dir/").load_data()
    print("Document ID:", documents[0].doc_id, "Document Hash:", documents[0].hash)

    ## For LanceDB Enterprise :
    # vector_store = LanceDBVectorStore(
    #     uri="db://db_name", # your remote DB URI
    #     api_key="sk_..", # lancedb enterprise api key
    #     region="your-region" # the region you configured
    #     host_override="https://your-host.com" # if you have a custom host, otherwise omit this
    # )

    vector_store = LanceDBVectorStore(
        uri="./lancedb", mode="overwrite", query_type="vector"
    )
    storage_context = StorageContext.from_defaults(vector_store=vector_store)

    index = VectorStoreIndex.from_documents(documents, storage_context=storage_context)
    lance_filter = "metadata.file_name = 'paul_graham_essay.txt' "
    retriever = index.as_retriever(vector_store_kwargs={"where": lance_filter})
    response = retriever.retrieve("What did the author do growing up?")
    # --8<-- [end:frameworks_llamaindex_quick_start]

    # --8<-- [start:frameworks_llamaindex_filtering]
    from llama_index.core.vector_stores import (
        FilterCondition,
        FilterOperator,
        MetadataFilter,
        MetadataFilters,
    )

    query_filters = MetadataFilters(
        filters=[
            MetadataFilter(
                key="creation_date", operator=FilterOperator.EQ, value="2024-05-23"
            ),
            MetadataFilter(key="file_size", value=75040, operator=FilterOperator.GT),
        ],
        condition=FilterCondition.AND,
    )
    # --8<-- [end:frameworks_llamaindex_filtering]

    # --8<-- [start:frameworks_llamaindex_hybrid_search]
    from lancedb.rerankers import ColbertReranker

    reranker = ColbertReranker()
    vector_store._add_reranker(reranker)

    query_engine = index.as_query_engine(
        filters=query_filters,
        vector_store_kwargs={
            "query_type": "hybrid",
        },
    )

    response = query_engine.query("How much did Viaweb charge per month?")
    # --8<-- [end:frameworks_llamaindex_hybrid_search]

    # --8<-- [start:frameworks_llamaindex_add_reranker]
    from lancedb.rerankers import ColbertReranker

    reranker = ColbertReranker()
    vector_store._add_reranker(reranker)
    # --8<-- [end:frameworks_llamaindex_add_reranker]


def test_frameworks_pydantic_examples() -> None:
    require_flag("RUN_PYDANTIC_SNIPPETS")
    pytest.importorskip("pyarrow")

    # --8<-- [start:frameworks_pydantic_imports]
    import tempfile
    from pathlib import Path

    import lancedb
    from lancedb.pydantic import LanceModel, Vector

    # --8<-- [end:frameworks_pydantic_imports]
    # --8<-- [start:frameworks_pydantic_base_model]
    class LanceDocs(LanceModel):
        text: str
        vector: Vector(2)

    # --8<-- [end:frameworks_pydantic_base_model]

    # --8<-- [start:frameworks_pydantic_set_url]
    db = lancedb.connect(str(Path(tempfile.mkdtemp()) / "pydantic-docs"))
    # --8<-- [end:frameworks_pydantic_set_url]

    # --8<-- [start:frameworks_pydantic_vector_field]
    import pyarrow as pa
    import pydantic
    from lancedb.pydantic import Vector, pydantic_to_schema

    class MyModel(pydantic.BaseModel):
        id: int
        url: str
        embeddings: Vector(768)

    schema = pydantic_to_schema(MyModel)
    assert schema == pa.schema(
        [
            pa.field("id", pa.int64(), False),
            pa.field("url", pa.utf8(), False),
            pa.field("embeddings", pa.list_(pa.float32(), 768)),
        ]
    )
    # --8<-- [end:frameworks_pydantic_vector_field]

    # --8<-- [start:frameworks_pydantic_type_conversion]
    from typing import List, Optional

    import pyarrow as pa
    import pydantic
    from lancedb.pydantic import Vector, pydantic_to_schema

    class FooModel(pydantic.BaseModel):
        id: int
        s: str
        vec: Vector(1536)  # fixed_size_list<item: float32>[1536]
        li: List[int]

    schema = pydantic_to_schema(FooModel)
    assert schema == pa.schema(
        [
            pa.field("id", pa.int64(), False),
            pa.field("s", pa.utf8(), False),
            pa.field("vec", pa.list_(pa.float32(), 1536)),
            pa.field("li", pa.list_(pa.int64()), False),
        ]
    )
    # --8<-- [end:frameworks_pydantic_type_conversion]

    # --8<-- [start:frameworks_pydantic_base_example]
    table = db.create_table("docs", schema=LanceDocs, mode="overwrite")
    table.add(
        [
            {"text": "hello world", "vector": [1.0, 0.0]},
            {"text": "goodbye world", "vector": [0.0, 1.0]},
        ]
    )
    results = table.search("hello world").limit(1).to_pydantic(LanceDocs)
    print(results[0].text)
    # --8<-- [end:frameworks_pydantic_base_example]


# Platform integrations


def test_platforms_dlt_examples() -> None:
    require_flag("RUN_DLT_SNIPPETS")
    pytest.importorskip("dlt")

    # --8<-- [start:platforms_dlt_pipeline]
    # Import necessary modules
    import dlt
    from rest_api import rest_api_source

    # Configure the REST API source
    movies_source = rest_api_source(
        {
            "client": {
                "base_url": "https://www.omdbapi.com/",
                "auth": {  # authentication strategy for the OMDb API
                    "type": "api_key",
                    "name": "apikey",
                    "api_key": dlt.secrets[
                        "sources.rest_api.api_token"
                    ],  # read API credentials directly from secrets.toml
                    "location": "query",
                },
                "paginator": {  # pagination strategy for the OMDb API
                    "type": "page_number",
                    "base_page": 1,
                    "total_path": "totalResults",
                    "maximum_page": 5,
                },
            },
            "resources": [  # list of API endpoints to request
                {
                    "name": "movie_search",
                    "endpoint": {
                        "path": "/",
                        "params": {
                            "s": "godzilla",
                            "type": "movie",
                        },
                    },
                }
            ],
        }
    )

    if __name__ == "__main__":
        # Create a pipeline object
        pipeline = dlt.pipeline(
            pipeline_name="movies_pipeline",
            destination="lancedb",  # this tells dlt to load the data into LanceDB
            dataset_name="movies_data_pipeline",
        )

        # Run the pipeline
        load_info = pipeline.run(movies_source)

        # pretty print the information on data that was loaded
        print(load_info)
    # --8<-- [end:platforms_dlt_pipeline]

    # --8<-- [start:platforms_dlt_adapter_import]
    from dlt.destinations.adapters import lancedb_adapter

    # --8<-- [end:platforms_dlt_adapter_import]
    # --8<-- [start:platforms_dlt_adapter_usage]
    load_info = pipeline.run(
        lancedb_adapter(
            movies_source,
            embed="Title",
        )
    )
    # --8<-- [end:platforms_dlt_adapter_usage]


def test_platforms_duckdb_examples() -> None:
    require_flag("RUN_DUCKDB_SNIPPETS")
    pytest.importorskip("duckdb")

    # --8<-- [start:platforms_duckdb_create_table]
    import lancedb

    db = lancedb.connect("data/sample-lancedb")
    data = [
        {"vector": [3.1, 4.1], "item": "foo", "price": 10.0},
        {"vector": [5.9, 26.5], "item": "bar", "price": 20.0},
    ]
    table = db.create_table("pd_table", data=data)
    # --8<-- [end:platforms_duckdb_create_table]

    # --8<-- [start:platforms_duckdb_query_table]
    import duckdb

    arrow_table = table.to_lance()

    duckdb.query("SELECT * FROM arrow_table")
    # --8<-- [end:platforms_duckdb_query_table]

    # --8<-- [start:platforms_duckdb_mean_price]
    duckdb.query("SELECT mean(price) FROM arrow_table")
    # --8<-- [end:platforms_duckdb_mean_price]


def test_frameworks_agno_openai_examples() -> None:
    require_flag("RUN_AGNO_SNIPPETS")
    pytest.importorskip("agno")
    pytest.importorskip("youtube_transcript_api")

    # --8<-- [start:frameworks_agno_setup]
    import os
    import re

    from agno.agent import Agent
    from agno.knowledge.embedder.openai import OpenAIEmbedder
    from agno.knowledge.knowledge import Knowledge
    from agno.models.openai import OpenAIResponses
    from agno.vectordb.lancedb import LanceDb, SearchType
    from youtube_transcript_api import YouTubeTranscriptApi

    if "OPENAI_API_KEY" not in os.environ:
        os.environ["OPENAI_API_KEY"] = "sk-..."

    def extract_video_id(youtube_url: str) -> str:
        match = re.search(r"(?<=v=)[\w-]+", youtube_url) or re.search(
            r"(?<=be/)[\w-]+", youtube_url
        )
        if not match:
            raise ValueError("Could not parse YouTube video ID from URL")
        return match.group(0)

    knowledge = Knowledge(
        vector_db=LanceDb(
            uri="./tmp/lancedb",
            table_name="youtube_transcripts",
            search_type=SearchType.hybrid,
            embedder=OpenAIEmbedder(id="text-embedding-3-small"),
        ),
    )
    # --8<-- [end:frameworks_agno_setup]

    # --8<-- [start:frameworks_agno_ingest_youtube]
    youtube_url = "https://www.youtube.com/watch?v=wl6mFyXoxos"
    video_id = extract_video_id(youtube_url)
    ytt = YouTubeTranscriptApi()
    transcript_segments = ytt.fetch(video_id, languages=["en", "en-US"]).to_raw_data()
    transcript_text = " ".join(segment["text"] for segment in transcript_segments)

    knowledge.insert(
        name=f"YouTube Transcript ({video_id})",
        text_content=transcript_text,
        metadata={"source": "youtube", "video_id": video_id, "video_url": youtube_url},
    )
    # --8<-- [end:frameworks_agno_ingest_youtube]

    # --8<-- [start:frameworks_agno_agent]
    agent = Agent(
        model=OpenAIResponses(id="gpt-5-mini"),
        knowledge=knowledge,
        search_knowledge=True,
        instructions="Search the transcript and answer only from retrieved context.",
        markdown=True,
    )
    # --8<-- [end:frameworks_agno_agent]

    # --8<-- [start:frameworks_agno_cli_chat]
    agent.print_response(
        "Summarize the loaded video transcript in 5 concise bullet points.",
        stream=True,
    )
    while True:
        question = input("You: ").strip()
        if question.lower() in {"exit", "quit", "bye"}:
            break
        agent.print_response(question, stream=True)
    # --8<-- [end:frameworks_agno_cli_chat]


def test_platforms_voxel51_examples() -> None:
    require_flag("RUN_VOXEL51_SNIPPETS")
    pytest.importorskip("fiftyone")

    # --8<-- [start:platforms_voxel51_load_dataset]
    import fiftyone as fo
    import fiftyone.brain as fob
    import fiftyone.zoo as foz

    # Step 1: Load your data into FiftyOne
    dataset = foz.load_zoo_dataset("quickstart")
    # --8<-- [end:platforms_voxel51_load_dataset]

    # --8<-- [start:platforms_voxel51_compute_similarity]
    # Steps 2 and 3: Compute embeddings and create a similarity index
    lancedb_index = fob.compute_similarity(
        dataset,
        model="clip-vit-base32-torch",
        brain_key="lancedb_index",
        backend="lancedb",
    )
    # --8<-- [end:platforms_voxel51_compute_similarity]

    # --8<-- [start:platforms_voxel51_sort_by_similarity]
    # Step 4: Query your data
    query = dataset.first().id  # query by sample ID
    view = dataset.sort_by_similarity(
        query,
        brain_key="lancedb_index",
        k=10,  # limit to 10 most similar samples
    )
    # --8<-- [end:platforms_voxel51_sort_by_similarity]

    # --8<-- [start:platforms_voxel51_cleanup]
    # Step 5 (optional): Cleanup

    # Delete the LanceDB table
    lancedb_index.cleanup()

    # Delete run record from FiftyOne
    dataset.delete_brain_run("lancedb_index")
    # --8<-- [end:platforms_voxel51_cleanup]

    if False:
        # --8<-- [start:platforms_voxel51_backend_flag]
        import fiftyone.brain as fob

        # Re-run similarity creation using the LanceDB backend explicitly
        fob.compute_similarity(
            dataset,
            model="clip-vit-base32-torch",
            brain_key="lancedb_index",
            backend="lancedb",
        )
        # --8<-- [end:platforms_voxel51_backend_flag]

    # --8<-- [start:platforms_voxel51_brain_config]
    import fiftyone.brain as fob

    # Print your current brain config
    print(fob.brain_config)
    # --8<-- [end:platforms_voxel51_brain_config]

    if False:
        # --8<-- [start:platforms_voxel51_backend_params]
        lancedb_index = fob.compute_similarity(
            dataset,
            model="clip-vit-base32-torch",
            backend="lancedb",
            brain_key="lancedb_index",
            table_name="your-table",
            metric="euclidean",
            uri="/tmp/lancedb",
        )
        # --8<-- [end:platforms_voxel51_backend_params]


def test_platforms_pandas_examples() -> None:
    require_flag("RUN_PANDAS_SNIPPETS")
    pytest.importorskip("pandas")

    # --8<-- [start:platforms_pandas_imports]
    import asyncio
    import tempfile
    from pathlib import Path

    import lancedb
    import pandas as pd

    # --8<-- [end:platforms_pandas_imports]
    # --8<-- [start:platforms_pandas_create_table]
    pandas_df = pd.DataFrame(
        [
            {"id": "1", "text": "dragon", "vector": [0.9, 0.1, 0.3]},
            {"id": "2", "text": "griffin", "vector": [0.4, 0.5, 0.2]},
            {"id": "3", "text": "phoenix", "vector": [0.7, 0.3, 0.6]},
        ]
    )
    pandas_db = lancedb.connect(str(Path(tempfile.mkdtemp()) / "pandas-demo"))
    pandas_table = pandas_db.create_table("creatures", data=pandas_df, mode="overwrite")
    # --8<-- [end:platforms_pandas_create_table]

    # --8<-- [start:platforms_pandas_vector_search]
    pandas_results = (
        pandas_table.search([0.9, 0.1, 0.3])
        .select(["text", "_distance"])
        .limit(1)
        .to_pandas()
    )
    print(pandas_results)
    # --8<-- [end:platforms_pandas_vector_search]

    # --8<-- [start:platforms_pandas_async_example]
    async def run_pandas_async_example() -> None:
        async_db = await lancedb.connect_async(
            str(Path(tempfile.mkdtemp()) / "pandas-async")
        )
        async_df = pd.DataFrame(
            [
                {"id": "10", "text": "sage", "vector": [0.6, 0.4, 0.8]},
                {"id": "11", "text": "bard", "vector": [0.2, 0.7, 0.3]},
            ]
        )
        async_table = await async_db.create_table(
            "creatures_async", data=async_df, mode="overwrite"
        )
        async_results = await (
            async_table.search([0.6, 0.4, 0.8])
            .select(["text", "_distance"])
            .limit(1)
            .to_pandas()
        )
        print(async_results)

    asyncio.run(run_pandas_async_example())
    # --8<-- [end:platforms_pandas_async_example]


def test_platforms_polars_examples() -> None:
    require_flag("RUN_POLARS_SNIPPETS")
    pytest.importorskip("polars")

    # --8<-- [start:platforms_polars_imports]
    import tempfile
    from pathlib import Path

    import lancedb
    import polars as pl
    from lancedb.pydantic import LanceModel, Vector

    # --8<-- [end:platforms_polars_imports]
    # --8<-- [start:platforms_polars_create_table]
    birds = pl.DataFrame(
        {
            "text": ["phoenix", "sparrow"],
            "vector": [
                [0.1, 0.2, 0.3],
                [0.8, 0.6, 0.5],
            ],
        }
    )
    polars_db = lancedb.connect(str(Path(tempfile.mkdtemp()) / "polars-demo"))
    polars_table = polars_db.create_table(
        "birds", data=birds.to_arrow(), mode="overwrite"
    )
    # --8<-- [end:platforms_polars_create_table]

    # --8<-- [start:platforms_polars_vector_search]
    polars_results = (
        polars_table.search([0.1, 0.2, 0.3])
        .select(["text", "_distance"])
        .limit(1)
        .to_polars()
    )
    print(polars_results)
    # --8<-- [end:platforms_polars_vector_search]

    # --8<-- [start:platforms_polars_lazyframe]
    lazy_frame = polars_table.to_polars().lazy()
    print(lazy_frame.select(["text"]).collect())
    # --8<-- [end:platforms_polars_lazyframe]

    # --8<-- [start:platforms_polars_pydantic]
    class BirdModel(LanceModel):
        text: str
        vector: Vector(3)

    schema_table = polars_db.create_table(
        "birds_schema", schema=BirdModel, mode="overwrite"
    )
    schema_table.add(birds.to_dicts())
    # --8<-- [end:platforms_polars_pydantic]
