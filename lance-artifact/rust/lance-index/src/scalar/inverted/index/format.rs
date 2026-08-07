// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use super::*;

// Version 0: Arrow TokenSetFormat (legacy)
// Version 1: Fst TokenSetFormat with per-doc compressed positions
// Version 2: Fst TokenSetFormat with shared posting-list position streams.
// Version 3: Reader capability for configurable posting blocks, analyzer
// metadata, and element-document coordinates.
pub const INVERTED_INDEX_VERSION_V1: u32 = 1;
pub const INVERTED_INDEX_VERSION_V2: u32 = 2;
pub const INVERTED_INDEX_VERSION_V3: u32 = 3;
pub const TOKENS_FILE: &str = "tokens.lance";
pub const INVERT_LIST_FILE: &str = "invert.lance";
pub const DOCS_FILE: &str = "docs.lance";
pub const METADATA_FILE: &str = "metadata.lance";

/// Partitions searched per CPU-pool task. Each chunk loads concurrently and
/// then scores sequentially so query concurrency does not flood the pool with
/// one small task per partition. `LANCE_FTS_SEARCH_CHUNK=1` restores the
/// per-partition task shape.
pub(super) fn fts_search_chunk() -> usize {
    static CHUNK: LazyLock<usize> = LazyLock::new(|| {
        std::env::var("LANCE_FTS_SEARCH_CHUNK")
            .ok()
            .and_then(|value| value.parse().ok())
            .filter(|&value| value >= 1)
            .unwrap_or(16)
    });
    *CHUNK
}

pub const TOKEN_COL: &str = "_token";
pub const TOKEN_ID_COL: &str = "_token_id";
pub const TOKEN_FST_BYTES_COL: &str = "_token_fst_bytes";
pub const TOKEN_NEXT_ID_COL: &str = "_token_next_id";
pub const TOKEN_TOTAL_LENGTH_COL: &str = "_token_total_length";
pub const FREQUENCY_COL: &str = "_frequency";
pub const POSITION_COL: &str = "_position";
pub const COMPRESSED_POSITION_COL: &str = "_compressed_position";
pub const POSITION_BLOCK_OFFSET_COL: &str = "_position_block_offset";
pub const POSTING_COL: &str = "_posting";
pub const IMPACT_COL: &str = "_impacts";
pub const MAX_SCORE_COL: &str = "_max_score";
pub const LENGTH_COL: &str = "_length";
pub const BLOCK_MAX_SCORE_COL: &str = "_block_max_score";
pub const NUM_TOKEN_COL: &str = "_num_tokens";
pub const DOC_INDEX_COL: &str = "_doc_index";
pub const DOC_INDEX_STORAGE_PREFIX: &str = "_doc_index_";
pub const SCORE_COL: &str = "_score";
pub const TOKEN_SET_FORMAT_KEY: &str = "token_set_format";
pub const POSTING_TAIL_CODEC_KEY: &str = "posting_tail_codec";
pub const FTS_FORMAT_VERSION_KEY: &str = "format_version";
pub const POSITIONS_LAYOUT_KEY: &str = "positions_layout";
pub const POSITIONS_CODEC_KEY: &str = "positions_codec";
pub const POSTING_BLOCK_SIZE_KEY: &str = "posting_block_size";
pub const POSTING_TAIL_CODEC_FIXED32_V1: &str = "fixed32_v1";
pub const POSTING_TAIL_CODEC_VARINT_DELTA_V1: &str = "varint_delta_v1";
pub const POSITIONS_LAYOUT_SHARED_STREAM_V2: &str = "shared_stream_v2";
pub const POSITIONS_CODEC_VARINT_DOC_DELTA_V2: &str = "varint_doc_delta_v2";
pub const POSITIONS_CODEC_PACKED_DELTA_V1: &str = "packed_delta_v1";
pub const DELETED_FRAGMENTS_COL: &str = "deleted_fragments";

// Just a heuristic when we need to pre-allocate memory for tokens
pub const ESTIMATED_MAX_TOKENS_PER_ROW: usize = 4 * 1024;

pub static SCORE_FIELD: LazyLock<Field> =
    LazyLock::new(|| Field::new(SCORE_COL, DataType::Float32, true));
pub static DOC_INDEX_FIELD: LazyLock<Field> = LazyLock::new(|| {
    Field::new(
        DOC_INDEX_COL,
        DataType::List(Arc::new(Field::new("item", DataType::UInt32, false))),
        false,
    )
});
pub static FTS_SCHEMA: LazyLock<SchemaRef> =
    LazyLock::new(|| Arc::new(Schema::new(vec![ROW_ID_FIELD.clone(), SCORE_FIELD.clone()])));
pub static ELEMENT_FTS_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    Arc::new(Schema::new(vec![
        ROW_ID_FIELD.clone(),
        DOC_INDEX_FIELD.clone(),
        SCORE_FIELD.clone(),
    ]))
});

pub fn fts_schema(document_granularity: DocumentGranularity) -> SchemaRef {
    if document_granularity.is_list_element() {
        ELEMENT_FTS_SCHEMA.clone()
    } else {
        FTS_SCHEMA.clone()
    }
}
pub(super) static ROW_ID_SCHEMA: LazyLock<SchemaRef> =
    LazyLock::new(|| Arc::new(Schema::new(vec![ROW_ID_FIELD.clone()])));

pub fn resolve_fts_format_version(
    value: Option<&str>,
) -> std::result::Result<InvertedListFormatVersion, Error> {
    match value {
        Some(value) => value.parse(),
        None => Ok(default_fts_format_version()),
    }
}

pub fn default_fts_format_version() -> InvertedListFormatVersion {
    InvertedListFormatVersion::V2
}

pub fn current_fts_format_version() -> InvertedListFormatVersion {
    default_fts_format_version()
}

pub fn max_supported_fts_format_version() -> InvertedListFormatVersion {
    InvertedListFormatVersion::V3
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum InvertedListFormatVersion {
    V1,
    #[default]
    V2,
    V3,
}

impl InvertedListFormatVersion {
    pub fn from_posting_tail_codec(codec: PostingTailCodec) -> Self {
        match codec {
            PostingTailCodec::Fixed32 => Self::V1,
            PostingTailCodec::VarintDelta => Self::V2,
        }
    }

    pub fn from_posting_tail_codec_and_block_size(
        codec: PostingTailCodec,
        block_size: usize,
    ) -> Result<Self> {
        validate_block_size(block_size)?;
        let format_version = match (codec, block_size) {
            (PostingTailCodec::Fixed32, LEGACY_BLOCK_SIZE) => Self::V1,
            (PostingTailCodec::VarintDelta, LEGACY_BLOCK_SIZE) => Self::V2,
            (PostingTailCodec::VarintDelta, 256) => Self::V3,
            (PostingTailCodec::Fixed32, 256) => {
                return Err(Error::invalid_input(
                    "FTS format_version=3 requires the varint-delta posting tail codec".to_string(),
                ));
            }
            _ => unreachable!("validate_block_size limits supported block sizes"),
        };
        validate_format_version_block_size(format_version, block_size)?;
        Ok(format_version)
    }

    pub fn index_version(self) -> u32 {
        match self {
            Self::V1 => INVERTED_INDEX_VERSION_V1,
            Self::V2 => INVERTED_INDEX_VERSION_V2,
            Self::V3 => INVERTED_INDEX_VERSION_V3,
        }
    }

    pub fn posting_tail_codec(self) -> PostingTailCodec {
        match self {
            Self::V1 => PostingTailCodec::Fixed32,
            Self::V2 | Self::V3 => PostingTailCodec::VarintDelta,
        }
    }

    pub fn position_codec(self) -> Option<PositionStreamCodec> {
        match self {
            Self::V1 => None,
            Self::V2 | Self::V3 => Some(PositionStreamCodec::PackedDelta),
        }
    }

    pub fn uses_shared_position_stream(self) -> bool {
        matches!(self, Self::V2 | Self::V3)
    }
}

impl FromStr for InvertedListFormatVersion {
    type Err = Error;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s.trim() {
            "1" | "v1" | "V1" => Ok(Self::V1),
            "2" | "v2" | "V2" => Ok(Self::V2),
            "3" | "v3" | "V3" => Ok(Self::V3),
            other => Err(Error::index(format!(
                "unsupported FTS format version {}, expected 1, 2, or 3",
                other
            ))),
        }
    }
}

pub fn default_fts_format_version_for_block_size(
    block_size: usize,
) -> Result<InvertedListFormatVersion> {
    validate_block_size(block_size)?;
    match block_size {
        LEGACY_BLOCK_SIZE => Ok(InvertedListFormatVersion::V2),
        256 => Ok(InvertedListFormatVersion::V3),
        _ => unreachable!("validate_block_size limits supported block sizes"),
    }
}

pub fn validate_format_version_block_size(
    format_version: InvertedListFormatVersion,
    block_size: usize,
) -> Result<()> {
    validate_block_size(block_size)?;
    match (format_version, block_size) {
        (InvertedListFormatVersion::V1 | InvertedListFormatVersion::V2, LEGACY_BLOCK_SIZE)
        | (InvertedListFormatVersion::V3, _) => Ok(()),
        (InvertedListFormatVersion::V1 | InvertedListFormatVersion::V2, 256) => {
            Err(Error::invalid_input(format!(
                "FTS format_version={} is incompatible with block_size=256; use format_version=3",
                format_version.index_version()
            )))
        }
        _ => unreachable!("validate_block_size limits supported block sizes"),
    }
}
#[derive(Copy, Clone, Debug, Eq, PartialEq, Hash, Default)]
pub enum TokenSetFormat {
    Arrow,
    #[default]
    Fst,
}

impl Display for TokenSetFormat {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Arrow => f.write_str("arrow"),
            Self::Fst => f.write_str("fst"),
        }
    }
}

impl FromStr for TokenSetFormat {
    type Err = Error;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s.trim() {
            "" => Ok(Self::Arrow),
            "arrow" => Ok(Self::Arrow),
            "fst" => Ok(Self::Fst),
            other => Err(Error::index(format!(
                "unsupported token set format {}",
                other
            ))),
        }
    }
}

impl DeepSizeOf for TokenSetFormat {
    fn deep_size_of_children(&self, _: &mut lance_core::deepsize::Context) -> usize {
        0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum PositionStreamCodec {
    VarintDocDelta,
    #[default]
    PackedDelta,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum PostingTailCodec {
    Fixed32,
    #[default]
    VarintDelta,
}

impl PostingTailCodec {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Fixed32 => POSTING_TAIL_CODEC_FIXED32_V1,
            Self::VarintDelta => POSTING_TAIL_CODEC_VARINT_DELTA_V1,
        }
    }

    fn from_metadata_value(value: &str) -> Result<Self> {
        match value.trim() {
            POSTING_TAIL_CODEC_FIXED32_V1 => Ok(Self::Fixed32),
            POSTING_TAIL_CODEC_VARINT_DELTA_V1 => Ok(Self::VarintDelta),
            other => Err(Error::index(format!(
                "unsupported posting tail codec {}",
                other
            ))),
        }
    }
}

pub(in super::super) fn parse_posting_tail_codec(
    metadata: &HashMap<String, String>,
) -> Result<PostingTailCodec> {
    Ok(metadata
        .get(POSTING_TAIL_CODEC_KEY)
        .map(|codec| PostingTailCodec::from_metadata_value(codec))
        .transpose()?
        .unwrap_or(PostingTailCodec::Fixed32))
}

pub(in super::super) fn parse_posting_block_size(
    metadata: &HashMap<String, String>,
) -> Result<usize> {
    metadata
        .get(POSTING_BLOCK_SIZE_KEY)
        .map(|value| {
            let block_size = value.parse::<usize>().map_err(|err| {
                Error::index(format!(
                    "invalid {POSTING_BLOCK_SIZE_KEY} metadata value {value:?}: {err}"
                ))
            })?;
            validate_block_size(block_size)
        })
        .transpose()
        .map(|block_size| block_size.unwrap_or(LEGACY_BLOCK_SIZE))
}

impl PositionStreamCodec {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::VarintDocDelta => POSITIONS_CODEC_VARINT_DOC_DELTA_V2,
            Self::PackedDelta => POSITIONS_CODEC_PACKED_DELTA_V1,
        }
    }

    fn from_metadata_value(value: &str) -> Result<Self> {
        match value.trim() {
            POSITIONS_CODEC_VARINT_DOC_DELTA_V2 => Ok(Self::VarintDocDelta),
            POSITIONS_CODEC_PACKED_DELTA_V1 => Ok(Self::PackedDelta),
            other => Err(Error::index(format!(
                "unsupported positions codec {}",
                other
            ))),
        }
    }
}

pub(super) fn parse_shared_position_codec(
    metadata: &HashMap<String, String>,
) -> Result<PositionStreamCodec> {
    if let Some(codec) = metadata.get(POSITIONS_CODEC_KEY) {
        return PositionStreamCodec::from_metadata_value(codec);
    }

    match metadata
        .get(POSITIONS_LAYOUT_KEY)
        .map(|layout| layout.as_str())
    {
        Some(POSITIONS_LAYOUT_SHARED_STREAM_V2) => Ok(PositionStreamCodec::VarintDocDelta),
        _ => Ok(PositionStreamCodec::VarintDocDelta),
    }
}

pub(in super::super) fn parse_format_version_from_metadata(
    metadata: &HashMap<String, String>,
) -> Result<InvertedListFormatVersion> {
    if let Some(value) = metadata.get(FTS_FORMAT_VERSION_KEY) {
        let format_version = InvertedListFormatVersion::from_str(value)?;
        let block_size = parse_posting_block_size(metadata)?;
        validate_format_version_block_size(format_version, block_size)?;
        return Ok(format_version);
    }
    let block_size = parse_posting_block_size(metadata)?;
    if block_size == 256 {
        if metadata
            .get(POSTING_TAIL_CODEC_KEY)
            .map(|_| parse_posting_tail_codec(metadata))
            .transpose()?
            .is_some_and(|posting_tail_codec| posting_tail_codec != PostingTailCodec::VarintDelta)
        {
            return Err(Error::index(
                "FTS block_size=256 requires the varint-delta posting tail codec".to_string(),
            ));
        }
        return Ok(InvertedListFormatVersion::V3);
    }
    if metadata.contains_key(POSITIONS_CODEC_KEY) || metadata.contains_key(POSITIONS_LAYOUT_KEY) {
        return Ok(InvertedListFormatVersion::V2);
    }
    if parse_posting_tail_codec(metadata)? == PostingTailCodec::VarintDelta {
        Ok(InvertedListFormatVersion::V2)
    } else {
        Ok(InvertedListFormatVersion::V1)
    }
}
