// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use super::*;

// at indexing, we use HashMap because we need it to be mutable,
// at searching, we use fst::Map because it's more efficient
#[derive(Debug, Clone)]
pub enum TokenMap {
    HashMap(HashMap<String, u32>),
    Fst(fst::Map<Vec<u8>>),
}

impl Default for TokenMap {
    fn default() -> Self {
        Self::HashMap(HashMap::new())
    }
}

impl DeepSizeOf for TokenMap {
    fn deep_size_of_children(&self, ctx: &mut lance_core::deepsize::Context) -> usize {
        match self {
            Self::HashMap(map) => map.deep_size_of_children(ctx),
            Self::Fst(map) => map.as_fst().size(),
        }
    }
}

impl TokenMap {
    pub fn len(&self) -> usize {
        match self {
            Self::HashMap(map) => map.len(),
            Self::Fst(map) => map.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

// TokenSet is a mapping from tokens to token ids
#[derive(Debug, Clone, Default, DeepSizeOf)]
pub struct TokenSet {
    // token -> token_id
    pub(crate) tokens: TokenMap,
    pub(crate) next_id: u32,
    total_length: usize,
}

impl TokenSet {
    pub fn into_mut(self) -> Self {
        let tokens = match self.tokens {
            TokenMap::HashMap(map) => map,
            TokenMap::Fst(map) => {
                let mut new_map = HashMap::with_capacity(map.len());
                let mut stream = map.into_stream();
                while let Some((token, token_id)) = stream.next() {
                    new_map.insert(String::from_utf8_lossy(token).into_owned(), token_id as u32);
                }

                new_map
            }
        };

        Self {
            tokens: TokenMap::HashMap(tokens),
            next_id: self.next_id,
            total_length: self.total_length,
        }
    }

    pub fn len(&self) -> usize {
        self.tokens.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn to_batch(self, format: TokenSetFormat) -> Result<RecordBatch> {
        match format {
            TokenSetFormat::Arrow => self.into_arrow_batch(),
            TokenSetFormat::Fst => self.into_fst_batch(),
        }
    }

    fn into_arrow_batch(self) -> Result<RecordBatch> {
        let mut token_builder = StringBuilder::with_capacity(self.tokens.len(), self.total_length);
        let mut token_id_builder = UInt32Builder::with_capacity(self.tokens.len());

        match self.tokens {
            TokenMap::Fst(map) => {
                let mut stream = map.stream();
                while let Some((token, token_id)) = stream.next() {
                    token_builder.append_value(String::from_utf8_lossy(token));
                    token_id_builder.append_value(token_id as u32);
                }
            }
            TokenMap::HashMap(map) => {
                for (token, token_id) in map.into_iter().sorted_unstable() {
                    token_builder.append_value(token);
                    token_id_builder.append_value(token_id);
                }
            }
        }

        let token_col = token_builder.finish();
        let token_id_col = token_id_builder.finish();

        let schema = arrow_schema::Schema::new(vec![
            arrow_schema::Field::new(TOKEN_COL, DataType::Utf8, false),
            arrow_schema::Field::new(TOKEN_ID_COL, DataType::UInt32, false),
        ]);

        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(token_col) as ArrayRef,
                Arc::new(token_id_col) as ArrayRef,
            ],
        )?;
        Ok(batch)
    }

    fn into_fst_batch(mut self) -> Result<RecordBatch> {
        let fst_map = match std::mem::take(&mut self.tokens) {
            TokenMap::Fst(map) => map,
            TokenMap::HashMap(map) => Self::build_fst_from_map(map)?,
        };
        let bytes = fst_map.into_fst().into_inner();

        let mut fst_builder = LargeBinaryBuilder::with_capacity(1, bytes.len());
        fst_builder.append_value(bytes);
        let fst_col = fst_builder.finish();

        let mut next_id_builder = UInt32Builder::with_capacity(1);
        next_id_builder.append_value(self.next_id);
        let next_id_col = next_id_builder.finish();

        let mut total_length_builder = UInt64Builder::with_capacity(1);
        total_length_builder.append_value(self.total_length as u64);
        let total_length_col = total_length_builder.finish();

        let schema = arrow_schema::Schema::new(vec![
            arrow_schema::Field::new(TOKEN_FST_BYTES_COL, DataType::LargeBinary, false),
            arrow_schema::Field::new(TOKEN_NEXT_ID_COL, DataType::UInt32, false),
            arrow_schema::Field::new(TOKEN_TOTAL_LENGTH_COL, DataType::UInt64, false),
        ]);

        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(fst_col) as ArrayRef,
                Arc::new(next_id_col) as ArrayRef,
                Arc::new(total_length_col) as ArrayRef,
            ],
        )?;
        Ok(batch)
    }

    fn build_fst_from_map(map: HashMap<String, u32>) -> Result<fst::Map<Vec<u8>>> {
        let mut entries: Vec<_> = map.into_iter().collect();
        entries.sort_unstable_by(|(lhs, _), (rhs, _)| lhs.cmp(rhs));
        let mut builder = fst::MapBuilder::memory();
        for (token, token_id) in entries {
            builder
                .insert(&token, token_id as u64)
                .map_err(|e| Error::index(format!("failed to insert token {}: {}", token, e)))?;
        }
        Ok(builder.into_map())
    }

    pub async fn load(reader: Arc<dyn IndexReader>, format: TokenSetFormat) -> Result<Self> {
        match format {
            TokenSetFormat::Arrow => Self::load_arrow(reader).await,
            TokenSetFormat::Fst => Self::load_fst(reader).await,
        }
    }

    async fn load_arrow(reader: Arc<dyn IndexReader>) -> Result<Self> {
        let batch = reader.read_range(0..reader.num_rows(), None).await?;

        let (tokens, next_id, total_length) = spawn_blocking(move || {
            let mut next_id = 0;
            let mut total_length = 0;
            let mut tokens = fst::MapBuilder::memory();

            let token_col = batch[TOKEN_COL].as_string::<i32>();
            let token_id_col = batch[TOKEN_ID_COL].as_primitive::<datatypes::UInt32Type>();

            for (token, &token_id) in token_col.iter().zip(token_id_col.values().iter()) {
                let token =
                    token.ok_or(Error::index("found null token in token set".to_owned()))?;
                next_id = next_id.max(token_id + 1);
                total_length += token.len();
                tokens.insert(token, token_id as u64).map_err(|e| {
                    Error::index(format!("failed to insert token {}: {}", token, e))
                })?;
            }

            Ok::<_, Error>((tokens.into_map(), next_id, total_length))
        })
        .await
        .map_err(|err| Error::execution(format!("failed to spawn blocking task: {}", err)))??;

        Ok(Self {
            tokens: TokenMap::Fst(tokens),
            next_id,
            total_length,
        })
    }

    async fn load_fst(reader: Arc<dyn IndexReader>) -> Result<Self> {
        let batch = reader.read_range(0..reader.num_rows(), None).await?;
        if batch.num_rows() == 0 {
            return Err(Error::index("token set batch is empty".to_owned()));
        }

        let fst_col = batch[TOKEN_FST_BYTES_COL].as_binary::<i64>();
        let bytes = fst_col.value(0);
        let map = fst::Map::new(bytes.to_vec())
            .map_err(|e| Error::index(format!("failed to load fst tokens: {}", e)))?;

        let total_length_col =
            batch[TOKEN_TOTAL_LENGTH_COL].as_primitive::<datatypes::UInt64Type>();

        // Token ids are dense `[0, len)`, so `next_id` must equal the token count. Recompute
        // it instead of trusting the persisted value, which writers before #7115 could leave
        // stale. Mirrors `load_arrow`.
        let next_id = map.len() as u32;

        let total_length = total_length_col
            .values()
            .first()
            .copied()
            .ok_or(Error::index(
                "token total length column is empty".to_owned(),
            ))?;

        Ok(Self {
            tokens: TokenMap::Fst(map),
            next_id,
            total_length: usize::try_from(total_length).map_err(|_| {
                Error::index(format!(
                    "token total length {} overflows usize",
                    total_length
                ))
            })?,
        })
    }

    pub fn add(&mut self, token: String) -> u32 {
        let next_id = self.next_id();
        let len = token.len();
        let token_id = match self.tokens {
            TokenMap::HashMap(ref mut map) => *map.entry(token).or_insert(next_id),
            _ => unreachable!("tokens must be HashMap while indexing"),
        };

        // add token if it doesn't exist
        if token_id == next_id {
            self.next_id += 1;
            self.total_length += len;
        }

        token_id
    }

    pub(crate) fn get_or_add(&mut self, token: &str) -> u32 {
        let next_id = self.next_id;
        match self.tokens {
            TokenMap::HashMap(ref mut map) => {
                if let Some(&token_id) = map.get(token) {
                    return token_id;
                }

                map.insert(token.to_owned(), next_id);
            }
            _ => unreachable!("tokens must be HashMap while indexing"),
        }

        self.next_id += 1;
        self.total_length += token.len();
        next_id
    }

    pub(crate) fn into_mutable(self) -> Self {
        let Self {
            tokens,
            next_id,
            total_length,
        } = self;
        match tokens {
            TokenMap::HashMap(_) => Self {
                tokens,
                next_id,
                total_length,
            },
            TokenMap::Fst(map) => {
                let mut mutable = HashMap::new();
                let mut stream = map.stream();
                while let Some((token, token_id)) = stream.next() {
                    mutable.insert(String::from_utf8_lossy(token).into_owned(), token_id as u32);
                }
                Self {
                    tokens: TokenMap::HashMap(mutable),
                    next_id,
                    total_length,
                }
            }
        }
    }

    pub fn get(&self, token: &str) -> Option<u32> {
        match self.tokens {
            TokenMap::HashMap(ref map) => map.get(token).copied(),
            TokenMap::Fst(ref map) => map.get(token).map(|id| id as u32),
        }
    }

    // the `removed_token_ids` must be sorted
    pub fn remap(&mut self, removed_token_ids: &[u32]) {
        if removed_token_ids.is_empty() {
            return;
        }

        let mut map = match std::mem::take(&mut self.tokens) {
            TokenMap::HashMap(map) => map,
            TokenMap::Fst(map) => {
                let mut new_map = HashMap::with_capacity(map.len());
                let mut stream = map.into_stream();
                while let Some((token, token_id)) = stream.next() {
                    new_map.insert(String::from_utf8_lossy(token).into_owned(), token_id as u32);
                }

                new_map
            }
        };

        let mut retained_length = 0;
        map.retain(
            |token, token_id| match removed_token_ids.binary_search(token_id) {
                Ok(_) => false,
                Err(index) => {
                    *token_id -= index as u32;
                    retained_length += token.len();
                    true
                }
            },
        );

        self.tokens = TokenMap::HashMap(map);

        // The retain above compacts the surviving token ids into a dense `[0, len)`
        // range, so `next_id` (handed to the next new token) must follow them down.
        // `total_length` likewise must drop the removed tokens' bytes; it is persisted
        // and feeds memory accounting, so a stale value drifts across remap/merge cycles.
        self.next_id = self.tokens.len() as u32;
        self.total_length = retained_length;
    }

    pub fn next_id(&self) -> u32 {
        self.next_id
    }

    pub(crate) fn memory_size(&self) -> usize {
        match &self.tokens {
            TokenMap::HashMap(map) => {
                self.total_length
                    + map.capacity()
                        * (std::mem::size_of::<String>()
                            + std::mem::size_of::<u32>()
                            + std::mem::size_of::<usize>())
            }
            TokenMap::Fst(map) => map.as_fst().size(),
        }
    }
}
