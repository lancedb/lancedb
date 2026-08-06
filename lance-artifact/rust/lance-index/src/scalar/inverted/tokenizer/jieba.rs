// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use std::{fs::File, io::BufReader, path::Path, path::PathBuf};

use lance_core::{Error, Result};
use lance_tokenizer::{JiebaTokenizer, TextAnalyzerBuilder};
use serde::{Deserialize, Serialize, de::DeserializeOwned};

#[derive(Serialize, Deserialize, Default)]
pub struct JiebaConfig {
    main: Option<String>,
    users: Option<Vec<String>>,
}

pub const JIEBA_LANGUAGE_MODEL_CONFIG_FILE: &str = "config.json";

pub trait JiebaTokenizerBuilder: Sized {
    type Config: DeserializeOwned + Default;

    fn load(p: &Path) -> Result<Self> {
        if !p.is_dir() {
            return Err(Error::invalid_input(format!(
                "Invalid directory path: {}",
                p.display()
            )));
        }
        let config_path = p.join(JIEBA_LANGUAGE_MODEL_CONFIG_FILE);
        let config = if config_path.exists() {
            let file = File::open(config_path)?;
            let reader = BufReader::new(file);
            serde_json::from_reader::<BufReader<File>, Self::Config>(reader)?
        } else {
            Self::Config::default()
        };
        Self::new(config, p)
    }

    fn new(config: Self::Config, root: &Path) -> Result<Self>;

    fn build(&self) -> Result<TextAnalyzerBuilder>;
}

pub struct JiebaBuilder {
    root: PathBuf,
    config: JiebaConfig,
}

impl JiebaBuilder {
    fn main_dict_path(&self) -> PathBuf {
        if let Some(p) = &self.config.main {
            return self.root.join(p);
        }
        self.root.join("dict.txt")
    }

    fn user_dict_paths(&self) -> Vec<PathBuf> {
        let Some(users) = &self.config.users else {
            return vec![];
        };
        users.iter().map(|p| self.root.join(p)).collect()
    }
}

impl JiebaTokenizerBuilder for JiebaBuilder {
    type Config = JiebaConfig;

    fn new(config: Self::Config, root: &Path) -> Result<Self> {
        Ok(Self {
            config,
            root: root.to_path_buf(),
        })
    }

    fn build(&self) -> Result<TextAnalyzerBuilder> {
        let main_dict_path = &self.main_dict_path();
        let file = std::fs::File::open(main_dict_path)?;
        let mut f = std::io::BufReader::new(file);
        let mut jieba = jieba_rs::Jieba::with_dict(&mut f).map_err(|e| {
            Error::invalid_input(format!(
                "Failed to load Jieba dictionary from {}: {}",
                main_dict_path.display(),
                e
            ))
        })?;
        for user_dict_path in &self.user_dict_paths() {
            let file = std::fs::File::open(user_dict_path)?;
            let mut f = std::io::BufReader::new(file);
            jieba.load_dict(&mut f).map_err(|e| {
                Error::invalid_input(format!(
                    "Failed to load Jieba user dictionary from {}: {}",
                    user_dict_path.display(),
                    e
                ))
            })?
        }
        Ok(JiebaTokenizer::new(jieba).analyzer_builder())
    }
}
