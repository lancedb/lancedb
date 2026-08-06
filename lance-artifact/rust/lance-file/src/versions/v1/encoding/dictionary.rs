// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Dictionary encoding.
//!

use std::fmt;
use std::sync::Arc;

use arrow_array::cast::{as_dictionary_array, as_primitive_array};
use arrow_array::types::{
    ArrowDictionaryKeyType, Int8Type, Int16Type, Int32Type, Int64Type, UInt8Type, UInt16Type,
    UInt32Type, UInt64Type,
};
use arrow_array::{Array, ArrayRef, DictionaryArray, PrimitiveArray, UInt32Array};
use arrow_schema::DataType;

use lance_core::{Error, Result};
use lance_io::{
    ReadBatchParams,
    traits::{Reader, Writer},
};

use super::plain::{PlainDecoder, PlainEncoder};

/// Encoder for Dictionary encoding.
pub struct DictionaryEncoder<'a> {
    writer: &'a mut dyn Writer,
    key_type: &'a DataType,
}

impl<'a> DictionaryEncoder<'a> {
    pub fn new(writer: &'a mut dyn Writer, key_type: &'a DataType) -> Self {
        Self { writer, key_type }
    }

    async fn write_typed_array<T: ArrowDictionaryKeyType>(
        &mut self,
        arrs: &[&dyn Array],
    ) -> Result<usize> {
        assert!(!arrs.is_empty());
        let data_type = arrs[0].data_type();
        let pos = self.writer.tell().await?;
        let mut plain_encoder = PlainEncoder::new(self.writer, data_type);

        let keys = arrs
            .iter()
            .map(|a| {
                let dict_arr = as_dictionary_array::<T>(*a);
                dict_arr.keys() as &dyn Array
            })
            .collect::<Vec<_>>();

        plain_encoder.encode(keys.as_slice()).await?;
        Ok(pos)
    }
}

impl DictionaryEncoder<'_> {
    pub async fn encode(&mut self, array: &[&dyn Array]) -> Result<usize> {
        use DataType::*;

        match self.key_type {
            UInt8 => self.write_typed_array::<UInt8Type>(array).await,
            UInt16 => self.write_typed_array::<UInt16Type>(array).await,
            UInt32 => self.write_typed_array::<UInt32Type>(array).await,
            UInt64 => self.write_typed_array::<UInt64Type>(array).await,
            Int8 => self.write_typed_array::<Int8Type>(array).await,
            Int16 => self.write_typed_array::<Int16Type>(array).await,
            Int32 => self.write_typed_array::<Int32Type>(array).await,
            Int64 => self.write_typed_array::<Int64Type>(array).await,
            _ => Err(Error::schema(format!(
                "DictionaryEncoder: unsupported key type: {:?}",
                self.key_type
            ))),
        }
    }
}

/// Decoder for Dictionary encoding.
pub struct DictionaryDecoder<'a> {
    reader: &'a dyn Reader,
    /// The start position of the key array in the file.
    position: usize,
    /// Number of the rows in this batch.
    length: usize,
    /// The dictionary data type
    data_type: &'a DataType,
    /// Value array,
    value_arr: ArrayRef,
}

impl fmt::Debug for DictionaryDecoder<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DictionaryDecoder")
            .field("position", &self.position)
            .field("length", &self.length)
            .field("data_type", &self.data_type)
            .field("value_arr", &self.value_arr)
            .finish()
    }
}

impl<'a> DictionaryDecoder<'a> {
    pub fn new(
        reader: &'a dyn Reader,
        position: usize,
        length: usize,
        data_type: &'a DataType,
        value_arr: ArrayRef,
    ) -> Self {
        assert!(matches!(data_type, DataType::Dictionary(_, _)));
        Self {
            reader,
            position,
            length,
            data_type,
            value_arr,
        }
    }

    async fn decode_impl(&self, params: impl Into<ReadBatchParams>) -> Result<ArrayRef> {
        let index_type = if let DataType::Dictionary(key_type, _) = &self.data_type {
            assert!(key_type.as_ref().is_dictionary_key_type());
            key_type.as_ref()
        } else {
            return Err(Error::arrow(format!(
                "Not a dictionary type: {}",
                self.data_type
            )));
        };

        let decoder = PlainDecoder::new(self.reader, index_type, self.position, self.length)?;
        let keys = decoder.get(params.into()).await?;

        match index_type {
            DataType::Int8 => self.make_dict_array::<Int8Type>(keys).await,
            DataType::Int16 => self.make_dict_array::<Int16Type>(keys).await,
            DataType::Int32 => self.make_dict_array::<Int32Type>(keys).await,
            DataType::Int64 => self.make_dict_array::<Int64Type>(keys).await,
            DataType::UInt8 => self.make_dict_array::<UInt8Type>(keys).await,
            DataType::UInt16 => self.make_dict_array::<UInt16Type>(keys).await,
            DataType::UInt32 => self.make_dict_array::<UInt32Type>(keys).await,
            DataType::UInt64 => self.make_dict_array::<UInt64Type>(keys).await,
            _ => Err(Error::arrow(format!(
                "Dictionary encoding does not support index type: {index_type}",
            ))),
        }
    }

    async fn make_dict_array<T: ArrowDictionaryKeyType + Sync + Send>(
        &self,
        index_array: ArrayRef,
    ) -> Result<ArrayRef> {
        let keys: PrimitiveArray<T> = as_primitive_array(index_array.as_ref()).clone();
        Ok(Arc::new(DictionaryArray::try_new(
            keys,
            self.value_arr.clone(),
        )?))
    }
}

impl DictionaryDecoder<'_> {
    pub async fn decode(&self) -> Result<ArrayRef> {
        self.decode_impl(..).await
    }

    pub async fn take(&self, indices: &UInt32Array) -> Result<ArrayRef> {
        self.decode_impl(indices.clone()).await
    }

    pub async fn get(&self, params: impl Into<ReadBatchParams>) -> Result<ArrayRef> {
        self.decode_impl(params).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use arrow_array::StringArray;
    use arrow_buffer::ArrowNativeType;
    use lance_core::utils::tempfile::TempStdFile;
    use lance_io::local::LocalObjectReader;
    use tokio::io::AsyncWriteExt;

    async fn test_dict_decoder_for_type<T: ArrowDictionaryKeyType>() {
        let value_array: StringArray = vec![Some("a"), Some("b"), Some("c"), Some("d")]
            .into_iter()
            .collect();
        let value_array_ref = Arc::new(value_array) as ArrayRef;

        let keys1: PrimitiveArray<T> = vec![T::Native::from_usize(0), T::Native::from_usize(1)]
            .into_iter()
            .collect();
        let arr1: DictionaryArray<T> =
            DictionaryArray::try_new(keys1, value_array_ref.clone()).unwrap();

        let keys2: PrimitiveArray<T> = vec![T::Native::from_usize(1), T::Native::from_usize(3)]
            .into_iter()
            .collect();
        let arr2: DictionaryArray<T> =
            DictionaryArray::try_new(keys2, value_array_ref.clone()).unwrap();

        let keys1_ref = arr1.keys() as &dyn Array;
        let keys2_ref = arr2.keys() as &dyn Array;
        let arrs: Vec<&dyn Array> = vec![keys1_ref, keys2_ref];

        let path = TempStdFile::default();

        let pos;
        {
            let mut object_writer = tokio::fs::File::create(&path).await.unwrap();
            let mut encoder = PlainEncoder::new(&mut object_writer, arr1.keys().data_type());
            pos = encoder.encode(arrs.as_slice()).await.unwrap();
            AsyncWriteExt::shutdown(&mut object_writer).await.unwrap();
        }

        let reader = LocalObjectReader::open_local_path(&path, 2048, None)
            .await
            .unwrap();
        let decoder = DictionaryDecoder::new(
            reader.as_ref(),
            pos,
            arr1.len() + arr2.len(),
            arr1.data_type(),
            value_array_ref.clone(),
        );

        let decoded_data = decoder.decode().await.unwrap();
        let expected_data: DictionaryArray<T> = vec!["a", "b", "b", "d"].into_iter().collect();
        assert_eq!(
            &expected_data,
            decoded_data
                .as_any()
                .downcast_ref::<DictionaryArray<T>>()
                .unwrap()
        );
    }

    #[tokio::test]
    async fn test_dict_decoder() {
        test_dict_decoder_for_type::<Int8Type>().await;
        test_dict_decoder_for_type::<Int16Type>().await;
        test_dict_decoder_for_type::<Int32Type>().await;
        test_dict_decoder_for_type::<Int64Type>().await;

        test_dict_decoder_for_type::<UInt8Type>().await;
        test_dict_decoder_for_type::<UInt16Type>().await;
        test_dict_decoder_for_type::<UInt32Type>().await;
        test_dict_decoder_for_type::<UInt64Type>().await;
    }
}
