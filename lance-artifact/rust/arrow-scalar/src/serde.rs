// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Binary serialization for [`ArrowScalar`].
//!
//! Default format (with type prefix):
//! ```text
//! | varint: format_string_len | raw: format_string_bytes |
//! | varint: null_flag (0 = non-null, 1 = null) |
//! | varint: num_buffers |                          (only if non-null)
//! | varint: buffer_0_len | ... | varint: buffer_{n-1}_len |  (only if non-null)
//! | raw: buffer_0 bytes  | ... | raw: buffer_{n-1} bytes  |  (only if non-null)
//! ```
//!
//! The format string uses the
//! [Arrow C Data Interface](https://arrow.apache.org/docs/format/CDataInterface.html#data-type-description-format-strings)
//! encoding. Use [`EncodeOptions`] / [`DecodeOptions`] to omit the type prefix
//! when the caller already knows the data type.

use std::borrow::Cow;
use std::sync::Arc;

use arrow_array::make_array;
use arrow_buffer::Buffer;
use arrow_data::ArrayDataBuilder;
use arrow_schema::{ArrowError, DataType, IntervalUnit, TimeUnit};

use crate::ArrowScalar;

type Result<T> = std::result::Result<T, ArrowError>;

/// Options for [`ArrowScalar::encode_with_options`].
pub struct EncodeOptions {
    /// When `true` (the default), the Arrow C Data Interface format string
    /// for the scalar's data type is prepended as a varint-length-prefixed
    /// UTF-8 string. Set to `false` to omit the type prefix (the caller
    /// must then supply the `DataType` at decode time).
    pub include_data_type: bool,
}

impl Default for EncodeOptions {
    fn default() -> Self {
        Self {
            include_data_type: true,
        }
    }
}

/// Options for [`ArrowScalar::decode_with_options`].
#[derive(Default)]
pub struct DecodeOptions<'a> {
    /// When `Some`, the data type is taken from this value and the encoded
    /// bytes are assumed to contain no type prefix. When `None` (the
    /// default), the data type is read from the encoded format-string prefix.
    pub data_type: Option<&'a DataType>,
}

/// Encode a `u64` as a variable-length integer (LEB128).
///
/// Values below 128 use a single byte; the maximum encoding is 10 bytes.
pub fn encode_varint(out: &mut Vec<u8>, mut value: u64) {
    loop {
        let byte = (value & 0x7F) as u8;
        value >>= 7;
        if value == 0 {
            out.push(byte);
            return;
        }
        out.push(byte | 0x80);
    }
}

/// Decode a variable-length integer (LEB128) from `buf` at the given `offset`.
///
/// On success, `offset` is advanced past the consumed bytes.
pub fn decode_varint(buf: &[u8], offset: &mut usize) -> Result<u64> {
    let mut result: u64 = 0;
    let mut shift = 0u32;
    loop {
        if *offset >= buf.len() {
            return Err(ArrowError::InvalidArgumentError(
                "Invalid varint: unexpected EOF".to_string(),
            ));
        }
        let byte = buf[*offset];
        *offset += 1;

        result |= u64::from(byte & 0x7F) << shift;
        if byte & 0x80 == 0 {
            return Ok(result);
        }
        shift += 7;
        if shift >= 64 {
            return Err(ArrowError::InvalidArgumentError(
                "Invalid varint: too many bytes".to_string(),
            ));
        }
    }
}

/// Convert a [`DataType`] to its Arrow C Data Interface format string.
///
/// Only non-nested types are supported (nested types are already rejected by
/// [`ArrowScalar::encode`]).
fn data_type_to_format_string(dtype: &DataType) -> Result<Cow<'static, str>> {
    match dtype {
        DataType::Null => Ok("n".into()),
        DataType::Boolean => Ok("b".into()),
        DataType::Int8 => Ok("c".into()),
        DataType::UInt8 => Ok("C".into()),
        DataType::Int16 => Ok("s".into()),
        DataType::UInt16 => Ok("S".into()),
        DataType::Int32 => Ok("i".into()),
        DataType::UInt32 => Ok("I".into()),
        DataType::Int64 => Ok("l".into()),
        DataType::UInt64 => Ok("L".into()),
        DataType::Float16 => Ok("e".into()),
        DataType::Float32 => Ok("f".into()),
        DataType::Float64 => Ok("g".into()),
        DataType::Binary => Ok("z".into()),
        DataType::LargeBinary => Ok("Z".into()),
        DataType::Utf8 => Ok("u".into()),
        DataType::LargeUtf8 => Ok("U".into()),
        DataType::BinaryView => Ok("vz".into()),
        DataType::Utf8View => Ok("vu".into()),
        DataType::FixedSizeBinary(n) => Ok(Cow::Owned(format!("w:{n}"))),
        DataType::Decimal32(p, s) => Ok(Cow::Owned(format!("d:{p},{s},32"))),
        DataType::Decimal64(p, s) => Ok(Cow::Owned(format!("d:{p},{s},64"))),
        DataType::Decimal128(p, s) => Ok(Cow::Owned(format!("d:{p},{s}"))),
        DataType::Decimal256(p, s) => Ok(Cow::Owned(format!("d:{p},{s},256"))),
        DataType::Date32 => Ok("tdD".into()),
        DataType::Date64 => Ok("tdm".into()),
        DataType::Time32(TimeUnit::Second) => Ok("tts".into()),
        DataType::Time32(TimeUnit::Millisecond) => Ok("ttm".into()),
        DataType::Time64(TimeUnit::Microsecond) => Ok("ttu".into()),
        DataType::Time64(TimeUnit::Nanosecond) => Ok("ttn".into()),
        DataType::Timestamp(TimeUnit::Second, None) => Ok("tss:".into()),
        DataType::Timestamp(TimeUnit::Millisecond, None) => Ok("tsm:".into()),
        DataType::Timestamp(TimeUnit::Microsecond, None) => Ok("tsu:".into()),
        DataType::Timestamp(TimeUnit::Nanosecond, None) => Ok("tsn:".into()),
        DataType::Timestamp(TimeUnit::Second, Some(tz)) => Ok(Cow::Owned(format!("tss:{tz}"))),
        DataType::Timestamp(TimeUnit::Millisecond, Some(tz)) => Ok(Cow::Owned(format!("tsm:{tz}"))),
        DataType::Timestamp(TimeUnit::Microsecond, Some(tz)) => Ok(Cow::Owned(format!("tsu:{tz}"))),
        DataType::Timestamp(TimeUnit::Nanosecond, Some(tz)) => Ok(Cow::Owned(format!("tsn:{tz}"))),
        DataType::Duration(TimeUnit::Second) => Ok("tDs".into()),
        DataType::Duration(TimeUnit::Millisecond) => Ok("tDm".into()),
        DataType::Duration(TimeUnit::Microsecond) => Ok("tDu".into()),
        DataType::Duration(TimeUnit::Nanosecond) => Ok("tDn".into()),
        DataType::Interval(IntervalUnit::YearMonth) => Ok("tiM".into()),
        DataType::Interval(IntervalUnit::DayTime) => Ok("tiD".into()),
        DataType::Interval(IntervalUnit::MonthDayNano) => Ok("tin".into()),
        other => Err(ArrowError::InvalidArgumentError(format!(
            "Cannot encode data type as format string: {other:?}"
        ))),
    }
}

/// Parse an Arrow C Data Interface format string back to a [`DataType`].
///
/// Only non-nested types are supported.
fn format_string_to_data_type(fmt: &str) -> Result<DataType> {
    match fmt {
        "n" => Ok(DataType::Null),
        "b" => Ok(DataType::Boolean),
        "c" => Ok(DataType::Int8),
        "C" => Ok(DataType::UInt8),
        "s" => Ok(DataType::Int16),
        "S" => Ok(DataType::UInt16),
        "i" => Ok(DataType::Int32),
        "I" => Ok(DataType::UInt32),
        "l" => Ok(DataType::Int64),
        "L" => Ok(DataType::UInt64),
        "e" => Ok(DataType::Float16),
        "f" => Ok(DataType::Float32),
        "g" => Ok(DataType::Float64),
        "z" => Ok(DataType::Binary),
        "Z" => Ok(DataType::LargeBinary),
        "u" => Ok(DataType::Utf8),
        "U" => Ok(DataType::LargeUtf8),
        "vz" => Ok(DataType::BinaryView),
        "vu" => Ok(DataType::Utf8View),
        "tdD" => Ok(DataType::Date32),
        "tdm" => Ok(DataType::Date64),
        "tts" => Ok(DataType::Time32(TimeUnit::Second)),
        "ttm" => Ok(DataType::Time32(TimeUnit::Millisecond)),
        "ttu" => Ok(DataType::Time64(TimeUnit::Microsecond)),
        "ttn" => Ok(DataType::Time64(TimeUnit::Nanosecond)),
        "tDs" => Ok(DataType::Duration(TimeUnit::Second)),
        "tDm" => Ok(DataType::Duration(TimeUnit::Millisecond)),
        "tDu" => Ok(DataType::Duration(TimeUnit::Microsecond)),
        "tDn" => Ok(DataType::Duration(TimeUnit::Nanosecond)),
        "tiM" => Ok(DataType::Interval(IntervalUnit::YearMonth)),
        "tiD" => Ok(DataType::Interval(IntervalUnit::DayTime)),
        "tin" => Ok(DataType::Interval(IntervalUnit::MonthDayNano)),
        other => {
            let parts: Vec<&str> = other.splitn(2, ':').collect();
            match parts.as_slice() {
                ["w", num_bytes] => {
                    let n = num_bytes.parse::<i32>().map_err(|_| {
                        ArrowError::InvalidArgumentError(
                            "FixedSizeBinary requires an integer byte count".to_string(),
                        )
                    })?;
                    Ok(DataType::FixedSizeBinary(n))
                }
                ["d", extra] => {
                    let dec_parts: Vec<&str> = extra.splitn(3, ',').collect();
                    match dec_parts.as_slice() {
                        [precision, scale] => {
                            let p = precision.parse::<u8>().map_err(|_| {
                                ArrowError::InvalidArgumentError(
                                    "Decimal requires an integer precision".to_string(),
                                )
                            })?;
                            let s = scale.parse::<i8>().map_err(|_| {
                                ArrowError::InvalidArgumentError(
                                    "Decimal requires an integer scale".to_string(),
                                )
                            })?;
                            Ok(DataType::Decimal128(p, s))
                        }
                        [precision, scale, bits] => {
                            let p = precision.parse::<u8>().map_err(|_| {
                                ArrowError::InvalidArgumentError(
                                    "Decimal requires an integer precision".to_string(),
                                )
                            })?;
                            let s = scale.parse::<i8>().map_err(|_| {
                                ArrowError::InvalidArgumentError(
                                    "Decimal requires an integer scale".to_string(),
                                )
                            })?;
                            match *bits {
                                "32" => Ok(DataType::Decimal32(p, s)),
                                "64" => Ok(DataType::Decimal64(p, s)),
                                "128" => Ok(DataType::Decimal128(p, s)),
                                "256" => Ok(DataType::Decimal256(p, s)),
                                _ => Err(ArrowError::InvalidArgumentError(format!(
                                    "Unsupported decimal bit width: {bits}"
                                ))),
                            }
                        }
                        _ => Err(ArrowError::InvalidArgumentError(format!(
                            "Invalid decimal format string: d:{extra}"
                        ))),
                    }
                }
                ["tss", ""] => Ok(DataType::Timestamp(TimeUnit::Second, None)),
                ["tsm", ""] => Ok(DataType::Timestamp(TimeUnit::Millisecond, None)),
                ["tsu", ""] => Ok(DataType::Timestamp(TimeUnit::Microsecond, None)),
                ["tsn", ""] => Ok(DataType::Timestamp(TimeUnit::Nanosecond, None)),
                ["tss", tz] => Ok(DataType::Timestamp(TimeUnit::Second, Some(Arc::from(*tz)))),
                ["tsm", tz] => Ok(DataType::Timestamp(
                    TimeUnit::Millisecond,
                    Some(Arc::from(*tz)),
                )),
                ["tsu", tz] => Ok(DataType::Timestamp(
                    TimeUnit::Microsecond,
                    Some(Arc::from(*tz)),
                )),
                ["tsn", tz] => Ok(DataType::Timestamp(
                    TimeUnit::Nanosecond,
                    Some(Arc::from(*tz)),
                )),
                _ => Err(ArrowError::InvalidArgumentError(format!(
                    "Unsupported format string: {other:?}"
                ))),
            }
        }
    }
}

impl ArrowScalar {
    /// Serialize this scalar to a self-describing binary representation.
    ///
    /// The data type is encoded as a format-string prefix so that
    /// [`decode`](Self::decode) can reconstruct the scalar without external
    /// type information. Use [`encode_with_options`](Self::encode_with_options)
    /// to omit the prefix when the caller already knows the type.
    ///
    /// Only non-nested scalars are supported. Null scalars are encoded as a
    /// null flag with no buffer data.
    pub fn encode(&self) -> Result<Vec<u8>> {
        self.encode_with_options(&EncodeOptions::default())
    }

    /// Serialize this scalar with the given [`EncodeOptions`].
    pub fn encode_with_options(&self, options: &EncodeOptions) -> Result<Vec<u8>> {
        let array = self.as_array();
        let data = array.to_data();
        if !data.child_data().is_empty() {
            return Err(ArrowError::InvalidArgumentError(
                "Cannot encode nested scalar".to_string(),
            ));
        }

        let mut out = Vec::with_capacity(64);

        if options.include_data_type {
            let fmt = data_type_to_format_string(array.data_type())?;
            encode_varint(&mut out, fmt.len() as u64);
            out.extend_from_slice(fmt.as_bytes());
        }

        if self.is_null() {
            encode_varint(&mut out, 1); // null_flag = 1
        } else {
            encode_varint(&mut out, 0); // null_flag = 0
            let buffers = data.buffers();
            encode_varint(&mut out, buffers.len() as u64);
            for b in buffers {
                encode_varint(&mut out, b.len() as u64);
            }
            for b in buffers {
                out.extend_from_slice(b.as_slice());
            }
        }
        Ok(out)
    }

    /// Deserialize a scalar from the self-describing binary representation
    /// produced by [`encode`](Self::encode).
    ///
    /// The data type is read from the format-string prefix in the encoded
    /// bytes. Use [`decode_with_options`](Self::decode_with_options) to supply
    /// the type externally when the prefix was omitted at encode time.
    pub fn decode(buf: &[u8]) -> Result<Self> {
        Self::decode_with_options(buf, &DecodeOptions::default())
    }

    /// Deserialize a scalar with the given [`DecodeOptions`].
    pub fn decode_with_options(buf: &[u8], options: &DecodeOptions) -> Result<Self> {
        let mut offset = 0;

        let data_type = match options.data_type {
            Some(dt) => dt.clone(),
            None => {
                let fmt_len = decode_varint(buf, &mut offset)? as usize;
                if offset + fmt_len > buf.len() {
                    return Err(ArrowError::InvalidArgumentError(
                        "Invalid scalar buffer: unexpected EOF reading format string".to_string(),
                    ));
                }
                let fmt_str = std::str::from_utf8(&buf[offset..offset + fmt_len]).map_err(|e| {
                    ArrowError::InvalidArgumentError(format!(
                        "Invalid format string: not valid UTF-8: {e}"
                    ))
                })?;
                offset += fmt_len;
                format_string_to_data_type(fmt_str)?
            }
        };

        let null_flag = decode_varint(buf, &mut offset)?;
        if null_flag == 1 {
            if offset != buf.len() {
                return Err(ArrowError::InvalidArgumentError(
                    "Invalid scalar buffer: trailing bytes after null flag".to_string(),
                ));
            }
            return Self::new_null(&data_type);
        }

        let num_buffers = decode_varint(buf, &mut offset)? as usize;

        let mut buffer_lens = Vec::with_capacity(num_buffers);
        for _ in 0..num_buffers {
            buffer_lens.push(decode_varint(buf, &mut offset)? as usize);
        }

        let mut buffers = Vec::with_capacity(num_buffers);
        for len in &buffer_lens {
            if offset + len > buf.len() {
                return Err(ArrowError::InvalidArgumentError(
                    "Invalid scalar buffer: unexpected EOF".to_string(),
                ));
            }
            buffers.push(Buffer::from_vec(buf[offset..offset + len].to_vec()));
            offset += len;
        }

        if offset != buf.len() {
            return Err(ArrowError::InvalidArgumentError(
                "Invalid scalar buffer: trailing bytes".to_string(),
            ));
        }

        let mut builder = ArrayDataBuilder::new(data_type).len(1).null_count(0);
        for b in buffers {
            builder = builder.add_buffer(b);
        }
        let array = make_array(builder.build()?);
        Self::try_from_array(array)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_array::{
        ArrayRef, BinaryViewArray, Int32Array, StringArray, StringViewArray,
        TimestampMicrosecondArray,
    };
    use arrow_schema::DataType;
    use rstest::rstest;

    use super::*;
    use crate::ArrowScalar;

    #[test]
    fn test_varint_roundtrip() {
        for value in [0u64, 1, 127, 128, 16383, 16384, u64::MAX] {
            let mut buf = Vec::new();
            encode_varint(&mut buf, value);
            let mut offset = 0;
            let decoded = decode_varint(&buf, &mut offset).unwrap();
            assert_eq!(decoded, value);
            assert_eq!(offset, buf.len());
        }
    }

    #[test]
    fn test_varint_small_is_one_byte() {
        let mut buf = Vec::new();
        encode_varint(&mut buf, 42);
        assert_eq!(buf.len(), 1);
        assert_eq!(buf[0], 42);
    }

    #[rstest]
    #[case::int32(Arc::new(Int32Array::from(vec![42])) as ArrayRef)]
    #[case::string(Arc::new(StringArray::from(vec!["hello"])) as ArrayRef)]
    #[case::string_view(Arc::new(StringViewArray::from(vec!["hello world, long string view"])) as ArrayRef)]
    #[case::binary_view(Arc::new(BinaryViewArray::from(vec![b"\xDE\xAD\xBE\xEF".as_ref()])) as ArrayRef)]
    fn test_encode_decode_roundtrip(#[case] array: ArrayRef) {
        let scalar = ArrowScalar::try_from_array(array).unwrap();
        let encoded = scalar.encode().unwrap();
        let decoded = ArrowScalar::decode(&encoded).unwrap();
        assert_eq!(scalar, decoded);
        assert_eq!(scalar.data_type(), decoded.data_type());
    }

    #[rstest]
    #[case::int32(Arc::new(Int32Array::from(vec![42])) as ArrayRef, DataType::Int32)]
    #[case::string(Arc::new(StringArray::from(vec!["hello"])) as ArrayRef, DataType::Utf8)]
    #[case::string_view(Arc::new(StringViewArray::from(vec!["hello view"])) as ArrayRef, DataType::Utf8View)]
    #[case::binary_view(Arc::new(BinaryViewArray::from(vec![b"\xCA\xFE".as_ref()])) as ArrayRef, DataType::BinaryView)]
    fn test_encode_decode_without_type_prefix(#[case] array: ArrayRef, #[case] dt: DataType) {
        let scalar = ArrowScalar::try_from_array(array).unwrap();
        let opts = EncodeOptions {
            include_data_type: false,
        };
        let encoded = scalar.encode_with_options(&opts).unwrap();
        let decode_opts = DecodeOptions {
            data_type: Some(&dt),
        };
        let decoded = ArrowScalar::decode_with_options(&encoded, &decode_opts).unwrap();
        assert_eq!(scalar, decoded);
    }

    #[test]
    fn test_null_encode_decode_roundtrip() {
        let array: ArrayRef = Arc::new(Int32Array::from(vec![None]));
        let scalar = ArrowScalar::try_from_array(array).unwrap();
        assert!(scalar.is_null());
        let encoded = scalar.encode().unwrap();
        let decoded = ArrowScalar::decode(&encoded).unwrap();
        assert!(decoded.is_null());
        assert_eq!(decoded.data_type(), &DataType::Int32);
        assert_eq!(scalar, decoded);
    }

    #[test]
    fn test_null_encode_decode_without_type_prefix() {
        let array: ArrayRef = Arc::new(StringArray::from(vec![Option::<&str>::None]));
        let scalar = ArrowScalar::try_from_array(array).unwrap();
        let opts = EncodeOptions {
            include_data_type: false,
        };
        let encoded = scalar.encode_with_options(&opts).unwrap();
        let decode_opts = DecodeOptions {
            data_type: Some(&DataType::Utf8),
        };
        let decoded = ArrowScalar::decode_with_options(&encoded, &decode_opts).unwrap();
        assert!(decoded.is_null());
        assert_eq!(decoded.data_type(), &DataType::Utf8);
    }

    #[test]
    fn test_decode_trailing_bytes() {
        let scalar = ArrowScalar::from(42i32);
        let mut encoded = scalar.encode().unwrap();
        encoded.push(0xFF);
        assert!(ArrowScalar::decode(&encoded).is_err());
    }

    #[test]
    fn test_encoded_bytes_contain_format_prefix() {
        let scalar = ArrowScalar::from(42i32);
        let encoded = scalar.encode().unwrap();
        // First byte is varint length of format string "i" (length 1)
        assert_eq!(encoded[0], 1);
        // Second byte is the format string itself
        assert_eq!(encoded[1], b'i');
    }

    #[rstest]
    #[case::null(DataType::Null, "n")]
    #[case::boolean(DataType::Boolean, "b")]
    #[case::int8(DataType::Int8, "c")]
    #[case::uint8(DataType::UInt8, "C")]
    #[case::int16(DataType::Int16, "s")]
    #[case::uint16(DataType::UInt16, "S")]
    #[case::int32(DataType::Int32, "i")]
    #[case::uint32(DataType::UInt32, "I")]
    #[case::int64(DataType::Int64, "l")]
    #[case::uint64(DataType::UInt64, "L")]
    #[case::float16(DataType::Float16, "e")]
    #[case::float32(DataType::Float32, "f")]
    #[case::float64(DataType::Float64, "g")]
    #[case::binary(DataType::Binary, "z")]
    #[case::large_binary(DataType::LargeBinary, "Z")]
    #[case::utf8(DataType::Utf8, "u")]
    #[case::large_utf8(DataType::LargeUtf8, "U")]
    #[case::binary_view(DataType::BinaryView, "vz")]
    #[case::utf8_view(DataType::Utf8View, "vu")]
    #[case::date32(DataType::Date32, "tdD")]
    #[case::date64(DataType::Date64, "tdm")]
    #[case::fixed_size_binary(DataType::FixedSizeBinary(16), "w:16")]
    #[case::decimal128(DataType::Decimal128(10, 2), "d:10,2")]
    #[case::decimal256(DataType::Decimal256(38, 10), "d:38,10,256")]
    #[case::timestamp_us_utc(
        DataType::Timestamp(TimeUnit::Microsecond, Some(Arc::from("UTC"))),
        "tsu:UTC"
    )]
    #[case::timestamp_ns_none(DataType::Timestamp(TimeUnit::Nanosecond, None), "tsn:")]
    #[case::duration_s(DataType::Duration(TimeUnit::Second), "tDs")]
    #[case::interval_ym(DataType::Interval(IntervalUnit::YearMonth), "tiM")]
    fn test_format_string_roundtrip(#[case] dt: DataType, #[case] expected_fmt: &str) {
        let fmt = data_type_to_format_string(&dt).unwrap();
        assert_eq!(fmt.as_ref(), expected_fmt);
        let roundtripped = format_string_to_data_type(&fmt).unwrap();
        assert_eq!(roundtripped, dt);
    }

    #[test]
    fn test_timestamp_with_tz_roundtrip() {
        let array: ArrayRef = Arc::new(
            TimestampMicrosecondArray::from(vec![1_000_000]).with_timezone("America/New_York"),
        );
        let scalar = ArrowScalar::try_from_array(array).unwrap();
        let encoded = scalar.encode().unwrap();
        let decoded = ArrowScalar::decode(&encoded).unwrap();
        assert_eq!(scalar, decoded);
        assert_eq!(scalar.data_type(), decoded.data_type());
    }
}
