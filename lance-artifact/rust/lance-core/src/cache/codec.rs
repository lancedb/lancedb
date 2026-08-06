// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Serialization codecs for cache entries.
//!
//! Implement [`CacheCodecImpl`] on concrete types, then use
//! [`CacheCodec::from_impl`] to produce a type-erased codec for the cache.
//!
//! # Wire format
//!
//! Every serialized entry begins with a small hand-framed **envelope** so the
//! reader can validate it before trusting the body:
//!
//! ```text
//! [magic: 4B = b"LCE1"]
//! [envelope_version: u8]
//! [type_id_len: u16 LE][type_id: utf8]   # stable, author-assigned
//! [type_version: u32 LE]                 # per-type body schema version
//! <body, written by the type's CacheCodecImpl::serialize>
//! ```
//!
//! The envelope is deliberately *not* protobuf: it is the most
//! stability-critical part, must parse robustly against arbitrary bytes
//! (including data written by older, pre-stabilization builds), and never
//! changes shape. Bodies use protobuf headers, where field-number evolution
//! pays off.
//!
//! # Decode outcome
//!
//! Deserialization never propagates a parse failure as a hard error into the
//! cache path. Anything the reader cannot confidently interpret — absent or
//! wrong magic, an unknown `envelope_version`, a `type_id` mismatch, an
//! unsupported `type_version`, or a body decode error — becomes
//! [`CacheDecode::Miss`]. A backend turns `Miss` into a normal cache miss and
//! recomputes the value. This is what lets data written by an older format
//! self-heal: it simply fails the magic check and is regenerated.

use std::io::Write;
use std::sync::Arc;

use bytes::Bytes;

use crate::{Error, Result};

use super::{CacheEntryReader, CacheEntryWriter};

// ---------------------------------------------------------------------------
// Envelope
// ---------------------------------------------------------------------------

/// Magic bytes that prefix every stabilized cache entry.
///
/// An ASCII tag (`0x4C 0x43 0x45 0x31`) chosen so it cannot collide with any
/// pre-stabilization blob: those began with either a small little-endian
/// length (tens of bytes) or a small tag byte, never these values.
///
/// Exported so backends can cheaply identify Lance cache entries (e.g. when
/// scanning a persistent store at startup) without hardcoding the bytes —
/// prefer [`has_cache_envelope`] over comparing against this directly.
pub const MAGIC: [u8; 4] = *b"LCE1";

/// Returns `true` if `data` begins with the cache-entry [`MAGIC`].
///
/// A cheap prefix check for backends that need to recognize Lance cache
/// entries without fully [`deserialize`](CacheCodec::deserialize)-ing them. A
/// `true` result only means the framing looks like ours; the entry can still
/// decode to a [`Miss`](CacheDecode::Miss) (e.g. wrong `type_id`).
pub fn has_cache_envelope(data: &[u8]) -> bool {
    data.get(..MAGIC.len()) == Some(&MAGIC[..])
}

/// Version of the envelope framing itself. Bumped only if the outer frame
/// (magic/version/type_id/type_version layout) ever changes — expected never.
const ENVELOPE_VERSION: u8 = 1;

/// Parsed envelope borrowed from the input bytes.
struct ParsedEnvelope<'a> {
    type_id: &'a str,
    type_version: u32,
    /// Offset of the first body byte within the input.
    body_offset: usize,
}

/// Parse and validate the envelope at the start of `data`.
///
/// Returns `None` for anything that is not a well-formed envelope this build
/// understands (wrong/absent magic, unknown `envelope_version`, truncation,
/// non-utf8 `type_id`). Callers translate `None` into [`CacheDecode::Miss`].
fn parse_envelope(data: &Bytes) -> Option<ParsedEnvelope<'_>> {
    let bytes = data.as_ref();
    let mut off = 0usize;

    let magic = bytes.get(off..off + 4)?;
    if magic != MAGIC {
        return None;
    }
    off += 4;

    if *bytes.get(off)? != ENVELOPE_VERSION {
        return None;
    }
    off += 1;

    let type_id_len = u16::from_le_bytes(bytes.get(off..off + 2)?.try_into().ok()?) as usize;
    off += 2;

    let type_id = std::str::from_utf8(bytes.get(off..off + type_id_len)?).ok()?;
    off += type_id_len;

    let type_version = u32::from_le_bytes(bytes.get(off..off + 4)?.try_into().ok()?);
    off += 4;

    Some(ParsedEnvelope {
        type_id,
        type_version,
        body_offset: off,
    })
}

/// Write the envelope for `type_id`/`type_version`, returning the number of
/// bytes written (the body's starting offset).
fn write_envelope(writer: &mut dyn Write, type_id: &str, type_version: u32) -> Result<usize> {
    let type_id_len = u16::try_from(type_id.len()).map_err(|_| {
        Error::io(format!(
            "cache codec type_id too long ({} bytes, max {})",
            type_id.len(),
            u16::MAX
        ))
    })?;

    writer.write_all(&MAGIC)?;
    writer.write_all(&[ENVELOPE_VERSION])?;
    writer.write_all(&type_id_len.to_le_bytes())?;
    writer.write_all(type_id.as_bytes())?;
    writer.write_all(&type_version.to_le_bytes())?;

    Ok(4 + 1 + 2 + type_id.len() + 4)
}

// ---------------------------------------------------------------------------
// CacheDecode — first-class cache-miss outcome
// ---------------------------------------------------------------------------

/// Why a cache entry could not be decoded into the expected type.
///
/// Carried by [`CacheDecode::Miss`] so backends can emit targeted metrics
/// (e.g. distinguish "evicting due to a stale format" from "type collision")
/// without re-parsing. Every reason maps to the same behavior — recompute via
/// the loader — so callers that don't care can ignore it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CacheMissReason {
    /// Absent or wrong magic, unknown `envelope_version`, truncated framing, or
    /// a non-utf8 `type_id`. Typically an entry written by a pre-stabilization
    /// or otherwise foreign build.
    InvalidEnvelope,
    /// Well-formed envelope, but its `type_id` names a different entry type than
    /// the codec reading it.
    TypeMismatch,
    /// Written by a newer build whose `type_version` this build does not
    /// understand and must not attempt to interpret.
    VersionTooNew,
    /// Envelope validated, but the body failed to decode (truncation, a
    /// malformed protobuf header, an IPC error, etc.).
    BodyError,
}

/// Outcome of deserializing a cache entry.
///
/// `Miss` means the bytes could not be confidently decoded into `T`; the
/// [`CacheMissReason`] says why. A backend treats any `Miss` exactly like a key
/// that was never present: recompute via the loader.
#[derive(Debug)]
pub enum CacheDecode<T> {
    Hit(T),
    Miss(CacheMissReason),
}

impl<T> CacheDecode<T> {
    pub fn hit(self) -> Option<T> {
        match self {
            Self::Hit(v) => Some(v),
            Self::Miss(_) => None,
        }
    }
}

// ---------------------------------------------------------------------------
// CacheCodecImpl — trait for serializable cache entry types
// ---------------------------------------------------------------------------

/// Serialization trait for cache entries.
///
/// **Experimental**: the serialized format is not yet covered by a stability
/// guarantee and may change between releases. When it does stabilize, the
/// rules are: `TYPE_ID`, protobuf field numbers, and enum values are
/// append-only forever; format changes that protobuf cannot express
/// transparently bump [`CURRENT_VERSION`](Self::CURRENT_VERSION).
///
/// Implement this on concrete types that need to survive serialization through
/// a persistent cache backend, then wire it into a
/// [`CacheKey`](super::CacheKey) via [`CacheCodec::from_impl`].
///
/// The envelope (magic/version/type_id/type_version) is written and validated
/// by the [`CacheCodec`] wrapper. [`serialize`](Self::serialize) writes only
/// the body — a header followed by sections in a fixed, version-keyed order —
/// and [`deserialize`](Self::deserialize) reads them back in that same order.
/// The read sequence mirroring the write sequence for each `type_version` is
/// the invariant the implementor owns.
pub trait CacheCodecImpl: Send + Sync {
    /// Stable identity for this entry type. **Must not change once shipped.**
    /// This is a deliberate author-assigned string, not `std::any::type_name`
    /// (which is not stable across compiler versions).
    const TYPE_ID: &'static str;

    /// Body schema version this build writes. Bump when the body layout
    /// changes in a way protobuf field additions cannot express transparently
    /// (adding/removing/reordering sections, a raw-blob encoding change, etc.).
    const CURRENT_VERSION: u32;

    /// Write the body: a header, then sections in a fixed order.
    fn serialize(&self, writer: &mut CacheEntryWriter<'_>) -> Result<()>;

    /// Reconstruct from the body. Branch on
    /// [`reader.version()`](CacheEntryReader::version) for backward compat;
    /// sections are read in write order.
    fn deserialize(reader: &mut CacheEntryReader<'_>) -> Result<Self>
    where
        Self: Sized;
}

// ---------------------------------------------------------------------------
// CacheCodec — type-erased codec passed to backends
// ---------------------------------------------------------------------------

pub(crate) type ArcAny = Arc<dyn std::any::Any + Send + Sync>;

/// Type-erased codec for serializing and deserializing cache entries.
///
/// `CacheCodec` carries the entry's stable `type_id`/`version` plus two plain
/// function pointers — it is `Copy` and has no heap allocation. Construct one
/// via [`CacheCodec::from_impl`] for types that implement [`CacheCodecImpl`],
/// or [`CacheCodec::new`] for custom cases (e.g. when the orphan rule prevents
/// a direct impl).
#[derive(Copy, Clone)]
pub struct CacheCodec {
    type_id: &'static str,
    version: u32,
    serialize_body: fn(&ArcAny, &mut CacheEntryWriter<'_>) -> Result<()>,
    deserialize_body: fn(&mut CacheEntryReader<'_>) -> Result<ArcAny>,
}

impl std::fmt::Debug for CacheCodec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CacheCodec")
            .field("type_id", &self.type_id)
            .field("version", &self.version)
            .finish_non_exhaustive()
    }
}

fn serialize_via_impl<T: CacheCodecImpl + 'static>(
    any: &ArcAny,
    writer: &mut CacheEntryWriter<'_>,
) -> Result<()> {
    let val = any
        .downcast_ref::<T>()
        .expect("CacheCodec::serialize called with wrong type (this is a bug in the cache layer)");
    val.serialize(writer)
}

fn deserialize_via_impl<T: CacheCodecImpl + 'static>(
    reader: &mut CacheEntryReader<'_>,
) -> Result<ArcAny> {
    let val = T::deserialize(reader)?;
    Ok(Arc::new(val) as ArcAny)
}

impl CacheCodec {
    /// Create a `CacheCodec` from explicit body function pointers.
    ///
    /// Prefer [`from_impl`](Self::from_impl) when the value type implements
    /// [`CacheCodecImpl`]. Use this for types where a direct impl isn't
    /// possible (e.g. the orphan rule prevents it). `type_id` and `version`
    /// play the same role as the corresponding [`CacheCodecImpl`] constants.
    pub fn new(
        type_id: &'static str,
        version: u32,
        serialize_body: fn(&ArcAny, &mut CacheEntryWriter<'_>) -> Result<()>,
        deserialize_body: fn(&mut CacheEntryReader<'_>) -> Result<ArcAny>,
    ) -> Self {
        Self {
            type_id,
            version,
            serialize_body,
            deserialize_body,
        }
    }

    /// Create a `CacheCodec` from a [`CacheCodecImpl`] implementation.
    pub fn from_impl<T: CacheCodecImpl + 'static>() -> Self {
        Self {
            type_id: T::TYPE_ID,
            version: T::CURRENT_VERSION,
            serialize_body: serialize_via_impl::<T>,
            deserialize_body: deserialize_via_impl::<T>,
        }
    }

    /// Return the stable entry type identity.
    ///
    /// Persistent and tiered backends can use this metadata to route encoded
    /// values without retaining a readable logical cache key.
    pub const fn type_id(&self) -> &'static str {
        self.type_id
    }

    /// Serialize `value` into `writer`: envelope first, then the body.
    pub fn serialize(&self, value: &ArcAny, writer: &mut dyn Write) -> Result<()> {
        let body_offset = write_envelope(writer, self.type_id, self.version)?;
        let mut entry_writer = CacheEntryWriter::with_pos(writer, body_offset);
        (self.serialize_body)(value, &mut entry_writer)
    }

    /// Deserialize an entry from `data`.
    ///
    /// Never fails: any non-fatal failure to interpret the bytes becomes a
    /// [`CacheDecode::Miss`] with the reason why (see [`CacheMissReason`]).
    /// Reading from an in-memory [`Bytes`] cannot do I/O, so there is no fault
    /// channel — a miss is the only non-`Hit` outcome.
    pub fn deserialize(&self, data: &Bytes) -> CacheDecode<ArcAny> {
        let Some(envelope) = parse_envelope(data) else {
            log::debug!("cache entry rejected: missing or invalid envelope");
            return CacheDecode::Miss(CacheMissReason::InvalidEnvelope);
        };

        if envelope.type_id != self.type_id {
            log::debug!(
                "cache entry type_id mismatch: got {:?}, expected {:?}",
                envelope.type_id,
                self.type_id
            );
            return CacheDecode::Miss(CacheMissReason::TypeMismatch);
        }

        // A version newer than this build writes was produced by a newer build
        // whose body layout we cannot assume to understand. Older/equal versions
        // are the impl's responsibility to handle (branching on reader.version()).
        if envelope.type_version > self.version {
            log::debug!(
                "cache entry {:?} has unsupported type_version {} (this build writes {})",
                self.type_id,
                envelope.type_version,
                self.version
            );
            return CacheDecode::Miss(CacheMissReason::VersionTooNew);
        }

        let mut reader = CacheEntryReader::new(data, envelope.body_offset, envelope.type_version);
        match (self.deserialize_body)(&mut reader) {
            Ok(value) => CacheDecode::Hit(value),
            Err(e) => {
                log::debug!(
                    "cache entry {:?} v{} failed to decode: {e}",
                    self.type_id,
                    envelope.type_version
                );
                CacheDecode::Miss(CacheMissReason::BodyError)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A trivial codec used to exercise the envelope and miss semantics
    /// without pulling in arrow-backed payloads.
    #[derive(Debug, PartialEq)]
    struct Widget {
        n: u32,
    }

    impl CacheCodecImpl for Widget {
        const TYPE_ID: &'static str = "test.Widget";
        const CURRENT_VERSION: u32 = 1;

        fn serialize(&self, writer: &mut CacheEntryWriter<'_>) -> Result<()> {
            writer.write_raw(&self.n.to_le_bytes())
        }

        fn deserialize(reader: &mut CacheEntryReader<'_>) -> Result<Self> {
            let bytes = reader.read_raw()?;
            let n = u32::from_le_bytes(
                bytes
                    .as_ref()
                    .try_into()
                    .map_err(|_| Error::io("bad widget".to_string()))?,
            );
            Ok(Self { n })
        }
    }

    fn serialize_widget(widget: &Widget) -> Bytes {
        let codec = CacheCodec::from_impl::<Widget>();
        let any: ArcAny = Arc::new(Widget { n: widget.n });
        let mut buf = Vec::new();
        codec.serialize(&any, &mut buf).unwrap();
        Bytes::from(buf)
    }

    /// The miss reason, or `None` if the decode was a hit.
    fn miss_reason(data: &Bytes) -> Option<CacheMissReason> {
        match deserialize_widget(data) {
            CacheDecode::Hit(_) => None,
            CacheDecode::Miss(reason) => Some(reason),
        }
    }

    fn deserialize_widget(data: &Bytes) -> CacheDecode<Widget> {
        let codec = CacheCodec::from_impl::<Widget>();
        match codec.deserialize(data) {
            CacheDecode::Hit(any) => {
                CacheDecode::Hit(Arc::try_unwrap(any.downcast::<Widget>().unwrap()).unwrap())
            }
            CacheDecode::Miss(reason) => CacheDecode::Miss(reason),
        }
    }

    #[test]
    fn envelope_roundtrip_hits() {
        let bytes = serialize_widget(&Widget { n: 0xDEADBEEF });
        // Sanity: the entry starts with the magic.
        assert_eq!(&bytes[..4], b"LCE1");
        let decoded = deserialize_widget(&bytes).hit().unwrap();
        assert_eq!(decoded, Widget { n: 0xDEADBEEF });
    }

    #[test]
    fn has_cache_envelope_detects_magic() {
        let bytes = serialize_widget(&Widget { n: 1 });
        assert!(has_cache_envelope(&bytes));
        assert!(has_cache_envelope(&MAGIC)); // exactly the magic, nothing after
        assert!(!has_cache_envelope(b"LCE")); // too short
        assert!(!has_cache_envelope(b"JUNK and more"));
        assert!(!has_cache_envelope(&[]));
    }

    #[test]
    fn wrong_magic_is_miss() {
        let mut bytes = serialize_widget(&Widget { n: 7 }).to_vec();
        bytes[0] = b'X';
        assert_eq!(
            miss_reason(&Bytes::from(bytes)),
            Some(CacheMissReason::InvalidEnvelope)
        );
    }

    #[test]
    fn pre_stabilization_blob_is_miss() {
        // An old unstable blob led with a small u64 LE length prefix (a JSON
        // header of tens of bytes) — no magic. It must self-heal to a miss.
        let mut blob = Vec::new();
        blob.extend_from_slice(&(42u64).to_le_bytes());
        blob.extend_from_slice(&[0u8; 42]);
        assert_eq!(
            miss_reason(&Bytes::from(blob)),
            Some(CacheMissReason::InvalidEnvelope)
        );

        // A different unstable shape led with a small u8 tag (0/1/2).
        assert_eq!(
            miss_reason(&Bytes::from(vec![0u8, 1, 2, 3])),
            Some(CacheMissReason::InvalidEnvelope)
        );
    }

    #[test]
    fn unknown_envelope_version_is_miss() {
        let mut bytes = serialize_widget(&Widget { n: 7 }).to_vec();
        bytes[4] = 0xFF; // envelope_version byte
        assert_eq!(
            miss_reason(&Bytes::from(bytes)),
            Some(CacheMissReason::InvalidEnvelope)
        );
    }

    #[test]
    fn type_id_mismatch_is_miss() {
        // Hand-build an envelope with a foreign type_id but valid framing.
        let mut buf = Vec::new();
        write_envelope(&mut buf, "some.OtherType", 1).unwrap();
        buf.extend_from_slice(&(4u64).to_le_bytes());
        buf.extend_from_slice(&99u32.to_le_bytes());
        assert_eq!(
            miss_reason(&Bytes::from(buf)),
            Some(CacheMissReason::TypeMismatch)
        );
    }

    #[test]
    fn unsupported_future_type_version_is_miss() {
        // An entry written by a newer build (higher type_version) must miss
        // rather than be misread by this build.
        let mut buf = Vec::new();
        write_envelope(&mut buf, Widget::TYPE_ID, Widget::CURRENT_VERSION + 1).unwrap();
        lance_arrow::ipc::write_len_prefixed_bytes(&mut buf, &9u32.to_le_bytes()).unwrap();
        assert_eq!(
            miss_reason(&Bytes::from(buf)),
            Some(CacheMissReason::VersionTooNew)
        );
    }

    #[test]
    fn truncated_envelope_is_miss() {
        let bytes = serialize_widget(&Widget { n: 7 });
        for cut in [0, 1, 4, 5, 7, 9] {
            assert_eq!(
                miss_reason(&bytes.slice(..cut.min(bytes.len()))),
                Some(CacheMissReason::InvalidEnvelope),
                "truncating to {cut} bytes should miss as InvalidEnvelope"
            );
        }
    }

    #[test]
    fn body_decode_error_is_miss() {
        // Valid envelope, but the body is too short for the widget.
        let mut buf = Vec::new();
        write_envelope(&mut buf, Widget::TYPE_ID, Widget::CURRENT_VERSION).unwrap();
        buf.extend_from_slice(&(1u64).to_le_bytes());
        buf.push(0u8);
        assert_eq!(
            miss_reason(&Bytes::from(buf)),
            Some(CacheMissReason::BodyError)
        );
    }

    #[test]
    fn reader_exposes_envelope_version() {
        // type_version travels through the envelope to reader.version().
        let mut buf = Vec::new();
        write_envelope(&mut buf, Widget::TYPE_ID, 7).unwrap();
        let body_off = buf.len();
        // A widget body so the codec can decode it.
        lance_arrow::ipc::write_len_prefixed_bytes(&mut buf, &5u32.to_le_bytes()).unwrap();
        let data = Bytes::from(buf);

        let mut r = CacheEntryReader::new(&data, body_off, 7);
        assert_eq!(r.version(), 7);
        assert_eq!(r.read_raw().unwrap().as_ref(), 5u32.to_le_bytes());
    }
}
