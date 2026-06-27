//! WAL payload envelope: lets one [`crate::record::WalRecord`] carry many
//! domain rows in a single appended-and-fsynced unit.
//!
//! The on-disk record format (length-prefixed, CRC-protected) is unchanged —
//! only the contents of `WalRecord::payload` are interpreted differently. This
//! lets a fleet upgrade roll out without segment migrations.
//!
//! Encoded layout inside the record payload:
//!
//! ```text
//! [magic:4 = "ZWB1"][encoding:1][count:u32 BE][row_0_len:u32 BE][row_0][row_1_len:u32 BE][row_1]...
//! ```
//!
//! For backward compatibility, anything that does not start with the `ZWB1`
//! magic is treated as a single-row payload in the legacy `row_json_v1` shape
//! (the format used before this module existed).

use bytes::{Buf, BufMut, Bytes, BytesMut};

/// Magic bytes that prefix every batch envelope. Distinct from the leading
/// byte of a valid JSON document (`{`, `[`, digit, etc.) so the legacy
/// detector in [`decode`] cannot misidentify a JSON-row payload.
pub const BATCH_MAGIC: &[u8; 4] = b"ZWB1";

/// Encoding tag for the row sequence inside a [`BatchPayload`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum BatchEncoding {
    /// One newline-free JSON object per row. Same wire shape the legacy
    /// per-row WAL writer produced, just grouped into one envelope so a
    /// caller can append-and-fsync many rows in one operation.
    JsonRowsV1 = 1,
}

impl BatchEncoding {
    fn from_u8(v: u8) -> Option<Self> {
        match v {
            1 => Some(Self::JsonRowsV1),
            _ => None,
        }
    }
}

/// Errors from envelope decoding. Encoding cannot fail.
#[derive(Debug, thiserror::Error)]
pub enum BatchDecodeError {
    #[error("payload too short for batch envelope ({0} bytes)")]
    TooShort(usize),

    #[error("unknown batch encoding tag: {0}")]
    UnknownEncoding(u8),

    #[error("row {index} length {needed} exceeds remaining buffer {remaining}")]
    RowTruncated {
        index: u32,
        needed: usize,
        remaining: usize,
    },

    #[error("declared row count {declared} but only decoded {decoded} before EOF")]
    CountMismatch { declared: u32, decoded: u32 },
}

/// Encode a sequence of row payloads (each already JSON-serialized) into a
/// single envelope suitable for `WalRecord::payload`.
///
/// The caller owns the per-row encoding choice; this function only frames the
/// bytes. Empty input is allowed and produces a valid zero-row envelope.
pub fn encode_json_rows<I, T>(rows: I) -> Bytes
where
    I: IntoIterator<Item = T>,
    T: AsRef<[u8]>,
{
    let rows: Vec<T> = rows.into_iter().collect();
    let total_payload: usize = rows.iter().map(|r| r.as_ref().len()).sum();
    let mut buf =
        BytesMut::with_capacity(BATCH_MAGIC.len() + 1 + 4 + rows.len() * 4 + total_payload);

    buf.put_slice(BATCH_MAGIC);
    buf.put_u8(BatchEncoding::JsonRowsV1 as u8);
    buf.put_u32(rows.len() as u32);
    for row in &rows {
        let bytes = row.as_ref();
        buf.put_u32(bytes.len() as u32);
        buf.put_slice(bytes);
    }
    buf.freeze()
}

/// Decoded view of one batch envelope.
#[derive(Debug, Clone)]
pub struct BatchPayload {
    pub encoding: BatchEncoding,
    pub rows: Vec<Bytes>,
}

/// Decode `payload`. Returns:
///
/// - `Ok(Some(BatchPayload))` if the payload starts with [`BATCH_MAGIC`] and
///   decodes cleanly. The caller should iterate `rows` and apply each one.
/// - `Ok(None)` if the payload does NOT start with the magic. The caller must
///   treat the whole payload as one legacy row (the pre-batch encoding).
/// - `Err(_)` on a structural decoding error after the magic was matched.
pub fn decode(payload: &Bytes) -> Result<Option<BatchPayload>, BatchDecodeError> {
    if payload.len() < BATCH_MAGIC.len() || &payload[..BATCH_MAGIC.len()] != BATCH_MAGIC {
        return Ok(None);
    }

    if payload.len() < BATCH_MAGIC.len() + 1 + 4 {
        return Err(BatchDecodeError::TooShort(payload.len()));
    }

    let mut cursor = &payload[BATCH_MAGIC.len()..];
    let enc_byte = cursor.get_u8();
    let encoding =
        BatchEncoding::from_u8(enc_byte).ok_or(BatchDecodeError::UnknownEncoding(enc_byte))?;
    let count = cursor.get_u32();

    let mut rows = Vec::with_capacity(count as usize);
    for i in 0..count {
        if cursor.remaining() < 4 {
            return Err(BatchDecodeError::CountMismatch {
                declared: count,
                decoded: i,
            });
        }
        let row_len = cursor.get_u32() as usize;
        if cursor.remaining() < row_len {
            return Err(BatchDecodeError::RowTruncated {
                index: i,
                needed: row_len,
                remaining: cursor.remaining(),
            });
        }
        let consumed_so_far = payload.len() - cursor.remaining();
        let row = payload.slice(consumed_so_far..consumed_so_far + row_len);
        cursor.advance(row_len);
        rows.push(row);
    }

    Ok(Some(BatchPayload { encoding, rows }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip_empty_batch_is_valid() {
        let encoded = encode_json_rows::<_, &[u8]>(Vec::<&[u8]>::new());
        let decoded = decode(&encoded).unwrap().expect("magic-prefixed payload");
        assert_eq!(decoded.encoding, BatchEncoding::JsonRowsV1);
        assert!(decoded.rows.is_empty());
    }

    #[test]
    fn round_trip_multiple_rows_preserves_order_and_bytes() {
        let rows = [
            br#"{"id":"a"}"#.as_slice(),
            br#"{"id":"b"}"#.as_slice(),
            br#"{"id":"c"}"#.as_slice(),
        ];
        let encoded = encode_json_rows(rows.iter().copied());
        let decoded = decode(&encoded).unwrap().unwrap();
        assert_eq!(decoded.rows.len(), 3);
        assert_eq!(&decoded.rows[0][..], br#"{"id":"a"}"#);
        assert_eq!(&decoded.rows[1][..], br#"{"id":"b"}"#);
        assert_eq!(&decoded.rows[2][..], br#"{"id":"c"}"#);
    }

    #[test]
    fn legacy_json_payload_returns_none() {
        // A pre-batch payload is just a JSON document. decode() must report
        // `Ok(None)` so the caller falls back to the single-row legacy path.
        let payload = Bytes::from_static(br#"{"id":"legacy","value":42}"#);
        assert!(decode(&payload).unwrap().is_none());
    }

    #[test]
    fn empty_payload_is_legacy_shape() {
        // Empty bytes never match the magic; treated as legacy (the legacy
        // single-row decoder will fail JSON parsing — that's the right error).
        let payload = Bytes::new();
        assert!(decode(&payload).unwrap().is_none());
    }

    #[test]
    fn short_magic_prefixed_payload_errors() {
        // Magic alone — no encoding byte or count.
        let payload = Bytes::from_static(b"ZWB1");
        assert!(matches!(
            decode(&payload),
            Err(BatchDecodeError::TooShort(_))
        ));
    }

    #[test]
    fn unknown_encoding_errors() {
        let mut buf = BytesMut::new();
        buf.put_slice(BATCH_MAGIC);
        buf.put_u8(0xFE); // unknown encoding
        buf.put_u32(0);
        let payload = buf.freeze();
        assert!(matches!(
            decode(&payload),
            Err(BatchDecodeError::UnknownEncoding(0xFE))
        ));
    }

    #[test]
    fn truncated_row_payload_errors() {
        // Declare a 100-byte row but supply only 10 bytes.
        let mut buf = BytesMut::new();
        buf.put_slice(BATCH_MAGIC);
        buf.put_u8(BatchEncoding::JsonRowsV1 as u8);
        buf.put_u32(1); // one row
        buf.put_u32(100); // declared row length
        buf.put_slice(&[b'x'; 10]); // only 10 bytes
        let payload = buf.freeze();
        assert!(matches!(
            decode(&payload),
            Err(BatchDecodeError::RowTruncated {
                index: 0,
                needed: 100,
                remaining: 10
            })
        ));
    }

    #[test]
    fn missing_length_prefix_errors() {
        // Declare 2 rows, supply one full row, then stop before next length.
        let mut buf = BytesMut::new();
        buf.put_slice(BATCH_MAGIC);
        buf.put_u8(BatchEncoding::JsonRowsV1 as u8);
        buf.put_u32(2); // two rows
        buf.put_u32(3); // row 0 length
        buf.put_slice(b"abc");
        // missing row 1 length entirely
        let payload = buf.freeze();
        assert!(matches!(
            decode(&payload),
            Err(BatchDecodeError::CountMismatch {
                declared: 2,
                decoded: 1
            })
        ));
    }

    #[test]
    fn single_row_batch_matches_raw_row_bytes() {
        // A batch with one row should contain that row byte-exactly so the
        // domain decoder gets the same input as the legacy single-row path.
        let original = br#"{"trace_id":"abc","span_id":"def"}"#;
        let encoded = encode_json_rows(std::iter::once(original.as_slice()));
        let decoded = decode(&encoded).unwrap().unwrap();
        assert_eq!(decoded.rows.len(), 1);
        assert_eq!(&decoded.rows[0][..], original);
    }
}
