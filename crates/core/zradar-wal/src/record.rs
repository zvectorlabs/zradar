/// On-disk WAL record format as specified in SPEC-DISK-WAL §3.2.
///
/// Each record is length-prefixed with a CRC32 integrity check:
///   [4 byte length][4 byte CRC32][1 byte signal_type][16 byte tenant_id]
///   [16 byte project_id][8 byte arrival_timestamp_ns][8 byte assigned_offset]
///   [N byte payload]
use bytes::{Buf, BufMut, Bytes, BytesMut};
use uuid::Uuid;

/// Header size: length(4) + crc(4) + signal(1) + tenant(16) + project(16) + ts(8) + offset(8) = 57
pub const RECORD_HEADER_SIZE: usize = 4 + 4 + 1 + 16 + 16 + 8 + 8;

/// Signal type discriminator stored in each WAL record.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum SignalType {
    Trace = 0,
    Metric = 1,
    Log = 2,
    /// NeMo Evaluator scores — Phase 1 R1.8 / OQ8.
    Score = 3,
}

impl SignalType {
    pub fn from_u8(v: u8) -> Option<Self> {
        match v {
            0 => Some(Self::Trace),
            1 => Some(Self::Metric),
            2 => Some(Self::Log),
            3 => Some(Self::Score),
            _ => None,
        }
    }
}

/// A single WAL record ready for serialization or returned from deserialization.
#[derive(Debug, Clone)]
pub struct WalRecord {
    pub signal_type: SignalType,
    pub tenant_id: Uuid,
    pub project_id: Uuid,
    pub arrival_timestamp_ns: i64,
    pub assigned_offset: u64,
    pub payload: Bytes,
}

/// Errors that can occur when reading a WAL record from a segment.
#[derive(Debug, thiserror::Error)]
pub enum RecordReadError {
    #[error("torn write: incomplete length prefix at offset {offset}")]
    TornWriteIncomplete { offset: u64 },

    #[error(
        "torn write: CRC mismatch at offset {offset} (expected {expected:#010x}, got {actual:#010x})"
    )]
    TornWriteCrcMismatch {
        offset: u64,
        expected: u32,
        actual: u32,
    },

    #[error(
        "torn write: record truncated at offset {offset}, need {need} bytes but only {have} available"
    )]
    TornWriteTruncated {
        offset: u64,
        need: usize,
        have: usize,
    },

    #[error("invalid signal type {value} at offset {offset}")]
    InvalidSignalType { offset: u64, value: u8 },

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
}

impl WalRecord {
    /// Serialize this record into a byte buffer suitable for appending to a segment.
    ///
    /// Layout: [length:4][crc32:4][signal:1][tenant:16][project:16][ts:8][offset:8][payload:N]
    /// where `length` = total bytes after the length field (i.e., the record size minus 4).
    pub fn serialize(&self) -> Bytes {
        let payload_len = self.payload.len();
        let body_len = 4 + 1 + 16 + 16 + 8 + 8 + payload_len; // crc + header fields + payload
        let total_len = 4 + body_len; // length prefix + body

        let mut buf = BytesMut::with_capacity(total_len);

        // Length prefix (does NOT include itself)
        buf.put_u32(body_len as u32);

        // Placeholder for CRC (will overwrite)
        let crc_pos = buf.len();
        buf.put_u32(0);

        // Body (what gets CRC'd)
        buf.put_u8(self.signal_type as u8);
        buf.put_slice(self.tenant_id.as_bytes());
        buf.put_slice(self.project_id.as_bytes());
        buf.put_i64(self.arrival_timestamp_ns);
        buf.put_u64(self.assigned_offset);
        buf.put_slice(&self.payload);

        // Compute CRC over everything after the CRC field
        let crc_data = &buf[crc_pos + 4..];
        let crc = crc32fast::hash(crc_data);
        buf[crc_pos..crc_pos + 4].copy_from_slice(&crc.to_be_bytes());

        buf.freeze()
    }

    /// Attempt to deserialize one record from `data` starting at `data_offset` within the
    /// segment. Returns `(record, bytes_consumed)` on success.
    pub fn deserialize(data: &[u8], segment_offset: u64) -> Result<(Self, usize), RecordReadError> {
        if data.len() < 4 {
            return Err(RecordReadError::TornWriteIncomplete {
                offset: segment_offset,
            });
        }

        let mut cursor = data;
        let body_len = cursor.get_u32() as usize;

        if data.len() < 4 + body_len {
            return Err(RecordReadError::TornWriteTruncated {
                offset: segment_offset,
                need: 4 + body_len,
                have: data.len(),
            });
        }

        let body = &data[4..4 + body_len];

        // First 4 bytes of body are the CRC
        let stored_crc = u32::from_be_bytes([body[0], body[1], body[2], body[3]]);
        let actual_crc = crc32fast::hash(&body[4..]);

        if stored_crc != actual_crc {
            return Err(RecordReadError::TornWriteCrcMismatch {
                offset: segment_offset,
                expected: stored_crc,
                actual: actual_crc,
            });
        }

        let mut cursor = &body[4..]; // skip CRC
        let signal_byte = cursor.get_u8();
        let signal_type =
            SignalType::from_u8(signal_byte).ok_or(RecordReadError::InvalidSignalType {
                offset: segment_offset,
                value: signal_byte,
            })?;

        let mut tenant_bytes = [0u8; 16];
        tenant_bytes.copy_from_slice(&cursor[..16]);
        cursor.advance(16);
        let tenant_id = Uuid::from_bytes(tenant_bytes);

        let mut project_bytes = [0u8; 16];
        project_bytes.copy_from_slice(&cursor[..16]);
        cursor.advance(16);
        let project_id = Uuid::from_bytes(project_bytes);

        let arrival_timestamp_ns = cursor.get_i64();
        let assigned_offset = cursor.get_u64();

        let payload = Bytes::copy_from_slice(cursor);

        let total_consumed = 4 + body_len;

        Ok((
            Self {
                signal_type,
                tenant_id,
                project_id,
                arrival_timestamp_ns,
                assigned_offset,
                payload,
            },
            total_consumed,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_record(signal: SignalType, offset: u64) -> WalRecord {
        WalRecord {
            signal_type: signal,
            tenant_id: Uuid::new_v4(),
            project_id: Uuid::new_v4(),
            arrival_timestamp_ns: 1_700_000_000_000_000_000,
            assigned_offset: offset,
            payload: Bytes::from(vec![0xAB; 64]),
        }
    }

    #[test]
    fn test_round_trip_all_signal_types() {
        for signal in [
            SignalType::Trace,
            SignalType::Metric,
            SignalType::Log,
            SignalType::Score,
        ] {
            let rec = make_record(signal, 42);
            let serialized = rec.serialize();
            let (deserialized, consumed) = WalRecord::deserialize(&serialized, 0).unwrap();

            assert_eq!(consumed, serialized.len());
            assert_eq!(deserialized.signal_type, signal);
            assert_eq!(deserialized.tenant_id, rec.tenant_id);
            assert_eq!(deserialized.project_id, rec.project_id);
            assert_eq!(deserialized.arrival_timestamp_ns, rec.arrival_timestamp_ns);
            assert_eq!(deserialized.assigned_offset, rec.assigned_offset);
            assert_eq!(deserialized.payload, rec.payload);
        }
    }

    #[test]
    fn test_signal_type_from_u8_round_trip() {
        // SignalType::Score is the Phase 1 R1.8 addition; assert the discriminant
        // contract directly so a future renumbering can't silently shadow it.
        assert_eq!(SignalType::from_u8(0), Some(SignalType::Trace));
        assert_eq!(SignalType::from_u8(1), Some(SignalType::Metric));
        assert_eq!(SignalType::from_u8(2), Some(SignalType::Log));
        assert_eq!(SignalType::from_u8(3), Some(SignalType::Score));
        assert_eq!(SignalType::from_u8(4), None);
        assert_eq!(SignalType::from_u8(255), None);
    }

    #[test]
    fn test_crc_mismatch_returns_torn_write() {
        let rec = make_record(SignalType::Trace, 1);
        let mut data = rec.serialize().to_vec();
        // Corrupt a byte in the payload area (after CRC field)
        let last = data.len() - 1;
        data[last] ^= 0xFF;

        let err = WalRecord::deserialize(&data, 100).unwrap_err();
        assert!(matches!(err, RecordReadError::TornWriteCrcMismatch { .. }));
    }

    #[test]
    fn test_truncated_length_prefix() {
        let data = [0x00, 0x01]; // only 2 bytes, need 4 for length
        let err = WalRecord::deserialize(&data, 50).unwrap_err();
        assert!(matches!(err, RecordReadError::TornWriteIncomplete { .. }));
    }

    #[test]
    fn test_truncated_body() {
        let rec = make_record(SignalType::Metric, 7);
        let serialized = rec.serialize();
        // Truncate to half the serialized length
        let truncated = &serialized[..serialized.len() / 2];
        let err = WalRecord::deserialize(truncated, 0).unwrap_err();
        assert!(matches!(err, RecordReadError::TornWriteTruncated { .. }));
    }
}

#[cfg(test)]
mod proptests {
    use super::*;
    use proptest::prelude::*;

    fn arb_signal() -> impl Strategy<Value = SignalType> {
        prop_oneof![
            Just(SignalType::Trace),
            Just(SignalType::Metric),
            Just(SignalType::Log),
            Just(SignalType::Score),
        ]
    }

    proptest! {
        /// Random payload sizes and CRC corruptions never panic; always produce
        /// clean torn-write detection on corrupt data.
        #[test]
        fn prop_random_sizes_and_corruptions_never_panic(
            signal in arb_signal(),
            payload_len in 0usize..4096,
            offset in 0u64..1_000_000,
            corrupt_pos in 0usize..1024,
            corrupt_byte in 0u8..=255u8,
        ) {
            let rec = WalRecord {
                signal_type: signal,
                tenant_id: Uuid::nil(),
                project_id: Uuid::nil(),
                arrival_timestamp_ns: 12345,
                assigned_offset: offset,
                payload: Bytes::from(vec![0xBB; payload_len]),
            };

            // Valid round-trip should always work
            let serialized = rec.serialize();
            let (decoded, consumed) = WalRecord::deserialize(&serialized, 0).unwrap();
            prop_assert_eq!(consumed, serialized.len());
            prop_assert_eq!(decoded.assigned_offset, offset);

            // Corrupt a byte (if in range) and verify no panic
            if corrupt_pos < serialized.len() {
                let mut corrupted = serialized.to_vec();
                corrupted[corrupt_pos] ^= corrupt_byte.max(1); // ensure at least one bit flip
                // Should either succeed (if CRC still matches, extremely unlikely)
                // or produce a torn-write / invalid error — never panic
                let _ = WalRecord::deserialize(&corrupted, 0);
            }

            // Truncation at every byte should never panic
            for trunc_len in 0..serialized.len() {
                let _ = WalRecord::deserialize(&serialized[..trunc_len], 0);
            }
        }
    }
}
