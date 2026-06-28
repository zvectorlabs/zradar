/// WAL segment file management: writing and reading records within a single
/// segment file on disk.
///
/// Segment files use this on-disk format:
///   [magic: 4 bytes "ZWAL"][version: 1 byte][segment_id: 8 bytes (u64 BE)]
///   [record...][record...]...
///
/// Each record is written using the format defined in `record.rs`.
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};

use crate::record::{RecordReadError, WalRecord};

/// Magic bytes at the start of every segment file.
pub const SEGMENT_MAGIC: &[u8; 4] = b"ZWAL";

/// Current format version.
pub const SEGMENT_VERSION: u8 = 1;

/// Header size: magic(4) + version(1) + segment_id(8) = 13 bytes.
pub const SEGMENT_HEADER_SIZE: u64 = 13;

/// Error when segment header is invalid.
#[derive(Debug, thiserror::Error)]
pub enum SegmentError {
    #[error("invalid segment magic")]
    InvalidMagic,

    #[error("unsupported segment version {0}")]
    UnsupportedVersion(u8),

    #[error("IO error: {0}")]
    Io(#[from] io::Error),

    #[error("record error: {0}")]
    Record(#[from] RecordReadError),
}

/// Writes records to a single segment file.
pub struct SegmentWriter {
    file: std::fs::File,
    segment_id: u64,
    current_size: u64,
    path: PathBuf,
}

impl SegmentWriter {
    /// Create a new segment file with the given id. Writes the header immediately.
    pub fn create(dir: &Path, segment_id: u64) -> Result<Self, SegmentError> {
        let path = segment_path(dir, segment_id);
        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&path)?;

        // Write header
        file.write_all(SEGMENT_MAGIC)?;
        file.write_all(&[SEGMENT_VERSION])?;
        file.write_all(&segment_id.to_be_bytes())?;

        Ok(Self {
            file,
            segment_id,
            current_size: SEGMENT_HEADER_SIZE,
            path,
        })
    }

    /// Append a pre-serialized record to this segment.
    /// Returns the byte offset within the segment where the record starts.
    pub fn append(&mut self, data: &[u8]) -> Result<u64, SegmentError> {
        let offset = self.current_size;
        self.file.write_all(data)?;
        self.current_size += data.len() as u64;
        Ok(offset)
    }

    /// Flush the OS write buffer. Does NOT fsync (that's handled by GroupCommitFsyncer).
    pub fn flush(&mut self) -> Result<(), SegmentError> {
        self.file.flush()?;
        Ok(())
    }

    /// Issue an fsync on the underlying file descriptor.
    pub fn fsync(&self) -> Result<(), SegmentError> {
        self.file.sync_all()?;
        Ok(())
    }

    /// Current size of this segment in bytes (header + records).
    pub fn size(&self) -> u64 {
        self.current_size
    }

    /// The id of this segment.
    pub fn id(&self) -> u64 {
        self.segment_id
    }

    /// The path to this segment file on disk.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Re-open an existing segment file for appending (does not validate header).
    pub fn open_existing(
        path: PathBuf,
        file: std::fs::File,
        segment_id: u64,
        current_size: u64,
    ) -> Self {
        Self {
            file,
            segment_id,
            current_size,
            path,
        }
    }
}

/// Reads records sequentially from a segment file, detecting torn writes at the tail.
#[derive(Debug)]
pub struct SegmentReader {
    data: Vec<u8>,
    segment_id: u64,
    pos: u64,
}

impl SegmentReader {
    /// Open and read an existing segment file. Validates the header.
    pub fn open(dir: &Path, segment_id: u64) -> Result<Self, SegmentError> {
        let path = segment_path(dir, segment_id);
        Self::open_path(&path)
    }

    /// Open a segment from a specific path.
    pub fn open_path(path: &Path) -> Result<Self, SegmentError> {
        let mut file = std::fs::File::open(path)?;
        let mut data = Vec::new();
        file.read_to_end(&mut data)?;

        if data.len() < SEGMENT_HEADER_SIZE as usize {
            return Err(SegmentError::Io(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "segment file too small for header",
            )));
        }

        // Validate magic
        if &data[0..4] != SEGMENT_MAGIC {
            return Err(SegmentError::InvalidMagic);
        }

        // Validate version
        let version = data[4];
        if version != SEGMENT_VERSION {
            return Err(SegmentError::UnsupportedVersion(version));
        }

        // Read segment_id
        let mut id_bytes = [0u8; 8];
        id_bytes.copy_from_slice(&data[5..13]);
        let segment_id = u64::from_be_bytes(id_bytes);

        Ok(Self {
            data,
            segment_id,
            pos: SEGMENT_HEADER_SIZE,
        })
    }

    /// The segment id read from the header.
    pub fn segment_id(&self) -> u64 {
        self.segment_id
    }

    /// Total size of the segment file in bytes.
    pub fn file_size(&self) -> u64 {
        self.data.len() as u64
    }

    /// Read the next record. Returns `None` at clean EOF, `Err` on torn write.
    pub fn next_record(&mut self) -> Result<Option<WalRecord>, SegmentError> {
        if self.pos >= self.data.len() as u64 {
            return Ok(None);
        }

        let remaining = &self.data[self.pos as usize..];
        if remaining.is_empty() {
            return Ok(None);
        }

        match WalRecord::deserialize(remaining, self.pos) {
            Ok((record, consumed)) => {
                self.pos += consumed as u64;
                Ok(Some(record))
            }
            Err(e) => Err(SegmentError::Record(e)),
        }
    }

    /// Current read position within the segment (byte offset from start of file).
    pub fn position(&self) -> u64 {
        self.pos
    }

    /// Returns the byte offset at which torn-write recovery should truncate.
    /// Call this after a torn-write error to get the safe truncation point.
    pub fn truncation_point(&self) -> u64 {
        self.pos
    }
}

/// Truncate a segment file at the given byte offset, removing torn-write garbage.
pub fn truncate_segment(dir: &Path, segment_id: u64, at_offset: u64) -> Result<(), SegmentError> {
    let path = segment_path(dir, segment_id);
    let file = std::fs::OpenOptions::new().write(true).open(&path)?;
    file.set_len(at_offset)?;
    file.sync_all()?;
    Ok(())
}

/// Truncate a segment file by path.
pub fn truncate_segment_at_path(path: &Path, at_offset: u64) -> Result<(), SegmentError> {
    let file = std::fs::OpenOptions::new().write(true).open(path)?;
    file.set_len(at_offset)?;
    file.sync_all()?;
    Ok(())
}

/// Compute the filename for a segment: `{segment_id:020}.seg`
pub fn segment_path(dir: &Path, segment_id: u64) -> PathBuf {
    dir.join(format!("{:020}.seg", segment_id))
}

/// Update the `current.seg` symlink to point to the active segment.
pub fn update_current_symlink(dir: &Path, segment_id: u64) -> Result<(), SegmentError> {
    let link_path = dir.join("current.seg");
    let target = format!("{:020}.seg", segment_id);

    // Remove old symlink if it exists (ignore errors)
    let _ = std::fs::remove_file(&link_path);

    #[cfg(unix)]
    std::os::unix::fs::symlink(&target, &link_path)?;

    #[cfg(not(unix))]
    {
        // On non-unix, write a text file with the target name
        std::fs::write(&link_path, &target)?;
    }

    Ok(())
}

/// List all segment files in the WAL directory, sorted by segment_id.
pub fn list_segments(dir: &Path) -> Result<Vec<u64>, SegmentError> {
    let mut segments = Vec::new();

    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let name = entry.file_name();
        let name_str = name.to_string_lossy();
        if name_str.ends_with(".seg")
            && name_str != "current.seg"
            && let Ok(id) = name_str.trim_end_matches(".seg").parse::<u64>()
        {
            segments.push(id);
        }
    }

    segments.sort_unstable();
    Ok(segments)
}

#[cfg(test)]
mod tests {
    #[allow(unused_imports)]
    use uuid::Uuid;
    #[allow(unused_imports)]
    use zradar_models::WorkspaceId;

    use super::*;
    use crate::record::{SignalType, WalRecord};
    use bytes::Bytes;
    use tempfile::TempDir;

    fn sample_record(offset: u64) -> WalRecord {
        WalRecord {
            signal_type: SignalType::Trace,
            workspace_id: WorkspaceId::new(),
            arrival_timestamp_ns: 1_700_000_000_000_000_000,
            assigned_offset: offset,
            payload: Bytes::from(vec![0xDE; 128]),
        }
    }

    #[test]
    fn test_segment_write_and_read() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path();

        let mut writer = SegmentWriter::create(dir, 1).unwrap();
        assert_eq!(writer.id(), 1);
        assert_eq!(writer.size(), SEGMENT_HEADER_SIZE);

        let rec1 = sample_record(1);
        let rec2 = sample_record(2);
        writer.append(&rec1.serialize()).unwrap();
        writer.append(&rec2.serialize()).unwrap();
        writer.fsync().unwrap();

        let mut reader = SegmentReader::open(dir, 1).unwrap();
        assert_eq!(reader.segment_id(), 1);

        let r1 = reader.next_record().unwrap().unwrap();
        assert_eq!(r1.assigned_offset, 1);
        assert_eq!(r1.workspace_id, rec1.workspace_id);

        let r2 = reader.next_record().unwrap().unwrap();
        assert_eq!(r2.assigned_offset, 2);

        assert!(reader.next_record().unwrap().is_none());
    }

    #[test]
    fn test_segment_header_validation() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("bad.seg");

        // Write invalid magic
        std::fs::write(&path, b"XXXX\x01\x00\x00\x00\x00\x00\x00\x00\x01").unwrap();
        let err = SegmentReader::open_path(&path).unwrap_err();
        assert!(matches!(err, SegmentError::InvalidMagic));
    }

    #[test]
    fn test_segment_symlink() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path();

        update_current_symlink(dir, 5).unwrap();
        let link = dir.join("current.seg");
        assert!(link.exists() || link.symlink_metadata().is_ok());
    }

    #[test]
    fn test_list_segments() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path();

        SegmentWriter::create(dir, 3).unwrap();
        SegmentWriter::create(dir, 1).unwrap();
        SegmentWriter::create(dir, 7).unwrap();
        update_current_symlink(dir, 7).unwrap();

        let ids = list_segments(dir).unwrap();
        assert_eq!(ids, vec![1, 3, 7]);
    }

    #[test]
    fn test_torn_write_truncation() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path();

        let mut writer = SegmentWriter::create(dir, 1).unwrap();
        let rec = sample_record(1);
        writer.append(&rec.serialize()).unwrap();
        writer.fsync().unwrap();

        let valid_size = writer.size();

        // Append garbage (simulating torn write)
        writer.append(&[0xFF; 10]).unwrap();
        writer.fsync().unwrap();

        let mut reader = SegmentReader::open(dir, 1).unwrap();
        let r1 = reader.next_record().unwrap().unwrap();
        assert_eq!(r1.assigned_offset, 1);

        // Next read should produce an error
        let err = reader.next_record().unwrap_err();
        assert!(matches!(err, SegmentError::Record(_)));

        // Truncate at the last good position
        let trunc_point = reader.truncation_point();
        assert_eq!(trunc_point, valid_size);
        truncate_segment(dir, 1, trunc_point).unwrap();

        // Re-read should be clean
        let mut reader2 = SegmentReader::open(dir, 1).unwrap();
        reader2.next_record().unwrap().unwrap();
        assert!(reader2.next_record().unwrap().is_none());
    }
}
