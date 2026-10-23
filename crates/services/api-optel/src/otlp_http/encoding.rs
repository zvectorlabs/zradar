//! `Content-Encoding` handling for OTLP/HTTP request bodies.
//!
//! Exporters may compress request bodies with `gzip` (OTLP specification) or
//! `zstd` (common in collectors). The decoded size is capped: decompression
//! stops as soon as the output passes the limit and the request gets
//! `413 Payload Too Large`, so a small compressed body cannot expand into an
//! unbounded allocation ("decompression bomb"). Undecodable bodies get
//! `400 Bad Request`; other encodings `415 Unsupported Media Type`.

use std::io::Read;

use axum::body::Bytes;
use axum::http::{HeaderMap, StatusCode, header};

/// Supported request body encodings.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ContentEncoding {
    Identity,
    Gzip,
    Zstd,
}

impl ContentEncoding {
    /// Parse `Content-Encoding`. Absent or `identity` means uncompressed.
    /// Several encodings (stacked or repeated headers) are not supported.
    pub(crate) fn from_headers(headers: &HeaderMap) -> Result<Self, StatusCode> {
        let mut values = headers.get_all(header::CONTENT_ENCODING).iter();
        let Some(value) = values.next() else {
            return Ok(Self::Identity);
        };
        if values.next().is_some() {
            return Err(StatusCode::UNSUPPORTED_MEDIA_TYPE);
        }
        let value = value
            .to_str()
            .map_err(|_| StatusCode::UNSUPPORTED_MEDIA_TYPE)?
            .trim();
        if value.is_empty() || value.eq_ignore_ascii_case("identity") {
            Ok(Self::Identity)
        } else if value.eq_ignore_ascii_case("gzip") || value.eq_ignore_ascii_case("x-gzip") {
            Ok(Self::Gzip)
        } else if value.eq_ignore_ascii_case("zstd") {
            Ok(Self::Zstd)
        } else {
            Err(StatusCode::UNSUPPORTED_MEDIA_TYPE)
        }
    }
}

/// Decode `body` to at most `limit` bytes. Decompression runs on the blocking
/// pool: it is CPU-bound.
pub(crate) async fn decode_body(
    encoding: ContentEncoding,
    body: Bytes,
    limit: usize,
) -> Result<Bytes, StatusCode> {
    if encoding == ContentEncoding::Identity {
        return if body.len() > limit {
            Err(StatusCode::PAYLOAD_TOO_LARGE)
        } else {
            Ok(body)
        };
    }
    tokio::task::spawn_blocking(move || decompress(encoding, &body, limit))
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .map(Bytes::from)
}

/// Smallest zstd window accepted regardless of `limit`: streaming zstd
/// encoders commonly use an 8 MiB window even for small payloads.
const MIN_ZSTD_WINDOW_LOG: u32 = 23;
/// Largest zstd window accepted (zstd's own default decoder limit, 128 MiB).
const MAX_ZSTD_WINDOW_LOG: u32 = 27;

fn decompress(
    encoding: ContentEncoding,
    input: &[u8],
    limit: usize,
) -> Result<Vec<u8>, StatusCode> {
    let reader: Box<dyn Read + '_> = match encoding {
        ContentEncoding::Identity => Box::new(input),
        ContentEncoding::Gzip => Box::new(flate2::read::MultiGzDecoder::new(input)),
        ContentEncoding::Zstd => {
            let mut decoder =
                zstd::stream::read::Decoder::with_buffer(input).map_err(|_| bad_request())?;
            // The window is allocated up front from the frame header; bound it
            // so a crafted header cannot claim a huge buffer.
            decoder
                .window_log_max(zstd_window_log_max(limit))
                .map_err(|_| bad_request())?;
            Box::new(decoder)
        }
    };

    // Read one byte past the limit to tell "exactly at the limit" from "over".
    let mut out = Vec::with_capacity(input.len().saturating_mul(4).min(limit));
    reader
        .take(limit as u64 + 1)
        .read_to_end(&mut out)
        .map_err(|_| bad_request())?;
    if out.len() > limit {
        return Err(StatusCode::PAYLOAD_TOO_LARGE);
    }
    Ok(out)
}

fn bad_request() -> StatusCode {
    StatusCode::BAD_REQUEST
}

fn zstd_window_log_max(limit: usize) -> u32 {
    let needed = usize::BITS - limit.saturating_sub(1).leading_zeros();
    needed.clamp(MIN_ZSTD_WINDOW_LOG, MAX_ZSTD_WINDOW_LOG)
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use axum::http::HeaderValue;
    use std::io::Write;

    pub(crate) fn gzip(data: &[u8]) -> Vec<u8> {
        let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::fast());
        encoder.write_all(data).unwrap();
        encoder.finish().unwrap()
    }

    pub(crate) fn zstd(data: &[u8]) -> Vec<u8> {
        zstd::stream::encode_all(data, 3).unwrap()
    }

    fn headers(values: &[&'static str]) -> HeaderMap {
        let mut map = HeaderMap::new();
        for value in values {
            map.append(header::CONTENT_ENCODING, HeaderValue::from_static(value));
        }
        map
    }

    #[test]
    fn parses_supported_encodings() {
        assert_eq!(
            ContentEncoding::from_headers(&headers(&[])),
            Ok(ContentEncoding::Identity)
        );
        assert_eq!(
            ContentEncoding::from_headers(&headers(&["identity"])),
            Ok(ContentEncoding::Identity)
        );
        assert_eq!(
            ContentEncoding::from_headers(&headers(&["GZIP"])),
            Ok(ContentEncoding::Gzip)
        );
        assert_eq!(
            ContentEncoding::from_headers(&headers(&["x-gzip"])),
            Ok(ContentEncoding::Gzip)
        );
        assert_eq!(
            ContentEncoding::from_headers(&headers(&[" zstd "])),
            Ok(ContentEncoding::Zstd)
        );
    }

    #[test]
    fn rejects_unsupported_or_stacked_encodings() {
        for case in [
            headers(&["br"]),
            headers(&["deflate"]),
            headers(&["gzip, zstd"]),
            headers(&["gzip", "gzip"]),
        ] {
            assert_eq!(
                ContentEncoding::from_headers(&case),
                Err(StatusCode::UNSUPPORTED_MEDIA_TYPE)
            );
        }
    }

    #[tokio::test]
    async fn round_trips_within_the_limit() {
        let data = b"otlp payload ".repeat(100);
        for (encoding, body) in [
            (ContentEncoding::Identity, data.clone()),
            (ContentEncoding::Gzip, gzip(&data)),
            (ContentEncoding::Zstd, zstd(&data)),
        ] {
            let decoded = decode_body(encoding, Bytes::from(body), data.len())
                .await
                .unwrap();
            assert_eq!(decoded, data, "{encoding:?}");
        }
    }

    #[tokio::test]
    async fn decompression_bombs_stop_at_the_limit() {
        // 64 MiB of zeros compresses to a few tens of KiB.
        let bomb = vec![0u8; 64 * 1024 * 1024];
        let limit = 1024 * 1024;
        for (encoding, body) in [
            (ContentEncoding::Gzip, gzip(&bomb)),
            (ContentEncoding::Zstd, zstd(&bomb)),
        ] {
            assert!(body.len() < limit, "{encoding:?} bomb is small on the wire");
            assert_eq!(
                decode_body(encoding, Bytes::from(body), limit).await,
                Err(StatusCode::PAYLOAD_TOO_LARGE),
                "{encoding:?}"
            );
        }
        // Uncompressed bodies are bounded by the same limit.
        assert_eq!(
            decode_body(
                ContentEncoding::Identity,
                Bytes::from(vec![0u8; limit + 1]),
                limit
            )
            .await,
            Err(StatusCode::PAYLOAD_TOO_LARGE)
        );
    }

    #[tokio::test]
    async fn concatenated_gzip_members_count_toward_the_limit() {
        let mut body = gzip(&[1u8; 600]);
        body.extend(gzip(&[2u8; 600]));
        assert_eq!(
            decode_body(ContentEncoding::Gzip, Bytes::from(body.clone()), 1_000).await,
            Err(StatusCode::PAYLOAD_TOO_LARGE)
        );
        assert_eq!(
            decode_body(ContentEncoding::Gzip, Bytes::from(body), 1_200)
                .await
                .unwrap()
                .len(),
            1_200
        );
    }

    #[tokio::test]
    async fn exactly_at_the_limit_is_accepted() {
        let data = vec![7u8; 4096];
        let decoded = decode_body(ContentEncoding::Gzip, Bytes::from(gzip(&data)), 4096)
            .await
            .unwrap();
        assert_eq!(decoded.len(), 4096);
    }

    #[tokio::test]
    async fn corrupt_bodies_are_bad_requests() {
        for encoding in [ContentEncoding::Gzip, ContentEncoding::Zstd] {
            assert_eq!(
                decode_body(encoding, Bytes::from_static(b"not compressed"), 1024).await,
                Err(StatusCode::BAD_REQUEST),
                "{encoding:?}"
            );
        }
    }

    #[test]
    fn zstd_window_is_bounded() {
        assert_eq!(zstd_window_log_max(1), MIN_ZSTD_WINDOW_LOG);
        assert_eq!(zstd_window_log_max(8 * 1024 * 1024), 23);
        assert_eq!(zstd_window_log_max(8 * 1024 * 1024 + 1), 24);
        assert_eq!(zstd_window_log_max(usize::MAX), MAX_ZSTD_WINDOW_LOG);
    }

    #[tokio::test]
    async fn zstd_frames_claiming_an_oversized_window_are_rejected() {
        // A frame encoded with a 256 MiB window (window log 28) must not make
        // the decoder allocate it.
        let mut encoder = zstd::stream::write::Encoder::new(Vec::new(), 1).unwrap();
        encoder.long_distance_matching(true).unwrap();
        encoder.window_log(28).unwrap();
        encoder.write_all(&[3u8; 1024]).unwrap();
        let body = encoder.finish().unwrap();
        assert_eq!(
            decode_body(ContentEncoding::Zstd, Bytes::from(body), 8 * 1024 * 1024).await,
            Err(StatusCode::BAD_REQUEST)
        );
    }
}
