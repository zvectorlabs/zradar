//! DEPRECATED: Compression removed in favor of JSONB storage
//! PostgreSQL JSONB provides better queryability than compressed BYTEA
//! This module is kept for reference only
//! 
//! Gzip compression utilities for large text fields (legacy)

use anyhow::{Context, Result};
use flate2::write::{GzEncoder, GzDecoder};
use flate2::Compression;
use std::io::Write;

/// Compress text using gzip
/// 
/// # Performance
/// - Typical compression ratio: 5-10x for LLM prompts/completions
/// - Empty strings return empty Vec (no compression overhead)
pub fn compress_text(text: &str) -> Result<Vec<u8>> {
    if text.is_empty() {
        return Ok(Vec::new());
    }
    
    let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
    encoder
        .write_all(text.as_bytes())
        .context("Failed to write to gzip encoder")?;
    
    encoder
        .finish()
        .context("Failed to finalize gzip compression")
}

/// Decompress text from gzip
/// 
/// # Errors
/// - Returns error if data is corrupted or not valid gzip
pub fn decompress_text(data: &[u8]) -> Result<String> {
    if data.is_empty() {
        return Ok(String::new());
    }
    
    let mut decoder = GzDecoder::new(Vec::new());
    decoder
        .write_all(data)
        .context("Failed to write to gzip decoder")?;
    
    let decompressed = decoder
        .finish()
        .context("Failed to decompress gzip data")?;
    
    String::from_utf8(decompressed)
        .context("Decompressed data is not valid UTF-8")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compress_decompress_roundtrip() {
        let original = "This is a test prompt with some repetitive text. ".repeat(100);
        
        let compressed = compress_text(&original).unwrap();
        let decompressed = decompress_text(&compressed).unwrap();
        
        assert_eq!(original, decompressed);
        assert!(compressed.len() < original.len());
        
        // Should achieve decent compression on repetitive text
        let ratio = original.len() as f64 / compressed.len() as f64;
        assert!(ratio > 3.0, "Compression ratio should be > 3x, got {}", ratio);
    }

    #[test]
    fn test_empty_string() {
        let compressed = compress_text("").unwrap();
        assert!(compressed.is_empty());
        
        let decompressed = decompress_text(&compressed).unwrap();
        assert!(decompressed.is_empty());
    }

    #[test]
    fn test_typical_llm_response() {
        let llm_response = r#"{
            "model": "gpt-4",
            "choices": [{
                "message": {
                    "role": "assistant",
                    "content": "Here's a detailed explanation of the concept..."
                }
            }],
            "usage": {
                "prompt_tokens": 150,
                "completion_tokens": 500,
                "total_tokens": 650
            }
        }"#.repeat(10);
        
        let compressed = compress_text(&llm_response).unwrap();
        let ratio = llm_response.len() as f64 / compressed.len() as f64;
        
        println!("Original: {} bytes", llm_response.len());
        println!("Compressed: {} bytes", compressed.len());
        println!("Ratio: {:.2}x", ratio);
        
        assert!(ratio > 4.0);
    }
}

