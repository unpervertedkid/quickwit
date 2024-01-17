// Copyright (C) 2024 Quickwit, Inc.
//
// Quickwit is offered under the AGPL v3.0 and as commercial software.
// For commercial licensing, contact us at hello@quickwit.io.
//
// AGPL:
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as
// published by the Free Software Foundation, either version 3 of the
// License, or (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program. If not, see <http://www.gnu.org/licenses/>.

use brotli::Decompressor;
use flate2::read::GzDecoder;
use std::io::Read;
use zstd::stream::read::Decoder;
use warp::{Filter,Rejection};
use warp::reject::Reject;

#[derive(Debug)]
struct UnsupportedCompressionAlgorithm;

impl Reject for UnsupportedCompressionAlgorithm {}

// Supported compression algorithms.
enum CompressionAlgorithm {
    Gzip,
    Brotli,
    Zstd,
}

// Helper function to decompress data based on the compression algorithm.
async fn decompress_body(
    algorithm: CompressionAlgorithm,
    body: &[u8],
) -> Result<Vec<u8>, Rejection> {
    match algorithm {
        CompressionAlgorithm::Gzip => {
            let mut d = GzDecoder::new(body);
            let mut decompressed_body = Vec::new();
            d.read_to_end(&mut decompressed_body)
                .map_err(|_| warp::reject())?;
            Ok(decompressed_body)
        }
        CompressionAlgorithm::Brotli => {
            let mut decompressor = Decompressor::new(body, 4096 /* buffer size */);
            let mut decompressed_body = Vec::new();
            decompressor
                .read_to_end(&mut decompressed_body)
                .map_err(|_| warp::reject())?;
            Ok(decompressed_body)
        }
        CompressionAlgorithm::Zstd => {
            let mut decoder = Decoder::new(body).map_err(|_| warp::reject())?;
            let mut decompressed_body = Vec::new();
            decoder
                .read_to_end(&mut decompressed_body)
                .map_err(|_| warp::reject())?;
            Ok(decompressed_body)
        }
    }
}

// Decompression filter.
pub(crate) fn decompress() -> impl Filter<Extract = (Vec<u8>,), Error = Rejection> + Clone {
    warp::header::optional::<String>("content-encoding")
        .and(warp::body::bytes())
        .and_then(
            |encoding: Option<String>,
             body: bytes::Bytes| async move {
                match encoding.as_deref() {
                    Some("gzip") => decompress_body(CompressionAlgorithm::Gzip, &body).await,
                    Some("br") => decompress_body(CompressionAlgorithm::Brotli, &body).await,
                    Some("zstd") => decompress_body(CompressionAlgorithm::Zstd, &body).await,
                    None => Ok(body.to_vec()), // Pass through for uncompressed bodies
                    _ => Err(warp::reject::custom(UnsupportedCompressionAlgorithm)),
                }
            },
        )
}