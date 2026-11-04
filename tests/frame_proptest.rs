//! Property tests for the record frame codec.

use bytes::Bytes;
use ledgerlog::record::{decode, decode_all, encode_parts, Decoded, Record};
use proptest::prelude::*;

const MAX: usize = 1 << 20;

fn arb_record() -> impl Strategy<Value = Record> {
    (
        any::<u64>(),
        any::<i64>(),
        proptest::option::of(proptest::collection::vec(any::<u8>(), 0..64)),
        proptest::collection::vec(any::<u8>(), 0..512),
    )
        .prop_map(|(offset, timestamp, key, value)| Record {
            offset,
            timestamp,
            key: key.map(Bytes::from),
            value: Bytes::from(value),
        })
}

proptest! {
    #[test]
    fn encode_decode_roundtrip(r in arb_record()) {
        let bytes = r.encode();
        prop_assert_eq!(bytes.len(), r.encoded_len());
        prop_assert_eq!(decode(&bytes, MAX).unwrap(), Decoded::Record(r, bytes.len()));
    }

    #[test]
    fn encode_parts_matches_record_encode(r in arb_record()) {
        let mut buf = Vec::new();
        encode_parts(&mut buf, r.offset, r.timestamp, r.key.as_deref(), &r.value);
        prop_assert_eq!(buf, r.encode());
    }

    #[test]
    fn concatenated_frames_decode_in_order(rs in proptest::collection::vec(arb_record(), 0..20)) {
        let mut buf = Vec::new();
        for r in &rs {
            r.encode_into(&mut buf);
        }
        prop_assert_eq!(decode_all(&buf, MAX).unwrap(), rs);
    }

    #[test]
    fn every_strict_prefix_is_incomplete(r in arb_record(), frac in 0.0f64..1.0) {
        let bytes = r.encode();
        let cut = ((bytes.len() as f64) * frac) as usize;
        prop_assert_eq!(decode(&bytes[..cut], MAX).unwrap(), Decoded::Incomplete);
    }

    #[test]
    fn any_single_byte_corruption_is_detected(
        r in arb_record(),
        idx in any::<prop::sample::Index>(),
        flip in 1u8..=255,
    ) {
        let mut bytes = r.encode();
        let i = idx.index(bytes.len());
        bytes[i] ^= flip;
        // A damaged frame must never decode back into a valid record.
        match decode(&bytes, MAX) {
            Ok(Decoded::Record(..)) => prop_assert!(false, "corruption at byte {} went undetected", i),
            Ok(Decoded::Incomplete) | Err(_) => {}
        }
    }

    #[test]
    fn decoding_garbage_never_panics(bytes in proptest::collection::vec(any::<u8>(), 0..256)) {
        let _ = decode(&bytes, MAX);
        let _ = decode_all(&bytes, MAX);
    }
}
