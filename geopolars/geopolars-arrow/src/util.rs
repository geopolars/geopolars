use polars::datatypes::DataType;
use polars::export::arrow::array::Array;
use polars::export::num;
use polars::prelude::{ArrowDataType, Series};

pub enum GeoArrowType {
    Point,
    LineString,
    Polygon,
    WKB,
}

pub fn get_geoarrow_array_type(arr: &dyn Array) -> GeoArrowType {
    match arr.data_type() {
        ArrowDataType::Binary => GeoArrowType::WKB,
        ArrowDataType::Struct(_) => GeoArrowType::Point,
        ArrowDataType::List(dt) | ArrowDataType::LargeList(dt) => match dt.data_type() {
            ArrowDataType::Struct(_) => GeoArrowType::LineString,
            ArrowDataType::List(_) | ArrowDataType::LargeList(_) => GeoArrowType::Polygon,
            _ => panic!("Unexpected inner list type: {:?}", dt),
        },
        dt => panic!("Unexpected geoarrow type: {:?}", dt),
    }
}

pub fn get_geoarrow_type(series: &Series) -> GeoArrowType {
    match series.dtype() {
        DataType::Binary => GeoArrowType::WKB,
        DataType::Struct(_) => GeoArrowType::Point,
        DataType::List(dt) => match *dt.clone() {
            DataType::Struct(_) => GeoArrowType::LineString,
            DataType::List(_) => GeoArrowType::Polygon,
            _ => panic!("Unexpected inner list type: {}", dt),
        },

        dt => panic!("Unexpected geoarrow type: {}", dt),
    }
}

/// Get the index of the chunk and the index of the value in that chunk
// From: https://github.com/pola-rs/polars/blob/f8bb5aaa9bb8f8c3c9365933a062758478fb63ad/polars/polars-core/src/chunked_array/ops/downcast.rs#L76-L83
#[inline]
pub(crate) fn index_to_chunked_index(series: &Series, index: usize) -> (usize, usize) {
    if series.chunks().len() == 1 {
        return (0, index);
    }

    _index_to_chunked_index(series.chunk_lengths(), index)
}

/// This logic is same as the impl on ChunkedArray
/// The difference is that there is less indirection because the caller should preallocate
/// `chunk_lens` once. On the `ChunkedArray` we indirect through an `ArrayRef` which is an indirection
/// and a vtable.
// From: https://github.com/pola-rs/polars/blob/f8bb5aaa9bb8f8c3c9365933a062758478fb63ad/polars/polars-core/src/utils/mod.rs#L822-L846
#[inline]
pub(crate) fn _index_to_chunked_index<
    I: Iterator<Item = Idx>,
    Idx: PartialOrd + std::ops::AddAssign + std::ops::SubAssign + num::Zero + num::One,
>(
    chunk_lens: I,
    index: Idx,
) -> (Idx, Idx) {
    let mut index_remainder = index;
    let mut current_chunk_idx = num::Zero::zero();

    for chunk_len in chunk_lens {
        if chunk_len > index_remainder {
            break;
        } else {
            index_remainder -= chunk_len;
            current_chunk_idx += num::One::one();
        }
    }
    (current_chunk_idx, index_remainder)
}
