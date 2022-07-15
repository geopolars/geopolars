from __future__ import annotations

from typing import TYPE_CHECKING, Union

import polars
from geopolars.internals import GeoDataFrame, GeoSeries
from polars import DataFrame, Series

if TYPE_CHECKING:  # pragma: no cover
    import pyarrow as pa


def from_arrow(
    a: Union[pa.Table, pa.Array, pa.ChunkedArray]
) -> Union[GeoDataFrame, GeoSeries]:
    """Convert from Arrow data to GeoSeries or GeoDataFrame"""
    # TODO: this should probably have a check that the data is indeed geographic? And return a bare
    # Series/DataFrame if it isn't?
    output = polars.from_arrow(a)
    if isinstance(output, Series):
        return GeoSeries(output)

    assert isinstance(
        output, DataFrame
    ), "Output of polars.from_arrow expected to be Series or DataFrame"
    return GeoDataFrame(output)
