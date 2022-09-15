from __future__ import annotations

import json
import sys
from pathlib import Path

from polars import read_ipc

from geopolars import GeoDataFrame

try:
    import pyarrow

    _PYARROW_AVAILABLE = True
except ImportError:
    _PYARROW_AVAILABLE = False

if sys.version_info >= (3, 8):
    from typing import Literal
else:
    from typing_extensions import Literal


__all__ = ["available", "get_path"]

HERE = Path(__file__).parent.resolve()
available = ("naturalearth_cities", "nybb", "naturalearth_lowres")

Available = Literal["naturalearth_cities", "nybb", "naturalearth_lowres"]


def get_path(dataset: Available) -> Path:
    """
    Get the path to the data file.

    Parameters
    ----------
    dataset : str
        The name of the dataset. See ``geopolars.datasets.available`` for
        all options.

    Examples
    --------
    >>> geopolars.datasets.get_path("naturalearth_lowres")  # doctest: +SKIP
    '.../python3.8/site-packages/geopolars/datasets/naturalearth_lowres/naturalearth_lowres.arrow'
    """
    if dataset in available:
        return HERE / (dataset + ".arrow")
    else:
        msg = f"The dataset '{dataset}' is not available. "
        msg += f"Available datasets are {', '.join(available)}"
        raise ValueError(msg)


def read_dataset(dataset: Available) -> GeoDataFrame:
    # TODO: this should probably go through the main from_arrow function in convert?
    # I.e. from_arrow would check if geoarrow metadata exists on the table?
    if not _PYARROW_AVAILABLE:
        raise ImportError("pyarrow required to read default dataset")

    path = get_path(dataset)
    df = read_ipc(path, memory_map=False)
    with pyarrow.ipc.open_file(path) as reader:
        geo_meta = json.loads(reader.schema.metadata[b"geo"])

    crs = geo_meta["columns"][geo_meta["primary_column"]]["crs"]
    return GeoDataFrame(df, crs=crs)
