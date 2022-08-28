from pathlib import Path

from polars import read_ipc

from geopolars import GeoDataFrame

__all__ = ["available", "get_path"]

HERE = Path(__file__).parent.resolve()
available = ("naturalearth_cities", "nybb", "naturalearth_lowres")


def get_path(dataset: str) -> Path:
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


def read_dataset(dataset: str):
    path = get_path(dataset)
    df = read_ipc(path, memory_map=False)
    return GeoDataFrame(df)
