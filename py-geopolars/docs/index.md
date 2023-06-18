# GeoPolars

GeoPolars extends the [Polars](https://github.com/pola-rs/polars) DataFrame library for use with geospatial data.

## Description

With heavy inspiration from [`GeoPandas`](https://geopandas.org/), GeoPolars has two main goals:

- Faster multithreaded operations than GeoPandas while keeping an easy-to-use, high-level interface.
- Better data interoperability without copies, due to its [`GeoArrow`](https://github.com/geoarrow/geoarrow) core.

At this point, GeoPolars is a **prototype** and should not be considered production-ready.

## Installation

GeoPolars is alpha software but can be installed from PyPI:

```bash
pip install --pre geopolars
```

(`-pre` is necessary to allow installation of an alpha release). The publishing processs includes
binary wheels for many platforms, so it should be easy to install, without needing to compile the
underlying Rust code from source.
