# GeoPolars

<!-- Layout copied from rio-tiler -->
<!-- https://github.com/cogeotiff/rio-tiler/blob/c6b097aa5b6f1bae5231d17db7d595a0bb2a7b26/README.md -->
<p align="center">
  <img src="static/logo.svg" height="300px" alt="geopolars"></a>
</p>
<p align="center">
  <em>
    Geospatial DataFrames for Rust and Python
  </em>
</p>
<p align="center">
  <a href="https://github.com/geopolars/geopolars/actions?query=workflow%3ARust" target="_blank">
      <img src="https://github.com/geopolars/geopolars/workflows/Rust/badge.svg" alt="Test">
  </a>
  <!-- <a href="https://codecov.io/gh/cogeotiff/rio-tiler" target="_blank">
      <img src="https://codecov.io/gh/cogeotiff/rio-tiler/branch/master/graph/badge.svg" alt="Coverage">
  </a> -->
  <a href="https://pypi.org/project/geopolars" target="_blank">
      <img src="https://img.shields.io/pypi/v/geopolars?color=%2334D058&label=PyPI%20version" alt="PyPI Package version">
  </a>
  <!-- <a href="https://anaconda.org/conda-forge/rio-tiler" target="_blank">
      <img src="https://img.shields.io/conda/v/conda-forge/rio-tiler.svg" alt="Conda Forge">
  </a> -->
  <!-- <a href="https://pypistats.org/packages/rio-tiler" target="_blank">
      <img src="https://img.shields.io/pypi/dm/rio-tiler.svg" alt="Downloads">
  </a> -->
  <a href="https://github.com/geopolars/geopolars/blob/master/LICENSE" target="_blank">
      <img src="https://img.shields.io/github/license/geopolars/geopolars.svg" alt="Downloads">
  </a>
  <!-- <a href="https://mybinder.org/v2/gh/cogeotiff/rio-tiler/master?filepath=docs%2Fexamples%2F" target="_blank" alt="Binder">
      <img src="https://mybinder.org/badge_logo.svg" alt="Binder">
  </a> -->
</p>

## Summary

GeoPolars extends the [Polars][polars] DataFrame library for use with geospatial data.

- Uses [GeoArrow][geo-arrow-spec] as the internal memory model.
- Written in Rust
- Bindings to Python (and WebAssembly in the future)
- Multithreading capable

At this point, GeoPolars is a **prototype** and should not be considered production-ready.

## Use from..

### Rust

GeoPolars is [published to crates.io](https://crates.io/crates/geopolars) under the name `geopolars`.

Documentation is available at [docs.rs/geopolars](https://docs.rs/geopolars).

### Python

An early alpha (`v0.1.0-alpha.4`) is published to PyPI:

```
pip install --pre geopolars
```

The publishing processs includes binary wheels for many platforms, so it should be easy to install, without needing to compile the underlying Rust code from source.

### WebAssembly

Polars itself does not yet exist in WebAssembly, though there has been discussion about adding bindings for it. The long-term goal of GeoPolars is to have a WebAssembly API as well.

## Comparison with GeoPandas

Imitation is the sincerest form of flattery! GeoPandas — and its underlying libraries of `shapely` and `GEOS` — is an incredible production-ready tool.

GeoPolars is nowhere near the functionality or stability of GeoPandas, but competition is good and, due to its pure-Rust core, GeoPolars will be much easier to use in WebAssembly.

## Future work

The biggest pieces of future work are:

- Store geometries in the efficient Arrow-native format, rather than as WKB buffers (as the prototype currently does). This is blocked on Polars, which doesn't currently support Arrow `FixedSizeList` data types, but they've recently [expressed openness](https://github.com/pola-rs/polars/issues/4014#issuecomment-1212376538) to adding minimal `FixedSizeList` support.
- Enable `georust/geo` algorithms to access Arrow data with zero copy. The prototype currently copies WKB geometries into `geo` structs on each geometry operation, which is expensive.

  This is blocked on adding support to the `geo` library for geometry access traits, which is a large undertaking. See [georust/geo/discussions/838](https://github.com/georust/geo/discussions/838). I've started exploration on this

- Implement GeoArrow extension types for seamless handling of CRS metadata in Rust, rather than in the Python wrapper.

[polars]: https://github.com/pola-rs/polars
[geo-arrow-spec]: https://github.com/geoarrow/geoarrow
