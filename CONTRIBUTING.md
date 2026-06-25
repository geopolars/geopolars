# Contributing to GeoPolars

Thanks for your interest in contributing! This document covers everything you need to get the Rust workspace building and running tests locally.

## Project status

GeoPolars was unblocked in November 2025 when Polars added support for [Arrow extension types](https://github.com/pola-rs/polars/pull/25322). Development is now active but early-stage. See [issue #245](https://github.com/pola-rs/geopolars/issues/245) for the current roadmap and the best place to coordinate with the maintainer before starting large pieces of work.

> **Note:** The `py-geopolars/` directory contains the old Python bindings from the pre-2025 prototype. It is not the focus of active development and is kept for reference only.

## Prerequisites

- **Rust ≥ 1.85** (edition 2024 is required). Install or update via [rustup](https://rustup.rs/):

  ```bash
  rustup update stable
  ```

- **rustfmt** and **clippy** (bundled with the standard toolchain):

  ```bash
  rustup component add rustfmt clippy
  ```

No other system dependencies are required — the workspace uses pure Rust crates and fetches Polars directly from its git repository.

## Workspace layout

```
geopolars/
├── geopolars/
│   ├── geopolars-arrow/      # Converts between geoarrow-rs (arrow-rs) and Polars (polars-arrow) via Arrow C FFI
│   └── geopolars-extension/  # Registers GeoArrow geometry types in Polars' extension type registry
└── py-geopolars/             # Old Python bindings (not actively developed)
```

### geopolars-arrow

Provides the bridge between the two Arrow implementations in the crate graph:

- [`geoarrow-rs`](https://github.com/geoarrow/geoarrow-rs) uses [`arrow-rs`](https://github.com/apache/arrow-rs)
- Polars uses its own fork, `polars-arrow`

Both implement the [Arrow C Data Interface](https://arrow.apache.org/docs/format/CDataInterface.html), so conversion goes through FFI (`transmute` across matching struct layouts). Key functions: `geoarrow_array_to_polars`, `polars_array_to_geoarrow`, `arrow_field_to_polars`, `polars_field_to_arrow`.

### geopolars-extension

Wraps each GeoArrow type (`PointType`, `LineStringType`, …, `WkbType`, `WktType`) as a Polars [`ExtensionTypeImpl`](https://docs.rs/polars-core/latest/polars_core/datatypes/extension/trait.ExtensionTypeImpl.html) and provides a factory for dynamic creation. Call `register_all_extensions()` once at startup to make Polars aware of all GeoArrow extension types.

## Building

```bash
cargo build
```

The first build fetches Polars from its git repository (pinned to a specific commit in `Cargo.toml`) and compiles it — this takes a few minutes. Subsequent builds are fast thanks to incremental compilation.

## Testing

```bash
cargo test
```

## Linting

```bash
cargo clippy       # must pass with no warnings
cargo fmt --check  # check formatting without modifying files
cargo fmt          # auto-format
```

CI runs `clippy --all -- -D warnings` and `fmt -- --check`, so fix warnings before opening a PR.

## Making a contribution

1. **Find or open an issue.** Check [open issues](https://github.com/pola-rs/geopolars/issues) for good starting points (especially those labelled `good first issue`). For larger work, comment on [#245](https://github.com/pola-rs/geopolars/issues/245) first to coordinate with the maintainer.

2. **Fork and branch.**

   ```bash
   gh repo fork pola-rs/geopolars --clone
   cd geopolars
   git checkout -b feat/your-feature
   ```

3. **Make your changes.** Keep commits focused. Add tests for new behaviour.

4. **Check before pushing.**

   ```bash
   cargo test
   cargo clippy --all -- -D warnings
   cargo fmt --check
   ```

5. **Open a pull request** against `pola-rs/geopolars:main`.

## Key concepts

### GeoArrow

[GeoArrow](https://github.com/geoarrow/geoarrow) is a specification for storing geospatial vector data in Apache Arrow memory. Each geometry type (Point, LineString, …) maps to a specific Arrow physical layout (e.g. a Point is a `FixedSizeList<2, Float64>` for interleaved XY coordinates). CRS and other metadata travel as Arrow extension type metadata.

GeoPolars uses [`geoarrow-rs`](https://crates.io/crates/geoarrow-array) as its GeoArrow implementation.

### Arrow extension types

Polars supports Arrow extension types as of late 2025. An extension type is an Arrow type with a `ARROW:extension:name` metadata key attached to a field; Polars uses a registry (`register_extension_type`) to map names to concrete Rust types that implement `ExtensionTypeImpl`. `geopolars-extension` registers all eleven GeoArrow types into that registry.

### Coordinate types and dimensions

GeoArrow types carry two layout parameters:
- **`CoordType`**: `Interleaved` (coordinates packed as `XYXYXY`) or `Separated` (separate `X`, `Y` buffers in a struct)
- **`Dimension`**: `XY`, `XYZ`, `XYM`, or `XYZM`

These determine the physical Arrow storage type and are preserved through the `geopolars-arrow` FFI bridge.
