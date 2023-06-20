# Contributing Docs

Thanks for considering contributing to GeoPolars!

This document will help you set up your local environment.

## Getting Started

- Rust Toolchain
- Install Python
- Install Poetry
- Run `poetry install` from the `py-geopolars/` directory.

## Building

### Development

To build for development just run:

```bash
poetry run poe develop
```

Under the hood this will:

- Use [Maturin](https://maturin.rs/) to build and package the Python and Rust code in this folder into a Python wheel.
- Install this Python wheel into the virtual environment managed by Poetry (`.venv/` in this folder).

Then open up IPython with

```bash
poetry run ipython
```

Inside IPython, `import geopolars` should work.

Note that this will compile the Rust binary with `--debug`, which is faster to compile but much, much slower at runtime. If you wish to develop with a release build, run

```bash
poetry run poe develop-release
```

### Release

To build a wheel for releasing, run:

```bash
poetry run poe build
```

This will put a wheel (for the current system's architecture) in `dist/`.

## Docs

### Local Development

```bash
poetry run mkdocs serve
```

I'm not sure why but I couldn't get `mike` serve to work.

### Deploy

We use [`mike`](https://github.com/jimporter/mike) for documentation deployment so that we can have multiple versions of the docs deployed simultaneously:

```bash
poetry run mike deploy --push --no-redirect VERSION latest
```

```bash
poetry run mike deploy --push --no-redirect 0.1.0-alpha.4 latest
```
