# Contributing Docs

## Getting Started

- Rust Toolchain
- Install Poetry
- `poetry install`

## Building

The Python bindings use [Maturin](https://maturin.rs/) to build and package the Python and Rust code in this folder.

Maturin supports [PEP 517](https://peps.python.org/pep-0517/), so pip _should_ also work, but at this point I've only gotten `pip` to work on CI.

### Compile a development binary in your current environment

```
cd py-geopolars
python -m venv env
source ./env/bin/activate
pip install maturin
maturin develop
```

### Run

```
python example.py
```

### Compile a **release** build

`$ maturin build --release`

This will place a wheel for your local Python version + OS + Architecture into `./target/wheels/`.

## Docs

### Local Development

```bash
poetry run mkdocs serve
```

### Deploy

How to do with `mike`.
