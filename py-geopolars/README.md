# Compile Custom Rust functions and use in python polars

## Compile a development binary in your current environment

```
virtualenv env
source ./env/bin/activate
pip install maturin
maturin develop
```

## Run

```
python example.py
```

## Compile a **release** build

`$ maturin develop --release`
