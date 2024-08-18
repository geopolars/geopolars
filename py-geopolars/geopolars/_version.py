from importlib.metadata import PackageNotFoundError, version

try:
    __version__ = version("geopolars")
except PackageNotFoundError:
    __version__ = "uninstalled"
