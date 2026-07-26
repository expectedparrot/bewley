from importlib.metadata import PackageNotFoundError, version as _package_version

__all__ = ["__version__"]

try:
    __version__ = _package_version("bewley")
except PackageNotFoundError:  # running from a source tree without installation
    __version__ = "0.4.0"
