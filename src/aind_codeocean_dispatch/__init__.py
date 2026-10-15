"""Launch, monitor, and capture batches of Code Ocean capsule runs."""

from importlib.metadata import PackageNotFoundError, version

try:
    __version__ = version("aind-codeocean-dispatch")
except PackageNotFoundError:
    __version__ = "0.0.0.dev0"
