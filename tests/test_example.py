"""Example test module."""

import aind_codeocean_dispatch


def test_version():
    """Test that version is defined."""
    assert aind_codeocean_dispatch.__version__ is not None
    assert isinstance(aind_codeocean_dispatch.__version__, str)
