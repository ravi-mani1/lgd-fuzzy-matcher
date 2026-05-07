"""Tests for Streamlit authentication helpers."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from app_auth import _hostname_from_host_header, _is_localhost_host


def test_hostname_from_host_header():
    assert _hostname_from_host_header("localhost:8501") == "localhost"
    assert _hostname_from_host_header("127.0.0.1:8501") == "127.0.0.1"
    assert _hostname_from_host_header("[::1]:8501") == "::1"


def test_is_localhost_host():
    assert _is_localhost_host("localhost:8501") is True
    assert _is_localhost_host("127.0.0.1:8501") is True
    assert _is_localhost_host("[::1]:8501") is True
    assert _is_localhost_host("example.com") is False
