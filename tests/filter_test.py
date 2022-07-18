import pytest
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from spider import Spider


@pytest.fixture
def sp():
    return Spider("https://www.google.com", 10)


def test_filter_only_host_links(sp):
    links = {
        "https://www.google.com",
        "https://www.bla.com/",
        "https://www.google.com/search",
    }
    assert set(sp.filter_only_host_links(links)) == {
        "https://www.google.com",
        "https://www.google.com/search",
    }


def test_normalize_relative_links(sp: Spider):
    links = {
        "https://www.google.com/media/",
        "/search",
    }
    assert set(sp.normalize_relative_links(links)) == {
        "https://www.google.com/media/",
        "https://www.google.com/search",
    }
