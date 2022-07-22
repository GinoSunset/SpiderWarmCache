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


def test_remove_duplication_links(sp: Spider):
    links = {
        "https://www.google.com/media/",
        "https://www.google.com/media/",
    }
    assert set(sp.remove_duplicates_links(links)) == {
        "https://www.google.com/media/",
    }


def test_remove_query_param(sp: Spider):
    links = {
        "https://www.google.com/?q=1",
        "https://www.google.com/?q=1&w=2",
        "https://www.google.com/media/?q=1&w=2",
        "https://www.google.com/media/?q=1&w=2#1",
    }
    assert set(sp.remove_query_params(links)) == {
        "https://www.google.com/",
        "https://www.google.com/",
        "https://www.google.com/media/",
        "https://www.google.com/media/",
    }
