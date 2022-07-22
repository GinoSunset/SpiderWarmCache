import argparse
import aiohttp
import asyncio
from copy import copy
from urllib.parse import urlparse, urljoin
from typing import Iterable, List, Set


from bs4 import BeautifulSoup


def args_parse():
    parser = argparse.ArgumentParser(description="Spider")
    parser.add_argument("-u", "--url", help="URL to start spider", required=True)
    parser.add_argument("-t", "--timeout", help="Timeout", default=10, type=int)
    parser.add_argument(
        "-np",
        "-–no-parent",
        help="Do not ever ascend to the parent directory when retrieving recursively.",
        action="store_true",
    )
    parser.add_argument(
        "--span-hosts",
        help="The option turns on host spanning, thus allowing  to visit any host referenced by a link. ",
        action="store_true",
        default=False,
    )
    parser.add_argument(
        "-nq",
        "--no-query-params",
        help="remove from url query params",
        action="store_true",
    )
    args = parser.parse_args()
    print(args)
    return args


class Spider:
    def __init__(
        self,
        url: str,
        timeout: int,
        span_hosts=False,
        no_parent=False,
        run_session=True,
        no_query_param=False,
    ):
        self.url = url
        self.aio_timeout = aiohttp.ClientTimeout(total=timeout)
        self.span_hosts = span_hosts
        self.session = None
        self.no_parent = no_parent
        self.no_query_param = no_query_param
        self.base_url = self.get_base_url(url)
        self.visited_urls = set()
        self.success_visited_urls = set()
        self.to_work_urls = set()
        self.filters = self.get_list_filters()

    def get_list_filters(self):
        # TODO: remove unnecessary filter
        filters = [
            self.normalize_relative_links,
            self.remove_query_params,
            self.remove_duplicates_links,
            self.filter_only_host_links,
            self.remove_not_parent_links,
            self.remove_visited_urls,
            self.remove_to_work_urls,
        ]
        return filters

    def get_base_url(self, url):
        return urlparse(url).netloc

    async def download_page(self, url: str):

        try:
            async with self.session.get(url) as response:
                page = await response.read()
        except asyncio.TimeoutError:
            print(f"[-] url {url} not available.")
        except Exception as e:
            print(f"[x] Other error {url}. Error: {e} ")
        else:
            print(f"[+] url {url} visited")
            self.success_visited_urls.add(url)
            return page
        finally:
            self.visited_urls.add(url)

    async def run_spider(self):
        print(f"Start warm {self.url} page")
        async with aiohttp.ClientSession(timeout=self.aio_timeout) as session:
            self.session = session
            await self.download_urls(self.url)
        print(f"Completed")
        print(f"Visited urls: {len(self.visited_urls)}")
        print(f"Success visited urls: {len(self.success_visited_urls)}")

    def filter_only_host_links(self, links: Iterable[str], *args) -> Iterable[str]:
        if self.span_hosts is False:
            return [link for link in links if self.get_base_url(link) == self.base_url]
        return links

    def normalize_relative_links(self, links: Iterable[str], url=None) -> List[str]:
        if url is None:
            url = self.url
        norm_links = []
        for link in links:
            if not self.get_base_url(link):
                link = urljoin(url, link)
            norm_links.append(link)
        return norm_links

    def remove_not_parent_links(self, links: Iterable[str], *args) -> List[str]:
        # TODO: add test
        if self.no_parent is True:
            return [link for link in links if self.url in link]
        return links

    def remove_visited_urls(self, links, *args):
        # TODO: add test
        return [link for link in links if link not in self.visited_urls]

    def remove_to_work_urls(self, links, *args):
        return [link for link in links if link not in self.to_work_urls]

    def filter_links(self, links, url):
        filter_links = copy(links)
        for filter_ in self.filters:
            filter_links = filter_(filter_links, url)
        return filter_links

    def remove_duplicates_links(self, links: Iterable[str], *args) -> Set[str]:
        return set(links)

    def remove_query_params(self, links, *args) -> List[str]:
        return [urljoin(url, urlparse(url).path) for url in links]

    async def download_urls(self, url):
        page = await self.download_page(url)
        if not page:
            return
        all_links = self.get_all_links_from_page(page)
        filtering_links = self.filter_links(all_links, url)
        tasks_subcategory = []
        for link in filtering_links:
            # TODO: add custom filters
            # TODO: add
            tasks_subcategory.append(asyncio.create_task(self.download_urls(link)))
        self.to_work_urls.update(filtering_links)
        await asyncio.gather(*tasks_subcategory)

    @staticmethod
    def get_all_links_from_page(page):
        soup = BeautifulSoup(page, "html.parser")
        all_links = []
        for link in soup.find_all("a"):
            if link.get("href"):
                all_links.append(link.get("href"))
        return all_links


if __name__ == "__main__":
    print("Start spider")
    args = args_parse()
    s = Spider(
        url=args.url,
        timeout=args.timeout,
        no_parent=args.np,
        span_hosts=args.span_hosts,
    )
    asyncio.run(s.run_spider())
