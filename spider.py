import argparse
import aiohttp
import asyncio
import time
from copy import copy
from urllib.parse import urlparse, urljoin
from typing import Iterable, List, Set


from bs4 import BeautifulSoup

TIMES_DICT = {}


class Profiler(aiohttp.TraceConfig):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.on_request_start.append(on_request_start)
        self.on_request_end.append(on_request_end)
        self.on_connection_queued_start.append(on_connection_queued_start)
        self.on_connection_queued_end.append(on_connection_queued_end)


def args_parse():
    parser = argparse.ArgumentParser(description="Spider")
    parser.add_argument("-u", "--url", help="URL to start spider", required=True)
    parser.add_argument(
        "-t", "--timeout", help="Timeout. By default [10]", default=10, type=int
    )
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
    parser.add_argument(
        "-sv",
        "--ssl-verify",
        help="By default ssl not verify",
        action="store_true",
    )
    args = parser.parse_args()
    print(args)
    return args


async def on_request_start(session, trace_config_ctx, params):
    trace_config_ctx.start = asyncio.get_event_loop().time()
    trace_config_ctx.url = params.url


async def on_request_end(session, trace_config_ctx, params):
    elapsed_time = asyncio.get_event_loop().time() - trace_config_ctx.start
    # TODO: add params to disable time
    TIMES_DICT[str(params.url)] = elapsed_time


async def on_connection_queued_start(session, trace_config_ctx, params):
    trace_config_ctx.start_qu = asyncio.get_event_loop().time()


async def on_connection_queued_end(session, trace_config_ctx, params):
    elapsed_time = asyncio.get_event_loop().time() - trace_config_ctx.start_qu
    # TODO: add params to disable time
    TIMES_DICT[str(trace_config_ctx.url)] = elapsed_time


class Spider:
    def __init__(
        self,
        url: str,
        timeout: int,
        span_hosts=False,
        no_parent=False,
        run_session=True,
        no_query_param=False,
        ssl_verify=False,
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
        self.ssl_verify = ssl_verify

    def get_list_filters(self):
        filters = [
            self.normalize_relative_links,
            self.remove_query_params,
            self.remove_duplicates_links,
            self.filter_only_host_links,
            self.remove_not_parent_links,
            self.remove_visited_urls,
            self.remove_to_work_urls,
        ]
        if self.no_query_param is False:
            filters.remove(self.remove_query_params)
        if self.no_parent is False:
            filters.remove(self.remove_not_parent_links)
        if self.span_hosts is False:
            filters.remove(self.filter_only_host_links)
        return filters

    def get_base_url(self, url):
        return urlparse(url).netloc

    async def download_page(self, url: str, session=None):
        if session is None:
            session = self.session
        try:
            async with session.get(url) as response:
                page = await response.read()
        except asyncio.TimeoutError:
            print(f"[-] url {url} not available.")
        except Exception as e:
            print(f"[x] Other error {url}. Error: {e} ")
        else:
            print(f"[+][{TIMES_DICT.get(url,0.0):7.3f}] url {url} visited ")
            self.success_visited_urls.add(url)
            return page
        finally:
            self.visited_urls.add(url)

    async def run_spider(self):
        print(f"Start warm {self.url} page")
        async with aiohttp.ClientSession(
            connector=aiohttp.TCPConnector(ssl=self.ssl_verify),
            timeout=self.aio_timeout,
            trace_configs=[Profiler()],
        ) as session:
            self.session = session

            await self.download_urls(self.url)
        print(f"Completed")
        print(f"Visited urls: {len(self.visited_urls)}")
        print(f"Success visited urls: {len(self.success_visited_urls)}")

    def filter_only_host_links(self, links: Iterable[str], *args) -> Iterable[str]:
        return [link for link in links if self.get_base_url(link) == self.base_url]

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
        return [link for link in links if self.url in link]

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

    async def download_urls(self, url, session=None):
        page = await self.download_page(url, session)
        if not page:
            return
        all_links = self.get_all_links_from_page(page)
        filtering_links = self.filter_links(all_links, url)
        tasks_subcategory = []
        current_session = aiohttp.ClientSession(
            connector=aiohttp.TCPConnector(ssl=self.ssl_verify),
            timeout=self.aio_timeout,
            trace_configs=[Profiler()],
        )
        for link in filtering_links:
            # TODO: add custom filters
            tasks_subcategory.append(
                asyncio.create_task(self.download_urls(link, session=current_session))
            )
        self.to_work_urls.update(filtering_links)
        async with current_session:
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
    start_time = time.time()
    args = args_parse()
    s = Spider(
        url=args.url,
        timeout=args.timeout,
        no_parent=args.np,
        span_hosts=args.span_hosts,
        no_query_param=args.no_query_params,
        ssl_verify=args.ssl_verify,
    )
    asyncio.run(s.run_spider())
    end_time = time.time() - start_time

    print(f"Completed time {end_time:.3f} sec")
