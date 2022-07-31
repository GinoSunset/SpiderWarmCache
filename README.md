SpiderWarmCache
================
This is a script for warming up your site's cache.

optional arguments:
| Option name                | description                                                                              |
| -------------------------- | ---------------------------------------------------------------------------------------- |
| `-h`, `--help`             | show this help message and exit                                                          |
| `-u` URL, `--url` URL      | URL to start spider                                                                      |
| `-t` TIMEOUT, `--timeout`  | TIMEOUT [10] sec by default                                                              |
| `-np`, `-–no-parent`       | Do not ever ascend to the parent directory when retrieving recursively.                  |
| `--span-hosts`             | The option turns on host spanning, thus allowing to visit any host referenced by a link. |
| `-nq`, `--no-query-params` | remove from url query params                                                             |
| `-sv` `--ssl-verify`       | Verify ssl, default `False`                                                              |
