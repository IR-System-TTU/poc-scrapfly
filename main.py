import asyncio
import json
import math
from typing import Dict, List, Tuple
from urllib.parse import urlencode, urljoin

from loguru import logger as log
from scrapfly import ScrapeApiResponse, ScrapeConfig, ScrapflyClient


def create_search_url(query: str, page=1, sort="price_low") -> str:
    """create url for a single walmart search page"""
    return "https://www.walmart.com/search?" + urlencode(
        {
            "q": query,
            "sort": sort,
            "page": page,
            "affinityOverride": "default",
        }
    )


def parse_search(result: ScrapeApiResponse) -> Tuple[Dict, int]:
    """extract search results from search HTML response"""
    log.debug(f"parsing search page {result.context['url']}")
    data = result.selector.xpath('//script[@id="__NEXT_DATA__"]/text()').get()
    if (data != None):
        data = json.loads(data)

        total_results = data["props"]["pageProps"]["initialData"]["searchResult"]["itemStacks"][0]["count"]
        results = data["props"]["pageProps"]["initialData"]["searchResult"]["itemStacks"][0]["items"]
        # there are other results types such as ads or placeholders - filter them out:
        results = [result for result in results if result["__typename"] == "Product"]
        log.info(f"parsed {len(results)} search product previews")
        return results, total_results
    else:
        return {}, 0


async def discover_walmart(search: str, session: ScrapflyClient) -> List[Dict]:
    """search walmart for products based on search string"""
    log.info(f"searching walmart for {search}")
    first_page = await session.async_scrape(ScrapeConfig(create_search_url(query=search), country="US", asp=True))
    previews, total_items = parse_search(first_page)
    max_page = math.ceil(total_items / 40)
    log.info(f"found total {max_page} pages of results ({total_items} products)")
    if max_page > 25:
        max_page = 25
    other_page_urls = [create_search_url(query=search, page=i) for i in range(2, max_page + 1)]
    async for result in session.concurrent_scrape(
        [ScrapeConfig(url, country="US", asp=True) for url in other_page_urls]
    ):
        previews.extend(parse_search(result)[0])
    log.info(f"parsed total {len(previews)} pages of results ({total_items} products)")
    return previews


def parse_product(result: ScrapeApiResponse):
    """parse walmart product from product page response"""
    data = result.selector.xpath('//script[@id="__NEXT_DATA__"]/text()').get()
    if (data != None):
        data = json.loads(data)
        _product_raw = data["props"]["pageProps"]["initialData"]["data"]["product"]
        wanted_product_keys = [
            "availabilityStatus",
            "averageRating",
            "brand",
            "id",
            "imageInfo",
            "manufacturerName",
            "name",
            "orderLimit",
            "orderMinLimit",
            "priceInfo",
            "shortDescription",
            "type",
        ]
        product = {k: v for k, v in _product_raw.items() if k in wanted_product_keys}
        reviews_raw = data["props"]["pageProps"]["initialData"]["data"]["reviews"]
        return {"product": product, "reviews": reviews_raw}


async def _scrape_products_by_url(urls: List[str], session: ScrapflyClient):
    """scrape walmart products by urls"""
    log.info(f"scraping {len(urls)} product urls (in chunks of 50)")
    results = []
    async for result in session.concurrent_scrape([ScrapeConfig(url=url, asp=True, country="US") for url in urls]):
        product = parse_product(result)
        if (product != None):
            results.append(product)
    return results


async def scrape_walmart(search: str, session: ScrapflyClient):
    """scrape walmart products by search term"""
    search_results = await discover_walmart(search, session=session)
    product_urls = [
        urljoin("https://www.walmart.com/", product_preview["canonicalUrl"]) for product_preview in search_results
    ]
    return await _scrape_products_by_url(product_urls, session=session)


async def run():
    scrapfly = ScrapflyClient(key="81defce0bfd240b9aefd65cd238e77b2", max_concurrency=10)
    with scrapfly as session:
        result_products = await scrape_walmart("spider", session=session)
        print(result_products)
        return result_products

if __name__ == "__main__":
    asyncio.run(run())