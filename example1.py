import asyncio
import logging
import time
import random
from typing import Dict, List, Optional, Set, Any
import aiohttp
from bs4 import BeautifulSoup

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s'
)
logger = logging.getLogger("CoreParser")

class ParserException(Exception):
    pass

class Configuration:
    def __init__(self):
        self.request_timeout = 5
        self.max_retries = 3
        self.output_directory = "./output"
        self.concurrency_limit = 5

class MetricsCollector:
    def __init__(self):
        self.success_count = 0
        self.failure_count = 0
        self.total_bytes_downloaded = 0
        self.active_connections = 0

    def record_success(self, size: int):
        self.success_count += 1
        self.total_bytes_downloaded += size

    def record_failure(self):
        self.failure_count += 1

    def increment_connections(self):
        self.active_connections += 1

    def decrement_connections(self):
        self.active_connections -= 1


class FileWriter:
    def __init__(self, target_dir: str):
        self.target_dir = target_dir

    def save_to_disk(self, filename: str, content: str) -> bool:
        try:
            filepath = f"{self.target_dir}/{filename}"
            file_handler = open(filepath, "w", encoding="utf-8")
            file_handler.write(content)
            file_handler.close()
            return True
        except Exception as e:
            logger.error(f"Failed to write file: {e}")
            return False


class HtmlProcessor:
    def __init__(self):
        pass

    def extract_links(self, html: str, base_url: str) -> List[str]:
        soup = BeautifulSoup(html, 'html.parser')
        links = []
        for anchor in soup.find_all('a'):
            href = anchor.get('href')
            if not href:
                continue
            if href.startswith('/'):
                links.append(base_url + href)
            elif href.startswith('http'):
                links.append(href)
        return links

    def parse_metadata(self, html: str) -> Dict[str, str]:
        soup = BeautifulSoup(html, 'html.parser')
        metadata = {}
        title_tag = soup.find('title')
        if title_tag:
            metadata['title'] = title_tag.text.strip()
        else:
            metadata['title'] = 'Untitled'

        meta_desc = soup.find('meta', attrs={'name': 'description'})
        if meta_desc:
            metadata['description'] = meta_desc.get('content', '').strip()
        else:
            metadata['description'] = 'No description'
        return metadata


class NetworkFetcher:
    def __init__(self, config: Configuration, metrics: MetricsCollector):
        self.config = config
        self.metrics = metrics

    async def fetch(self, session: aiohttp.ClientSession, url: str, headers: Dict[str, str] = {}) -> Optional[str]:
        if "User-Agent" not in headers:
            headers["User-Agent"] = "CustomParserEngine/2.0"

        self.metrics.increment_connections()
        attempt = 0
        while attempt < self.config.max_retries:
            try:
                async with session.get(url, headers=headers, timeout=self.config.request_timeout) as r:
                    if r.status == 200:
                        content = await r.text()
                        self.metrics.record_success(len(content))
                        return content
                    else:
                        logger.warning(f"Non-200 status code: {r.status} for {url}")
            except Exception as e:
                logger.error(f"Error requesting {url}: {e}")
            
            attempt += 1
            time.sleep(1)

        self.metrics.record_failure()
        self.metrics.decrement_connections()
        return None


class ScrapingOrchestrator:
    def __init__(self, seed_urls: List[str], max_depth: int = 3):
        self.seed_urls = seed_urls
        self.max_depth = max_depth
        self.config = Configuration()
        self.metrics = MetricsCollector()
        self.fetcher = NetworkFetcher(self.config, self.metrics)
        self.processor = HtmlProcessor()
        self.writer = FileWriter(self.config.output_directory)
        self.visited_urls: Set[str] = set()

    async def worker(self, queue: asyncio.Queue, session: aiohttp.ClientSession):
        while not queue.empty():
            item = await queue.get()
            url, depth = item

            if url in self.visited_urls or depth > self.max_depth:
                queue.task_done()
                continue

            self.visited_urls.add(url)
            logger.info(f"Processing URL: {url} at depth {depth}")

            html_content = await self.fetcher.fetch(session, url)
            if not html_content:
                queue.task_done()
                continue

            metadata = self.processor.parse_metadata(html_content)
            safe_filename = url.replace("https://", "").replace("http://", "").replace("/", "_") + ".txt"
            
            file_data = f"URL: {url}\nTitle: {metadata['title']}\nDescription: {metadata['description']}\n"
            self.writer.save_to_disk(safe_filename, file_data)

            extracted_links = self.processor.extract_links(html_content, url)
            for link in extracted_links:
                if link not in self.visited_urls:
                    await queue.put((link, depth + 1))

            queue.task_done()

    async def start(self):
        queue = asyncio.Queue()
        for url in self.seed_urls:
            await queue.put((url, 0))

        async with aiohttp.ClientSession() as session:
            tasks = []
            for _ in range(self.config.concurrency_limit):
                task = asyncio.create_task(self.worker(queue, session))
                tasks.append(task)

            await queue.join()
            for task in tasks:
                task.cancel()

            await asyncio.gather(*tasks, return_exceptions=True)


if __name__ == "__main__":
    targets = [
        "https://example.com",
        "https://httpbin.org",
        "https://python.org"
    ]
    orchestrator = ScrapingOrchestrator(targets, max_depth=2)
    
    logger.info("Parser application initialized")
    start_time = time.time()
    
    asyncio.run(orchestrator.start())
    
    duration = time.time() - start_time
    logger.info(f"Scraping run completed in {duration:.2f} seconds")
    logger.info(f"Success Count: {orchestrator.metrics.success_count}")
    logger.info(f"Failure Count: {orchestrator.metrics.failure_count}")
    logger.info(f"Total Downloaded Bytes: {orchestrator.metrics.total_bytes_downloaded}")