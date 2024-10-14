import asyncio
import aiohttp
import aiofiles
import os
import logging
import hashlib
import time
from urllib.parse import urlparse, unquote
from typing import List, Dict, Optional
from aiolimiter import AsyncLimiter
from tqdm.asyncio import tqdm
from collections import deque
from opentelemetry.trace.status import Status, StatusCode

from pypdl.config import Config
from pypdl.telemetry import tracer, download_counter, download_size_histogram, download_duration_histogram


logger = logging.getLogger(__name__)

class AsyncFileDownloader:
    def __init__(self, urls: List[str]):
        self.urls = deque(urls)  # Use deque for efficient popping from both ends
        self.output_folder = Config.OUTPUT_FOLDER
        self.max_concurrent = Config.MAX_CONCURRENT
        self.retry_attempts = Config.RETRY_ATTEMPTS
        self.timeout = Config.TIMEOUT
        self.chunk_size = Config.CHUNK_SIZE
        self.batch_size = Config.BATCH_SIZE
        self.rate_limiter = AsyncLimiter(self.max_concurrent)
        self.bandwidth_limiter = AsyncLimiter(Config.MAX_BANDWIDTH * 1024 * 1024 if Config.MAX_BANDWIDTH > 0 else float('inf'))
        self.total_bytes_downloaded = 0
        self.start_time = None
        self.successful_downloads = 0
        self.failed_downloads = 0
        self.progress_bar = None

    async def __aenter__(self):
        self.session = aiohttp.ClientSession(connector=aiohttp.TCPConnector(limit=self.max_concurrent))
        self.start_time = time.time()
        logger.info("AsyncFileDownloader session started")
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.session.close()
        self.log_telemetry()
        logger.info("AsyncFileDownloader session ended")

    def generate_filename(self, url: str) -> str:
        parsed_url = urlparse(url)
        original_filename = unquote(os.path.basename(parsed_url.path)) or 'unnamed_file'
        url_hash = hashlib.md5(url.encode()).hexdigest()
        return f"{url_hash}_{original_filename}"

    async def download_file(self, url: str) -> Dict[str, str]:
        with tracer.start_as_current_span("download_file") as span:
            span.set_attribute("url", url)
            filename = self.generate_filename(url)
            filepath = os.path.join(self.output_folder, filename)
            temp_filepath = f"{filepath}.temp"

            if os.path.exists(filepath):
                logger.info(f"File already exists: {filename}")
                self.successful_downloads += 1
                download_counter.add(1, {"status": "already_exists"})
                return {"url": url, "status": "success", "filename": filename, "bytes_downloaded": 0}

            for attempt in range(self.retry_attempts):
                try:
                    async with self.rate_limiter:
                        start_time = time.time()
                        headers = {}
                        if os.path.exists(temp_filepath):
                            headers['Range'] = f'bytes={os.path.getsize(temp_filepath)}-'
                        
                        async with self.session.get(url, timeout=aiohttp.ClientTimeout(total=self.timeout), headers=headers) as response:
                            if response.status == 416:  # Range Not Satisfiable, file is complete
                                os.rename(temp_filepath, filepath)
                                logger.info(f"File already complete: {filename}")
                                return {"url": url, "status": "success", "filename": filename, "bytes_downloaded": 0}
                            
                            if response.status not in [200, 206]:
                                raise aiohttp.ClientResponseError(
                                    response.request_info,
                                    response.history,
                                    status=response.status,
                                    message=f"HTTP {response.status}",
                                    headers=response.headers
                                )

                            total_size = int(response.headers.get('content-length', 0))
                            mode = 'ab' if response.status == 206 else 'wb'
                            
                            async with aiofiles.open(temp_filepath, mode) as file:
                                bytes_downloaded = 0
                                async for chunk in response.content.iter_chunked(self.chunk_size):
                                    await self.bandwidth_limiter.acquire(len(chunk))
                                    await file.write(chunk)
                                    bytes_downloaded += len(chunk)
                                    self.total_bytes_downloaded += len(chunk)

                    os.rename(temp_filepath, filepath)
                    duration = time.time() - start_time
                    logger.info(f"Successfully downloaded: {filename} ({bytes_downloaded} bytes)")
                    self.successful_downloads += 1
                    
                    download_counter.add(1, {"status": "success"})
                    download_size_histogram.record(bytes_downloaded)
                    download_duration_histogram.record(duration)

                    span.set_status(Status(StatusCode.OK))
                    return {"url": url, "status": "success", "filename": filename, "bytes_downloaded": bytes_downloaded}
                
                except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                    logger.warning(f"Attempt {attempt + 1} failed for {url}: {str(e)}")
                    if attempt == self.retry_attempts - 1:
                        logger.error(f"Failed to download {url} after {self.retry_attempts} attempts")
                        self.failed_downloads += 1
                        download_counter.add(1, {"status": "failed"})
                        span.set_status(Status(StatusCode.ERROR, str(e)))
                        return {"url": url, "status": "error", "message": str(e)}
                    await asyncio.sleep(2 ** attempt)  # Exponential backoff

    async def producer(self, queue: asyncio.Queue):
        while self.urls:
            batch = [self.urls.popleft() for _ in range(min(self.batch_size, len(self.urls)))]
            await queue.put(batch)
        await queue.put(None)  # Signal that all URLs have been processed

    async def consumer(self, queue: asyncio.Queue):
        while True:
            batch = await queue.get()
            if batch is None:
                break
            tasks = [self.download_file(url) for url in batch]
            results = await asyncio.gather(*tasks)
            for result in results:
                if result['status'] == 'success':
                    self.progress_bar.update(1)
            queue.task_done()

    async def download_all(self) -> List[Dict[str, str]]:
        with tracer.start_as_current_span("download_all"):
            logger.info(f"Starting download of {len(self.urls)} files")
            os.makedirs(self.output_folder, exist_ok=True)
            
            queue = asyncio.Queue(maxsize=self.max_concurrent)
            self.progress_bar = tqdm(total=len(self.urls), desc="Downloading", unit="file")

            producer_task = asyncio.create_task(self.producer(queue))
            consumers = [asyncio.create_task(self.consumer(queue)) for _ in range(self.max_concurrent)]

            await asyncio.gather(producer_task, *consumers)
            self.progress_bar.close()

            logger.info(f"Completed download of {len(self.urls)} files")
            return []  # We're not collecting results anymore to save memory

    async def send_webhook_notification(self):
        if not Config.WEBHOOK_URL:
            return

        payload = {
            "message": "Download job completed",
            "total_files": len(self.urls),
            "successful_downloads": self.successful_downloads,
            "failed_downloads": self.failed_downloads
        }

        async with aiohttp.ClientSession() as session:
            try:
                async with session.post(Config.WEBHOOK_URL, json=payload) as response:
                    if response.status != 200:
                        logger.error(f"Failed to send webhook notification. Status: {response.status}")
                    else:
                        logger.info("Webhook notification sent successfully")
            except Exception as e:
                logger.error(f"Error sending webhook notification: {str(e)}")

    def log_telemetry(self):
        duration = time.time() - self.start_time
        avg_speed = self.total_bytes_downloaded / duration / 1024 / 1024  # MB/s
        logger.info(f"Download Summary:")
        logger.info(f"  Total files: {len(self.urls)}")
        logger.info(f"  Successful downloads: {self.successful_downloads}")
        logger.info(f"  Failed downloads: {self.failed_downloads}")
        logger.info(f"  Total bytes downloaded: {self.total_bytes_downloaded:,} bytes")
        logger.info(f"  Total duration: {duration:.2f} seconds")
        logger.info(f"  Average download speed: {avg_speed:.2f} MB/s")


async def main():
    """
    Main function to demonstrate the usage of the AsyncFileDownloader.

    This function reads URLs from a CSV file and initiates the download process.
    It serves as an example of how to use the AsyncFileDownloader class.
    """
    import pandas as pd
    filename = '/media/emanuele/ai/property_scraper/demos/downloads/pvp/attachments/urls.csv'
    df = pd.read_csv(filename)
    urls = df['0'].to_list()[:1350]
    output_folder = "files"

    async with AsyncFileDownloader(urls) as downloader:
        await downloader.download_all()
    await downloader.send_webhook_notification()

if __name__ == "__main__":
    asyncio.run(main())