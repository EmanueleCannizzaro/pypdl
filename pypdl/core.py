import asyncio
import aiohttp
import aiofiles
import os
import logging
from urllib.parse import urlparse
from typing import List, Dict
from tqdm.asyncio import tqdm
import hashlib

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class AsyncFileDownloader:
    def __init__(self, urls: List[str], output_folder: str, max_concurrent: int = 5):
        self.urls = urls
        self.output_folder = output_folder
        self.max_concurrent = max_concurrent
        self.session = None
        self.semaphore = asyncio.Semaphore(max_concurrent)

    async def __aenter__(self):
        self.session = aiohttp.ClientSession()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.session.close()

    def generate_filename(self, url: str) -> str:
        """Generate a unique filename based on the URL."""
        parsed_url = urlparse(url)
        original_filename = os.path.basename(parsed_url.path) or 'unnamed_file'
        url_hash = hashlib.md5(url.encode()).hexdigest()#[:8]
        return f"{url_hash}_{original_filename}"

    async def download_file(self, url: str) -> Dict[str, str]:
        """Download a single file asynchronously."""
        filename = self.generate_filename(url)
        filepath = os.path.join(self.output_folder, filename)

        if not os.path.exists(filepath):
            try:
                async with self.semaphore:
                    async with self.session.get(url) as response:
                        if response.status != 200:
                            return {"url": url, "status": "error", "message": f"HTTP {response.status}"}

                        total_size = int(response.headers.get('content-length', 0))
                        
                        async with aiofiles.open(filepath, 'wb') as file:
                            progress_bar = tqdm(total=total_size, unit='B', unit_scale=True, desc=filename)
                            async for chunk in response.content.iter_chunked(8192):
                                await file.write(chunk)
                                progress_bar.update(len(chunk))
                            progress_bar.close()

                return {"url": url, "status": "success", "filename": filename}
            except Exception as e:
                logger.error(f"Error downloading {url}: {str(e)}")
                return {"url": url, "status": "error", "message": str(e)}
        else:
            return {"url": url, "status": "success", "filename": filename}

    async def download_all(self) -> List[Dict[str, str]]:
        """Download all files concurrently."""
        os.makedirs(self.output_folder, exist_ok=True)
        tasks = [self.download_file(url) for url in self.urls]
        return await tqdm.gather(*tasks, desc="Overall Progress")

async def download(urls: List[str], output_folder: str):
    async with AsyncFileDownloader(urls, output_folder) as downloader:
        results = await downloader.download_all()

    # Print summary
    successful = sum(1 for result in results if result['status'] == 'success')
    failed = sum(1 for result in results if result['status'] == 'error')
    logger.info(f"Download Summary: {successful} successful, {failed} failed")

    # Log errors
    for result in results:
        if result['status'] == 'error':
            logger.error(f"Failed to download {result['url']}: {result.get('message', 'Unknown error')}")

def main():
    import pandas as pd
    filename = '/media/emanuele/ai/property_scraper/demos/downloads/pvp/attachments/urls.csv'
    df = pd.read_csv(filename)
    urls_to_download  = df['0'].to_list()[:105]

    # urls_to_download = [
    #     # PDF Files
    #     "https://www.w3.org/WAI/ER/tests/xhtml/testfiles/resources/pdf/dummy.pdf",
    #     "https://www.adobe.com/support/products/enterprise/knowledgecenter/media/c4611_sample_explain.pdf",
    #     "https://www.hq.nasa.gov/alsj/a17/A17_FlightPlan.pdf",
    #     "https://www.irs.gov/pub/irs-pdf/f1040.pdf",
        
    #     # Images
    #     "https://upload.wikimedia.org/wikipedia/commons/thumb/b/b6/Image_created_with_a_mobile_phone.png/1280px-Image_created_with_a_mobile_phone.png",
    #     "https://upload.wikimedia.org/wikipedia/commons/thumb/d/d7/Green_Sea_Turtle_grazing_seagrass.jpg/1280px-Green_Sea_Turtle_grazing_seagrass.jpg",
    #     "https://www.nasa.gov/sites/default/files/thumbnails/image/main_image_star-forming_region_carina_nircam_final-5mb.jpg",
        
    #     # Documents
    #     "https://www.justice.gov/sites/default/files/opa/press-releases/attachments/2015/03/04/doj_report_on_shooting_of_michael_brown_1.pdf",
    #     "https://assets.publishing.service.gov.uk/government/uploads/system/uploads/attachment_data/file/1026148/O-TTAD005_-_OFFICIAL_STATISTICS_-_Officer_and_Volunteer_Diversity_2021_publication.ods",
    #     "https://github.com/apache/apache-website/raw/main/README.md",
        
    #     # Data Files
    #     "https://data.cityofnewyork.us/api/views/kku6-nxdu/rows.csv?accessType=DOWNLOAD",
    #     "https://www.stats.govt.nz/assets/Uploads/Annual-enterprise-survey/Annual-enterprise-survey-2021-financial-year-provisional/Download-data/annual-enterprise-survey-2021-financial-year-provisional-csv.csv",
        
    #     # Audio
    #     "https://file-examples.com/storage/fe8c7eef0c6364f6c9504cc/2017/11/file_example_MP3_700KB.mp3",
        
    #     # Video
    #     "https://file-examples.com/storage/fe8c7eef0c6364f6c9504cc/2017/04/file_example_MP4_480_1_5MG.mp4",
        
    #     # Compressed Files
    #     "https://www.learningcontainer.com/wp-content/uploads/2020/05/sample-zip-file.zip",
    #     "https://file-examples.com/storage/fe8c7eef0c6364f6c9504cc/2017/02/file_example_TIFF_1MB.tiff",
        
    #     # Executable (for demonstration, be cautious with downloading executables)
    #     "https://the.earth.li/~sgtatham/putty/latest/w64/putty.exe",
        
    #     # Large File (for testing download speed and progress bar)
    #     "https://speed.hetzner.de/100MB.bin"
    # ]
    output_folder = "downloaded_files"

    asyncio.run(download(urls_to_download, output_folder))

if __name__ == "__main__":
    main()