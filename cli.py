# import asyncio
# from rich import print

# from pypdl.benchmark import run_benchmark

# async def main():
#     results = await run_benchmark("test.db", 10, "/tmp/downloads")
#     print(results)

# asyncio.run(main())

import typer
import asyncio
from typing import List
from .downloader import AsyncFileDownloader
from .config import Config

app = typer.Typer()

@app.command()
def download(
    urls: List[str] = typer.Argument(..., help="URLs of files to download"),
    output: str = typer.Option("downloaded_files", "--output", "-o", help="Output folder for downloaded files")
):
    """
    Download files from given URLs.
    """
    Config.OUTPUT_FOLDER = output
    asyncio.run(download_files(urls))

async def download_files(urls: List[str]):
    async with AsyncFileDownloader(urls) as downloader:
        await downloader.download_all()

if __name__ == "__main__":
    app()