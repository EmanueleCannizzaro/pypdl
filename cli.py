import asyncio
from rich import print

from pypdl.benchmark import run_benchmark

async def main():
    results = await run_benchmark("test.db", 10, "/tmp/downloads")
    print(results)

asyncio.run(main())
