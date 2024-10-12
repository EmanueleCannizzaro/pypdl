import pytest

import asyncio
import os
from pypdl.benchmark import run_benchmark
from pypdl.storage import generate_file_path

@pytest.mark.asyncio
@pytest.mark.parametrize("num_items", [10, 50, 100])
async def test_benchmark(tmp_path, num_items):
    """
    Test the benchmark function with different numbers of items.
    
    :param tmp_path: pytest fixture that provides a temporary directory unique to the test invocation
    :param num_items: parameterized input for the number of items to benchmark
    """
    db_file = tmp_path / "test.db"
    base_path = tmp_path / "downloads"

    results = await run_benchmark(str(db_file), num_items, str(base_path))

    assert results["total_items"] == num_items
    assert results["downloaded_count"] == num_items
    assert results["total_time"] < 60, f"Benchmark took too long: {results['total_time']:.2f} seconds"
    assert results["avg_time_per_file"] < 0.5, f"Average time per file too high: {results['avg_time_per_file']:.4f} seconds"

def test_file_path_generation():
    base_path = "downloads"
    url = "https://miglioriaste.it/aste/upload/images/Foto-RA-EI-78-2023-9.jpg"
    file_path = generate_file_path(url, base_path)
    assert file_path.startswith("downloads/")
    assert file_path.endswith(os.path.basename(url))
    assert len(file_path.split("/")[-1]) > 32  # Ensure there's a hash in the filename
