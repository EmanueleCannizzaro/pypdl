from setuptools import setup, find_packages

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setup(
    name="async-file-downloader",
    version="0.1.0",
    author="Your Name",
    author_email="your.email@example.com",
    description="An asynchronous file downloader with advanced features",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/yourusername/async-file-downloader",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
    ],
    python_requires=">=3.7",
    install_requires=[
        "aiohttp",
        "aiofiles",
        "tqdm",
        "opentelemetry-api",
        "opentelemetry-sdk",
        "opentelemetry-exporter-otlp",
        "typer",
        "python-dotenv",
        "aiolimiter",
    ],
    entry_points={
        "console_scripts": [
            "async-downloader=async_file_downloader.cli:app",
        ],
    },
)