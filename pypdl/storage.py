import os
import hashlib
from pathlib import Path
from urllib.parse import urlparse


def get_file_hash(path: str):
    """Simulate CPU-intensive processing on the downloaded file"""
    with open(path, 'rb') as f:
        return hashlib.md5(f.read()).hexdigest()

def get_hash(url: str):    
    return hashlib.md5(url.encode()).hexdigest()

def generate_file_path(url: str, base_path: str) -> str:
    """Generate a file path for a given URL."""
    parsed_url = urlparse(url)
    
    # Use the domain as the first level directory
    domain = parsed_url.netloc
    
    # Use the path as subdirectories, removing the filename
    path = Path(parsed_url.path).parent
    
    # Generate a unique filename using a hash of the full URL
    url_hash = hashlib.md5(url.encode()).hexdigest()
    original_filename = os.path.basename(parsed_url.path)
    filename = f"{url_hash}_{original_filename}"
    
    # Combine all parts
    path = os.path.join(base_path, domain, str(path).lstrip('/'), filename)
    
    return path

def ensure_dir(file_path: str):
    """Ensure that the directory for the given file path exists."""
    directory = os.path.dirname(file_path)
    os.makedirs(directory, exist_ok=True)
