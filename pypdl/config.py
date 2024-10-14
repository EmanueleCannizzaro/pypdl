import os
from dotenv import load_dotenv

load_dotenv()

class Config:
    MAX_CONCURRENT = int(os.getenv('MAX_CONCURRENT', 5))
    RETRY_ATTEMPTS = int(os.getenv('RETRY_ATTEMPTS', 3))
    TIMEOUT = int(os.getenv('TIMEOUT', 300))
    CHUNK_SIZE = int(os.getenv('CHUNK_SIZE', 8192))
    OTLP_ENDPOINT = os.getenv('OTLP_ENDPOINT', 'http://localhost:4317')
    OUTPUT_FOLDER = os.getenv('OUTPUT_FOLDER', 'downloaded_files')
    WEBHOOK_URL = os.getenv('WEBHOOK_URL', '')
    MAX_BANDWIDTH = float(os.getenv('MAX_BANDWIDTH', 0))  # In MB/s, 0 means no limit
    BATCH_SIZE = int(os.getenv('BATCH_SIZE', 100))  # Number of URLs to process in each batch