import aiofiles
import aiohttp
from aiohttp import ClientError, ServerDisconnectedError, ClientPayloadError
import asyncio
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
from functools import partial
import os
from sqlmodel import SQLModel, Field, Session, create_engine, select, update
from typing import List
from urllib.parse import urlparse

from pypdl.models import Hyperlink, Dossier
from pypdl.storage import get_hash, get_file_hash


async def download_file(session, url, folder_path, item_id, semaphore, max_retries=3):
    filename = os.path.basename(urlparse(url).path) or 'unnamed_file'
    filepath = os.path.join(folder_path, f"{item_id}_{filename}")
    
    for attempt in range(max_retries):
        try:
            async with semaphore:
                async with session.get(url) as response:
                    if response.status == 200:
                        async with aiofiles.open(filepath, 'wb') as f:
                            while True:
                                chunk = await response.content.read(8192)
                                if not chunk:
                                    break
                                await f.write(chunk)
                        print(f"Successfully downloaded: {url} for item {item_id}")
                        return True
                    else:
                        print(f"Failed to download {url} for item {item_id}. Status: {response.status}")
        except (ClientError, ServerDisconnectedError, ClientPayloadError, asyncio.TimeoutError) as e:
            if attempt < max_retries - 1:
                wait_time = 2 ** attempt  # Exponential backoff
                print(f"Error downloading {url} for item {item_id}. Retrying in {wait_time} seconds... Error: {str(e)}")
                await asyncio.sleep(wait_time)
            else:
                print(f"Failed to download {url} for item {item_id} after {max_retries} attempts. Error: {str(e)}")
    return False

async def process_item(session, dossier, folder_path, semaphore, executor):
    loop = asyncio.get_event_loop()
    download_tasks = [
        download_file(session, hyperlink.url, folder_path, dossier.id, semaphore)
        for hyperlink in dossier.hyperlinks
    ]
    download_results = await asyncio.gather(*download_tasks)
    
    if all(download_results):
        # Process files using a ProcessPoolExecutor
        paths = [os.path.join(folder_path, f"{dossier.id}_{os.path.basename(urlparse(hyperlink.url).path)}") for hyperlink in dossier.hyperlinks]
        # process_results = await loop.run_in_executor(executor, lambda: list(executor.map(get_hash, paths)))
        process_results = [True] * len(paths)
        
        print(f"Processed files for item {dossier.id}. Results: {process_results}")
        return True
    else:
        print(f"Not all files were downloaded successfully for item {dossier.id}.")
        return False

async def download_files_for_items(dossiers: List[Dossier], folder_path: str, db_session: Session, max_concurrent=10):
    os.makedirs(folder_path, exist_ok=True)
    semaphore = asyncio.Semaphore(max_concurrent)
    
    async with aiohttp.ClientSession() as session:
        with ProcessPoolExecutor() as process_executor:
            with ThreadPoolExecutor() as thread_executor:
                tasks = [
                    process_item(session, dossier, folder_path, semaphore, process_executor)
                    for dossier in dossiers
                ]
                results = await asyncio.gather(*tasks)

                # Update database in a separate thread to avoid blocking the event loop
                update_db = partial(update_database, db_session, dossiers, results)
                await asyncio.get_event_loop().run_in_executor(thread_executor, update_db)

def update_database(db_session, dossiers, results):
    print(dossiers)
    print(results)
    for dossier, success in zip(dossiers, results):
        if success:
            db_session.execute(update(Dossier).where(Dossier.id == dossier.id).values(attachment_files_downloaded=True))
    db_session.commit()
    print("Database updated successfully.")

def main():
    engine = create_engine("sqlite:///database.db")
    dossiers = [
        Dossier(
            hyperlinks=[
                Hyperlink(url="https://miglioriaste.it/aste/upload/images/Foto-RA-EI-78-2023-9.jpg"), 
                Hyperlink(url="https://miglioriaste.it/aste/upload/images/Foto-RA-EI-78-2023-10.jpg"),
                Hyperlink(url="https://miglioriaste.it/aste/upload/images/Planimetria-RA-EI-78-2023-6.jpg")
            ]
        ),
        Dossier(
            hyperlinks=[
                Hyperlink(url="https://pvp-documenti.apps.pvc-os-caas01-rs.polostrategiconazionale.it/allegati/4245194/LOTTO 2 - Documentazione fotografica.pdf?versionId=deedf4d8-4f0f-41ec-bd36-8ac7cdefff46"),
                Hyperlink(url="https://pvp-documenti.apps.pvc-os-caas01-rs.polostrategiconazionale.it/allegati/4245194/Perizia per pubblicità LOTTO 2.pdf?versionId=dee6aeb4-fba2-48b9-9618-a462796cd804"),
                Hyperlink(url="https://pvp-documenti.apps.pvc-os-caas01-rs.polostrategiconazionale.it/allegati/4245194/Vendita sincrona mista 13.12.2024.pdf?versionId=e9347d9d-a331-4438-83c3-296cbcf9c905"),
                Hyperlink(url="https://pvp-documenti.apps.pvc-os-caas01-rs.polostrategiconazionale.it/allegati/4245194/nomina delegato.pdf?versionId=ed5d36f8-deb0-496b-8826-e0185dca6750")
            ]
        )
    ]
    with Session(engine) as session:
        asyncio.run(download_files_for_items(dossiers, 'downloads', session))

if __name__ == '__main__':
    main()