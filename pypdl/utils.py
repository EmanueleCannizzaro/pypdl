import asyncio
from concurrent.futures import ProcessPoolExecutor
from datetime import datetime
import os
import random
from sqlmodel import Session, select

from pypdl.models import Hyperlink, Dossier
from pypdl.storage import generate_file_path, ensure_dir

def populate_database(engine, num_items, base_url="http://example.com"):
    with Session(engine) as session:
        for i in range(num_items):
            if i % 10 == 0:
                hyperlinks = []
            url = f"{base_url}/file{i}.pdf"
            hyperlink = Hyperlink(url=url)
            hyperlinks.append(hyperlink)
            session.add(hyperlink)
            if i % 10 == 9:
                dossier = Dossier(folder=base_url, hyperlinks=hyperlinks)
                session.add(dossier)
        session.commit()

async def mock_download_file(url, path:str):
    await asyncio.sleep(0.01 + 0.02 * random.random())
    ensure_dir(path)
    # Simulate file creation
    with open(path, 'w') as f:
        f.write(f"Content of {url}")
    return True

async def download_and_update(session: Session, dossier, folder:str):
    # Process files using a ProcessPoolExecutor
    with ProcessPoolExecutor() as executor:
        dossiers = session.exec(select(Dossier)).all()
        for dossier in dossiers:
            for hyperlink in dossier.hyperlinks:
                if hyperlink.url:
                    hyperlink.path = generate_file_path(hyperlink.url, folder)
                if hyperlink.path:
                    hyperlink.filename = os.path.basename(hyperlink.path)
                    success = await mock_download_file(hyperlink.url, hyperlink.path)
                    # if success:
                    hyperlink.downloaded = True
                    hyperlink.download_date = datetime.now()
                session.add(hyperlink)
                session.commit()
                session.refresh(hyperlink)
            dossier.attachment_files_downloaded = True
            session.add(dossier)
            session.commit()
            session.refresh(dossier)
        # session.commit()

async def process_downloads(engine, folder):
    async with asyncio.TaskGroup() as tg:
        with Session(engine) as session:
            dossiers = session.exec(select(Dossier).where(Dossier.attachment_files_downloaded == False)).all()
            for dossier in dossiers:
                tg.create_task(download_and_update(session, dossier, folder))
        session.commit()

