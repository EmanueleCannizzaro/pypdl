import asyncio
from concurrent.futures import ProcessPoolExecutor
from sqlmodel import SQLModel, create_engine, Session, select
import time

from pypdl.models import Hyperlink, Dossier
from pypdl.utils import populate_database, process_downloads


async def run_benchmark(db_file: str, num_items: int, folder: str):
    engine = create_engine(f"sqlite:///{db_file}")
    SQLModel.metadata.create_all(engine)

    populate_database(engine, num_items)

    start_time = time.time()

    await process_downloads(engine, folder)

    # Process files using a ProcessPoolExecutor
    files = []
    with ProcessPoolExecutor() as executor:
        with Session(engine) as session:
            dossiers = session.exec(select(Dossier)).all()
            for dossier in dossiers:
                for hyperlink in dossier.hyperlinks:
                    if hyperlink.path:
                        files.append(hyperlink.path)

    # Process files using a ProcessPoolExecutor
    processed_files = []
    with ProcessPoolExecutor() as executor:
        with Session(engine) as session:
            dossiers = session.exec(select(Dossier).where(Dossier.attachment_files_downloaded == True)).all()
            for dossier in dossiers:
                for hyperlink in dossier.hyperlinks:
                    if hyperlink.path:
                        processed_files.append(hyperlink.path)
    end_time = time.time()

    total_time = end_time - start_time
    avg_time_per_file = total_time / num_items

    with Session(engine) as session:
        downloaded_urls = session.exec(select(Hyperlink).where(Hyperlink.downloaded == True)).all()

    results = {
        "total_items": num_items,
        "total_time": total_time,
        "avg_time_per_file": avg_time_per_file,
        "file_count": len(files),
        "processed_file_count": len(processed_files),
        "downloaded_url_count": len(downloaded_urls),
    }

    return results