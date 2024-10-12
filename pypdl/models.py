from datetime import datetime
from sqlalchemy import Column, JSON
from sqlmodel import Field, Relationship, SQLModel
from typing import List, Optional


class Hyperlink(SQLModel, table=True):
    __tablename__ = "hyperlinks"

    id: Optional[int] = Field(default=None, primary_key=True)
    url: str
    filename: Optional[str]
    path: Optional[str]
    downloaded: bool = Field(default=False)
    download_date: Optional[datetime]
    dossier_id: Optional[int] = Field(default=None, foreign_key="dossiers.id")
    dossier: Optional["Dossier"] = Relationship(back_populates="hyperlinks")


class Dossier(SQLModel, table=True):
    __tablename__ = "dossiers"

    id: Optional[int] = Field(default=None, primary_key=True)
    folder: Optional[str]
    attachment_files_downloaded: bool = Field(default=False)
    # hyperlink_ids: List[int] = Field(default=[], foreign_key="hyperlinks.dossiers_id")
    # hyperlink_ids: List[int] = Field(default=[], sa_column=Column(JSON))
    hyperlinks: List[Hyperlink] = Relationship(back_populates="dossier")
    # hyperlinks: List[Hyperlink] = Field(default=[], sa_column=Column(JSON))


Hyperlink.update_forward_refs()