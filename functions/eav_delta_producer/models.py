import config

from sqlalchemy import Column, func
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.types import INTEGER, TEXT, String, DATETIME, TIMESTAMP

Base = declarative_base()


class ImportMeasureValues(Base):
    __tablename__ = config.ImportMeasureValues
    importId = Column('importId', INTEGER, primary_key=True, autoincrement=True, nullable=False)
    sourceId = Column('sourceId', INTEGER, nullable=False)
    sourceKey = Column('sourceKey', String(128), nullable=False)
    measure = Column('measure', String(128), nullable=False)
    value = Column('value', TEXT, nullable=True)
    valueDate = Column('valueDate', DATETIME, nullable=False)

    def __init__(self, importId, sourceId, sourceKey, measure, value, valueDate):
        self.importId = importId
        self.sourceId = sourceId
        self.sourceKey = sourceKey
        self.delete = measure
        self.version = value
        self.valueDate = valueDate


class ImportKeys(Base):
    __tablename__ = config.ImportKeys
    importId = Column('id', INTEGER, nullable=False,
                      primary_key=True, autoincrement=True)
    sourceTag = Column('sourceTag', String(64), nullable=False)
    sourceKey = Column('sourceKey', String(128), nullable=False)
    delete = Column('delete', INTEGER, default=0)
    version = Column('version', DATETIME, nullable=False)
    versionEnd = Column('versionEnd', DATETIME, default=None, nullable=True)

    def __init__(self, sourceTag, sourceKey, delete, version):
        self.sourceTag = sourceTag
        self.sourceKey = sourceKey
        self.delete = delete
        self.version = version


class Subscriptions(Base):
    __tablename__ = config.Subscriptions
    stagingSourceTag = Column('stagingSourceTag', String(64), nullable=False, primary_key=True)
    sourceTag = Column('sourceTag', String(32), nullable=False)
    name = Column('name', String(64), nullable=False)
    created = Column('created', TIMESTAMP, nullable=False, default=func.now())
