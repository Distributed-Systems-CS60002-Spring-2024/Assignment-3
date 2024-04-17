from sqlalchemy import Boolean, Column, ForeignKey, Integer, String
from sqlalchemy.orm import relationship

from .database import Base,engine, SessionLocal
_type_lookup = {"Number":Integer,"String":String}

class ShardT(Base):
    __tablename__="shardt"
    # _id=Column(Integer, primary_key=True, autoincrement=True)
    Stud_id_low=Column(Integer)
    Shard_id=Column(Integer)
    Shard_size=Column(Integer)
    valid_idx=Column(Integer)

class MapT(Base):
    __tablename__="mapt"
    # _id=Column(Integer,primary_key=True,autoincrement=True)
    server_id=Column(Integer)
    Shard_id=Column(Integer)


def add_shard_meta_entry(shard_info) -> None:
    Base.metadata.create_all(engine)
    session = SessionLocal()

    all_entries = list()
    for shard in shard_info:
        stud_id = shard["Stud_id_low"]
        shard_id = int(shard["Shard_id"][2:])
        shard_size = shard["Shard_size"]
        valid_idx = shard["valid_idx"]

        new_entry = ShardT()
        new_entry.Shard_id = shard_id
        new_entry.Shard_size = shard_size
        new_entry.Stud_id_low = stud_id
        new_entry.valid_idx = valid_idx

        session.add(new_entry)
        all_entries.append(new_entry)

    print("###################################################################################################")
    print(all_entries)
    # session.add_all(all_entries)
    session.commit()

def add_map_meta_entry(mapping_info) -> None:
    Base.metadata.create_all(engine)
    session = SessionLocal()

    all_entries = []
    for entry in mapping_info:
        server_id = entry["Server"]
        shard_ids = entry["Shards"]

        for shard in shard_ids:
            new_mapping = MapT()
            new_mapping.Shard_id = shard
            new_mapping.server_id = server_id
            all_entries.append(new_mapping)

    session.add_all(all_entries)
    session.commit()