from sqlalchemy import Boolean, Column, ForeignKey, Integer, String
from sqlalchemy.orm import relationship
from app.validation import *
from .database import Base,engine
_type_lookup = {"Number":Integer,"String":String}

def init_db(schema:Schema,shard:str):
    clsdict = {"__tablename__":shard}
    columns=list(schema.columns)
    clsdict.update(
        {
        columns[i]:Column(_type_lookup[schema.dtypes[i].value],primary_key=(columns[i]=="Stud_id"))
        for i in range (0,len(columns))
        }
    )
    return type(shard,(Base,),clsdict)

def create_schema(schema,shard:str):
    Schema=init_db(schema,shard)
    Base.metadata.create_all(engine)
    return Schema
