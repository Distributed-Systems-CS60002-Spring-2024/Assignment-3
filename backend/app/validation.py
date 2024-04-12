from pydantic import BaseModel
from enum import Enum
class Datatype(str,Enum):
    NUMBER="Number"
    STRING="String"
class Schema(BaseModel):
    columns:set[str]
    dtypes:list[Datatype]
class Config_response(BaseModel):
    message:str=None
    status:str=None
class copy_payload(BaseModel):
    shards:set[str]
class Student_id(BaseModel):
    low:int
    high:int
class read_payload(BaseModel):
    shard:str
    Stud_id:Student_id
class Student_details(BaseModel):
    Stud_id:int
    Stud_name:str
    Stud_marks:int
class write_payload(BaseModel):
    shard:str
    curr_idx:int
    data:list[Student_details]
class update_payload(BaseModel):
    shard:str
    Stud_id:int
    data:Student_details
class del_payload(BaseModel):
    shard:str
    Stud_id:int