from enum import Enum
from typing import Annotated
from fastapi import Body,FastAPI,Response, status, HTTPException, Request
from fastapi.responses import RedirectResponse
from pydantic import BaseModel
import requests
import os
import datetime
# from .models import create_schema
from app.models import create_schema
from app import crud
from app.validation import *
from app.database import SessionLocal

SERVER_ID = os.environ.get('SERVER_ID')
SERVER_NAME = os.environ.get('SERVER_NAME')
app = FastAPI()
def get_db():
    db = SessionLocal()
    try:
        return db
    finally:
        db.close
@app.get("/")
async def root():
    return {"message:":"Hello from server"}

@app.post("/config")
async def config(schema:Schema,shards:Annotated[set[str],Body()])->Config_response:
    msg=""
    for shard in shards:
        table=create_schema(schema,shard)
        print(table)
        msg+=f"Server{SERVER_ID}:{shard}, "
    msg+="configured"
    res=Config_response()
    res.message=msg
    res.status="success"
    return res

@app.get("/heartbeat")
async def get_heartbeat():
    return {}

@app.post("/copy")
async def copy(shards:copy_payload):
    db=get_db()
    data=dict()
    for shard in shards.shards:
        data.update({shard:crud.read(db,shard)})
    data.update({"status":"success"})
    return data

@app.post("/read")
async def read(body:read_payload):
    db=get_db()
    data=crud.read(db,body.shard,body.Stud_id.low,body.Stud_id.high)
    response=dict()
    response.update({"data":data})
    response.update({"status":"success"})
    return response

@app.post("/write_old")
async def write(body:write_payload,response:Response):
    db=get_db()
    ret=crud.write(db,body.shard,body.data)
    res=dict()
    if ret!=-1:
        res.update({"message":f"student with Stud_id:{ret} already exists"})
        res.update({"status":"failure"})
        response.status_code=400
    else:
        res.update({"message":"Data entries added"})
        res.update({"current_idx":body.curr_idx+len(body.data)})
        res.update({"status":"success"})
        response.status_code=200
    return res
import hashlib
def calculate_hash(input_string, algorithm='sha256'):
    # Convert the input string to bytes
    input_bytes = input_string.encode('utf-8')
    
    # Initialize a hash object with the specified algorithm
    hasher = hashlib.new(algorithm)
    
    # Update the hash object with the input bytes
    hasher.update(input_bytes)
    
    # Get the hexadecimal digest of the hash
    hashed_string = hasher.hexdigest()
    
    return hashed_string
@app.post("/write")
async def write(body: Request,response:Response):
    now=datetime.now()
    timestamp = datetime.timestamp(now)
    global is_primary_server
    data = await body.json()
 
    
    if is_primary_server == 1:
        secondary_servers = requests.get(f"http://localhost:5001/secondary?shard={body.shard}").json()
        maj_cnt = sum(1 for server in secondary_servers if requests.post(f"http://{server}/write", json=data).status_code == 200)
        if maj_cnt < len(secondary_servers) / 2:
            raise HTTPException(status_code=500, detail="Write failed")
    
    session = SessionLocal()
    try:
        cnt = 0
        with open(f"{SERVER_ID}.log", "a") as log_file:
            for row in body.data:
                values = list(row.values())
                db=get_db()
                ret=crud.write(db,body.shard,body.data)
                res=dict()
                if ret!=-1:
                    res.update({"message":f"student with Stud_id:{ret} already exists"})
                    res.update({"status":"failure"})
                    response.status_code=400
                else:
                    res.update({"message":"Data entries added"})
                    res.update({"current_idx":body.curr_idx+len(body.data)})
                    res.update({"status":"success"})
                    response.status_code=200
                    log_str=f"{body.shard}-{row}-{timestamp}"
                    sha256=calculate_hash(log_str)
                    log_file.write(f"{sha256}-{log_str}\n")
                    cnt += 1
            log_file.close()
        session.commit()
    except Exception as e:
        session.rollback()
        raise HTTPException(status_code=500, detail="Database error")
    finally:
        session.close()

    if cnt == 0:
        raise HTTPException(status_code=500, detail="All data entries already exist")
    return {"message": "Data entries added", "status": "success"}

@app.put("/update_old")
async def update(body:update_payload,response:Response):
    db=get_db()
    result=crud.update(db,body.shard,body.Stud_id,body.data)
    res=dict()
    if(result==1):
        res.update({"message":f"Data entry for Stud_id:{body.Stud_id} updated"})
        res.update({"status":"success"})
        response.status_code=200
    else:
        res.update({"message":f"Stud_id:{body.Stud_id} doesn't exist"})
        res.update({"status":"failure"})
        response.status_code=404

    return res
@app.put("/update")
async def update(body:update_payload,response:Response):
    now=datetime.now()
    timestamp = datetime.timestamp(now)
    global is_primary_server

    if is_primary_server == 1:
        secondary_servers = requests.get(f"http://localhost:5001/secondary?shard={body.shard}").json()
        maj_cnt = sum(1 for server in secondary_servers if requests.post(f"http://{server}/write", json=data).status_code == 200)
        if maj_cnt < len(secondary_servers) / 2:
            raise HTTPException(status_code=500, detail="Write failed")
    
    session = SessionLocal()
    try:
        cnt = 0
        with open(f"{SERVER_ID}.log", "a") as log_file:
            for row in body.data:
                values = list(row.values())
                db=get_db()
                ret=crud.update(db,body.shard,body.data)
                res=dict()
                if ret==1:
                    res.update({"message":f"student with Stud_id:{ret} already exists"})
                    res.update({"status":"failure"})
                    response.status_code=400
                else:
                    res.update({"message":"Data entries added"})
                    res.update({"current_idx":body.curr_idx+len(body.data)})
                    res.update({"status":"success"})
                    response.status_code=200
                    log_str=f"{body.shard}-{row}-{timestamp}"
                    sha256=calculate_hash(log_str)
                    log_file.write(f"{sha256}-{log_str}\n")
                    cnt += 1
            log_file.close()
        session.commit()
    except Exception as e:
        session.rollback()
        raise HTTPException(status_code=500, detail="Database error")
    finally:
        session.close()

    if cnt == 0:
        raise HTTPException(status_code=500, detail="All data entries already exist")
    return {"message": "Data entries added", "status": "success"}



@app.delete("/del")
async def delete(body: del_payload, response: Response):
    now=datetime.now()
    timestamp = datetime.timestamp(now)
    global is_primary_server

    session = SessionLocal()
            # Send request to secondary servers if primary server
    if is_primary_server == 1:
        secondary_servers = requests.get(f"http://localhost:5001/secondary?shard={body.shard}").json()
        for server in secondary_servers:
            requests.delete(f"http://{server}/del", json={"shard": body.shard, "Stud_id": body.Stud_id})
    try:
        result = crud.delete(session, body.shard, body.Stud_id)
        if result == 1:
            response.status_code = 200
            res = {"message": f"Data entry for Stud_id:{body.Stud_id} deleted", "status": "success"}
        else:
            response.status_code = 404
            res = {"message": f"Stud_id:{body.Stud_id} doesn't exist", "status": "failure"}

        # Log the deletion
        with open(f"{SERVER_ID}.log", "a") as log_file:
            log_str=f"{body.shard}-{body.data}-{timestamp}"
            sha256=calculate_hash(log_str)
            log_file.write(f"{sha256}-{log_str}\n")
        session.commit()
    except Exception as e:
        session.rollback()
        raise HTTPException(status_code=500, detail="Database error")
    finally:
        session.close()





