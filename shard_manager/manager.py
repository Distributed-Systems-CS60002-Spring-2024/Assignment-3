from fastapi import FastAPI, HTTPException, BackgroundTasks
from pydantic import BaseModel
import docker
import random
import requests
import time
from threading import Thread
from sqlalchemy import create_engine, Column, String, Integer, Boolean
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

app = FastAPI()
client = docker.from_env()
network = "assgn3"
image = "server"
mysql_container = client.containers.get("database")
mysql_ip = mysql_container.attrs["NetworkSettings"]["Networks"]["assgn3"]["IPAddress"]
SQLALCHEMY_DATABASE_URL = f"mysql+pymysql://root:test@{mysql_ip}/meta_data"
engine = create_engine(SQLALCHEMY_DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

class MapT(Base):
    __tablename__ = "MapT"
    Shard_id = Column(Integer, primary_key=True, index=True)
    Server_id = Column(String, index=True)
    Primary_server = Column(Boolean, default=False)

Base.metadata.create_all(bind=engine)

class PrimaryElectRequest(BaseModel):
    shard_id: int

class PrimaryElectResponse(BaseModel):
    status: str
    message: str
    server_id: str

class AddServerRequest(BaseModel):
    server_name: str
    server_id: str

class AddServerResponse(BaseModel):
    status: str
    message: str

def send_heartbeats(name, server_id):
    while True:
        with SessionLocal() as session:
            result = session.query(MapT).filter(MapT.Server_id == name).all()
            if not result:
                break
            time.sleep(0.5)
            server = client.containers.get(name)
            ip_addr = server.attrs["NetworkSettings"]["Networks"]["n1"]["IPAddress"]
            try:
                requests.get(f"http://{ip_addr}:5000/heartbeat")
            except docker.errors.NotFound:
                with SessionLocal() as session:
                    result = session.query(MapT).filter(MapT.Server_id == name).all()
                    if not result:
                        break
                    client.containers.run(image=image, name=server, network=network, detach=True, environment={'SERVER_ID': server_id})
                    server = client.containers.get(name)
                    ip_addr = server.attrs["NetworkSettings"]["Networks"]["n1"]["IPAddress"]
                    result = session.query(MapT).filter(MapT.Server_id == name).all()
                    servers = {}
                    for row in result:
                        shard = row.Shard_id
                        result = session.query(MapT).filter(MapT.Shard_id == shard, MapT.Server_id != name).first()
                        if result:
                            serv = result.Server_id
                            servers.setdefault(serv, []).append(shard)
                for serv, shards in servers.items():
                    cont = client.containers.get(serv)
                    ip = cont.attrs["NetworkSettings"]["Networks"]["n1"]["IPAddress"]
                    data = {"shards": shards}
                    response = request.get(f"http://{ip}:5000/copy", json=data)
                    time.sleep(0.25)
                    if response.status_code == 200:
                        response_data = response.json()
                        for k, v in response_data.items():
                            post_data = {"shard": k, "data": v}
                            requests.post(f"http://{ip_addr}:5000/write", json=post_data)
                            time.sleep(0.25)
            except Exception as e:
                with SessionLocal() as session:
                    result = session.query(MapT).filter(MapT.Server_id == name).all()
                    if not result:
                        break
                    container = client.containers.get(server)
                    container.restart()
                    print(f"Server {name} is down, restarting")

@app.post("/primary_elect", response_model=PrimaryElectResponse)
async def primary_elect(request_data: PrimaryElectRequest):
    with SessionLocal() as session:
        result = session.query(MapT).filter(MapT.Shard_id == request_data.shard_id).all()
        if not result:
            raise HTTPException(status_code=400, detail="No server contains the shard replica")
        server_id = random.choice(result).Server_id
        session.query(MapT).filter(MapT.Shard_id == request_data.shard_id, MapT.Server_id == server_id).update({"Primary_server": True})
        session.commit()
    return {"status": "successful", "message": "Primary server elected", "server_id": server_id}

@app.post("/add_server", response_model=AddServerResponse)
async def add_server(request_data: AddServerRequest):
    thread = Thread(target=send_heartbeats, args=(request_data.server_name, request_data.server_id))
    thread.start()
    return {"status": "successful", "message": "Server added"}
@app.get("/secondary")
async def get_secondary_servers(shard: int):
    try:
        with SessionLocal() as session:
            result = session.query(MapT.Server_id).filter(MapT.Shard_id == shard, MapT.Primary_server == False).all()
            servers = [row[0] for row in result]
        return {"status": "successful", "message": "Secondary servers retrieved", "servers": servers}
    except Exception as e:
        raise HTTPException(status_code=400, detail="Error retrieving secondary servers")
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=5000)