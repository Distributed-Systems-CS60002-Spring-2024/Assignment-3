from enum import Enum
from typing import Annotated

import requests
from fastapi import Body,FastAPI,Response, status, Request
from fastapi.responses import RedirectResponse
from pydantic import BaseModel
import random
from app import crud
from app.validation import *
from app.database import SessionLocal, engine
from app.models import add_shard_meta_entry, add_map_meta_entry
from consistant_hash import ConsistantHash
from app.models import MapT

from apscheduler.schedulers.background import BackgroundScheduler
from flask import Flask, jsonify, request
from subprocess import Popen, PIPE

from utils import (
    get_container_rm_command,
    get_container_run_command,
    get_random_number,
    get_server_health,
    get_unhealty_servers,
)

import asyncio
import logging as log
import random
import requests

NETWORK_NAME = "load_balancer_network"

initial_servers = {}
ShardHashMaps = list()
givenSchema = dict()
lock_to_hashmap = {}

numServers = 0
active_servers = list()

lb = FastAPI()

def get_db():
    db = SessionLocal()
    try:
        return db
    finally:
        db.close

def get_servers_for_shard(shard_id : int) -> list[int]:
    session = SessionLocal()
    server_ids = session.query(Mapt.Server_id).filter(MapT.Shard_id == shard_id).all()

    server_ids = [result[0] for result in server_ids]

    return server_ids

def get_shards_id_from_strings(shards : list[str]) -> list[int]:
    return_list = list[int]
    for shard in shards:
        return_list.append(shard[2:])

    return return_list

def send_config_message(server, shard_list):
    try:
        server = "Server" + str(server)
        reply = requests.post(f'http://{server}:8000/config', json = {
                    "shard": shard_list,
                    "schema" : givenSchema
                })
        data = reply.json()
    except requests.exceptions.ConnectionError:
        message = '<ERROR> Could not configure the server'
        data = {'message': message, 'status': 'failure'}

    


def get_server_id(server_name : str) -> int:
    index = server_name.find("Server")
    temp_string = ""
    if(index != -1):
        temp_string = server_name[index + len("Server"):]
    else:
        return -1

    if(temp_string[0] == '['):
        # geneerate random ids
        return get_random_number(5)
    else:
        return int(temp_string)
    
def check_servers():
    global servers

    log.debug("Checking server health...")
    unhealthy_servers = asyncio.run(get_unhealty_servers(servers))
    print("Unhealthy servers: ", unhealthy_servers, flush=True)
    for server in unhealthy_servers:
        print(f"Removing {server}", flush=True)
        command = get_container_run_command(server, NETWORK_NAME)
        res = Popen(command, stdout=PIPE, stderr=PIPE)
        if res.returncode is not None:
            log.error(f"Error in adding {server}")
        else:
            log.info(f"Added {server}")


sched = BackgroundScheduler(daemon=True)
sched.add_job(check_servers, 'interval', seconds=30)
sched.start()



@lb.get("/")
async def root():
    return {"message:":"Hello from load balancer"}


@lb.route('/rep', methods=['GET'])
def rep():
    global servers

    healthy_servers = asyncio.run(get_server_health(servers))
    output = {
        'message': {
            'N': len(healthy_servers),
            'replicas': healthy_servers
        },
        'status': 'successful'
    }
    return jsonify(output), 200



@lb.post("/init")
async def init(request : Request):
    # how to take response for servers
    payload =  await request.json()

    print(payload)
    
    N = int(payload['N'])
    
    schema = payload['schema']
    shards = payload['shards']
    servers = payload['servers']

    msg=""
    shards_info = list()

    numServers = N

    givenSchema.update(schema)

    shard_ids = list()
    info_for_hashing = {}


    for info in shards:
        shard_data = dict()
        shard_data.update(info)

        shard_ids.append(info["Shard_id"])
        info_for_hashing.update({info["Shard_id"] : []})

        # what to keep in valid_ids
        shard_data.update({"valid_idx" : 0})

        shards_info.append(shard_data)

    # update the shardT table in the database
    if len(shards_info) > 0 :
       print(shards_info)
       add_shard_meta_entry(shards_info)

    # add the MapT table in the database
    mapping_info = list()
    for server, shard_list in servers.items():

        for shard in shard_ids:
            if shard in shard_list:
                # this means shard(string) and server(string) are associated
                # add in the datastructure
                info_for_hashing[shard].append(server)

        extracted_shard_list = [int(sh[2:]) for sh in shard_list]
        server_id = get_server_id(server)
        active_servers.append(server_id)
        mapping_info.append({"Server" : server_id, "Shards" : extracted_shard_list})

    if len(mapping_info): 
        add_map_meta_entry(mapping_info)

    # call /config on the servers after spawning them
    # no spawning of the servers


    
    # form the consistant hashmap of shards
    for shard, servers in info_for_hashing.items():
        newMap = ConsistantHash(shard_id=int(shard[2:]))
        newMap.build(server_list=servers)
        ShardHashMaps.append(newMap)

    # send the message
    msg="Configured Database"
    res=Config_response()
    res.message=msg
    res.status="success"
    return res


@lb.get("/status")
async def read_status():

    return_dict = {}
    return_dict.update({"schema" : givenSchema})

    session = SessionLocal()

    # get all the entries in ShardT
    shard_list = list()
    shard_entries = session.query('shardt').all()
    for entry in shard_entries:
        newdict = dict()
        newdict.update({"Stud_id_low" : entry["Stud_id_low"]})
        newdict.update({"Shard_id" : "sh" + str(entry["Shard_id"])})
        newdict.update({"Shard_size" : entry["Shard_size"]})
        shard_list.append(newdict)

    return_dict.update({"shards" : shard_list})

    # get all the entries in MapT
    map_entries = session.query('mapt').all()

    server_dict = dict()
    for entry in map_entries:
        server_dict.update({entry["Server_id"] : []})

    for entry in map_entries:
        server_dict[entry["Server_id"]].append(entry["Shard_id"])

    return_dict.update({"servers" : server_dict})

    return return_dict


@lb.post("/add")
async def add_server_shard(request : Request):
    payload = await request.json()

    n = int(payload['n'])
    new_shards = payload['new_shards']
    servers = payload['servers']


    if n > len(servers):
        return {
                "message" : "<Error> Number of new servers (n) is greater than newly added instances",
                "status" : "failure"
               }, 400
    
    # get the new shards

    # add them to shardt
    shards_info = list()
    new_added_shards = list()
    for shard in new_shards:
        shard_temp = dict()
        shard_temp.update(shard)

        new_added_shards.append(int(shard["Shard_id"][2:]))
        shard_temp.update({"valid_idx" : 0})

        shards_info.append(shard_temp)
    add_shard_meta_entry(shards_info)

    # get the servers corresponding to this new shard
    added_servers = []
    for server, shards in servers.items():
        server_id = get_server_id(server)
        server_name = "Server" + str(server_id)

        shards_id = get_shards_id_from_strings(shards)

        # call the config to the server
        # spawn the server
        hostname = server_name
        command = get_container_run_command(hostname, NETWORK_NAME)
        res = Popen(command, stdout=PIPE, stderr=PIPE)
        if res.returncode is not None:
            log.error(f"Error in adding {hostname}")
        else:
            log.info(f"Added {hostname}")

        # configure the server
            
        send_config_message(server_id, shards)
        numServers += 1

        added_servers.append(server_id)
        active_servers.append(server_id)

        #update the MapT table by adding server and shard mapping
        mapping_data = list()
        mapping_data.append({"Server" : server_id, "Shards" : shards_id})
        add_map_meta_entry(mapping_data)

        # update the hashmap
        for shard in shards_id:
            if shard in new_added_shards:
                # newly added shard
                newmap = ConsistantHash(shard_id=shard)
                newmap.add_server_to_hash(server_name)
                ShardHashMaps.append(newmap)

                # remove from the newly added shard
                new_added_shards.remove(shard)
            else:
                # shard already exists, just need to update
                for entry in ShardHashMaps:
                    if entry.shard_id == shard:
                        entry.add_server_to_hash(server_name)

    
    # sending the final response from the load balancer
    return_output = dict()
    message_string = "Add "
    for i in range(len(added_servers)):
        message_string += "Server:%s".format(added_servers[i])
        if i != len(added_servers) - 1:
            message_string += " and "


    return_output.update({"n" : numServers, "message" : message_string, "status" : "successful"})      




@lb.delete("/rm")
async def remove_server(request : Request):

    payload = await request.json()
    n = int(payload['n'])
    servers = payload['servers']

    if  n < len(servers):
        return {
            "message" : "<Error> Length of server list is more then removable instances",
            "status" : "failure"
        }, 400
    
    servers_to_remove = list()
    num_random = n
    # get the servers to remove
    for server in servers:
        servers_to_remove.append(get_server_id(server))
        active_servers.remove(get_server_id(server))
        num_random -= 1

    # select num_random server at random ro remove
    while(num_random):
        random_chosen_serve = random.choice(active_servers)
        active_servers.remove(random_chosen_serve)
        servers_to_remove.append(random_chosen_serve)
        num_random -= 1

    # we have the server_ids to remove from the database
    #----------------------------------------------------TURN OFF INSTANCES---------------------------------------------
    
    for server in servers_to_remove:
        # turn off
        server = "Server" + str(server)
        hostname = server
        command = get_container_rm_command(hostname)
        res = Popen(command, stdout=PIPE, stderr=PIPE)
        if res.returncode is not None:
            log.error(f"Error in removing {hostname}")
        else:
            log.info(f"Removed {hostname}")

        for hash in ShardHashMaps:
            hash.remove_server_from_hash(server)

    # update the mapt table
    # remove the entries in table corresponding to the removed servers
    session = SessionLocal()
    for rm_id in servers_to_remove:
        session.query(MapT).filter(MapT.Server_id == rm_id).delete()

    session.commit()
    

    # return the entries
    return_dict = {}
    return_dict.update({"message" : {"N" : len(active_servers), "servers" : ["Server"+str(sr) for sr in servers_to_remove]}, "status" : "successful"})

    return return_dict, 200

@lb.post("/read")
async def read_student_data(request : Request):
    payload = await request.json()
    Stud_id = payload['Stud_id']
    low = Stud_id["low"]
    high = Stud_id["high"]

    res = engine.execute("SELECT Shard_id FROM ShardT WHERE Stud_id_low <= %s AND Stud_id_low + Shard_size > %s".format(high,low))
    shard_list = res.fetchall()

    data = {}
    for shard in shard_list:
        # get the server corresponding to this shard
        server : int
        for entry in ShardHashMaps:
            if entry.shard_id == shard:

                # LOCKKKKK
                lock_to_hashmap[shard].acquire()
                server = entry.get_server_from_request(random.randint(0,99999))
                lock_to_hashmap[shard].release()

        # request the server for the data
        if server != None:
            try:
                reply = requests.post(f'http://{server}:{serverport}/read', json = {
                    "shard": shard,
                    "Stud_id": {"low": low, "high": high}
                })
                data[shard] = reply.json()
            except requests.exceptions.ConnectionError:
                message = '<ERROR> Server unavailable'
                data[shard] = {'message': message, 'status': 'failure'}
        else:
            message = '<ERROR> Server unavailable'
            data[shard] = {'message': message, 'status': 'failure'}

    final_data = []
    for shard in data:
        if data[shard]['status'] == 'success':
            final_data += data[shard]['data']

    return_dict = {}
    return_dict.update({"shards_queried" : shard_list, "data" : final_data, "status" : "success"})
    return return_dict, 200



@lb.put("/update")
async def update_entry(request : Request):
    payload = await request.json()
    Stud_id = payload["Stud_id"]
    data = payload["data"]

    res = engine.execute("SELECT Shard_id FROM ShardT WHERE Stud_id_low <= %s AND Stud_id_low + Shard_size > %s".format(Stud_id, Stud_id))
    shard = res.fetchone()

    # find the servers corresponding to this shard
    res = engine.execute("SELECT Server_id FROM mapt WHERE Shard_id = %s".format(shard))
    servers = res.fetchall()

    if len(servers) == 0:
        message = '<ERROR> No servers available for the shard'
        return {'message': message, 'status': 'failure'}, 400
    
    # update the entry in all servers

    lock_to_hashmap[shard].acquire()
    for server in servers:
        server = "Server" + str(server)
        try:
            reply = requests.put(f'http://{server}:8000/update', json = {
                "shard": shard,
                "Stud_id": Stud_id,
                "data": data
            })
            return reply.json(), reply.status_code
        except requests.exceptions.ConnectionError:
            message = '<ERROR> Server unavailable'
            return {'message': message, 'status': 'failure'}, 400
        
    lock_to_hashmap[shard].release()
    
    message = "Data entry for Stud_id:%s updated".format(Stud_id)


    return {'message': message, 'status':'success'}, 200
    

@lb.post("/write")
async def write_to_server(request : Request):
    payload = await request.json()
    data_to_write = payload["data"]

    # sort the data as per student id
    sorted_data = sorted(data_to_write, key=lambda x: x['Stud_id'])

    res = engine.execute("SELECT * FROM SHARDT")
    shard_data = res.fetchall()

    for shard in shard_data:
        data_to_insert = []
        while data_idx < len(sorted_data) and sorted_data[data_idx]['Stud_id'] < shard[0] + shard[2]:
            data_to_insert.append(sorted_data[data_idx])
            data_idx += 1
        
        if len(data_to_insert) == 0:
            continue
    
        
        # query all the servers having the shard
        res = engine.execute(f"SELECT Server_id FROM MapT WHERE Shard_id = {shard[1]}")
        servers = res.fetchall()

        # if no servers are available, return error
        if len(servers) == 0:
            message = '<ERROR> No servers available for the shard'
            return {'message': message, 'status': 'failure'}, 400
        
        lock_to_hashmap[shard[1]].acquire()
        for server in servers:
            try:

                shard_idx = shard[3]

                reply = requests.post(f'http://{server}:8000/write', json = {
                    "shard": shard[1],
                    "curr_idx": shard_idx,
                    "data": data_to_insert
                })

                if reply.status_code == 200:
                    newShardIndex = reply.json()['current_idx']

                
            except requests.exceptions.ConnectionError:
                message = '<ERROR> Server unavailable'
                return {'message': message, 'status': 'failure'}, 400
            
        # update the valid index of the shard
        session = SessionLocal()
        engine.execute(f"UPDATE ShardT SET valid_idx = {newShardIndex} WHERE Shard_id = {shard[1]}")

        session.commit()
        lock_to_hashmap[shard[1]].release()

    response ={
        "message": f"{len(sorted_data)} Data entries added",
        "status": "success"
    }

    return jsonify(response), 200


@lb.delete("/del")
async def delete(request : Request):
    payload = await request.json()
    Stud_id = payload["Stud_id"]

    res = engine.execute("SELECT Shard_id FROM ShardT WHERE Stud_id_low <= %s AND Stud_id_low + Shard_size > %s".format(Stud_id, Stud_id))
    shard = res.fetchone()

    # find the servers corresponding to this shard
    res = engine.execute("SELECT Server_id FROM mapt WHERE Shard_id = %s".format(shard))
    servers = res.fetchall()

    if len(servers) == 0:
        message = '<ERROR> No servers available for the shard'
        return {'message': message, 'status': 'failure'}, 400
    
    # update the entry in all servers
    lock_to_hashmap[shard].acquire()
    for server in servers:
        server = "Server" + str(server)
        try:
            reply = requests.delete(f'http://{server}:8000/del', json = {
                "shard": shard,
                "Stud_id": Stud_id,
            })
            return reply.json(), reply.status_code
        except requests.exceptions.ConnectionError:
            message = '<ERROR> Server unavailable'
            return {'message': message, 'status': 'failure'}, 400
        
    lock_to_hashmap[shard].release()
    
    message = "Data entry for Stud_id:%s deleted".format(Stud_id)


    return {'message': message, 'status':'success'}, 200