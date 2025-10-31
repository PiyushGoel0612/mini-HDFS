import os
import json
from utilities import update_json, update_log, fileSplit

current_config_file = open("current_config.json", "r")
current_config = json.load(current_config_file)

namenode = current_config["path_to_namenodes"]
datanode = current_config["path_to_datanodes"]

replicas = current_config["replication_factor"]
block_size = current_config["block_size"]
num_datanodes = current_config["num_datanodes"]

secondary_namenode_path = os.path.expandvars(current_config['secondary_namenode_path'])

def mkdir_command(path) :
    """
    Creates a new directory in the HDFS namespace under NameNode metadata.
    """
    mapping_file = open(namenode + "mapping_file.json", "r")
    mapping_data = json.load(mapping_file)

    par_path, user_dir = os.path.split(path)

    if path in mapping_data :
        mapping_file.close()
        update_log(f"Error encountered during mkdir command for {path} : Directory already exists")
        raise Exception(path, "Directory already exists")
    
    if par_path not in mapping_data :
        mapping_file.close()
        update_log(f"Error encountered during mkdir command on {path} : No parent directory found")
        raise Exception(par_path, "No such Directory")
    
    mapping_data[path] = []
    mapping_data[par_path].append(user_dir)
    update_log(f"Created new directory at {path}")
    update_json(mapping_data, namenode + "mapping_file.json")
    return

def rmdir_command(path) :
    """
    Removes an empty directory from NameNode metadata. Throws error if not empty.
    """
    mapping_file = open(namenode + "mapping_file.json", "r")
    mapping_data = json.load(mapping_file)

    par_path, user_dir = os.path.split(path)

    if not path in mapping_data:
        mapping_file.close()
        update_log(f"Error encountered during rmdir command on {path} : No such directory")
        raise Exception(path,"No such directory")

    if len(mapping_data[path]) != 0:
        mapping_file.close()
        update_log(f"Error encountered during rmdir commad on {path} : Directory is not empty")
        raise Exception(path,"Directory is not empty")
    
    del mapping_data[path]

    if par_path in mapping_data:
        mapping_data[par_path].remove(user_dir)

    update_log(f"Deleted directory at {path}")
    update_json(mapping_data, namenode + "mapping_file.json")
    return

def ls_command(path) :
    """
    Lists all files and subdirectories present at the given HDFS path.
    """
    mapping_file = open(namenode + "mapping_file.json", "r")
    mapping_data = json.load(mapping_file)

    if path not in mapping_data :
        mapping_file.close()
        raise Exception(path, "No such Directory")
    
    for entry in mapping_data[path] :
        if path + entry in mapping_data :
            print(f"{entry} (Directory)")
        else :
            print(f"{entry} (File)")
    
    mapping_file.close()
    return

def put_command(source, dest) :
    """
    Uploads a local file to HDFS, splits it into blocks, and distributes replicas across DataNodes. 
    Updates all NameNode metadata files (mapping_file.json, location_file.json, datanode_tracker.json).
    """
    mapping_file = open(namenode + "mapping_file.json", "r")
    mapping_data = json.load(mapping_file)
    src_par, src_file = os.path.split(source)

    if dest not in mapping_data :
        mapping_file.close()
        raise Exception(dest, "No such Directory")
    
    if src_file in mapping_data[dest] :
        mapping_file.close()
        raise Exception(dest + "/" + src_file, "File already exists")
    
    mapping_data[dest].append(src_file)
    update_json(mapping_data, namenode + "mapping_file.json")
    update_log(f"mapping file updated for {source} to {dest} put command")

    datanode_tracker_file = open(namenode + "datanode_tracker.json", "r")
    datanode_tracker_data = json.load(datanode_tracker_file)

    location_file = open(namenode + "location_file.json", "r")
    location_file_data = json.load(location_file)

    blocks = []
    file_split = fileSplit(source, block_size)

    for split_no, splt in enumerate(file_split) :
        blocks.append([])

        for _ in range(replicas) :
            next_block = None

            for i in range(num_datanodes) :
                i = (datanode_tracker_data["Next_datanode"] + i - 1) % num_datanodes + 1
                if 0 not in datanode_tracker_data["DN" + str(i)] : 
                    continue
                idx = datanode_tracker_data["DN" + str(i)].index(0)
                datanode_tracker_data["DN" + str(i)][idx] = 1
                next_block = "DN" + str(i) + "/block" + str(idx)
                datanode_tracker_data["Next_datanode"] = i % num_datanodes + 1
                break

            if next_block is None :
                update_log(f"Error encountered during pu command from {source} to {dest}")
                raise Exception("No datanodes free")
        
            blocks[split_no].append(next_block)

            file = open(datanode + "DataNodes/" + next_block,'w')
            file.write(splt)
            file.close()
    
    location_file_data[dest + "/" + src_file] = blocks

    update_log(f"New user file added : {dest + src_file}")
    update_json(location_file_data, namenode + "location_file.json")
    update_log(f"location_file updated for {source} to {dest} put command")
    update_json(datanode_tracker_data, namenode + "datanode_tracker.json")
    update_log(f"datanode_tracker updated for {source} to {dest} put command")

    location_file.close()
    datanode_tracker_file.close()

    return

def cat_command(path) :
    """
    Reads and prints the entire file stored in HDFS by reassembling its blocks from DataNodes.
    """
    location_file = open(namenode + "location_file.json", "r")
    location_file_data = json.load(location_file)

    if path not in location_file_data :
        location_file.close()
        raise Exception(path, "No such file exists.")

    for split in location_file_data[path] :
        for replica_block in split :
            replica_block = current_config["path_to_datanodes"] + "/DataNodes/" + replica_block
            if os.path.isfile(replica_block) :
                content = open(replica_block, "r").read()

                print(content, end = "")
                break
    
    return

def rm_command(path) :
    """
    Deletes a file from HDFS, removes its blocks from DataNodes, and updates metadata.
    """
    split = os.path.split(path)
    par_path, file = split[0], split[1]

    location_file = open(namenode + "location_file.json",'r+')
    location_data = json.load(location_file)

    if not path in location_data:
        location_file.close()
        raise Exception(path, "File does not exist")


    datanode_tracker = open(namenode + 'datanode_tracker.json', 'r+')
    datanode_details = json.load(datanode_tracker)

    for replica in location_data[path]:
        for file_blk in replica:
            DN_str, block = file_blk.split('/')
            blocknum = int(block[5:]) - 1

            datanode_details[DN_str][blocknum] = 0

            block_path = datanode + "DataNodes/" + file_blk
            os.remove(block_path)

    del location_data[path]
    update_log(f"File is removed from path : {path}")

    mapping_file = open(namenode + "mapping_file.json",'r+')
    mapping_data = json.load(mapping_file)
    mapping_data[par_path].remove(file)

    update_json(mapping_data, mapping_file)
    update_log(f"Mapping file updated for rm command of {path}")
    update_json(location_data, location_file)
    update_log(f"Location data file updated for rm command of {path}")
    update_json(datanode_details, datanode_tracker)
    update_log(f"Datanode tracker file updated for rm command of {path}")
