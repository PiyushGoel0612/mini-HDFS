import json
import shutil
import os
from utilities import update_log
import time

current_config = open("current_config.json", "r")
current_config = json.load(current_config)

def namenode_heartbeat() :

    try :
        location_file = open(current_config["path_to_namenodes"] + "location_file.json", "r")
        location_file_data = json.load(location_file)
    except :
        raise Exception("Namenode failure")
    
    prefix = current_config["path_to_datanodes"]+"DataNodes/"
    for file in location_file_data :
        for replicas in location_file_data[file] :
            for i in range(len(replicas)) :
                if not os.path.isfile(prefix + replicas[i]) :
                    update_log(f"Datanode {replicas[i]} doesnt exist, recreating !")
                    if i != 0 :
                        shutil.copy(prefix + replicas[0], prefix + replicas[i])
                    else :
                        while not os.path.isfile(prefix + replicas[i]) :
                            i += 1
                        for j in range(i) :
                            shutil.copy(prefix + replicas[i], prefix + replicas[j])
    
    print("Check complete !!")
    return

while(True):
    time.sleep(current_config["sync_period"])
    namenode_heartbeat()