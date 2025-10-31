import os
import json
from utilities import update_json

def load_function(setup_file_path = "current_config.json") :

    try : 
        config_file = open(setup_file_path, "r")
        config_file_data = json.load(config_file)
    except :
        raise Exception(setup_file_path, "File doesnt exist !")
    
    if setup_file_path != "current_config.json" :
        update_json(config_file_data, "current_config.json")
    
    datanode = os.path.expandvars(config_file_data["path_to_datanodes"])
    namenode = os.path.expandvars(config_file_data["path_to_namenodes"])

    if os.path.isdir(datanode) and os.path.isdir(namenode) :
        print("DFS Loaded succesfully")
        return True
    else :
        print("DFS Load unsuccessful")
        return False
