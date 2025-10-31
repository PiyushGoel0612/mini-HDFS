import os
import json
import datetime
from shutil import copytree
from utilities import update_json , update_log

def init_DFS(config_file_path):
    config_file = open(config_file_path)
    config = json.load(config_file)

    datanode = os.path.expandvars(config['path_to_datanodes'])
    namenode = os.path.expandvars(config['path_to_namenodes'])
    datanode_logs = os.path.expandvars(config['datanode_log_path'])

    if os.path.isdir(datanode):
        raise Exception("hdfs already exists")

    os.mkdir(datanode)
    os.mkdir(namenode)
    os.mkdir(datanode_logs)

    name_node_logfile_path = os.path.expandvars(config['namenode_log_path'])
    namenode_log_file = open(name_node_logfile_path,'a+')
    namenode_log_file.close()
    update_log("Created namenode log file")

    mapping_file = open(namenode + 'mapping_file.json','w')
    update_log("Created namenode mapping file")

    location_file = open(namenode + 'location_file.json','w')
    update_log("Created namenode location file")

    datanode_tracker = open(namenode + 'datanode_tracker.json', 'w')
    update_log("Created namenode datatracker file")

    datanode_data = {}
    datanode_data['Next_datanode'] = 1

    datanode_size = config['datanode_size']						

    directory = 'DataNodes'
    cur_path = os.path.join(datanode, directory)
    os.mkdir(cur_path)

    no_of_nodes = config['num_datanodes']
    for i in range(no_of_nodes):
        dirname = 'DN' + str(i+1)
        path = os.path.join(cur_path, dirname)
        os.mkdir(path)

        update_log(f"Created datanode at {path}")
        logpaths = os.path.join(datanode_logs,dirname)
        logpaths = logpaths +".txt"

        datanode_data[dirname] = [0 for _ in range(datanode_size)]

    datanode_tracker.write(json.dumps(datanode_data, indent=4))
    mapping_file.write(json.dumps({'/' : []}, indent=4))
    location_file.write(json.dumps({}, indent=4))

    datanode_tracker.close()
    mapping_file.close()
    location_file.close()

    os.mkdir(config["secondary_namenode_path"])
    copytree(namenode, config["secondary_namenode_path"], dirs_exist_ok=True)

    return True