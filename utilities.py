import os
import math
import json
import datetime

def update_log(log) :
    with open("current_config.json", "r") as current_config_file:
        current_config = json.load(current_config_file)

    namenode_log_file = open(current_config["namenode_log_path"], "a+")
    namenode_log_file.write(str(datetime.datetime.now()) + " : " + log + "\n")
    namenode_log_file.close()


def update_json(data, path) :
    file = open(path, "a+")
    file.seek(0)
    file.truncate(0)
    json.dump(data, file, indent=4)
    file.close()

def fileSplit(file_path, block_size) :
    file = open(file_path, "r")
    size = os.path.getsize(file_path)
    blocks = math.ceil(size / block_size)

    for _ in range(blocks):
        yield file.read(block_size)