project_structure = (
'''
/USER
│
├── /NAMENODE
│   ├── namenode_log.txt                     # Log file for all NameNode operations (puts, deletes, mapping updates)
│   │
│   ├── mapping_file.json                    # Directory structure and files hierarchy
│   │     ├── Example:
│   │     │   {
│   │     │     "/": ["input", "output"],
│   │     │     "/input": ["file1.txt", "file2.txt"],
│   │     │     "/output": []
│   │     │   }
│   │
│   ├── location_file.json                   # Tracks where each file’s blocks are physically stored (on DataNodes)
│   │     ├── Example:
│   │     │   {
│   │     │     "/input/file1.txt": [
│   │     │        ["DN1/block0", "DN2/block0", "DN3/block0"],   # replicas for Block 0
│   │     │        ["DN2/block1", "DN3/block1", "DN1/block1"]    # replicas for Block 1
│   │     │     ]
│   │     │   }
│   │
│   ├── datanode_tracker.json                # Tracks block occupancy across DataNodes
│   │     ├── Example:
│   │     │   {
│   │     │     "Next_datanode": 1,
│   │     │     "DN1": [0, 1, 0],   # 0=empty, 1=occupied
│   │     │     "DN2": [1, 1, 0],
│   │     │     "DN3": [0, 0, 0]
│   │     │   }
│   │
│   ├── checkpoints/                         # (Optional) Future extension for snapshots of mapping/location
│   │     ├── mapping_checkpoint_1.json
│   │     ├── location_checkpoint_1.json
│   │
│   └── secondary_namenode/ (optional)       # Could store periodic fsimage-style backups
│
│
├── /DATANODE
│   ├── /DATANODE_LOGS
│   │     ├── DN1.txt                       # Each DNx.log records allocation/removal timestamps for blocks
│   │     │    Example log entry:
│   │     │    2025-10-26 16:05:22 : block allocated block0
│   │     │    2025-10-26 16:08:01 : block removed block1
│   │     ├── DN2.txt
│   │     └── DN3.txt
│   │
│   └── /DataNodes
│         ├── /DN1
│         │     ├── block0
│         │     ├── block1
│         │     ├── block2
│         │
│         ├── /DN2
│         │     ├── block0
│         │     ├── block1
│         │
│         └── /DN3
│               ├── block0
│               ├── block1
│
│
└── /DFS
    ├── setup.json                           # Stores configuration (number of datanodes, replication, etc.)
    │     Example:
    │     {
    │       "num_datanodes": 3,
    │       "replication_factor": 2,
    │       "block_size": 1024,
    │       "path_to_datanodes": "/USER/DATANODE/",
    │       "path_to_namenodes": "/USER/NAMENODE/",
    │       "fs_path": "/USER/DFS/",
    │       "secondary_namenode_path": "/USER/NAMENODE/checkpoints/",
    │       "namenode_log_path": "/USER/NAMENODE/namenode_log.txt",
    │       "datanode_log_path": "/USER/DATANODE/DATANODE_LOGS/"
    │     }
    │
    └── /FILESYSTEM
          ├── user_home/
          ├── input/
          ├── output/
'''
)
