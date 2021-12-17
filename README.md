A simple-and-short version of raft implementation in python, only leader election and log-replication now.
## Simple raft demo
1. start raft cluster, which boots 5 raft servers on predefined ports in localhost
```
python start-raft.py
```
2. start client
```
python run-client.py
```
3. client will ask user to input next command. Current support:
   1. `debug` : print debug information including logs in server
   3. `exit` : exit program
   4. other string: append input string to raft log