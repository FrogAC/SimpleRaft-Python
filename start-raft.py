from raft.server import Server
import sys

if __name__ == '__main__':
  if len(sys.argv) == 2:
    Server.BaseInterval = float(sys.argv[1])
  nodes = {
    0 : 23333,
    1 : 23334,
    2 : 23335,
    3 : 23336,
    4 : 23337,
  }
  for nid in nodes:
    sv = Server(nid, nodes)
    sv.start()
