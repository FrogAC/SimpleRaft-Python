'''
simple raft implmentation
'''

from threading import Thread, Condition, RLock
import time, random, socket
import pickle

from .messages import *



class Server:
  BaseInterval = 2.0
  Follower = 0
  Candidate = 1
  Leader = 2
  StateToName = {Follower: 'Follower', Candidate: 'Candidate', Leader: 'Leader'}

  def __init__(self, nodeId, nodes):

    self.id = nodeId
    self.leaderId = -1
    self.state = -1
    self.currentTerm = 0
    self.votedFor = -1
    self.voteCount = 0

    self.log = []

    self.nodes = nodes
    self.port = nodes[self.id]
    self.clients = []  # queue of client requests (client, seqnum, logindex)

    self.commitIndex = 0  # index of highest log entry known to be committed
    self.lastApplied = 0  # index of highest log entry applied to state machine

    self.electionTimer = 0.0

    # leader states
    self.nextIndex = {}
    self.matchIndex = {}

    self.cv = Condition()

  def send(self, msg, receiver):
    so = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    data = pickle.dumps(msg)
    so.sendto(data, ('localhost', self.nodes[receiver]))
    so.close()

  def sendClient(self, msg, client):
    so = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    data = pickle.dumps(msg)
    so.sendto(data, ('localhost', client))
    so.close()

  def broadcast(self, msg):
    so = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    data = pickle.dumps(msg)
    for n in self.nodes:
      if n == self.id: continue
      so.sendto(data, ('localhost', self.nodes[n]))
    so.close()

  def listenFn(self):
    so = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    so.bind(("localhost", self.port))
    while True:
      data, addr = so.recvfrom(1024)
      th = Thread(target=raftReceiver, args=(self, pickle.loads(data), addr))
      th.start()
    so.close()

  def printDebug(self):
    print(
        '[Debug] \n id {} \n term {} \n state {} \n leader {}, vote for {} \n log [{}]'
        .format(self.id, self.currentTerm, self.StateToName[self.state],
                self.leaderId, self.votedFor,
                ','.join([l.command for l in self.log])))

  def start(self):
    # thread handle states, start as follower
    th = Thread(target=raftMain, args=(self,))
    th.start()
    # thread handle message responses
    th = Thread(target=self.listenFn, args=())
    th.start()


## RAFT FUNCTIONS #################################


def changeState(sv: Server, state):
  print(sv.id, 'term', sv.currentTerm, "change to state",
        Server.StateToName[state])
  sv.state = state
  if sv.state == Server.Follower:
    sv.electionTimer = time.time()
  if state == Server.Candidate:
    # start election
    sv.currentTerm += 1
    sv.votedFor = sv.id
    sv.electionTimer = time.time()
    sv.voteCount = 1
    msg = RequestVoteMsg(sv.id, sv.currentTerm)
    sv.broadcast(msg)
  elif state == Server.Leader:
    sv.leaderId = sv.id
    # set heartbeat timer
    sv.heartbeatTimer = time.time() - Server.BaseInterval
    # init states
    sv.nextIndex = {}
    sv.matchIndex = {}
    for node in sv.nodes:
      sv.nextIndex[node] = len(sv.log)
      sv.matchIndex[node] = 0


def raftMain(sv: Server):
  with sv.cv:
    # init to follower
    sv.currentTerm = 0
    sv.votedFor = -1
    changeState(sv, Server.Follower)

  actionTimer = time.time()
  # main loop
  while True:
    with sv.cv:
      if time.time() - actionTimer < 0.5 * Server.BaseInterval:
        continue
      actionTimer = time.time()

      if sv.state == Server.Follower:
        timeout = Server.BaseInterval + Server.BaseInterval * random.random()
        if time.time() - sv.electionTimer > timeout:
          print(sv.id, 'follower timeout')
          changeState(sv, Server.Candidate)

      elif sv.state == Server.Candidate:
        timeout = Server.BaseInterval + Server.BaseInterval * random.random()
        if time.time() - sv.electionTimer > timeout:
          print(sv.id, 'election timeout')
          changeState(sv, Server.Candidate)

      elif sv.state == Server.Leader:
        for node in sv.nodes:
          if node == sv.id: continue
          prevLogIndex = sv.nextIndex[node] - 1
          prevLogTerm = 0
          if prevLogIndex >= 0:
            prevLogTerm = sv.log[prevLogIndex].term
          # AppendEntriesMsg or Heartbeat
          if len(sv.log) - 1 >= sv.nextIndex[node]:
            entries = sv.log[prevLogIndex + 1:]
            msg = AppendEntriesMsg(sv.id, sv.currentTerm, entries,
                                   sv.commitIndex, prevLogIndex, prevLogTerm)
          else:
            msg = HeartbeatMsg(sv.id, sv.currentTerm, sv.commitIndex,
                               prevLogIndex, prevLogTerm)

          sv.send(msg, node)


def raftReceiver(sv: Server, msg, addr):
  with sv.cv:
    # print(sv.id, 'term', sv.currentTerm, 'receive message of type',
          # BaseMessage.TypeToName[msg.type])
    ## Client #########################
    if msg.type == BaseMessage.Client:
      msgSender = msg.sender
      msgCommand = msg.command
      msgSeqnum = msg.seqnum

      if msgCommand == 'debug':
        sv.printDebug()
      else:
        if sv.state == Server.Leader:
          # is leader: append to local, record
          sv.log.append(LogEntry(sv.currentTerm, len(sv.log), msgCommand))
          sv.clients.append((msgSender, msgSeqnum, len(sv.log) - 1))
          sv.commitIndex += 1

        else:
          # not leader: redirect to leader
          if sv.leaderId > 0:
            sv.send(msg, sv.leaderId)
          else:
            sv.sendClient(ClientMsgReply(msgSeqnum, False), msgSender)

      return

    ## Raft ###########################
    msgSender = msg.sender
    msgTerm = msg.term
    msgType = msg.type

    # rules for all servers
    if msgTerm > sv.currentTerm:
      sv.currentTerm = msgTerm
      sv.votedFor = -1
      changeState(sv, Server.Follower)

    # rules for each states
    if msgType == BaseMessage.RequestVote:
      if msgTerm >= sv.currentTerm and (sv.votedFor == -1 or
                                        sv.votedFor == msgSender):
        voteGranted = True
        sv.votedFor = msgSender
      else:
        voteGranted = False
      reply = RequestVoteResponseMsg(sv.id, sv.currentTerm, voteGranted)
      sv.send(reply, msgSender)

    elif msgType == BaseMessage.RequestVoteResponse:
      # if vote is for me
      if sv.state == Server.Candidate and msgTerm <= sv.currentTerm and msg.voteGranted:
        # check if I can become leader
        sv.voteCount += 1
        if sv.voteCount > (len(sv.nodes) - 1) // 2:
          changeState(sv, Server.Leader)

    elif msgType == BaseMessage.Heartbeat or msgType == BaseMessage.AppendEntries:
      sv.electionTimer = time.time()

      # if current election has end
      if sv.state == Server.Follower:
        sv.leaderId = msgSender
      if sv.state == Server.Candidate and msgTerm >= sv.currentTerm:
        # become follower
        sv.leaderId = msgSender
        changeState(sv, Server.Follower)

      # handle appendentires
      if msgType == BaseMessage.AppendEntries:
        # print(msg)

        msgEntries = msg.entries
        msgCommitIndex = msg.commitIndex
        msgPrevLogIndex = msg.prevLogIndex
        msgPrevLogTerm = msg.prevLogTerm

        # False if term or prevLogterm not exist
        if msgTerm < sv.currentTerm:
          sv.send(AppendEntriesResponseMsg(sv.id, sv.currentTerm, False, -1),
                  msgSender)
          return
        if len(sv.log) <= msgPrevLogIndex:
          print('bad prevLogIndex')
          sv.send(AppendEntriesResponseMsg(sv.id, sv.currentTerm, False, -1),
                  msgSender)
          return
        if msgPrevLogIndex >= 0 and sv.log[msgPrevLogIndex].term != msgPrevLogTerm:
          print('bad prevLogTerm', sv.log[msgPrevLogIndex].term,'!=',msgPrevLogTerm)
          sv.send(AppendEntriesResponseMsg(sv.id, sv.currentTerm, False, -1),
                  msgSender)
          return

        # logs conflicting
        newi = -1
        ei = 0
        for i, entry in enumerate(msgEntries):
          if len(sv.log) > entry.index and sv.log[entry.index].term != entry.term:
            # remove all logs after conflicting
            newi = entry.index
            ei = i
            sv.log = sv.log[:newi]
            break
        # print('new log at', newi, 'from entry', ei)
        # add new entry
        sv.log += msgEntries[ei:]
        # update commitindex
        if msgCommitIndex > sv.commitIndex:
          sv.commitIndex = min(msgCommitIndex,
                               msgPrevLogIndex + len(msgEntries[ei:]))
        
        nextIndex = sv.commitIndex + 1
        sv.send(
            AppendEntriesResponseMsg(sv.id, sv.currentTerm, True,
                                     nextIndex), msgSender)

    elif msgType == BaseMessage.AppendEntriesResponse:
      # print(msg)

      # leader only
      if sv.state != Server.Leader or sv.currentTerm != msgTerm: return
      if msg.success:
        # success: update nextIndex, matchIndex
        sv.nextIndex[msgSender] = msg.nextIndex
        sv.matchIndex[msgSender] = sv.nextIndex[msgSender] - 1
        # also reply clients
        if len(sv.clients) > 0:
          minMatchIndex = min([i for n,i in sv.matchIndex.items() if n != sv.id])
          client, seqnum, index = sv.clients[0]
          # print('clients', client, seqnum, index, 'min', minMatchIndex)
          while index is not None and minMatchIndex >= index:
            # print('min index', minMatchIndex, 'index', index)
            sv.sendClient(ClientMsgReply(seqnum, True), client)
            sv.clients.pop(0)
            index = None
            if len(sv.clients) > 0:
              client, seqnum, index = sv.clients[0]
      else:
        # false: dec nextIndex, retry
        sv.nextIndex[msgSender] -= 1
