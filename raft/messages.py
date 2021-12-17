class ClientMsg:

  def __init__(self, sender, seqnum, command):  # seqnum not use
    self.type = BaseMessage.Client  # hack here
    self.sender = sender
    self.seqnum = seqnum
    self.command = command


class ClientMsgReply:

  def __init__(self, seqnum, success):
    self.seqnum = seqnum
    self.success = success


class BaseMessage:
  AppendEntries = 0
  AppendEntriesResponse = 1
  RequestVote = 2
  RequestVoteResponse = 3
  Heartbeat = 4
  Client = 5

  TypeToName = {
      AppendEntries: 'AppendEntries',
      AppendEntriesResponse: 'AppendEntriesResponse',
      RequestVote: 'RequestVote',
      RequestVoteResponse: 'RequestVoteResponse',
      Heartbeat: 'Heartbeat',
      Client: 'Client',
  }

  def __init__(self, sender, term):
    self.sender = sender
    self.term = term
    self.type = -1


class LogEntry:

  def __init__(self, term, index, command):
    self.term = term
    self.command = command
    self.index = index


class RequestVoteMsg(BaseMessage):

  def __init__(self, sender, term):
    BaseMessage.__init__(self, sender, term)
    self.type = BaseMessage.RequestVote


class RequestVoteResponseMsg(BaseMessage):

  def __init__(self, sender, term, voteGranted):
    BaseMessage.__init__(self, sender, term)
    self.type = BaseMessage.RequestVoteResponse
    self.voteGranted = voteGranted


class HeartbeatMsg(BaseMessage):

  def __init__(self, sender, term, commitIndex, prevLogIndex, prevLogTerm):
    BaseMessage.__init__(self, sender, term)
    self.type = BaseMessage.Heartbeat
    self.commitIndex = commitIndex
    self.prevLogTerm = prevLogTerm
    self.prevLogIndex = prevLogIndex


class AppendEntriesMsg(BaseMessage):

  def __init__(self, sender, term, entries, commitIndex, prevLogIndex,
               prevLogTerm):
    BaseMessage.__init__(self, sender, term)
    self.type = BaseMessage.AppendEntries
    self.entries = entries
    self.commitIndex = commitIndex
    self.prevLogTerm = prevLogTerm
    self.prevLogIndex = prevLogIndex

  def __str__(self):
    return '[Append] \n commitIndex {} \n prevLogIndex {}, prevLogTerm {} \n entries [{}]'.format(
        self.commitIndex, self.prevLogIndex, self.prevLogTerm,
        ','.join([e.command for e in self.entries]))


class AppendEntriesResponseMsg(BaseMessage):

  def __init__(self, sender, term, success, nextIndex):
    BaseMessage.__init__(self, sender, term)
    self.type = BaseMessage.AppendEntriesResponse
    self.success = success
    self.nextIndex = nextIndex

  def __str__(self):
    return '[AppendResponse] nextIndex {}'.format(self.nextIndex)
