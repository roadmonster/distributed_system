"""
Define Message types needed for Raft communication
Author: Hao Li
2022-07-01
This is a free software release to the public
"""


class BaseMessage(object):
    AppendEntries = 0
    RequestVote = 1
    RequestVoteResponse = 2
    AppendEntriesResponse = 3

    def __init__(self, sender, receiver, term):
        self.sender = sender
        self.receiver = receiver
        self.term = term

class RequestVoteMsg(BaseMessage):

    def __init__(self, sender, receiver, term, data):
        super().__init__(self, sender, receiver, term)
        self.data = data
        self.type = BaseMessage.RequestVote


class VoteResponseMsg(BaseMessage):

    def __init__(self, sender, receiver, term, data):
        super().__init__(self, sender, receiver, term)
        self.type = BaseMessage.RequestVoteResponse
        self.data = data

class AppendEntriesMsg(BaseMessage):
    def __init__(self, sender, receiver, term, entries, commitIndex, prevLogIndex, prevLogTerm):
        super().__init__(self, sender, receiver, term)
        self.type = BaseMessage.AppendEntries
        self.entries = entries
        self.commitIndex = commitIndex
        self.prevLogTerm = prevLogTerm
        self.prevLogIndex = prevLogIndex

class AppendEntriesResponseMsg(BaseMessage):
    def __init__(self, sender, receiver, term, success, matchIndex):
        super().__init__(sender, receiver, term)
        self.success = success
        self.type = BaseMessage.AppendEntriesResponse
        self.matchIndex = matchIndex

class LogEntry(object):
    def __init__(self, term, command, addr, uuid, _type=0) -> None:
        self.term = term
        self.commmand = command
        self.uuid = uuid
        self.addr = addr
        self.type = _type

class Request(object):
    def __init__(self, request_msg, uuid=0) -> None:
        self.request_msg = request_msg
        self.uuid = uuid
        self.type = 'client'

class RequestRedirect(Request):
    def __init__(self, request_msg, uuid, addr) -> None:
        super().__init__(request_msg, uuid)
        self.addr = addr
        self.type = 'redirect'

class ServerConfig(object):
    def __init__(self, poolsize, currentTerm, votedFor, log, peers) -> None:
        self.poolsize = poolsize
        self.currentTerm = currentTerm
        self.votedFor = votedFor
        self.log = log
        self.peers = peers

class ConfigChange(object):
    def __init__(self, new_config, uuid, phase, addr=None) -> None:
        self.new_config = new_config
        self.uuid = uuid
        self.addr = addr
        self.phase = phase
        self.type = 'change'

