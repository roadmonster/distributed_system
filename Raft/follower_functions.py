import pickle
import random
import socket
import time

from Raft.KThread import KThread
from Raft.messages import *

"""
Utility function for server's listen function to call on.
Acceptor function handles different kinds of incoming message,
set the state for current server and send communication to others
"""

def acceptor(server, data, addr):
    # entry point, deserialize the data
    msg = pickle.loads(data)

    # check the type ot the message: change, client, redirect, type 0-3
    _type = msg.type

    # handle config_change msg
    if _type == 'change':

        # initial informing phase, client telling leader the config change request
        if msg.phase == 1:
            print('Config change phase 1')

            # update server's change status
            server.during_change = 1

            # server's new picture of all node list 
            server.new = msg.new_config

            # server's old picture of all node list
            server.old = server.peers[:]

            # add server's id back to server's old picture
            server.old.append(server.id)
            if msg.addr != None:
                addr = msg.addr
            
            # create new entry
            newEntry = LogEntry(server.currentTerm, msg, addr, msg.uuid, 1)
            server.log.apend(newEntry)
            server.peers = list(set(server.old + server.new))
            server.peers.remove(server.id)
            server.save()
            print('Config change phase 1 applied')

        # second stage of commit, leader already spread the idea and follower waiting to commit
        else:
            print('Config change phase 2')
            server.during_change = 2
            server.new = msg.new_config
            if msg.addr != None:
                addr = msg.addr
            newEntry = LogEntry(server.currentTerm, msg, addr, msg.uuid, 2)
            server.log.append(newEntry)
            server.peers = server.new[:]
            if server.id in server.peers:
                server.peers.remove(server.id)
            server.save()
            print('Config change phase 2 applied, running peers')
        
        # if server's role is follower or candidate
        if server.role != 'leader':
            print('redirect config change to the leader')
            if server.leaderId != 0:
                redirect_target = server.leader_Id
            else:
                redirect_target = random.choice(server.peers)
            if msg.addr != None:
                addr = msg.addr
            
            redirect_msg = ConfigChange(msg.new_config, msg.uuid, msg.phase, addr)
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            s.sendto(pickle.dumps(redirect_msg), ("", server.addressbook[redirect_target]))
            s.close()
        return

    # there are two types of client request: show or purchase tickets
    if _type == 'client' or _type == 'redirect':
        if _type == 'redirect':
            addr = msg.addr
        
        # check the msg's request_msg
        msg_str = msg.request_msg

        # show all the log in this server up to the commitIndex and all the log
        if msg_str == 'show':
            state = server.poolsize
            committed_log = ''
            for idx in range(server.commitIndex):
                entry = server.lo[idx]
                if entry.type == 0:
                    committed_log += str(entry.command) + ' '
                elif entry.type == 1:
                    committed_log += 'new_old' + ' '
                else:
                    committed_log += 'new' + ' '
            all_log = ''
            for entry in server.log:
                if entry.type == 0:
                    all_log += str(entry.command) + ' '
                elif entry.type == 1:
                    committed_log += 'new_old' + ' '
                else:
                    committed_log += 'new' + ' '
            show_msg = 'state machine: ' + str(state) + '\n' 
            + 'commited log: ' + committed_log + '\n' 
            + 'all_log: ' + str(all_log) + '\n' 
            + 'status: ' + str(server.during_change)

            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            s.sendto(show_msg, addr)
            s.close()
        # handle purchase ticket
        else:
            ticket_num = int(msg_str.split()[1])
            if server.role == 'leader':
                print('I am the leader, customer wants to buy %d tickets'.format(ticket_num))
                # ticket not enough
                if ticket_num > server.poolsize:
                    print('tickets not enough')
                    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                    s.sendto('Not enough tickets', addr)
                    s.close()
                    return
                
                # check all my logs, check if exists a uuid that's the same as message's target uuid
                for idx, entry in enumerate(server.log):
                    if entry.uuid == msg.uuid:
                        # this index is within the committed index hence send confirmation
                        if server.commitIndex >= idx + 1:
                            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                            s.sendto('Your request has been fullfilled', addr)
                            s.close()
                        else:
                            # do nothing if this uuid in my log but not yet committed
                            pass
                        return
                
                # I don't find match uuid in my committed index, then I create a new entry
                newEntry = LogEntry(server.currentTerm, ticket_num, addr, msg.uuid)
                s.sendto('The leader gets your request', addr)
                s.close()
                server.log.append(newEntry)
                server.save()
            # redirecting to the leader
            else:
                print('redirecting the request to the leader')
                if server.leaderId != 0:
                    redirect_target = server.leaderId
                else:
                    redirect_target = random.choice(server.peers)
                redirect_msg = RequestRedirect(msg_str, msg.uuid, addr)
                s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                s.sendto(pickle.dumps(redirect_msg), ("", server.addressbook[redirect_target]))
                s.close()
        return
    _sender = msg.sender
    _term = msg.term

    # handle requestvote message
    if _type == 1:
        # my peers not include this new guy, no need to respond
        if _sender not in server.peers:
            return
        _msg = msg.data
        print('---------Get requestvote message---------')

        # split the msg data
        _msg = _msg.split()

        # log_info: (lastLogTerm, lastLogIndex)
        log_info = (int(_msg[0]), int(_msg[1]))

        # the requesting node's term is stale
        if _term < server.currentTerm:
            print('Rejected vote request due to old term')
            voteGranted = 0

        # requesting node has same current term as mine, grant the vote is log info object is newer than mine and I am not voting or voted to the sender
        elif _term == server.currentTerm:
            # this log info is newer than mine and this sender is whom I voted for or I can vote for it
            if log_info >= (server.lastLogTerm, server.lastLogIndex) and (server.votedFor == -1 or server.votedFor == _sender):
                voteGranted = 1
                server.votedFor = _sender
                server.save()
            else:
                # I have voted for others, then no vote granted, one node one vote
                voteGranted = 0
        else:
            # the msg is in newer term, hence I shall stop my election or turn from leader to follower
            server.currentTerm = _term
            server.save()
            # If I am the leader or the candidate, I step down, otherwise, nothing did
            server.step_down()

            # vote for the _sender if its has newer lastLogTerm or lastLogIndex
            if log_info >= (server.lastLogTerm, server.lastLogIndex):
                voteGranted = 1
                server.votedFor = _sender
                server.save()
            else:
                voteGranted = 0

            # create the vote response message
            reply = str(voteGranted)
            reply_msg = VoteResponseMsg(server.id, _sender, server.currentTerm, reply)
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            s.sendto(pickle.dumps(reply_msg), ("", server.addressbook[_sender]))
        
    # handle vote response message
    elif _type == 2:
        _msg = msg.data
        print('---------Get vote response message---------')
        voteGranted = int(_msg)
        if voteGranted:
            if server.during_change == 0:
                if server.role == 'candidate':
                    server.request_votes.remove(_sender)
                    server.numVotes += 1
                    if server.numVotes == server.majority:
                        print('Received majority votes, become leader at %d term'.format(server.currentTerm))
                        if server.election.is_alive():
                            server.election.kill()
                        # configure self to be leader
                        server.role = 'leader'
                        server.follower_state.kill()
                        server.leader_state = KThread(target = server.leader, args = ())
                        server.leader_state.start()
            elif server.during_change == 1:
                server.request_votes.remove(_sender)
                if _sender in server.old:
                    server.oldVotes += 1
                if _sender in server.new:
                    server.newVotes += 1
                majority_1 = len(server.old) / 2 + 1
                majority_2 = len(server.new) / 2 + 1
                if server.oldVotes >= majority_1 and server.newVotes >= majority_2:
                    print('Received majority votes, become leader at %d term'.format(server.currentTerm))
                    if server.election_is_alive():
                        server.election.kill()
                        server.role = 'leader'
                        server.follower_state.kill()
                        server.leader_state = KThread(target = server.leader, args = ())
                        server.leader_state.start()
            else:
                server.request_votes.remove(_sender)
                if _sender in server.peers:
                    server.newVotes += 1
                majority = len(server.new) / 2 + 1
                if server.newVotes >= majority:
                    print('Received majority votes, become leader at %d term'.format(server.currentTerm))
                    if server.election.is_alive():
                        server.election.kill()
                        server.leader_state = KThread(target = server.leader, args = ())
                        server.leader_state.start()
        else:
            # my voteRequest was responded with higher term
            # means I am behind the terms, and my request is not valid
            if _term > server.currentTerm:
                server.currentTerm = _term
                server.save()
                if server.role == 'candidate':
                    server.step_down()
            print('Vote rejected by %d'.format(_sender))
    # handle appendEntries msg
    elif _type == 0:
        print('---------Get AppendEntries message---------')
        entries = msg.entries
        leaderCommit = msg.commitIndex
        prevLogTerm = msg.prevLogTerm
        prevLogIndex = msg.prevLogIndex
        matchIndex = server.commitIndex

        if _term >= server.currentTerm:
            server.currentTerm = _term
            server.save()
            server.step_down()
            if server.role == 'follower':
                server.last_update = time.time()
            if prevLogIndex != 0:
                if len(server.log) >= prevLogIndex:
                    if server.log[prevLogIndex - 1].term == prevLogTerm:
                        success = 'True'
                        server.leaderId = _sender
                        if len(entries) != 0:
                            server.log = server.log[:prevLogIndex] + entries
                            matchIndex = len(server.log)
                            if entries[0].type == 1:
                                server.during_change = 1
                                server.new = entries[0].command.new_config[:]
                                server.old = server.peers[:]
                                server.old.append(server.id)
                                server.peers = list(set(server.old + server.new))
                                server.peers.remove(server.id)
                            elif entries[0].type == 2:
                                server.during_change = 2
                                server.new = entries[0].command.new_config[:]
                                server.peers = server.new[:]
                                server.peers.remove(server.id)
                                print('New config has been applied to follower')
                                server.save()
                    else:
                        success = 'False'
                else:
                    success = 'False'
            else:
                success = 'True'
                if len(entries) != 0:
                    server.log = server.log + entries
                    if entries[0].type == 1:
                        server.during_change = 1
                        server.new = entries[0].command.new_config[:]
                        server.old = server.peers[:]
                        server.old.append(server.id)
                        server.peers = list(set(server.old + server.new))
                        server.peers.remove(server.id)
                    elif entries[0].type == 2:
                        server.during_change = 2
                        server.new = entries[0].command.new_config[:]
                        server.peers = server.new[:]
                        server.peers.remove(server.id)
                        print('New config has been applied to follower')
                    server.save()
                    matchIndex = len(server.log)
                server.leaderId = _sender
        else:
            success = 'False'
        
        if leaderCommit > server.commitIndex:
            lastApplied = server.commitIndex
            # the common commit index should be the minimum of leader commit and the lengths of my log
            server.commitIndex = min(leaderCommit, len(server.log))
            # when commit index is further than last applied, we start from initial state 
            # then execute all the commands till the commitIndex
            if server.commitIndex > lastApplied:
                server.poolsize = server.initial_state
                for idx in range(1, server.commitIndex + 1):
                    # I am not in a change, then execute the command
                    if server.log[idx-1].type == 0:
                        server.poolsize -= server.log[idx -1].command
                    # I am in a change commit phase, then change my status into not into change
                    elif server.log[idx-1].type == 2:
                        server.during_change = 0
        reply_msg = AppendEntriesResponseMsg(server.id, _sender, server.currentTerm, success, matchIndex)
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.sendto(pickle.dumps(reply_msg), ("", server.addressbook[_sender]))
    

    # handle AppendEntriesResponse
    elif _type == 3:
        success = msg.success
        matchIndex = msg.matchIndex
        # my append entry request was rejected
        if success == 'False':
            # reponse's term is greater than current term
            # set my term same as response's term
            if _term > server.currentTerm:
                server.currentTerm = _term
                server.save()
                server.step_down()
            else:
                # response's term is older than mine but still reject it, means my nextIndex for sender should be decreased
                server.nextIndex[_sender] -= 1
        
        else:
            # My append entry req was accepted
            # when my nextIndex for this sender is less than the log
            # and the matchIndex for this sender is less than the response matchIndex
            # then update the nextIndex and matchIndex
            if server.nextIndex[_sender] <= len(server.log) and matchIndex > server.matchIndex[_sender]:
                server.matchIndex[_sender] = matchIndex
                server.nextIndex[_sender] += 1
            
            # If my commit index is less than the max of the matchIndex for all my followers
            if server.commitIndex < max(server.matchIndex.values()):
                start = server.commitIndex + 1
                # iterate from commitIndex's next to the maximum of match index for my followers
                # for each N check if there are more than half of peers have reached equal or beyond this index
                # if majority reach this index, we say this is the common sense and execute the commands up to this 
                # common sense point
                for N in range(start, max(server.matchIndex.values()) + 1):
                    # I am not in a change
                    if server.during_change == 0:
                        # counter for 
                        compare = 1
                        for peer, index in server.matchIndex.items():
                            # when peer's matching index is greater than 
                            if peer in server.peers and index >= N:
                                compare += 1
                        majority = (len(server.peers) + 1) / 2 + 1
                        if compare == server.majority and server.log[N-1].term == server.currentTerm:
                            # from commit index to 
                            for idx in range(server.commitIndex + 1, N + 1):
                                server.poolsize -= server.log[idx-1].command
                                server.save()
                                s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                                s.sendto('Your request is fullfilled',server.log[idx-1].addr)
                                s.close()
                                print('reply once')
                    # I am in the initial stage of change
                    elif server.during_change == 1:
                        majority_1 = len(server.old) / 2 + 1
                        majority_2 = len(server.new) / 2 + 1
                        votes_1 = 0
                        votes_2 = 0

                        # initialize the votes
                        if server.id in server.old:
                            votes_1 = 1
                        if server.id in server.new:
                            votes_2 = 1
                        
                        # iteratre the matchindex dict to count the votes
                        for peer, index in server.matchIndex.items():
                            if index >= N:
                                if peer in server.old:
                                    votes_1 += 1
                                if peer in server.new:
                                    votes_2 += 1
                        # current N gets more than half votes
                        if votes_1 >= majority_1 and votes_2 >= majority_2 and server.log[N-1].term == server.currentTerm:
                            # update current commit index
                            server.commitIndex = N
                            poolsize = server.initial_state
                            for idx in range(1, N+1):
                                # only compute when server not in change mode
                                if server.log[idx-1].type == 0:
                                    poolsize -= server.log[idx-1].command
                            server.poolsize = poolsize
                            server.save()
                            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                            s.sendto('Your request is fullfilled',server.log[idx-1].addr)
                            s.close()
                    # during_change == 2 which is confirming the changes
                    
                    else:
                        # find the majority first
                        majority = len(server.new) / 2 + 1
                        # have variable to hold the votes
                        votes = 0
                        # initialize value
                        if server.id in server.new:
                            votes = 1
                        
                        # compute the votes
                        for key, item in server.matchIndex.items():
                            if item >= N:
                                if key in server.new:
                                    votes += 1
                        # if N is geting more than half of the votes and the term is right, we execute the commands
                        if votes == majority and server.log[N-1].term == server.currentTerm:
                            for idx in range(server.commitIndex + 1, N+1):
                                # in this command, server not in change mode, execute
                                if server.log[idx-1].type == 0:
                                    server.poolsize -= server.log[idx-1].command
                                    server.save()
                                    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                                    s.sendto('Your request is fullfilled',server.log[idx-1].addr)
                                    s.close()
                                    server.commitIndex = idx
                                # server in confriming mode, only update the commit index
                                elif server.log[idx-1].type == 2:
                                    server.commitIndex = idx
                                    time.sleep(1)
                                    if not server.id in server.new:
                                        print('I am not in the new configuration of nodes')
                                        server.step_down()
                                    server.during_chnage = 0
                                    server.save()
                                    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                                    s.sendto('Your request is fullfilled',server.log[idx-1].addr)
                                    s.close()














