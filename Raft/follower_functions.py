import pickle
import random
import socket

from Raft.messages import *


def acceptor(server, data, addr):
    msg = pickle.loads(data)
    _type = msg.type

    # handle config_change msg
    if _type == 'change':
        if msg.phase == 1:
            print('Config change phase 1')
            server.during_change = 1
            server.new = msg.new_config
            server.old = server.peers[:]
            server.old.append(server.id)
            if msg.addr != None:
                addr = msg.addr
            newEntry = LogEntry(server.currentTerm, msg, addr, msg.uuid, 1)
            server.log.apend(newEntry)
            server.peers = list(set(server.old + server.new))
            server.peers.remove(server.id)
            server.save()
            print('Config change phase 1 applied')

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
    if _type == 'client' or _type == 'redirect':
        if _type == 'redirect':
            addr = msg.addr
        
        msg_str = msg.request_msg
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
        
        else:
            ticket_num = int(msg_str.split()[1])
            if server.role == 'leader':
                print('I am the leader, customer wants to buy %d tickets'.format(ticket_num))

                if ticket_num > server.poolsize:
                    print('tickets not enough')
                    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                    s.sendto('Not enough tickets', addr)
                    s.close()
                    return
                for idx, entry in enumerate(server.log):
                    if entry.uuid == msg.uuid:
                        if server.commitIndex >= idx + 1:
                            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                            s.sendto('Your request has been fullfilled', addr)
                            s.close()
                        else:
                            pass
                        return
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
        _msg = _msg.split()
        log_info = (int(_msg[0]), int(_msg[1]))
        if _term < server.currentTerm:
            print('Rejected vote request due to old term')
            voteGranted = 0
        elif _term == server.currentTerm:
            # this log info is newer than mine and this sender is whom I voted for or I can vote for it
            if log_info >= (server.lastLogTerm, server.lastLogIndex) and (server.votedFor == -1 or server.votedFor == _sender):
                voteGranted = 1
                server.votedFor = _sender
                server.save()
            else:
                voteGranted = 0
        else:
            # the msg is in newer term, hence I shall stop my election or turn from leader to follower
            server.currentTerm = _term
            server.save()
            server.step_down()

            if log_info >= (server.lastLogTerm, server.lastLogIndex):
                voteGranted = 1
                server.votedFor = _sender
                server.save()
            else:
                voteGranted = 0

            reply = str(voteGranted)
            reply_msg = VoteResponseMsg(server.id, _sender, server.currentTerm, reply)
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            s.sendto(pickle.dumps(reply_msg), ("", server.addressbook[_sender]))
        
        # handle vote response message
    elif _type == 2:
        # TODO
        pass

