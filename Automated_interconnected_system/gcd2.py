"""
code from Professor Kevin Lundeen
Group Coordinator Daemon
"""

import pickle
import socketserver
import socket
import sys

BUF_SZ = 1024

class GCD(socketserver.BaseRequestHandler):
    """
    accpet JOIN message
    parse it into (name, data)
    validate name
    parse data into(pid, address)
    validate structure
    further parse pid(days to birth, suid)
    validate
    parse address into (ip, port)
    validate ip is localhost
    validate port within required

    update data map
    {pid: address} by check {suid: pid} and update
    then check {(ip, port): pid} update
    check new pid exist in {pid: address} yes, then update(delete old pid (got from su_id: pid))
    no add new pid into {pid: address}
    return {pid: address}
    """
    listeners_by_id = {} # pid:address

    pids_by_listener = {} # address: pid

    pids_by_student = {} # su_id: pid

    localhost_ip = socket.gethostbyname('localhost')

    def handle(self) -> None:
        raw = self.request.rev(BUF_SZ)

        try:
            message = pickle.loads(raw)
        except (pickle.PickleError, KeyError):
            resp = bytes('Expected pickled msg, but got ' + str(raw)[:100], 'utf-8')
        else:
            try:
                resp_data = self.handle_join(message)
            except ValueError as err:
                resp_data = str(err)
            resp = pickle.dumps(resp_data)
        self.request.sendall(resp)
        self.request.shutdown(socket.SHUT_RDWR)
        self.request.close()
    
    @staticmethod
    def handle_join(message):
        """
        only process 'JOIN' message
        message structure as (data_name, data)
        
        data structured as (pid, address)
        pid structured as (days to birth, su_id)
        address structured as (host, port)
        """

        try:
            message_name, message_data = message
        except (ValueError, TypeError):
            raise ValueError('Malformed message')
        
        if message_name != 'JOIN':
            raise ValueError('Unsupported message {}'.format(message_name))
        
        try:
            process_id, listener_address = message_data
            listen_host, listen_port = listener_address
            days_to_birth, su_id = process_id
        except (ValueError, TypeError):
            raise ValueError('Malformed message data')
        if not (type(days_to_birth) is int and type(su_id) is int and 0 < days_to_birth < 366 and 1_000_000 <= su_id <= 10_000_000):
            raise ValueError('Malformed process id')
        
        try:
            listen_ip = socket.gethostbyname(listen_host)
        except Exception as err:
            raise ValueError(str(err))
        if not (type(listen_port) is int and 0 < listen_port < 65_536):
            raise ValueError('Invalid port number')
        if listen_ip != GCD.localhost_ip:
            raise ValueError('Only local group members currently allowed')
        listener = (listen_ip, listen_port)

        # alias for global dictionaries
        students = GCD.pids_by_student
        group = GCD.listeners_by_id
        listeners = GCD.pids_by_listener

        if su_id in students and students[su_id] != process_id:
            old_pid = students[su_id]
            del group[old_pid]
        students[su_id] = process_id
        group[process_id] = listener


        if listener in listeners and listeners[listener] != process_id:
            old_pid = listeners[listener]
            if old_pid in group:
                del group[old_pid]
        listeners[listener] = process_id
        return group
if __name__ == '__main__':
    if len(sys.argv) != 2:
        print('Usage: python gcd2.py gcd_port')
        exit(1)
    port = int(sys.argv[1])
    with socketserver.TCPServer(('',port), GCD) as server:
        server.serve_forever()