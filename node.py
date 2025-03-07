#!/usr/bin/env python3

#for CLA
import sys

# SHA-1-boozey
import hashlib

# mmmm socket programming
import socket

import pickle 

# don't want long ahh string on one line hate python for that
argstring = "Please use args in this fashion:\n"
argstring = argstring + "  To join as first node: ./node.py join -ip <IP> -p <port>\n"
argstring = argstring + "  To join ring: ./node.py join -ip <IP> -p <port> -c <IP+port>\n"

# list to hold open sockets incase keyboard interrupt
node = None

def main():

    connecting = False

    # arg stuffs
    if len(sys.argv) < 6:
        print(argstring)
    if sys.argv[1] != "join":
        print(argstring)
    for i in range(2, len(sys.argv)):
        arg = sys.argv[i]
        if arg == "-ip":
            # you could break it here. dont do that pls
            i += 1
            ip = sys.argv[i]
        elif arg == "-p":
            i += 1
            port = sys.argv[i]
        elif arg == "-c":
            i += 1
            connecting = True
            connection = sys.argv[i]

    # should now have all our args we need to get going !
    if connecting:
        connect_ring(ip, port,connection)
    else:
        init_ring(ip, port)


# binds to socket and sits waiting for messages to further set up the ring
def init_ring(ip, port):
    global node
    node = Chord_Node(ip, port)
    node.server_loop()

 
# binds to socket, sends off 'let me in' command, and goes to listen loop
def connect_ring(ip, port, connection):
    global node
    node = Chord_Node(ip, port)
    node.send_join(connection)
    node.server_loop()


class Node:
    ip = None # ip string of form "127.0.0.1"
    port = None # port int of form 8000
    hashname = None # hashname of ip:port, where we sit in chain

    # ip: string of ip
    # port: int port#
    def __init__(self, ip, port):
        self.ip = ip
        self.port = port
        self.ipv4 = ip + ':' + str(port)
        # idk what else to use... random string? seems crazy
        ip_encoded = self.ipv4.encode('utf-8')
        self.hashname = hashlib.sha1(ip_encoded).hexdigest()
    

# have not been oop-ing in..... awhile
# lets fix that
class Chord_Node:

    predecessor = None # Node, prev in chord chain
    successor = None # Node, next in chord chain
    me = None # Node, us!
    # TCP messin with my noggin. used to UDP tbh
    # need to have two sockets for each address.
    # clients need to be around for sending closing when done
    # servers need to be STABLE and around to handle incoming messages
    server = None # socket for listening
    clients = [] # sockets for sending
    data_queue = {} # holds our k/v datums

    # some initializations, sets up our socket and gets hashname
    # ip: string of format "127.0.0.1"
    # port: string of format "8000"
    def __init__(self, ip, port):
        self.me = Node(ip, int(port))

        # set up socket
        self.server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        # no block!
        self.server.setblocking(0)
        self.server.settimeout(0.5)
        # lemme resue pls, for the whole multiple client thing
        self.server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server.bind((self.me.ip, self.me.port))
    
    # nodes will sit here waiting for requests
    def server_loop(self):
        # set up queueueue size of 5
        self.server.listen(5)
        while True:
            try:
                # will throw TimeoutException if nothin waiting
                (client_socket, client_address) = self.server.accept()
                # what do they want !?!?!
                # MMMMMm make this better so we dont have max bytes yk
                data_bytes = client_socket.recv(2048)
                # deserialize. mmm pickles
                message = pickle.loads(data_bytes)

                # switch action
                match message["type"]:
                    case "CONNECT": 
                        self.recv_join(message)
                    case "UPDATE_PRED":
                        self.recv_update_pred(message)
                    case "UPDATE_SUCC":
                        self.recv_update_succ(message)
                    case _ :
                        pass
                
                client_socket.close()

            
            except TimeoutError as e:
                pass
            

    # attempts to join the network
    # sends off a message to the 'connection' node
    # this will be followed by update_successor() and predecessor
    # messages from other nodes to establish connection
    def send_join(self, connection):
        # gotta set up client so we can send messages and disconnect safely
        client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.clients.append(client)
    

        # connection of form = "127.0.0.1:8000"
        connection = connection.split(':')
        # you out there!?!?!
        client.connect((connection[0], int(connection[1])))
        # you tell em boss
        message = {'type':'CONNECT',
                   'hashname':self.me.hashname,
                   'ip':self.me.ip,
                   'port':self.me.port}
        print("making message!")
        # serialize (mmm cereal)
        to_send = pickle.dumps(message)
        # send it on over, we don't need to keep connection open
        client.send(to_send)
        client.shutdown(socket.SHUT_RDWR)
        client.close()
        self.clients.remove(client)



    def recv_join(self, message):
        print("recieved connect!")
        # case when buddy is solo dolo
        if self.successor == None or self.predecessor == None:
            self.successor = Node(message['ip'], message['port'])
            self.predecessor = Node(message['ip'], message['port']) 
            # doesn't matter in this case, but usually will be 
            # my successor has me as predecessor
            # my predecessor has me as successor
            self.send_update_pred(self.successor)
            self.send_update_succ(self.predecessor)
            return
        elif message['hashname'] < self.me.hashname:
            # either goes right here as pred or we punt off this message to our pred
            if message['hashname'] < self.predecessor.hashname:
                pass
            else:
                pass
        elif message['hashname'] > self.me.hashname:
            #either is our succ or we punt off message to our succ
            pass


    def send_update_pred(self, other_node):
        # he's alive!
        client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.clients.append(client)

        client.connect((other_node.ip, other_node.port))

        # updating it *to* the node specified
        message = {'type':'UPDATE_PRED',
                   'hashname':self.me.hashname,
                   'ip':self.me.ip,
                   'port':self.me.port}
        
        # cheerio
        to_send = pickle.dumps(message)

        client.send(to_send)
        client.shutdown(socket.SHUT_RDWR)
        client.close()
        self.clients.remove(client)

    def recv_update_pred(self, message):
        self.predecessor = Node(message['ip'], message['port'])
        print(f"Updated predecessor to: {message['ip']}:{message['port']}")


    # sends to other_node that it's new successor should be us
    def send_update_succ(self, other_node):
        # 
        client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.clients.append(client)

        client.connect((other_node.ip, other_node.port))

        # updating it *to* the node specified
        message = {'type':'UPDATE_SUCC',
                   'hashname':self.me.hashname,
                   'ip':self.me.ip,
                   'port':self.me.port}
        
        # cerealize
        to_send = pickle.dumps(message)

        # off to the os 
        client.send(to_send)
        client.shutdown(socket.SHUT_RDWR)
        client.close()
        self.clients.remove(client)
        
    
    def recv_update_succ(self, message):
        self.successor = Node(message['ip'], message['port'])
        print(f"Updated successor to: {message['ip']}:{message['port']}")
    
    def send_leave():

        pass

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        # properly closes the stuffs if we keyboard interrupt
        node.send_leave()
        if node != None:
            node.server.close()
            for client in node.clients:
                client.shutdown(socket.SHUT_RDWR)
                client.close()