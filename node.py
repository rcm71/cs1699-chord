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

# arg handling, creates Chord_Node
def main():

    connecting = False

    # arg stuffs
    if len(sys.argv) < 6:
        print(argstring)
        return
    if sys.argv[1] != "join":
        print(argstring)
        return
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
# ip: string ip addr to connect to
# port: string port # to connct to
def init_ring(ip, port):
    global node
    node = Chord_Node(ip, port)
    node.server_loop()

 
# binds to socket, sends off 'let me in' command, and goes to listen loop
#
# ip: string ip address
# port: string port to use
# connection: ipv4 of node to talk to first
def connect_ring(ip, port, connection):
    global node
    node = Chord_Node(ip, port)
    node.send_join(connection)
    node.server_loop()

# informational class to hold ipv4/name for chord nodes
class Node:
    ip = None # ip string of form "127.0.0.1"
    port = None # port int of form 8000
    hashname = None # hashname of ip:port, where we sit in chain

    # initializes 'Node' which holds data about chord servers
    #
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
# Chord_Node is used for the process' node in chord ring
# holds some info, ton of operations
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
    data_dict = {} # holds our k/v datums

    # some initializations, sets up our socket and gets hashname
    #
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
    
    # nodes will sit here waiting for requests, handle in dif methods
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
    #
    # connection: string ipv4 of where we ask to join from
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

    # handles an ask to join the ring.
    # may punt off the request if it's not our jursidiction
    # 
    # message: dict, CONNECT message, formatted as in send_join
    def recv_join(self, message):
        print("recieved connect!")
        # case when buddy is solo dolo
        if self.successor == None or self.predecessor == None:
            self.set_successor(message['ip'], message['port'])
            self.set_predecessor(message['ip'], message['port']) 
            # doesn't matter in this case, but usually will be 
            # my successor has me as predecessor
            # my predecessor has me as successor
            self.send_update_pred(self.successor, self.me)
            self.send_update_succ(self.predecessor, self.me)
            return
        elif message['hashname'] < self.me.hashname:
            # either goes right here as pred or we punt off this message to our pred
            if message['hashname'] < self.predecessor.hashname:
                self.punt_join(message, self.predecessor)
            else:
                # curr: pred -> me -> succ
                # want: pred -> new -> me -> succ
                new_node = Node(message['ip'], message['port'])
                # new gets our old predescessor
                self.send_update_pred(new_node, self.predecessor)
                self.send_update_succ(self.predecessor, new_node)
                # new pred is new node. let me know we are their successor
                self.set_predecessor(new_node.ip, new_node.port)
                self.send_update_succ(new_node, self.me)
        elif message['hashname'] > self.me.hashname:
            #either is our succ or we punt off message to our succ
            if message['hashname'] > self.successor.hashname:
                self.punt(message, self.successor)
            else:
                # curr: pred -> me -> succ
                # want: pred -> me -> new -> succ
                new_node = Node(message['ip'], message['port'])

                # new gets our old successor
                self.send_update_succ(new_node, self.successor)
                self.send_update_pred(self.successor, new_node)
                self.set_successo(new_node.ip, new_node.port)
                self.send_update_pred(new_node, self.me)
    
    # sends off 'join' message to another node, because the 
    # node attempting to join is not our predecessor or successor
    #
    # message: dict, CONNECT message from node attempting to join
    # goal_node: node info for destination of message
    def punt_join(self, message, goal_node):
        # create socket to send from
        client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.clients.append(client)
        client.connect((goal_node.ip, goal_node.port))

        # cap-n-crunch
        to_send = pickle.dumps(message)

        # send n shutdown
        client.send(to_send)
        client.shutdown(socket.SHUT_RDWR)
        client.close()
        self.clients.remove(client)

    # send message to update goal's predecessor to new
    #
    # goal_node: Node that needs to update it's predecessor
    # new_node: Node that goal needs to change it's predecessor to
    def send_update_pred(self, goal_node, new_pred):
        # he's alive!
        client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.clients.append(client)

        client.connect((goal_node.ip, goal_node.port))

        # updating it *to* the node specified
        message = {'type':'UPDATE_PRED',
                   'hashname':new_pred.hashname,
                   'ip':new_pred.ip,
                   'port':new_pred.port}
        
        # cheerio
        to_send = pickle.dumps(message)

        client.send(to_send)
        client.shutdown(socket.SHUT_RDWR)
        client.close()
        self.clients.remove(client)

    # recieves update predecessor message
    #
    # message: dict UPDATE_PRED as defined in send_update_pred()
    def recv_update_pred(self, message):
        self.set_predecessor(message['ip'], message['port'])
    
    # creates new Node w/ ip:port and sets as self.predecessor
    #
    # ip: string ip address to set predecessor
    # port: int port # to set predecessor
    def set_predecessor(self, ip, port):
        self.predecessor = Node(ip, port)
        print(f"Updated predecessor to: {ip}:{port}")

    # sends message to update goal's successor as new
    #
    # goal_node: Node that needs to update it's successor
    # new_succ: Node that goal needs to set as it's successor
    def send_update_succ(self, goal_node, new_succ):
        client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.clients.append(client)

        client.connect((goal_node.ip, goal_node.port))

        # updating it *to* the node specified
        message = {'type':'UPDATE_SUCC',
                   'hashname':new_succ.hashname,
                   'ip':new_succ.ip,
                   'port':new_succ.port}
        
        # cerealize - trix suck
        to_send = pickle.dumps(message)

        # off to the os 
        client.send(to_send)
        client.shutdown(socket.SHUT_RDWR)
        client.close()
        self.clients.remove(client)
        
    # recieves update successor message
    #
    # message: dict UPDATE_SUCC formatted as in send_update_succ
    def recv_update_succ(self, message):
        self.set_successor(message['ip'], message['port'])
    
    # sets self.successor to new created Node
    #
    # ip: string to create node
    # port: int to create node
    def set_successor(self, ip, port):
        self.successor = Node(ip, port)
        print(f"Updated successor to: {ip}:{port}")
    
    # lets the locals know that we gotta go...
    def send_leave(self):

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