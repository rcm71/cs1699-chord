#!/usr/bin/env python3


import sys

import socket

import pickle

import hashlib

argstring = "Please use args in this fashion:\n"
argstring += "\tTo tell a node to leave:\n"
argstring += "\t\t./client.py leave -ip <client-ip> -p <client-port> -n <chord-contact-ip>\n"
argstring += "\tTo lookup a key to find where it's located:\n"
argstring += "\t\t./client.py lookup -k <key> -n <chord-contact-ip>\n"
argstring += "\tTo insert data to Chord:\n"
argstring += "\t\t./client.py put -k <key> -v <value> -n <chord-contact-ip>"
argstring += "\tTo retrieve data from chord:\n"
argstring += "\t\t./client.py get -k <key> -n <chord-contact-ip>"


def main():
    ip = None
    port = None
    contact = None
    key = None
    value = None

    # no scenario has less than 6 args
    if len(sys.argv) < 6:
        print(argstring)
        return
    for i in range(2, len(sys.argv)):
        arg = sys.argv[i]
        if arg == "-ip":
            i += 1
            ip = sys.argv[i]
        elif arg == "-p":
            i += 1
            port = int(sys.argv[i])
        elif arg == "-n":
            i += 1
            contact = sys.argv[i]
        elif arg == "-k":
            i += 1
            key = sys.argv[i]
        elif arg == "-v":
            i += 1
            value = sys.argv[i]
    
    # all need a contact point
    if contact == None:
        print(argstring)
        return

    

    client_type = sys.argv[1]
    if client_type == "leave":
        if ip == None or port == None:
            print(argstring)
            return
        leave(ip, port, contact)
    elif client_type == "lookup":
        if key == None:
            print(argstring)
            return
        lookup(key, contact)
    elif client_type == "put":
        if key == None or value == None:
            print(argstring)
            return
        put(key, value, contact)
    elif client_type == "get":
        if key == None:
            print(argstring)
            return
        get(key, contact)
    

def leave(ip, port, contact):
    # create socket and connect to Chord_Node
    client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    contact = contact.split(":")
    client.connect((contact[0], int(contact[1])))

    # craft
    message = {"type":"LEAVE",
               "ip":ip,
               "port":port}
    
    to_send = pickle.dumps(message)

    client.send(to_send)
    client.close()

def lookup(key, contact):
    # gonna need a client to send message and connect to first node
    client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    contact = contact.split(":")
    client.connect((contact[0], int(contact[1])))
    # also gonna need a listneer endpoint to get a response
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    offset = 1
    while True:
        try:
            server.bind(("127.0.0.1", 6000+offset))
            break
        except:
            offset += 1
    address = server.getsockname()
    message = {'type':"LOOKUP",
               'ip':address[0],
               'port':address[1],
               'key':key}
    to_send = pickle.dumps(message)
    client.send(to_send)
    print(hashlib.sha1(key.encode('utf-8')).hexdigest())

    server.listen(1)
    (connection_socket, addr) = server.accept()
    data_byte = connection_socket.recv(2048)
    message = pickle.loads(data_byte)
    match message['type']:
        case "LOOKUP_SUCCESS":
            print(f"Data found at address: {message['ip']}:{message['port']}")
        case "LOOKUP_FAILURE":
            print(f"Failed to find data in DHT")
    connection_socket.close()
    client.close()
    server.close()

def put(key,value, contact):
    # crate client socket to sedn the k/v pair
    client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    contact = contact.split(":")
    client.connect((contact[0], int(contact[1])))
    # create message
    message = {'type':'PUT',
               'key':key,
               'value':value}
    # serialize
    to_send = pickle.dumps(message)

    # close it down
    client.send(to_send)
    client.close()
        
def get(key, contact):
    # ask for the loot
    client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    contact = contact.split(":")
    client.connect((contact[0], int(contact[1])))
    # listen for response
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    offset = 1
    while True:
        try:
            server.bind(("127.0.0.1", 6000+offset))
            break
        except:
            offset += 1
    address = server.getsockname()
    message = {'type':"GET",
               'ip':address[0],
               'port':address[1],
               'key':key}
    to_send = pickle.dumps(message)
    client.send(to_send)
    server.listen(1)
    (connection_socket, addr) = server.accept()
    data_byte = connection_socket.recv(2048)
    message = pickle.loads(data_byte)
    match message['type']:
        case "GET_SUCCESS":
            print(f"Key {key} retrived value {message['value']}")
        case "GET_FAILURE":
            print(f"Failed to find value with key {key}")
        case _:
            print('huh')
    connection_socket.close()
    client.close()
    server.close()



if __name__ == "__main__":
    main()