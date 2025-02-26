#!/usr/bin/env python3

#for CLA
import sys

# SHA-1-boozey
import hashlib

# don't want long ahh string on one line hate python for that
argstring = "Please use args in this fashion:\n"
argstring = argstring + "  To join as first node: ./node.py join -ip <IP> -p <port>\n"
argstring = argstring + "  To join ring: ./node.py join -ip <IP> -p <port> -c <IP+port>\n"


hash_name = "0000"
ip = "(.     -   .)"
port = "@_@"
connection = ">:("

def main():

    connecting = False
    global hash_name
    global port
    global ip

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
        elif arg == "c":
            i += 1
            connecting = True
            connection = sys.argv[i]

    # should now have all our args we need to get going !
    if connecting:
        connect_ring()
    else:
        init_ring()


def init_ring():
    print("init")

    

def connect_ring():
    print("connect")
    
            


if __name__ == "__main__":
    main()