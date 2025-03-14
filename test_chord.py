#!/usr/bin/env python3

import unittest
from my_chord import Chord_Node
import multiprocessing as mp
import time
import my_client
import ast


class ChordTests(unittest.TestCase):
    sockets = []
    procs = []


    def test_single_join(self):
        node = Chord_Node("127.0.0.1", 9000)
        self.sockets.append(node)
        (hash, succ, pred, data) = self.get_stats(node)
        self.assertTrue(node.server != None)
        self.assertTrue(node.me.ipv4 == succ and node.me.ipv4 == pred)

    def get_stats(self, node):
        with open(f"log/{node.me.ipv4}", 'r') as file:
            status = file.readlines()
            succ = status[1].split(" ")[1].rstrip("\n")
            pred = status[2].split(" ")[1].rstrip("\n")
            hash = status[0].split(" ")[1].rstrip("\n")
            data = status[3].split("Data:")[1].rstrip("\n")
            data = ast.literal_eval(data)
        return (hash, succ, pred, data)
    
    def test_ring_join(self):
        node1 = Chord_Node("127.0.0.1", 8000)
        self.sockets.append(node1)
        proc1 = mp.Process(target=node1.server_loop)
        proc1.start()
        self.procs.append(proc1)

        node2 = Chord_Node("127.0.0.1", 8002)
        proc2 = mp.Process(target=node2.server_loop)
        proc2.start()
        time.sleep(1)
        node2.send_join(node1.me.ipv4)
        self.procs.append(proc2)
        self.sockets.append(node2)

        node3 = Chord_Node("127.0.0.1", 8003)
        proc3 = mp.Process(target=node3.server_loop)
        proc3.start()
        time.sleep(1)
        node3.send_join(node1.me.ipv4)
        self.procs.append(proc3)
        self.sockets.append(node3)

        time.sleep(.5)
        (hash1,succ1,pred1,data1) = self.get_stats(node1)
        (hash2,succ2,pred2,data2) = self.get_stats(node2)
        (hash3,succ3,pred3,data3) = self.get_stats(node3)


        self.assertTrue(succ1 != pred1)
        self.assertTrue(succ2 != pred2)
        self.assertTrue(succ3 != pred3)

        self.assertTrue(node1.me.ipv4 == pred2 or node1.me.ipv4 == pred3)
        self.assertTrue(node1.me.ipv4 == succ2 or node1.me.ipv4 == succ3)

        self.assertTrue(node2.me.ipv4 == pred1 or node2.me.ipv4 == pred3)
        self.assertTrue(node2.me.ipv4 == succ1 or node2.me.ipv4 == succ3)

        self.assertTrue(node3.me.ipv4 == pred2 or node3.me.ipv4 == pred1)
        self.assertTrue(node3.me.ipv4 == succ2 or node3.me.ipv4 == succ1)
        
    def test_leave(self):
        node1 = Chord_Node("127.0.0.1", 10000)
        self.sockets.append(node1)
        proc1 = mp.Process(target=node1.server_loop)
        proc1.start()
        self.procs.append(proc1)

        node2 = Chord_Node("127.0.0.1", 10002)
        proc2 = mp.Process(target=node2.server_loop)
        proc2.start()
        time.sleep(1)
        node2.send_join(node1.me.ipv4)
        self.procs.append(proc2)
        self.sockets.append(node2)

        node3 = Chord_Node("127.0.0.1", 10003)
        proc3 = mp.Process(target=node3.server_loop)
        proc3.start()
        time.sleep(1)
        node3.send_join(node1.me.ipv4)
        self.procs.append(proc3)
        self.sockets.append(node3)

        time.sleep(.5)
        my_client.put("banana", "whaaat", node1.me.ipv4)
        my_client.put("strawberry", "yummy", node1.me.ipv4)
        my_client.put("orange", "peeled", node1.me.ipv4)

        time.sleep(.5)
        (hash2,succ2,pred2,data2) = self.get_stats(node2)

        my_client.leave(node2.me.ip, node2.me.port, node1.me.ipv4)
        time.sleep(.5)
        (hash1,succ1,pred1,data1) = self.get_stats(node1)
        (hash3,succ3,pred3,data3) = self.get_stats(node3)

        truth = True
        print(data1)
        print(data2)
        print(data3)
        for (k2,v2) in data2.items():
            print(v2 in data1.values())
            print(v2 in data3.values())
            if (v2 in data1.values()) or (v2 in data3.values()):
                truth = True
            else:
                truth = False

        self.assertTrue(truth)
        self.assertTrue(succ1 == pred1 and succ1 == node3.me.ipv4)
        self.assertTrue(succ3 == pred3 and pred3 == node1.me.ipv4)

    def test_lookup(self):
        node1 = Chord_Node("127.0.0.1", 11000)
        self.sockets.append(node1)
        proc1 = mp.Process(target=node1.server_loop)
        proc1.start()
        self.procs.append(proc1)

        node2 = Chord_Node("127.0.0.1", 11002)
        proc2 = mp.Process(target=node2.server_loop)
        proc2.start()
        time.sleep(1)
        node2.send_join(node1.me.ipv4)
        self.procs.append(proc2)
        self.sockets.append(node2)

        node3 = Chord_Node("127.0.0.1", 11003)
        proc3 = mp.Process(target=node3.server_loop)
        proc3.start()
        time.sleep(1)
        node3.send_join(node1.me.ipv4)
        self.procs.append(proc3)
        self.sockets.append(node3)

        time.sleep(.5)
        my_client.put("banana", "whaaat", node1.me.ipv4)
        my_client.put("strawberry", "yummy", node1.me.ipv4)
        my_client.put("orange", "peeled", node1.me.ipv4)

        time.sleep(.5)
        ip_result = my_client.lookup("orange", node1.me.ipv4)
        print(f"RESULT ++ {ip_result}")
        (hash1,succ1,pred1,data1) = self.get_stats(node1)
        (hash2,succ2,pred2,data2) = self.get_stats(node2)
        (hash3,succ3,pred3,data3) = self.get_stats(node3)
        print([data1,data2,data3])
        self.assertTrue(ip_result == node1.me.ipv4 or ip_result == node2.me.ipv4 or ip_result == node3.me.ipv4)
        bad_try = my_client.lookup("food", node1.me.ipv4)
        self.assertFalse(bad_try)

    def test_put(self):
        node1 = Chord_Node("127.0.0.1", 12000)
        self.sockets.append(node1)
        proc1 = mp.Process(target=node1.server_loop)
        proc1.start()
        self.procs.append(proc1)

        node2 = Chord_Node("127.0.0.1", 12002)
        proc2 = mp.Process(target=node2.server_loop)
        proc2.start()
        time.sleep(1)
        node2.send_join(node1.me.ipv4)
        self.procs.append(proc2)
        self.sockets.append(node2)

        node3 = Chord_Node("127.0.0.1", 12003)
        proc3 = mp.Process(target=node3.server_loop)
        proc3.start()
        time.sleep(1)
        node3.send_join(node1.me.ipv4)
        self.procs.append(proc3)
        self.sockets.append(node3)

        time.sleep(.5)
        my_client.put("banana", "whaaat", node1.me.ipv4)
        my_client.put("strawberry", "yummy", node1.me.ipv4)
        my_client.put("orange", "intact", node1.me.ipv4)
        my_client.put("orange", "peeled", node1.me.ipv4)

        time.sleep(.5)
        (k1,v1) = my_client.get("orange", node1.me.ipv4)
        (k2, v2) = my_client.get("fake", node1.me.ipv4)

        self.assertTrue(v2 == None)
        self.assertTrue(v1 == "peeled")
        print(v1)
        

    def teardown(self):
        for socket in self.sockets:
            socket.close()
        for process in mp.active_children():
            process.terminate()
            process.join()
            

if __name__ == "__main__":
    unittest.main()