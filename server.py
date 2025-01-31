# TODO: pickle isn't secure...
import pickle
import time

from threading import Thread

from net import RaftNet
from message import ClockTick
from controller import RaftController


class RaftServer:
    def __init__(self, nodenum, leader=False):
        self.nodenum = nodenum
        role = "FOLLOWER"
        if leader:
            role = "LEADER"
        self.net = RaftNet(nodenum)  # TODO: make this not be a network if we want
        self.controller = RaftController(nodenum, role)
        Thread(target=self.net.receive, args=[self.handle_message]).start()
        Thread(target=self.handle_incoming, args=[]).start()
        Thread(target=self.handle_outgoing, args=[]).start()

    @property
    def log(self):
        return self.controller.log.log_entries

    def handle_incoming(self):
        while True:
            try:
                self.controller.handle_incoming()
            except Exception as e:
                pass

    def handle_outgoing(self):
        while True:
            msg = self.controller.handle_outgoing()
            # Fan out requests to every node.
            for nodenum in [1, 2, 3, 4, 5]:
                if nodenum != self.nodenum:
                    Thread(target=self.send, args=[nodenum, msg]).start()

    def handle_message(self, msg):
        msg = pickle.loads(msg)
        resp = self.controller.receive(msg)
        if isinstance(resp, bytes):
            return resp
        elif resp:
            return b"ok"
        else:
            return b"fail"

    def send(self, nodenum, msg):
        self.net.send(nodenum, pickle.dumps(msg))

    def receive(self, msg):
        self.net.receive(msg)


def clock(controller):
    while True:
        interval = 0
        if controller.role == "LEADER":
            time.sleep(0.50)
            interval = 50.0
        else:
            time.sleep(0.1)
            interval = 100.0
        msg = ClockTick(interval)
        controller.receive(msg)


if __name__ == "__main__":
    server1 = RaftServer(1)
    server2 = RaftServer(2)
    server3 = RaftServer(3)
    server4 = RaftServer(4)
    server5 = RaftServer(5)

    Thread(target=clock, args=[server1.controller]).start()
    Thread(target=clock, args=[server2.controller]).start()
    Thread(target=clock, args=[server3.controller]).start()
    Thread(target=clock, args=[server4.controller]).start()
    Thread(target=clock, args=[server5.controller]).start()

    # def info():
    # while True:
    # print(f"{server1.controller.role}, {server2.controller.role}, {server3.controller.role}, {server4.controller.role}, {server5.controller.role}")
    # time.sleep(1)
    # Thread(target=info, args=[]).start()
