from threading import Thread
from socket import socket, AF_INET, SOCK_STREAM, SOL_SOCKET, SO_REUSEADDR
from message import send_message, recv_message

from config import SERVERS
from kvserver import KVServer

class RaftNet:
    def __init__(self, nodenum:int):
        # My own address
        self.nodenum = nodenum
        self.addr = SERVERS[nodenum]
        self.kv = KVServer(snapshot_dir=f"snapshots{self.nodenum}")

        self.sock = socket(AF_INET, SOCK_STREAM)
        self.sock.setsockopt(SOL_SOCKET, SO_REUSEADDR, True)
        self.sock.bind(self.addr)
        self.sock.listen()
        
    # Send a message to a specific node number
    def send(self, destination: int, message:bytes):
        addr = SERVERS.get(destination)

        # if destination == self.nodenum or not addr:
            # print('invalid destination', destination)
            # return

        try:
            sock = socket(AF_INET, SOCK_STREAM)
            sock.connect(addr)
            send_message(sock, message)       
            response = recv_message(sock)
            # print(response)

        except Exception as e:
            pass
        finally:
            sock.close()
        return True

    # Receive and return any message that was sent to me
    def receive(self, handle_message=None) -> bytes:
        def handle_messages(sock, handle_message):
            try:
                while True:
                    msg = recv_message(sock)
                    if handle_message:
                        resp = handle_message(msg)
                    else:
                        resp = self.kv.handle_message(msg)
                    send_message(sock, resp)
            except IOError:
                sock.close()

        while True:
            client, addr = self.sock.accept()
            # print('Connection from:', addr)
            Thread(target=handle_messages, args=[client, handle_message]).start()

