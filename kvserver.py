#!/usr/bin/env python
import os
import json

from socket import socket, AF_INET, SOCK_STREAM, SOL_SOCKET, SO_REUSEADDR
from message import send_message, recv_message
from threading import Thread


class KVServer:
    def __init__(self, snapshot_dir="snapshots"):
        self.snapshot_dir = snapshot_dir
        self.db = {}
        self.commands = {
            "set": self.set,
            "get": self.get,
            "delete": self.delete,
            "snapshot": self.snapshot,
            "restore": self.restore,
        }

        self.debug = {
            "keys": self.keys,
            "log": self.log,
        }

        self.cmd_log = []

    def handle_message(self, msg):
        return self._parse_message(msg)

    def _parse_message(self, msg):
        # TODO: handle non byte-string?
        data = msg.decode("utf-8").strip().split(" ")
        if data and any(data[0] == cmd for cmd in self.commands):
            resp, err = self.commands[data[0]](data[1:])

            # Append to the log if the command was successful
            if not err:
                self.append(data)

            return resp

        if data and data[0] in self.debug:
            # call the debug command
            return self.debug[data[0]]()

        return b"INVALID COMMAND"

    def set(self, args):
        if len(args) < 2:
            print(args)
            return b"SET needs at least 2 arguments", True

        key, value = args[0], " ".join(args[1:])
        self.db[key] = value

        print(self.db)

        return b"OK", False

    def get(self, args):
        if not args:
            return b"GET needs a key to look up"

        key = args[0]

        try:
            resp = self.db[key]
        except KeyError:
            return f"{key} not found".encode(encoding="utf-8"), True

        return resp.encode(encoding="utf-8"), False

    def delete(self, args):
        if not args:
            return b"DELETE needs a key to delete"

        key = args[0]

        try:
            del self.db[key]
        except KeyError:
            return f"{key} not found".encode(encoding="utf-8"), True

        return b"OK", False

    def snapshot(self, args):
        if not os.path.isdir(self.snapshot_dir):
            return b"invalid snapshot directory"

        if not args:
            return b"SNAPSHOT requires a name to store the snapshot under"

        filename = f"{args[0]}.json"

        try:
            with open(os.path.join(self.snapshot_dir, filename), "w") as f:
                f.write(json.dumps(self.db))
        except Exception as e:
            print(e)
            return b"error saving snapshot", True

        return b"OK", False

    def restore(self, args):
        if not os.path.isdir(self.snapshot_dir):
            return b"invalid snapshot directory"

        if not args:
            return b"RESTORE requires a name to restore from"

        filename = f"{args[0]}.json"

        try:
            with open(os.path.join(self.snapshot_dir, filename)) as f:
                self.db = json.loads(f.read())
        except Exception as e:
            print(e)
            return b"error restoring from snapshot", True

        return b"OK", False

    def append(self, msg):
        self.cmd_log.append(msg)

    def keys(self):
        return json.dumps(self.db).encode("utf-8")

    def log(self):
        return json.dumps(self.cmd_log).encode("utf-8")


def main(addr):

    KV = KVServer()

    def handle_messages(sock):
        try:
            while True:
                msg = recv_message(sock)
                resp = KV.handle_message(msg)
                send_message(sock, resp)
        except IOError:
            sock.close()

    sock = socket(AF_INET, SOCK_STREAM)
    sock.setsockopt(SOL_SOCKET, SO_REUSEADDR, True)
    sock.bind(addr)
    sock.listen()
    while True:
        client, addr = sock.accept()
        print("Connection from:", addr)
        Thread(target=handle_messages, args=[client]).start()  # <<<


if __name__ == "__main__":
    print("Starting kvserver...")
    main(("localhost", 16000))
