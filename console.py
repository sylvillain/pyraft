import pickle
from threading import Thread

from controller import NewCommandMessage

from net import RaftNet

def console():
    net = RaftNet()

    while True:
        cmd = input(f"> ")
        message = cmd.split()
        dest = message.pop(0)
        cmd = message.pop(0)
        if cmd == 'command':
            message = NewCommandMessage(' '.join(message))
        else:
            print("invalid command")
        print(net.send(int(dest), pickle.dumps(message)))

if __name__ == '__main__':
    import sys
    console()
