from dataclasses import dataclass
from log import LogEntry


def send_message(sock, msg):
    size = b"%10d" % len(msg)  # Make a 10-byte length field
    sock.sendall(size)
    sock.sendall(msg)


def recv_exactly(sock, nbytes):
    chunks = []
    while nbytes > 0:
        chunk = sock.recv(nbytes)
        if chunk == b"":
            raise IOError("Incomplete message")
        chunks.append(chunk)
        nbytes -= len(chunk)
    return b"".join(chunks)


def recv_message(sock):
    size = int(recv_exactly(sock, 10))
    msg = recv_exactly(sock, size)
    return msg


@dataclass
class AppendEntriesMessage:
    # Sent from leader to follower to replicate log entries
    prev_log_idx: int
    prev_log_term: int
    entries: list[LogEntry]
    leader_commit_index: int
    term: int


@dataclass
class AppendEntriesResponse:
    success: bool
    last_applied_index: int
    term: int
    nodenum: int


@dataclass
class NewCommandMessage:
    command: str


@dataclass
class RequestVoteMessage:
    term: int
    candidate_id: int
    last_log_index: int
    last_log_term: int


@dataclass
class RequestVoteResponse:
    success: bool
    term: int
    nodenum: int


@dataclass
class ClockTick:
    milliseconds: float
