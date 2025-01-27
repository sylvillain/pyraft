import queue
import random

from dataclasses import dataclass
from log import RaftLog, LogEntry

@dataclass
class AppendEntriesMessage:
    # Sent from leader to follower to replicate log entries
    prev_log_idx: int
    prev_log_term: int
    entries : list[LogEntry]
    leader_commit_index: int
    term: int

@dataclass
class AppendEntriesResponse:
    # TODO: this probably needs to track the success of each message
    success: bool
    last_applied_index: int
    # possibly not needed
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

class TimeoutMessage:
    pass


class RaftController:
    def __init__(self, nodenum:int, role='FOLLOWER'):
        self.nodenum = nodenum
        self.log = RaftLog()
        self.commit_index = 0
        self.outgoing_messages = queue.Queue()
        self.incoming_messages = queue.Queue()

        self.term = 0

        self.timeout = 0
        self.reset_timeout()


        self.candidate_id = None

        self._last_applied_indexes = {
            1: 0,
            2: 0,
            3: 0,
            4: 0,
            5: 0,
        }

        self.votes = {
            1: None,
            2: None,
            3: None,
            4: None,
            5: None,
        }

        if role == True:
            role = 'LEADER'
        self.role = role

    @property
    def leader(self):
        return self.role == 'LEADER'

    @property
    def last_applied(self):
        return len(self.log.log_entries) - 1

    @property
    def last_applied_index(self):
        return len(self.log.log_entries) - 1

    @property
    def last_applied_term(self):
        return self.log.log_entries[self.last_applied_term].term

    def reset_timeout(self):
        self.timeout = (random.random() * 1000) + 500 

    def handle_message(self, msg):
        if isinstance(msg, AppendEntriesMessage):
            return self._handle_append_entries_message(msg)
        elif isinstance(msg, AppendEntriesResponse):
            return self._handle_append_entries_response(msg)
        elif isinstance(msg, RequestVoteMessage):
            return self._handle_request_vote_message(msg)
        elif isinstance(msg, RequestVoteResponse):
            return self._handle_request_vote_response(msg)
        elif isinstance(msg, ClockTick):
            self._handle_clock_tick(msg)

        # This may not be needed if we make AppendEntriesMessage
        # work similarly...
        elif isinstance(msg, NewCommandMessage):
            return self._handle_add_new_command(msg)

    def _handle_append_entries_message(self, msg: AppendEntriesMessage):

        if msg.term > self.term:
            self.become_follower()

        self.term = msg.term
        self.reset_timeout()

        # TODO: if fail return AppendEntriesResp with idx that needs resolving
        success = self.log.append_entry(
            msg.prev_log_idx,
            msg.prev_log_term,
            msg.entries
        )

        # commit_index for a follower is the min of last_applied and the leader commit index
        self.commit_index = min(msg.leader_commit_index, self.last_applied)


        resp = AppendEntriesResponse(success, self.last_applied, self.term, self.nodenum)
        self.send(resp)

    def _handle_append_entries_response(self, msg: AppendEntriesResponse):
        if not self.leader:
            return

        if msg.term > self.term:
            self.become_follower()
            return

        self.update_last_applied(msg.nodenum, msg.last_applied_index)

        if msg.last_applied_index == self.last_applied:
            return

        if (self.leader 
            and msg.success 
            and msg.last_applied_index < self.last_applied_index):
            # send append and index + 1
            resp = AppendEntriesMessage(
                msg.last_applied_index,
                self.log.log_entries[msg.last_applied_index].term,
                [self.log.log_entries[msg.last_applied_index+1]],
                self.commit_index,
                self.term
            )

            self.send(resp)

        if self.leader and not msg.success and msg.last_applied_index < len(self.log.log_entries) - 1:
            # send append and index + 1
            prev_idx = msg.last_applied_index 
            logs = [self.log.log_entries[prev_idx+1]],
            resp = AppendEntriesMessage(
                prev_idx - 1,
                self.log.log_entries[prev_idx-1].term,
                # self.log.log_entries[prev_idx+1:],
                self.log.log_entries[prev_idx+1:],
                # len(self.log.log_entries),
                self.commit_index,
                self.term
            )
            self.send(resp)

        return True
    
    def _handle_request_vote_message(self, msg: RequestVoteMessage):
        # reply false in term 
        try:
            vote_granted = True
            if msg.term < self.term:
                vote_granted = False
            elif self.prev_log_idx > msg.last_log_index:
                vote_granted = False
            elif self.candidate_id is not None:
                vote_granted = False
            else:
                self.become_follower()
                self.candidate_id = msg.candidate_id
        except Exception as e:
            print(e)

        self.term = max(self.term, msg.term)

        resp = RequestVoteResponse(vote_granted, self.term, self.nodenum)
        self.send(resp)

    def _handle_request_vote_response(self, msg: RequestVoteResponse):
        try:
            if not msg.success or msg.term > self.term:
                self.become_follower()
                return
            elif self.role == 'CANDIDATE' and msg.term == self.term:
                self.votes[msg.nodenum] = True
            if len([vote for vote in self.votes.values() if vote]) >=3 and self.role != 'LEADER':
                self.become_leader()
                self.send_heartbeat()
        except Exception as e:
            print(self.votes)

    def send_heartbeat(self):
        self.send(AppendEntriesMessage(self.prev_log_idx, self.prev_log_term, [], self.commit_index, self.term))

    def _handle_clock_tick(self, msg: ClockTick):
        if self.role == 'LEADER':
            self.send_heartbeat()
            return
        self.timeout -= msg.milliseconds
        if self.role != 'LEADER' and self.timeout < 0:
            print(f"Node: {self.nodenum} timed out. Becoming candidate")
            self.become_candidate()
            self.reset_timeout()
    
    def _handle_add_new_command(self, msg: NewCommandMessage):
        if self.role != 'LEADER':
            return False
        entry = LogEntry(term=self.term, command=msg.command)
        success = self.append_entry(entry)
        print(f"{self.nodenum}: {self.log.log_entries}")
        if success:
            # 0 indexed but also we just added to the log.
            prev_idx = len(self.log.log_entries) - 2
            prev_term = self.log.log_entries[prev_idx].term
            self.send(
                AppendEntriesMessage(
                    prev_idx,
                    prev_term,
                    [entry],
                    # TODO: make this leader commit
                    len(self.log.log_entries) - 1,
                    self.term
                )
            )
        return success

    def append_entry(self, msg: LogEntry):
        # TODO: handling term/current term
        # msg.term
        prev_log_idx = len(self.log.log_entries) - 1
        prev_log_term = self.log.log_entries[-1].term
        return self.log.append_entry(prev_log_idx, prev_log_term, [msg])
    
    @property
    def prev_log_idx(self):
        return len(self.log.log_entries) - 1

    @property
    def prev_log_term(self):
        return self.log.log_entries[-1].term

    @property
    def last_applied_indexes(self):
        result = self._last_applied_indexes
        result[self.nodenum] = self.last_applied
        return result

    def update_last_applied(self, nodenum, n):
        self._last_applied_indexes[nodenum] = n

        try:
            # You cannot change the commit index unless the log matches the current term
            commit_index = min(sorted(self.last_applied_indexes.values(), reverse=True)[:3])
            if self.log.log_entries[commit_index].term == self.term:
                self.commit_index = commit_index
        except Exception as e:
            print(e)

    def send(self, msg):
        return self.queue_outgoing_message(msg)

    def receive(self, msg):
        return self.queue_incoming_message(msg)

    def handle_incoming(self):
        msg = self.incoming_messages.get()
        return self.handle_message(msg)

    def handle_outgoing(self):
        return self.outgoing_messages.get()

    def queue_incoming_message(self, msg):
        if isinstance(msg, NewCommandMessage) and self.role != 'LEADER':
            return b'command sent to node that is not leader'
        self.incoming_messages.put(msg)
        return True

    def queue_outgoing_message(self, msg):
        self.outgoing_messages.put(msg)
        return True

    def become_leader(self):
        self.role = 'LEADER'
        print(f"Node: {self.nodenum} becoming leader.")

    def become_candidate(self):
        self.role = 'CANDIDATE'
        self.term += 1
        self.candidate_id = self.nodenum
        self.votes = {
            1: None,
            2: None,
            3: None,
            4: None,
            5: None,
        }
        self.votes[self.nodenum] = True

        try:
            msg = RequestVoteMessage(
                term=self.term,
                candidate_id=self.nodenum,
                last_log_index=self.prev_log_idx,
                last_log_term=self.prev_log_term,
            )
        except Exception as e:
            print(e)
        self.send(msg)

    def become_follower(self):
        self.role = 'FOLLOWER'
        self.candidate_id = None
        self.votes = {
            1: None,
            2: None,
            3: None,
            4: None,
            5: None,
        }
        self.reset_timeout()


if __name__ == "__main__":
    controller1 = RaftController(1)
    controller1.receive(NewCommandMessage('set y 1'))
    controller1.receive(NewCommandMessage('set x 2'))
    controller1.receive(NewCommandMessage('set z 3'))
    assert controller1.incoming_messages.qsize() == 3
    assert controller1.log.log_entries == [LogEntry(0, '')]
    controller1.handle_incoming()
    controller1.handle_incoming()
    assert controller1.log.log_entries == [
            LogEntry(0, ''),
            LogEntry(0, 'set y 1'),
            LogEntry(0, 'set x 2'),
    ]
    assert controller1.incoming_messages.qsize() == 1

    controller2 = RaftController(2)

    while controller1.outgoing_messages.qsize():
        msg = controller1.outgoing_messages.get()
        controller2.receive(msg)
        controller2.handle_incoming()
    
