from controller import RaftController, NewCommandMessage, AppendEntriesMessage, AppendEntriesResponse, RequestVoteMessage, RequestVoteResponse, ClockTick
from log import RaftLog, LogEntry


class TestRaftLog:
    def test_raftlog(self):
        # Reply true with empty logentries (heartbeat?)
        rl = RaftLog()
        assert rl.append_entry(0, 1, []) == False

        # If an existing entry conflicts with a new one (same index
        # but different terms), delete the existing entry and all that
        # follow it 
        rl = RaftLog()
        rl.log_entries.append(LogEntry(1, ""))
        rl.log_entries.append(LogEntry(2, ""))
        rl.log_entries.append(LogEntry(3, ""))
        rl.term = 1
        # print("Remove entries that don't match")
        assert rl.append_entry(1, 1, [LogEntry(3, "noconflict")]) == True
        assert rl.log_entries == [LogEntry(term=0, command=''),
                                  LogEntry(term=1, command=''),
                                  LogEntry(term=3, command='noconflict')]

        # print("Should be idempotent")
        assert rl.append_entry(1, 1, [LogEntry(3, "noconflict")]) == True
        assert rl.log_entries == [LogEntry(term=0, command=''),
                                  LogEntry(term=1, command=''),
                                  LogEntry(term=3, command='noconflict')]

        assert rl.append_entry(1, 1, []) == True
        assert rl.log_entries == [LogEntry(term=0, command=''),
                                  LogEntry(term=1, command=''),
                                  LogEntry(term=3, command='noconflict')]

        assert rl.append_entry(1, 1, []) == True
        assert rl.log_entries == [LogEntry(term=0, command=''),
                                  LogEntry(term=1, command=''),
                                  LogEntry(term=3, command='noconflict')]

        assert rl.append_entry(2, 3, [LogEntry(3, "another message")]) == True
        assert rl.log_entries == [LogEntry(term=0, command=''),
                                  LogEntry(term=1, command=''),
                                  LogEntry(term=3, command='noconflict'),
                                  LogEntry(term=3, command='another message')]

        assert rl.append_entry(3, 3, [LogEntry(4, "multi1"), LogEntry(4, "multi2")]) == True
        assert rl.log_entries == [LogEntry(term=0, command=''),
                                  LogEntry(term=1, command=''),
                                  LogEntry(term=3, command='noconflict'),
                                  LogEntry(term=3, command='another message'),
                                  LogEntry(term=4, command="multi1"),
                                  LogEntry(term=4, command="multi2")]
    def test_mismatched_term(self):
        rl = RaftLog()
        rl.log_entries = [LogEntry(term=0, command=''),
                          LogEntry(term=1, command=''),
                          LogEntry(term=2, command=''),
                          LogEntry(term=2, command='noconflict'),
                          LogEntry(term=3, command='another message'),
                          LogEntry(term=4, command="multi1"),
                          LogEntry(term=4, command="multi2")]

        result = rl.append_entry(1, 1, [LogEntry(term=5, command='ahem')])
        assert result == True
        assert rl.log_entries == [LogEntry(term=0, command=''),
                                  LogEntry(term=1, command=''),
                                  LogEntry(term=5, command='ahem')]

        result = rl.append_entry(1, 1, [LogEntry(term=3, command='woah')])
        assert result == False


class TestRaftControllerReplication:
    def test_receive_new_command(self):
        controller = RaftController(1, True)
        # a client has sent a NewCommandMessage
        controller.receive(NewCommandMessage('set x 1'))
        assert controller.incoming_messages.qsize() == 1
        assert controller.outgoing_messages.qsize() == 0
        assert controller.log.log_entries == [
                LogEntry(0, ''),
        ]
        # handle incoming adds a log entry
        assert controller.handle_incoming()
        assert controller.incoming_messages.qsize() == 0
        # after the log is created make a message to "send" an AppendEntries message.
        assert controller.outgoing_messages.qsize() == 1
        assert controller.log.log_entries == [
                LogEntry(0, ''),
                LogEntry(0, 'set x 1'),
        ]
        msg = controller.outgoing_messages.get_nowait()
        assert msg == AppendEntriesMessage(0, 0, [LogEntry(0, 'set x 1')], 1, 0)

    def test_receive_append_entries_message(self):
        controller = RaftController(1, False)
        assert controller.log.log_entries == [LogEntry(0, '')]
        controller.receive(AppendEntriesMessage(0, 0, [LogEntry(0, 'set x 1')], 0, 0))
        controller.handle_incoming()
        assert controller.log.log_entries == [
            LogEntry(0, ''),
            LogEntry(0, 'set x 1'),
        ]
        assert controller.outgoing_messages.qsize() == 1
        resp = controller.outgoing_messages.get_nowait()
        assert resp == AppendEntriesResponse(True, 1, 0, 1)

        controller.receive(AppendEntriesMessage(1, 0, [LogEntry(0, 'set y 2')], 0, 0))
        controller.handle_incoming()
        assert controller.log.log_entries == [
            LogEntry(0, ''),
            LogEntry(0, 'set x 1'),
            LogEntry(0, 'set y 2'),
        ]
        assert controller.outgoing_messages.qsize() == 1
        resp = controller.outgoing_messages.get_nowait()
        assert resp == AppendEntriesResponse(True, 2, 0, 1)

        # duplicate messages should be idempotent
        controller.receive(AppendEntriesMessage(0, 0, [LogEntry(0, 'set x 1')], 0, 0))
        controller.handle_incoming()
        assert controller.log.log_entries == [
            LogEntry(0, ''),
            LogEntry(0, 'set x 1'),
            LogEntry(0, 'set y 2'),
        ]
        assert controller.outgoing_messages.qsize() == 1
        resp = controller.outgoing_messages.get_nowait()
        assert resp == AppendEntriesResponse(True, 2, 0, 1)

        # messages that differ from previous should toss the rest of the log
        controller.receive(AppendEntriesMessage(0, 0, [LogEntry(0, 'set x 3')], 0, 0))
        controller.handle_incoming()
        assert controller.log.log_entries == [
            LogEntry(0, ''),
            LogEntry(0, 'set x 3'),
        ]
        assert controller.outgoing_messages.qsize() == 1
        resp = controller.outgoing_messages.get_nowait()
        assert resp == AppendEntriesResponse(True, 1, 0, 1)

    def test_leader_receive_append_entries_response(self):
        controller1 = RaftController(1, True)
        assert controller1.log.log_entries == [LogEntry(0, '')]
        controller1.receive(NewCommandMessage('set x 1'))
        controller1.receive(NewCommandMessage('set y 2'))
        controller1.receive(NewCommandMessage('set z 3'))
        controller1.handle_incoming()
        controller1.handle_incoming()
        controller1.handle_incoming()
        assert controller1.log.log_entries == [
            LogEntry(0, ''),
            LogEntry(0, 'set x 1'),
            LogEntry(0, 'set y 2'),
            LogEntry(0, 'set z 3'),
        ]
        assert controller1.outgoing_messages.qsize() == 3

        controller2 = RaftController(2, False)
        while controller1.outgoing_messages.qsize():
            msg = controller1.outgoing_messages.get_nowait()
            controller2.receive(msg)
            controller2.handle_incoming()
            controller2.handle_outgoing()

        assert controller1.log.log_entries == controller2.log.log_entries

        controller1.log.log_entries = [
            LogEntry(0, ''),
            LogEntry(0, 'set x 1'),
            LogEntry(0, 'set y 4'),
            LogEntry(0, 'set z 3'),
        ]

        controller2.receive(AppendEntriesMessage(1, 0, [LogEntry(0, 'set y 4')], 0, 0))
        controller2.handle_incoming()
        assert controller2.outgoing_messages.qsize() == 1
        msg = controller2.outgoing_messages.get_nowait()
        # last applied...
        assert msg == AppendEntriesResponse(True, 2, 0, 2)

        # this should override the entry to y 4
        assert controller2.log.log_entries == [
            LogEntry(0, ''),
            LogEntry(0, 'set x 1'),
            LogEntry(0, 'set y 4'),
        ]

        # Ensure that we can play back when rewriting the log.
        controller1.receive(msg)
        controller1.handle_incoming()
        msg = controller1.outgoing_messages.get_nowait()
        assert msg == AppendEntriesMessage(2, 0, [LogEntry(0, 'set z 3')], 0, 0)

        controller2.receive(msg)
        controller2.handle_incoming()
        # should send success and its commit_index == 2
        # assert controller2.log.log_entries == controller1.log.log_entries

        assert controller2.outgoing_messages.qsize() == 1
        msg = controller2.outgoing_messages.get_nowait()

        # last_commit_index is min(leader_commit_index, last_applied_index)
        assert msg == AppendEntriesResponse(True, 3, 0, 2)
        assert controller2.last_applied == 3
        assert controller2.commit_index == 0

        controller1.receive(msg)
        assert controller1.incoming_messages.qsize() == 1
        controller1.handle_incoming()
        assert controller1.last_applied_indexes[2] == 3

        # commit_index is still 0 because leader.commit_index == 0
        assert controller1.last_applied_indexes == {1: 3, 2: 3, 3: 0, 4: 0, 5: 0}
        assert controller2.commit_index == 0
        


class TestRaftControllerCommits:
    def test_commit_index(self):
        controller1 = RaftController(1, True)
        # a client has sent a NewCommandMessage
        controller1.receive(NewCommandMessage('set x 1'))
        controller1.receive(NewCommandMessage('set y 2'))
        controller1.receive(NewCommandMessage('set z 2'))
        assert controller1.last_applied == 0
        assert controller1.last_applied_indexes == {1: 0, 2: 0, 3: 0, 4: 0, 5: 0}
        controller1.handle_incoming()
        controller1.handle_incoming()
        controller1.handle_incoming()
        assert controller1.last_applied == 3
        assert controller1.last_applied_indexes == {1: 3, 2: 0, 3: 0, 4: 0, 5: 0}

        controller1.receive(AppendEntriesResponse(True, 2, 0, 2))
        controller1.receive(AppendEntriesResponse(True, 2, 0, 3))
        controller1.handle_incoming()
        controller1.handle_incoming()
        assert controller1.last_applied_indexes == {1: 3, 2: 2, 3: 2, 4: 0, 5: 0}
        assert controller1.commit_index == 2
        controller1.receive(AppendEntriesResponse(True, 3, 0, 4))
        controller1.receive(AppendEntriesResponse(True, 3, 0, 5))
        controller1.handle_incoming()
        controller1.handle_incoming()
        assert controller1.last_applied_indexes == {1: 3, 2: 2, 3: 2, 4: 3, 5: 3}
        assert controller1.commit_index == 3

    def test_commit_index_failed_append(self):
        # when a message fails commit index should be the current leader commit index
        pass

    def test_resolve_last_applied(self):
        log1 = [LogEntry(0, ''), LogEntry(1, ''), LogEntry(1, ''), LogEntry(3, '')]
        log2 = [LogEntry(0, ''), LogEntry(1, ''), LogEntry(2, '')]
        # if commit_index > last applied apply log[last_applied] to state machine

    def test_resolve_match_index(self):
        pass

    def test_commit_index_crashes(self):
        # election process -- higher term on the log always wins
        pass


class TestRaftControllerConsensus:
    def test_vote_only_once(self):
        pass

    def test_higher_term_converts_leader_to_follower(self):
        # if request or response contains term T > currentTerm:
        # set currentTerm = T convert to follower 
        controller = RaftController(1, 'LEADER')
        assert controller.role == 'LEADER'
        controller.receive(AppendEntriesMessage(0, 0, [], 0, 2))
        controller.handle_incoming()
        # Receiving a message with a higher term should convert a leader to a follwer
        assert controller.role == 'FOLLOWER'

        controller = RaftController(1, 'LEADER')
        assert controller.role == 'LEADER'
        controller.receive(AppendEntriesResponse(False, 0, 1, 2))
        controller.handle_incoming()
        # Receiving a message with a higher term should convert a leader to a follwer
        assert controller.role == 'FOLLOWER'

        controller = RaftController(1, 'LEADER')
        assert controller.role == 'LEADER'
        controller.receive(RequestVoteMessage(term=2, candidate_id=2, last_log_index= 0, last_log_term=0))
        controller.handle_incoming()
        # Receiving a message with a higher term should convert a leader to a follwer
        assert controller.role == 'FOLLOWER'

        resp = controller.outgoing_messages.get_nowait()
        assert resp == RequestVoteResponse(True, 2, 1)
        assert controller.candidate_id == 2

        controller = RaftController(1, 'CANDIDATE')
        assert controller.role == 'CANDIDATE'
        controller.receive(RequestVoteResponse(True, 2, 1))
        controller.handle_incoming()
        # Receiving a message with a higher term should convert a leader to a follwer
        assert controller.role == 'FOLLOWER'

    def test_handle_request_vote_message(self):
        pass

    def test_handle_request_vote_response(self):
        pass

    def test_handle_clock_tick(self):
        controller = RaftController(1, 'FOLLOWER')
        assert controller.role == 'FOLLOWER'
        assert controller.term == 0
        controller.receive(ClockTick(10000.0))
        controller.handle_incoming()
        assert controller.term == 1
        assert controller.role == 'CANDIDATE'

