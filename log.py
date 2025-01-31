from itertools import zip_longest
from dataclasses import dataclass


@dataclass
class LogEntry:
    term: int
    command: str


class RaftLog:
    def __init__(self):
        self.log_entries = [LogEntry(0, "")]
        # do we need either of these?

    # TODO: might not want to use term here -- move that up a layer...
    # TODO: why do we need leader_commit here?
    def append_entry(self, prev_log_idx, prev_log_term, entries):
        if len(self.log_entries) <= prev_log_idx:
            return False

        if self.log_entries[prev_log_idx].term != prev_log_term:
            # print(prev_log_idx, prev_log_term)
            return False

        if entries:
            pairs = zip_longest(self.log_entries[prev_log_idx + 1 :], entries)
            for idx, pair in enumerate(pairs, start=prev_log_idx + 1):
                # current_entry is what the log already has
                # entry is the entry we want to add
                current_entry, append_entry = pair
                if (
                    current_entry
                    and append_entry
                    and current_entry.term > append_entry.term
                ):

                    return False

                if (
                    current_entry
                    and append_entry
                    and (
                        current_entry.term < append_entry.term
                        or current_entry.command != append_entry.command
                    )
                ):
                    # add entries from here on to end of log
                    # print(self.log_entries)
                    # print(f"term at index {idx} does not match. deleting entries")
                    self.log_entries = self.log_entries[:idx] + [append_entry]
                elif current_entry is None and append_entry:
                    self.log_entries.append(append_entry)

        return True

    def __str__(self):
        return str(self.log_entries)


if __name__ == "__main__":
    # Append tests...

    def test_raftlog():
        # Reply False if term < current_term
        rl = RaftLog()
        rl.current_term = 5
        assert rl.append_entry(0, 1, []) == False

        # Reply false if log doesn't contain an entry at prev_log_idx whose term
        # matches prev_log_term
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
        assert rl.log_entries == [
            LogEntry(term=0, command=""),
            LogEntry(term=1, command=""),
            LogEntry(term=3, command="noconflict"),
        ]

        # print("Should be idempotent")
        assert rl.append_entry(1, 1, [LogEntry(3, "noconflict")]) == True
        assert rl.log_entries == [
            LogEntry(term=0, command=""),
            LogEntry(term=1, command=""),
            LogEntry(term=3, command="noconflict"),
        ]

        assert rl.append_entry(1, 1, []) == True
        assert rl.log_entries == [
            LogEntry(term=0, command=""),
            LogEntry(term=1, command=""),
            LogEntry(term=3, command="noconflict"),
        ]

        assert rl.append_entry(1, 1, []) == True
        assert rl.log_entries == [
            LogEntry(term=0, command=""),
            LogEntry(term=1, command=""),
            LogEntry(term=3, command="noconflict"),
        ]

        assert rl.append_entry(2, 3, [LogEntry(3, "another message")]) == True
        assert rl.log_entries == [
            LogEntry(term=0, command=""),
            LogEntry(term=1, command=""),
            LogEntry(term=3, command="noconflict"),
            LogEntry(term=3, command="another message"),
        ]

        assert (
            rl.append_entry(3, 3, [LogEntry(4, "multi1"), LogEntry(4, "multi2")])
            == True
        )
        assert rl.log_entries == [
            LogEntry(term=0, command=""),
            LogEntry(term=1, command=""),
            LogEntry(term=3, command="noconflict"),
            LogEntry(term=3, command="another message"),
            LogEntry(term=4, command="multi1"),
            LogEntry(term=4, command="multi2"),
        ]

    test_raftlog()
