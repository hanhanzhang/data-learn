package com.sdu.data.flink.changelog;

import java.util.List;
import java.util.UUID;

import org.apache.flink.runtime.state.changelog.SequenceNumber;
import org.apache.flink.runtime.state.changelog.StateChange;

public class KafkaStateChangeSet {

    private final UUID logId;
    private final List<StateChange> changes;
    private final SequenceNumber sequenceNumber;

    public KafkaStateChangeSet(UUID logId, List<StateChange> changes, SequenceNumber sequenceNumber) {
        this.logId = logId;
        this.changes = changes;
        this.sequenceNumber = sequenceNumber;
    }

    public UUID getLogId() {
        return logId;
    }

    public List<StateChange> getChanges() {
        return changes;
    }

    public SequenceNumber getSequenceNumber() {
        return sequenceNumber;
    }
}
