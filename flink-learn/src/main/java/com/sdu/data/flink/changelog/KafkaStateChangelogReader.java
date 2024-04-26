package com.sdu.data.flink.changelog;

import java.io.IOException;

import org.apache.flink.runtime.state.changelog.StateChange;
import org.apache.flink.runtime.state.changelog.StateChangelogHandleReader;
import org.apache.flink.util.CloseableIterator;

public class KafkaStateChangelogReader implements StateChangelogHandleReader<KafkaChangelogStateHandle> {

    @Override
    public CloseableIterator<StateChange> getChanges(KafkaChangelogStateHandle handle) throws IOException {
        return null;
    }

}
