package com.sdu.data.flink.changelog;

import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.changelog.StateChangelogHandleReader;
import org.apache.flink.runtime.state.changelog.StateChangelogStorage;
import org.apache.flink.runtime.state.changelog.StateChangelogWriter;

public class KafkaStateChangelogStorage implements StateChangelogStorage<KafkaChangelogStateHandle> {
    @Override
    public StateChangelogWriter<KafkaChangelogStateHandle> createWriter(String operatorID, KeyGroupRange keyGroupRange, MailboxExecutor mailboxExecutor) {
        return null;
    }

    @Override
    public StateChangelogHandleReader<KafkaChangelogStateHandle> createReader() {
        return null;
    }
}
