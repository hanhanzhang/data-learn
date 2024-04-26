package com.sdu.data.flink.changelog;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;

import org.apache.flink.runtime.state.SnapshotResult;
import org.apache.flink.runtime.state.changelog.SequenceNumber;
import org.apache.flink.runtime.state.changelog.StateChangelogWriter;

public class KafkaStateChangelogWriter implements StateChangelogWriter<KafkaChangelogStateHandle> {

    private final String operatorID;

    public KafkaStateChangelogWriter(String operatorID) {
        this.operatorID = operatorID;
    }

    @Override
    public SequenceNumber initialSequenceNumber() {
        return null;
    }

    @Override
    public SequenceNumber nextSequenceNumber() {
        return null;
    }

    @Override
    public void appendMeta(byte[] value) throws IOException {

    }

    @Override
    public void append(int keyGroup, byte[] value) throws IOException {

    }

    @Override
    public CompletableFuture<SnapshotResult<KafkaChangelogStateHandle>> persist(SequenceNumber from, long checkpointId) throws IOException {
        return null;
    }

    @Override
    public void truncate(SequenceNumber to) {

    }

    @Override
    public void confirm(SequenceNumber from, SequenceNumber to, long checkpointId) {

    }

    @Override
    public void reset(SequenceNumber from, SequenceNumber to, long checkpointId) {

    }

    @Override
    public void truncateAndClose(SequenceNumber from) {

    }

    @Override
    public void close() {

    }
}
