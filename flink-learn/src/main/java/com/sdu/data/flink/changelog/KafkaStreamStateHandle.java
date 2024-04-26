package com.sdu.data.flink.changelog;

import static java.lang.String.format;

import java.io.IOException;
import java.util.Optional;

import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.runtime.state.PhysicalStateHandleID;
import org.apache.flink.runtime.state.StreamStateHandle;

public class KafkaStreamStateHandle implements StreamStateHandle {

    private final String operatorID;
    private final KafkaStreamStateHandleLocation location;
    private final long stateSize;

    public KafkaStreamStateHandle(
            String operatorID,
            KafkaStreamStateHandleLocation location,
            long stateSize) {
        this.operatorID = operatorID;
        this.location = location;
        this.stateSize = stateSize;
    }

    @Override
    public FSDataInputStream openInputStream() throws IOException {
        return new KafkaDataInputStream(
                operatorID,
                location.getProps(),
                location.getTopic(),
                location.getPartition(),
                location.getStartOffset(),
                location.getEndOffset(),
                location.getBufferSize()
        );
    }

    @Override
    public Optional<byte[]> asBytesIfInMemory() {
        return Optional.empty();
    }

    @Override
    public PhysicalStateHandleID getStreamStateHandleID() {
        return new PhysicalStateHandleID(
                format("%s#%d#%d-%d", location.getTopic(), location.getPartition(), location.getStartOffset(), location.getEndOffset())
        );
    }

    @Override
    public void discardState() throws Exception {
        // ignore, expire
    }

    @Override
    public long getStateSize() {
        return stateSize;
    }

}
