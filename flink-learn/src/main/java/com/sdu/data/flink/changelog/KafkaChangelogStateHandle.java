package com.sdu.data.flink.changelog;

import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.SharedStateRegistry;
import org.apache.flink.runtime.state.StateHandleID;
import org.apache.flink.runtime.state.changelog.ChangelogStateHandle;

import javax.annotation.Nullable;

public class KafkaChangelogStateHandle implements ChangelogStateHandle {

    @Override
    public String getStorageIdentifier() {
        return null;
    }

    @Override
    public KeyGroupRange getKeyGroupRange() {
        return null;
    }

    @Nullable
    @Override
    public KeyedStateHandle getIntersection(KeyGroupRange keyGroupRange) {
        return null;
    }

    @Override
    public StateHandleID getStateHandleId() {
        return null;
    }

    @Override
    public void registerSharedStates(SharedStateRegistry stateRegistry, long checkpointID) {

    }

    @Override
    public long getCheckpointedSize() {
        return 0;
    }

    @Override
    public void discardState() throws Exception {

    }

    @Override
    public long getStateSize() {
        return 0;
    }

}
