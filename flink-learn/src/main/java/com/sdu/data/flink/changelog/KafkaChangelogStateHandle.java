package com.sdu.data.flink.changelog;

import java.util.UUID;

import javax.annotation.Nullable;

import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.SharedStateRegistry;
import org.apache.flink.runtime.state.StateHandleID;
import org.apache.flink.runtime.state.changelog.ChangelogStateHandle;

public class KafkaChangelogStateHandle implements ChangelogStateHandle {

    private final KeyGroupRange keyGroupRange;
    // todo: miss delegate keyed state backend handle message
    private final KafkaStreamStateHandle stateHandle;
    private final StateHandleID stateHandleID;

    public KafkaChangelogStateHandle(
            KeyGroupRange keyGroupRange,
            KafkaStreamStateHandle stateHandle) {
        this(keyGroupRange, stateHandle, new StateHandleID(UUID.randomUUID().toString()));
    }

    private KafkaChangelogStateHandle(
            KeyGroupRange keyGroupRange,
            KafkaStreamStateHandle stateHandle,
            StateHandleID stateHandleID) {
        this.keyGroupRange = keyGroupRange;
        this.stateHandle = stateHandle;
        this.stateHandleID = stateHandleID;
    }

    @Override
    public String getStorageIdentifier() {
        return KafkaStateChangelogStorageFactory.IDENTIFIER;
    }

    @Override
    public KeyGroupRange getKeyGroupRange() {
        return keyGroupRange;
    }

    @Nullable
    @Override
    public KeyedStateHandle getIntersection(KeyGroupRange keyGroupRange) {
        // 状态恢复时需判断是否读取该StreamStateHandle
        return KeyGroupRange.EMPTY_KEY_GROUP_RANGE.equals(
                getKeyGroupRange().getIntersection(keyGroupRange)) ? null : this;
    }

    @Override
    public StateHandleID getStateHandleId() {
        return stateHandleID;
    }

    @Override
    public void registerSharedStates(SharedStateRegistry stateRegistry, long checkpointID) {
        // do nothing, because change log state not shared
    }

    @Override
    public long getCheckpointedSize() {
        return getStateSize();
    }

    @Override
    public void discardState() throws Exception {
        stateHandle.discardState();
    }

    @Override
    public long getStateSize() {
        return stateHandle.getStateSize();
    }

}
