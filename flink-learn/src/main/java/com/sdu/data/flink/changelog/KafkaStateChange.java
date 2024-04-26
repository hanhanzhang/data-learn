package com.sdu.data.flink.changelog;

import java.io.Serializable;

import org.apache.flink.util.Preconditions;

public class KafkaStateChange implements Serializable {

    public static final int META_KEY_GROUP = -1;

    private static final long serialVersionUID = 1L;
    private final byte[] operatorID;
    private final int keyGroup;
    private final byte[] change;

    KafkaStateChange(byte[] operatorID, byte[] change) {
        this(operatorID, META_KEY_GROUP, change);
    }

    KafkaStateChange(byte[] operatorID, int keyGroup, byte[] change) {
        this.operatorID = Preconditions.checkNotNull(operatorID);
        this.keyGroup = keyGroup;
        this.change = Preconditions.checkNotNull(change);
    }

    public static KafkaStateChange ofMetadataChange(byte[] operatorID, byte[] change) {
        return new KafkaStateChange(operatorID, change);
    }

    public static KafkaStateChange ofDataChange(byte[] operatorID, int keyGroup, byte[] change) {
        return new KafkaStateChange(operatorID, keyGroup, change);
    }

    public byte[] getOperatorID() {
        return operatorID;
    }

    public int getKeyGroup() {
        return keyGroup;
    }

    public byte[] getChange() {
        return change;
    }

    @Override
    public String toString() {
        return String.format("operatorID=%s, keyGroup=%d, dataSize=%d", new String(operatorID), keyGroup, change.length);
    }
}
