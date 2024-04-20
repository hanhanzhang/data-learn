package com.sdu.data.flink.operators.event;

import javax.annotation.Nullable;

public class HotUpdateAcknowledgeOperatorEvent extends HotUpdateBaseOperatorEvent {

    private final boolean status;
    private final Throwable cause;

    public HotUpdateAcknowledgeOperatorEvent(long eventId, boolean status, @Nullable Throwable cause) {
        super(eventId);
        this.status = status;
        this.cause = cause;
    }

    public boolean updateSuccess() {
        return status && cause == null;
    }

    public Throwable getCause() {
        return cause;
    }
}
