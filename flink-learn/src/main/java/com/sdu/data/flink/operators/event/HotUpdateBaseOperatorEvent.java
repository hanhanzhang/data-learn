package com.sdu.data.flink.operators.event;

import java.util.concurrent.atomic.AtomicLong;

import org.apache.flink.runtime.operators.coordination.OperatorEvent;

public abstract class HotUpdateBaseOperatorEvent implements OperatorEvent {

    public static AtomicLong EVENT_ID_CREATOR = new AtomicLong(0);

    private final long eventId;

    public HotUpdateBaseOperatorEvent() {
        this(EVENT_ID_CREATOR.getAndIncrement());
    }

    public HotUpdateBaseOperatorEvent(long eventId) {
        this.eventId = eventId;
    }

    public long getEventId() {
        return eventId;
    }

}
