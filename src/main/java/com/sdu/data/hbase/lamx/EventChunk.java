package com.sdu.data.hbase.lamx;

import java.io.Serializable;

public class EventChunk implements Serializable {

    private Event event;

    public void hold(Event event ) {
        this.event = event;
    }

    public Event getEvent() {
        return this.event;
    }
}
