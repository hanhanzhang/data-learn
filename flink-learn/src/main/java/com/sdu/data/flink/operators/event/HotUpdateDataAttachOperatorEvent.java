package com.sdu.data.flink.operators.event;

public class HotUpdateDataAttachOperatorEvent extends HotUpdateBaseOperatorEvent {

    private final String attachData;

    public HotUpdateDataAttachOperatorEvent(String attachData) {
        super();
        this.attachData = attachData;
    }

    public String getAttachData() {
        return attachData;
    }


}
