package com.sdu.data.flink.operators.config;

public interface HotConfigDetector {

    void open();

    String register(HotConfigDescriptor descriptor, HotConfigListener listener);

    void unregister(String subscribeTopic, HotConfigListener listener);

}
