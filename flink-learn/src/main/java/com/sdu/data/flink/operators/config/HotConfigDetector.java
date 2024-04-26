package com.sdu.data.flink.operators.config;

public interface HotConfigDetector {

    void open(HotConfigDescriptor descriptor);

    String register(HotConfigDescriptor descriptor, HotConfigListener listener);

    void unregister(HotConfigDescriptor descriptor, HotConfigListener listener);
}
