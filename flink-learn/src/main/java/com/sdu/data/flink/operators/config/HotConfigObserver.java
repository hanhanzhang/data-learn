package com.sdu.data.flink.operators.config;

public interface HotConfigObserver {

    void open(HotConfigDescriptor descriptor);

    String register(HotConfigDescriptor descriptor, HotConfigListener listener);

    void unregister(HotConfigDescriptor descriptor, HotConfigListener listener);
}
