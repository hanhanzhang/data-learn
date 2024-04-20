package com.sdu.data.flink.operators.config;

import java.util.HashMap;
import java.util.Map;

public class HotConfigManager {

    public static final HotConfigManager INSTANCE = new HotConfigManager();

    private final Map<HotConfigType, HotConfigDetector> detectors;

    private HotConfigManager() {
        detectors = new HashMap<>();
    }

    public String register(HotConfigDescriptor descriptor, HotConfigListener listener) {
        HotConfigDetector detector = detectors.get(descriptor.configType());
        if (detector == null) {
            synchronized (INSTANCE) {
                detector = detectors.get(descriptor.configType());
                if (detector == null) {
                    detector = descriptor.configType().createHotConfigDetector();
                    detector.open();
                    detectors.put(descriptor.configType(), detector);
                }
            }
        }
        return detector.register(descriptor, listener);
    }

    public void unregister(HotConfigDescriptor descriptor, HotConfigListener listener) {
        HotConfigDetector detector = detectors.get(descriptor.configType());
        if (detector == null) {
            return;
        }
        detector.unregister(descriptor.subscribeTopic(), listener);
    }
}
