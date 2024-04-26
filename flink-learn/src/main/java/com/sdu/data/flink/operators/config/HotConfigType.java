package com.sdu.data.flink.operators.config;

import com.sdu.data.flink.operators.config.files.HotConfigFileObserver;

public enum HotConfigType {

    FILE() {
        @Override
        public HotConfigObserver createHotConfigDetector(HotConfigDescriptor descriptor) {
            return HotConfigFileObserver.INSTANCE;
        }

    };

    public abstract HotConfigObserver createHotConfigDetector(HotConfigDescriptor descriptor);

}
