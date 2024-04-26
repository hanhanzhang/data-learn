package com.sdu.data.flink.operators.config;

import com.sdu.data.flink.operators.config.files.HotConfigFileDetector;

public enum HotConfigType {

    FILE() {
        @Override
        public HotConfigDetector createHotConfigDetector(HotConfigDescriptor descriptor) {
            return HotConfigFileDetector.INSTANCE;
        }

    };

    public abstract HotConfigDetector createHotConfigDetector(HotConfigDescriptor descriptor);

}
