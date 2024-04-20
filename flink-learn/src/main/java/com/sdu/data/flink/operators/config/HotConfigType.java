package com.sdu.data.flink.operators.config;

public enum HotConfigType {

    FILE() {
        @Override
        public HotConfigDetector createHotConfigDetector(String subscribeTopic) {
            return HotConfigFileDetector.INSTANCE;
        }

    };

    public abstract HotConfigDetector createHotConfigDetector(String subscribeTopic);

}
