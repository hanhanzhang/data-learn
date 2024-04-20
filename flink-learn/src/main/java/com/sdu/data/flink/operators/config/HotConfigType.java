package com.sdu.data.flink.operators.config;

public enum HotConfigType {

    FILE() {
        @Override
        public HotConfigDetector createHotConfigDetector() {
            return null;
        }

    };

    public abstract HotConfigDetector createHotConfigDetector();

}
