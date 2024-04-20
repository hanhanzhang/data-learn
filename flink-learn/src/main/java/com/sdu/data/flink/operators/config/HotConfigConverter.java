package com.sdu.data.flink.operators.config;

public interface HotConfigConverter<ORIGIN, CONFIG> {

    CONFIG convert(ORIGIN data);

}
