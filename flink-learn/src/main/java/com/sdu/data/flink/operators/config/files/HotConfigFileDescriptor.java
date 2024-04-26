package com.sdu.data.flink.operators.config.files;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

import org.apache.commons.io.FileUtils;

import com.sdu.data.flink.operators.config.HotConfigDescriptor;
import com.sdu.data.flink.operators.config.HotConfigType;

public class HotConfigFileDescriptor implements HotConfigDescriptor {

    private final long updateTimeoutMills;
    private final String filePath;
    private final long monitorIntervalMills;

    public HotConfigFileDescriptor(String filePath, long monitorIntervalMills, long updateTimeoutMills) {
        this.monitorIntervalMills = monitorIntervalMills;
        this.updateTimeoutMills = updateTimeoutMills;
        this.filePath = filePath;
    }

    public String getFilePath() {
        return filePath;
    }

    public long getMonitorIntervalMills() {
        return monitorIntervalMills;
    }

    @Override
    public HotConfigType configType() {
        return HotConfigType.FILE;
    }

    @Override
    public long updateTimeoutMills() {
        return updateTimeoutMills;
    }

    public static String readConfig(String path) {
        try {
            return FileUtils.readFileToString(new File(path), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new RuntimeException("failed read config from file '" + path + "'", e);
        }
    }
}
