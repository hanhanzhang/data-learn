package com.sdu.data.flink.operators.config.files;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import javax.annotation.concurrent.ThreadSafe;

import org.apache.commons.io.monitor.FileAlterationMonitor;
import org.apache.commons.io.monitor.FileAlterationObserver;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sdu.data.flink.operators.config.HotConfigDescriptor;
import com.sdu.data.flink.operators.config.HotConfigDetector;
import com.sdu.data.flink.operators.config.HotConfigListener;

@ThreadSafe
public class HotConfigFileDetector implements HotConfigDetector {

    private static final Logger LOG = LoggerFactory.getLogger(HotConfigFileDetector.class);

    public static final HotConfigFileDetector INSTANCE = new HotConfigFileDetector();

    // todo: file data read base FileSystem

    private final Object lock = new Object();
    private transient boolean opened = false;
    private transient FileAlterationMonitor monitor;
    private transient HotConfigFileAlterationListener alterationListener;
    private transient Map<String, FileAlterationObserver> fileObservers;

    @Override
    public void open(HotConfigDescriptor descriptor) {
        if (opened) {
            return;
        }
        synchronized (lock) {
            if (opened) {
                return;
            }
            Preconditions.checkArgument(descriptor instanceof HotConfigFileDescriptor);
            HotConfigFileDescriptor fileDescriptor = (HotConfigFileDescriptor) descriptor;
            initialize(fileDescriptor.getMonitorIntervalMills());
        }
    }

    private void initialize(long intervalMills) {
        try {
            opened = true;
            fileObservers = new HashMap<>();
            alterationListener = new HotConfigFileAlterationListener();
            monitor = new FileAlterationMonitor(intervalMills);
            monitor.start();
        } catch (Exception e) {
            throw new RuntimeException("failed initialize file config detector", e);
        }
    }

    @Override
    public String register(HotConfigDescriptor descriptor, HotConfigListener listener) {
        Preconditions.checkArgument(descriptor instanceof HotConfigFileDescriptor);
        HotConfigFileDescriptor fileDescriptor = (HotConfigFileDescriptor) descriptor;
        synchronized (lock) {
            try {
                FileAlterationObserver observer = fileObservers.get(fileDescriptor.getFilePath());
                if (observer != null) {
                    LOG.info("file observer already registered, path: {}", fileDescriptor.getFilePath());
                    return alterationListener.registerListener(fileDescriptor.getFilePath(), listener);
                }
                observer = new FileAlterationObserver(new File(fileDescriptor.getFilePath()));
                observer.initialize();
                fileObservers.put(fileDescriptor.getFilePath(), observer);
                monitor.addObserver(observer);
                observer.addListener(alterationListener);
                return alterationListener.registerListener(fileDescriptor.getFilePath(), listener);
            } catch (Exception e) {
                throw new RuntimeException("failed register file listener", e);
            }
        }
    }

    @Override
    public void unregister(HotConfigDescriptor descriptor, HotConfigListener listener) {
        Preconditions.checkArgument(descriptor instanceof HotConfigFileDescriptor);
        HotConfigFileDescriptor fileDescriptor = (HotConfigFileDescriptor) descriptor;
        boolean hasListeners = alterationListener.unregisterListener(fileDescriptor.getFilePath(), listener);
        if (!hasListeners) {
            synchronized (lock) {
                FileAlterationObserver observer = fileObservers.remove(fileDescriptor.getFilePath());
                if (observer != null) {
                    monitor.removeObserver(observer);
                }
            }
        }
    }

}
