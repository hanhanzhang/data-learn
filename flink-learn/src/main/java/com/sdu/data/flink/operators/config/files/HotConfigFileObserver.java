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
import com.sdu.data.flink.operators.config.HotConfigListener;
import com.sdu.data.flink.operators.config.HotConfigObserver;

@ThreadSafe
public class HotConfigFileObserver implements HotConfigObserver {

    private static final Logger LOG = LoggerFactory.getLogger(HotConfigFileObserver.class);

    public static final HotConfigFileObserver INSTANCE = new HotConfigFileObserver();

    // todo: file data read base FileSystem

    private final Object lock = new Object();
    private transient boolean opened = false;

    // FileAlterationMonitor是目录级监控器
    // HotConfigFileAlterationListener是目录级监听器, 负责监控目录下文件的变化
    // HotConfigListener是文件级监听器
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
        File observerFile = new File(fileDescriptor.getFilePath());
        Preconditions.checkArgument(observerFile.getParent() != null);
        String parentPath = observerFile.getParent();
        synchronized (lock) {
            try {
                FileAlterationObserver observer = fileObservers.get(parentPath);
                if (observer != null) {
                    LOG.info("file directory observer already registered, path: {}", parentPath);
                    return alterationListener.registerListener(parentPath, fileDescriptor.getFilePath(), listener);
                }
                // NOTE: 监控父目录, 否则无法检测到文件内容变化
                observer = new FileAlterationObserver(observerFile.getParentFile());
                observer.initialize();
                fileObservers.put(parentPath, observer);
                monitor.addObserver(observer);
                observer.addListener(alterationListener);
                return alterationListener.registerListener(parentPath, fileDescriptor.getFilePath(), listener);
            } catch (Exception e) {
                throw new RuntimeException("failed register file listener", e);
            }
        }
    }

    @Override
    public void unregister(HotConfigDescriptor descriptor, HotConfigListener listener) {
        Preconditions.checkArgument(descriptor instanceof HotConfigFileDescriptor);
        HotConfigFileDescriptor fileDescriptor = (HotConfigFileDescriptor) descriptor;
        File observerFile = new File(fileDescriptor.getFilePath());
        String parentPath = observerFile.getParent();
        synchronized (lock) {
            alterationListener.unregisterListener(
                    parentPath,
                    fileDescriptor.getFilePath(),
                    listener,
                    () -> {
                        LOG.info("remove observer, directory: {}", parentPath);
                        FileAlterationObserver observer = fileObservers.remove(parentPath);
                        monitor.removeObserver(observer);
                    });
        }
    }

}
