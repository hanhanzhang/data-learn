package com.sdu.data.flink.changelog;

import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.metrics.groups.TaskManagerJobMetricGroup;
import org.apache.flink.runtime.state.LocalRecoveryConfig;
import org.apache.flink.runtime.state.changelog.StateChangelogStorage;
import org.apache.flink.runtime.state.changelog.StateChangelogStorageFactory;
import org.apache.flink.runtime.state.changelog.StateChangelogStorageView;

import java.io.IOException;

// @see TaskManagerServices#fromConfiguration
//         |
//         +--> TaskExecutorStateChangelogStoragesManager#stateChangelogStorageForJob
//               |
//               +--> StateChangelogStorageLoader#load
public class KafkaStateChangelogStorageFactory implements StateChangelogStorageFactory {

    public static final String IDENTIFIER = "kafka";

    @Override
    public String getIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public StateChangelogStorage<?> createStorage(JobID jobID, Configuration configuration, TaskManagerJobMetricGroup metricGroup, LocalRecoveryConfig localRecoveryConfig) throws IOException {
        return null;
    }

    @Override
    public StateChangelogStorageView<?> createStorageView(Configuration configuration) throws IOException {
        return null;
    }

}
