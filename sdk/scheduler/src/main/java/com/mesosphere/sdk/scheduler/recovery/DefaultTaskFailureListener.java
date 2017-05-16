package com.mesosphere.sdk.scheduler.recovery;

import com.mesosphere.sdk.config.ConfigStore;
import com.mesosphere.sdk.offer.TaskUtils;
import com.mesosphere.sdk.specification.ServiceSpec;
import org.apache.mesos.Protos;
import com.mesosphere.sdk.offer.CommonIdUtils;
import com.mesosphere.sdk.offer.TaskException;
import com.mesosphere.sdk.state.StateStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

/**
 * This class provides a default implementation of the TaskFailureListener interface.
 */
public class DefaultTaskFailureListener implements TaskFailureListener {
    private final Logger logger = LoggerFactory.getLogger(getClass());
    private final StateStore stateStore;
    private final ConfigStore<ServiceSpec> configStore;

    public DefaultTaskFailureListener(StateStore stateStore, ConfigStore<ServiceSpec> configStore) {
        this.stateStore = stateStore;
        this.configStore = configStore;
    }

    @Override
    public void taskFailed(Protos.TaskID taskId) {
        try {
            Optional<Protos.TaskInfo> optionalTaskInfo = stateStore.fetchTask(CommonIdUtils.toTaskName(taskId));
            if (optionalTaskInfo.isPresent()) {
                FailureUtils.markFailed(TaskUtils.getPodInstance(configStore, optionalTaskInfo.get()), stateStore);
            } else {
                logger.error("TaskInfo for TaskID was not present in the StateStore: {}", taskId.getValue());
            }
        } catch (TaskException e) {
            logger.error(
                    String.format("Failed to fetch/store Task for taskId: %s with exception:", taskId.getValue()), e);
        }
    }
}
