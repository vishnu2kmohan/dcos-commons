package com.mesosphere.sdk.scheduler.recovery;

import com.google.protobuf.TextFormat;
import com.mesosphere.sdk.offer.taskdata.SchedulerLabelReader;
import com.mesosphere.sdk.offer.taskdata.SchedulerLabelWriter;
import com.mesosphere.sdk.specification.PodInstance;
import com.mesosphere.sdk.specification.TaskSpec;
import com.mesosphere.sdk.state.StateStore;
import org.apache.mesos.Protos;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.stream.Collectors;

/**
 * This class provides utility methods for the handling of failed Tasks.
 */
public class FailureUtils {
    private static final Logger LOGGER = LoggerFactory.getLogger(FailureUtils.class);

    /**
     * Check if a Task has been marked as permanently failed.
     *
     * @param taskInfo The Task to check for failure
     * @return {@code True} if the {@code taskInfo} has been marked, {@code false} otherwise
     */
    public static boolean isLabeledAsFailed(Protos.TaskInfo taskInfo) {
        return new SchedulerLabelReader(taskInfo).isPermanentlyFailed();
    }

    public static boolean isLabeledAsFailed(PodInstance podInstance, StateStore stateStore) {
        Collection<Protos.TaskInfo> taskInfos = getTaskInfos(podInstance, stateStore);
        if (taskInfos.isEmpty()) {
            return false;
        }

        return taskInfos.stream().allMatch(taskInfo -> isLabeledAsFailed(taskInfo));
    }

    /**
     * Mark a task as permanently failed.  This new marked Task should be persistently stored.
     *
     * @param taskInfo The Task to be marked.
     * @return The marked TaskInfo which may be a copy of the original TaskInfo.
     */
    public static Protos.TaskInfo markFailed(Protos.TaskInfo taskInfo) {
        return taskInfo.toBuilder()
                .setLabels(new SchedulerLabelWriter(taskInfo).setPermanentlyFailed().toProto())
                .build();
    }

    public static void markFailed(PodInstance podInstance, StateStore stateStore) {
        stateStore.storeTasks(
                getTaskInfos(podInstance, stateStore).stream()
                        .map(taskInfo -> markFailed(taskInfo))
                        .collect(Collectors.toList()));
    }

    /**
     * Remove the permanently failed label from the TaskInfo.
     */
    static Protos.TaskInfo clearFailed(Protos.TaskInfo taskInfo) {
        if (!isLabeledAsFailed(taskInfo)) {
            return taskInfo;
        }

        LOGGER.info("Clearing permanent failure mark from: {}", TextFormat.shortDebugString(taskInfo));
        return taskInfo.toBuilder()
                .setLabels(new SchedulerLabelWriter(taskInfo).clearPermanentlyFailed().toProto())
                .build();
    }

    public static Collection<Protos.TaskInfo> clearFailed(PodInstance podInstance, StateStore stateStore) {
        return getTaskInfos(podInstance, stateStore).stream()
                .map(taskInfo -> clearFailed(taskInfo))
                .collect(Collectors.toList());
    }

    private static Collection<Protos.TaskInfo> getTaskInfos(PodInstance podInstance, StateStore stateStore) {
        return podInstance.getPod().getTasks().stream()
                .map(taskSpec -> TaskSpec.getInstanceName(podInstance, taskSpec))
                .map(name -> stateStore.fetchTask(name))
                .filter(taskInfo -> taskInfo.isPresent())
                .map(taskInfo -> taskInfo.get())
                .collect(Collectors.toList());
    }
}
