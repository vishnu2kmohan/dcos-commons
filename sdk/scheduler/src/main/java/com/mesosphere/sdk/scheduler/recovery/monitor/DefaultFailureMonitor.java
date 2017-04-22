package com.mesosphere.sdk.scheduler.recovery.monitor;

import org.apache.mesos.Protos;

import com.mesosphere.sdk.offer.taskdata.SchedulerLabelReader;

/**
 * The DefaultFailureMonitor reports that tasks have Failed permanently when they are so labeled.
 */
public class DefaultFailureMonitor implements FailureMonitor {
    @Override
    public boolean hasFailed(Protos.TaskInfo task) {
        return new SchedulerLabelReader(task).isPermanentlyFailed();
    }
}
