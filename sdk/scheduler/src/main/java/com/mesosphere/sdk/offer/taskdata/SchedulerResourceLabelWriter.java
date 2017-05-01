package com.mesosphere.sdk.offer.taskdata;

import org.apache.mesos.Protos.Labels;
import org.apache.mesos.Protos.Resource;

/**
 * Provides write access to task labels which are (only) written by the Scheduler.
 */
public class SchedulerResourceLabelWriter {

    private final TaskDataWriter labels;

    /**
     * @see TaskDataWriter#TaskDataWriter()
     */
    public SchedulerResourceLabelWriter() {
        labels = new TaskDataWriter();
    }

    /**
     * @see TaskDataWriter#TaskDataWriter(java.util.Map)
     */
    public SchedulerResourceLabelWriter(Resource resource) {
        labels = LabelUtils.toDataWriter(resource.getReservation().getLabels());
    }

    /**
     * @see TaskDataWriter#TaskDataWriter(java.util.Map)
     */
    public SchedulerResourceLabelWriter(Resource.Builder resource) {
        labels = LabelUtils.toDataWriter(resource.getReservation().getLabels());
    }

    /**
     * Stores the provided port name/value against this resource. Used to identify what port value had been used for
     * dynamic ports.
     */
    public SchedulerResourceLabelWriter setPort(String portName, long portValue) {
        labels.put(LabelUtils.toPortLabelName(portName), Long.toString(portValue));
        return this;
    }

    /**
     * Returns a Protobuf representation of all contained entries.
     */
    public Labels toProto() {
        return LabelUtils.toProto(labels.map());
    }
}
