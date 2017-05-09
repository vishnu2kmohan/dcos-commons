package com.mesosphere.sdk.offer.taskdata;

import org.apache.mesos.Protos.Resource;

/**
 * Provides write access to task labels which are (only) written by the Scheduler.
 */
public class SchedulerResourceLabelWriter {

    private final TaskDataWriter labels;
    private final Resource resource;

    /**
     * @see TaskDataWriter#TaskDataWriter()
     */
    public SchedulerResourceLabelWriter() {
        this(Resource.getDefaultInstance());
    }

    /**
     * @see TaskDataWriter#TaskDataWriter(java.util.Map)
     */
    public SchedulerResourceLabelWriter(Resource resource) {
        this.labels = LabelUtils.toDataWriter(resource.getReservation().getLabels());
        this.resource = resource;
    }

    /**
     * @see TaskDataWriter#TaskDataWriter(java.util.Map)
     */
    public SchedulerResourceLabelWriter(Resource.Builder resource) {
        this(resource.build());
    }

    /**
     * Stores the resource reservation ID against this resource. Used to uniquely identify a resource which has been
     * reserved by the Scheduler.
     */
    public SchedulerResourceLabelWriter setResourceId(String resourceId) {
        labels.put(LabelConstants.RESOURCE_ID_KEY, resourceId);
        return this;
    }

    /**
     * Removes the resource reservation ID against this resource, or does nothing if the ID is not already
     */
    public SchedulerResourceLabelWriter clearResourceId() {
        labels.remove(LabelConstants.RESOURCE_ID_KEY);
        return this;
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
    public Resource toProto() {
        if (labels.map().isEmpty()
                && (!resource.hasReservation() || !resource.getReservation().hasLabels())) {
            return resource; // refrain from creating empty reservation object
        }
        Resource.Builder resourceBuilder = resource.toBuilder();
        resourceBuilder.getReservationBuilder().setLabels(LabelUtils.toProto(labels.map()));
        return resourceBuilder.build();
    }
}
