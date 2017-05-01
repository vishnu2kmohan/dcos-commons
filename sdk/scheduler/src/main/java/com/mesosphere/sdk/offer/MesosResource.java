package com.mesosphere.sdk.offer;

import java.util.Optional;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.mesos.Protos.Label;
import org.apache.mesos.Protos.Resource;
import org.apache.mesos.Protos.Resource.DiskInfo.Source;
import org.apache.mesos.Protos.Value;

import com.mesosphere.sdk.offer.taskdata.SchedulerResourceLabelReader;

/**
 * Wrapper around a Mesos {@link Resource}, combined with a resource ID string which should be present in the
 * {@link Resource} as a {@link Label}.
 **/
public class MesosResource {

    private final Resource resource;

    public MesosResource(Resource resource) {
        this.resource = resource;
    }

    public Resource getResource() {
        return resource;
    }

    public boolean isAtomic() {
        return resource.hasDisk()
            && resource.getDisk().hasSource()
            && resource.getDisk().getSource().getType().equals(Source.Type.MOUNT);
    }

    public String getName() {
        return resource.getName();
    }

    public Value.Type getType() {
        return resource.getType();
    }

    public Optional<String> getResourceId() {
        return new SchedulerResourceLabelReader(resource).getResourceId();
    }

    public boolean hasReservation() {
        return resource.hasReservation();
    }

    public Value getValue() {
        return ValueUtils.getValue(resource);
    }

    public String getRole() {
        return resource.getRole();
    }

    public String getPrincipal() {
        if (resource.hasReservation() && resource.getReservation().hasPrincipal()) {
            return resource.getReservation().getPrincipal();
        }

        return null;
    }

    @Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this);
    }
}
