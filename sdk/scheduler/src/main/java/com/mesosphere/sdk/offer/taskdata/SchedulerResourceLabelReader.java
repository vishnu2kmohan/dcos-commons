package com.mesosphere.sdk.offer.taskdata;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.apache.mesos.Protos.Resource;

import com.mesosphere.sdk.offer.TaskException;

/**
 * Provides read access to resource labels which are (only) read by the Scheduler.
 */
public class SchedulerResourceLabelReader {

    private final TaskDataReader labels;

    /**
     * @see TaskDataReader#TaskDataReader(String, String, java.util.Map)
     */
    public SchedulerResourceLabelReader(Resource resource) {
        labels = LabelUtils.toDataReader(resource.getName(), resource.getReservation().getLabels());
    }

    /**
     * @see TaskDataReader#TaskDataReader(String, String, java.util.Map)
     */
    public SchedulerResourceLabelReader(Resource.Builder resource) {
        labels = LabelUtils.toDataReader(resource.getName(), resource.getReservation().getLabels());
    }

    /**
     * Returns the resource ID for this resource, or an empty {@link Optional} if none is available.
     */
    public Optional<String> getResourceId() {
        return labels.getOptional(LabelConstants.RESOURCE_ID_KEY);
    }

    /**
     * Returns the value of the requested port name, or an empty {@link Optional} if none with that name is listed.
     */
    public Optional<Integer> getPortValue(String portName) {
        Optional<String> dynamicPortVal = labels.getOptional(LabelUtils.toPortLabelName(portName));
        return dynamicPortVal.isPresent()
                ? Optional.of(Integer.parseInt(dynamicPortVal.get()))
                : Optional.empty();
    }

    /**
     * Returns a mapping of all listed port names and their values.
     */
    public Map<String, Integer> getAllPortValues() throws TaskException {
        Map<String, Integer> portValues = new HashMap<>();
        for (String key : labels.getKeys()) {
            Optional<String> portName = LabelUtils.toPortName(key);
            if (portName.isPresent()) {
                portValues.put(portName.get(), Integer.parseInt(labels.getOrThrow(key)));
            }
        }
        return portValues;
    }
}
