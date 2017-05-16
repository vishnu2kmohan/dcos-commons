package com.mesosphere.sdk.offer.taskdata;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import org.apache.mesos.Protos.Attribute;
import org.apache.mesos.Protos.Environment;
import org.apache.mesos.Protos.Label;
import org.apache.mesos.Protos.Offer;
import org.apache.mesos.Protos.Resource;
import org.apache.mesos.Protos.TaskInfo;
import org.apache.mesos.Protos.TaskStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mesosphere.sdk.offer.TaskException;
import com.mesosphere.sdk.specification.PortSpec;
import com.mesosphere.sdk.specification.PortsSpec;
import com.mesosphere.sdk.specification.ResourceSpec;
import com.mesosphere.sdk.specification.TaskSpec;

/**
 * Provides read access to task labels which are (only) read by the Scheduler.
 */
public class SchedulerLabelReader {

    private static final Logger LOGGER = LoggerFactory.getLogger(SchedulerLabelReader.class);

    private final TaskDataReader labels;

    /**
     * @see TaskDataReader#TaskDataReader(String, String, java.util.Map)
     */
    public SchedulerLabelReader(TaskInfo taskInfo) {
        labels = LabelUtils.toDataReader(taskInfo.getName(), taskInfo.getLabels());
    }

    /**
     * @see TaskDataReader#TaskDataReader(String, String, java.util.Map)
     */
    public SchedulerLabelReader(TaskInfo.Builder taskInfoBuilder) {
        labels = LabelUtils.toDataReader(taskInfoBuilder.getName(), taskInfoBuilder.getLabels());
    }

    /**
     * Returns the task type string, which was embedded in the task.
     *
     * @throws TaskException if the type could not be found.
     */
    public String getType() throws TaskException {
        return labels.getOrThrow(LabelConstants.TASK_TYPE_LABEL);
    }

    /**
     * Returns the pod instance index of the task, or throws {@link TaskException} if no index data was found.
     *
     * @throws TaskException if the index data wasn't found
     * @throws NumberFormatException if parsing the index as an integer failed
     */
    public int getIndex() throws TaskException, NumberFormatException {
        return Integer.parseInt(labels.getOrThrow(LabelConstants.TASK_INDEX_LABEL));
    }

    /**
     * Returns the string representations of any {@link Offer} {@link Attribute}s which were embedded in the task.
     */
    public List<String> getOfferAttributeStrings() {
        Optional<String> joinedAttributes = labels.getOptional(LabelConstants.OFFER_ATTRIBUTES_LABEL);
        if (!joinedAttributes.isPresent()) {
            return new ArrayList<>();
        }
        return AttributeStringUtils.toStringList(joinedAttributes.get());
    }

    /**
     * Returns the hostname of the agent machine running the task.
     */
    public String getHostname() throws TaskException {
        return labels.getOrThrow(LabelConstants.OFFER_HOSTNAME_LABEL);
    }

    /**
     * Returns the ID referencing a configuration in a {@link ConfigStore} associated with the task.
     *
     * @return the ID of the target configuration for the provided {@link TaskInfo}
     * @throws TaskException when a TaskInfo is provided which does not contain a {@link Label} with
     *                       an indicated target configuration
     */
    public UUID getTargetConfiguration() throws TaskException {
        return UUID.fromString(labels.getOrThrow(LabelConstants.TARGET_CONFIGURATION_LABEL));
    }

    /**
     * Returns whether or not a readiness check succeeded.  If the indicated TaskInfo does not have
     * a readiness check, then this method indicates that the readiness check has passed.  Otherwise
     * failures to parse readiness checks are interpreted as readiness check failures.  If some value other
     * than "true" is present in the readiness check label of the TaskStatus, the readiness check has
     * failed.
     *
     * @param taskStatus A TaskStatus which may or may not contain a readiness check label
     * @return the result of a readiness check for the indicated TaskStatus
     */
    public boolean isReadinessCheckSucceeded(TaskStatus taskStatus) {
        Optional<String> healthCheckOptional = labels.getOptional(LabelConstants.READINESS_CHECK_LABEL);
        if (!healthCheckOptional.isPresent()) {
            // check not applicable: PASS
            return true;
        }

        Optional<String> readinessCheckResult = labels.getOptional(LabelConstants.READINESS_CHECK_PASSED_LABEL);
        if (!readinessCheckResult.isPresent()) {
            // check applicable, but passed bit not set: FAIL
            return false;
        }
        return readinessCheckResult.get().equals(LabelConstants.READINESS_CHECK_PASSED_LABEL_VALUE);
    }

    /**
     * Returns whether the task is marked as transient. This identifies a 'task' which isn't actually launched, but is
     * instead created to fill reserved resources.
     */
    public boolean isTransient() {
        // null is false
        return Boolean.valueOf(labels.getOptional(LabelConstants.TRANSIENT_FLAG_LABEL).orElse(null));
    }

    /**
     * Returns whether the task is marked as permanently failed. This identifies the task as needing replacement on a
     * new machine in the cluster (as opposed to needing a restart on the current machine).
     */
    public boolean isPermanentlyFailed() {
        // null is false
        return Boolean.valueOf(labels.getOptional(LabelConstants.PERMANENTLY_FAILED_LABEL).orElse(null));
    }

    /**
     * Returns the specified dynamic port value from the provided {@link TaskInfo}, or an empty {@link Optional} if a
     * value for the specified port name wasn't found.
     */
    public static Optional<Integer> getDynamicPortValue(Collection<Resource> resources, String portName) {
        for (Resource resource : resources) {
            Optional<Integer> portValue = new SchedulerResourceLabelReader(resource).getPortValue(portName);
            if (portValue.isPresent()) {
                return portValue;
            }
        }
        return Optional.empty();
    }

    /**
     * Returns a mapping of all assigned dynamic port values from the provided {@link TaskInfo}, using the provided
     * {@link TaskSpec} to determine the ports which are dynamic.
     */
    public static Map<String, Integer> getAllDynamicPortValues(Collection<Resource> resources, TaskSpec taskSpec)
            throws TaskException {
        Map<String, Integer> allResourcePortValues = new HashMap<>();
        for (Resource resource : resources) {
            allResourcePortValues.putAll(new SchedulerResourceLabelReader(resource).getAllPortValues());
        }
        Map<String, Integer> dynamicPortValues = new HashMap<>();
        for (PortSpec dynamicPortSpec : getDynamicPortSpecs(taskSpec)) {
            Integer portValue = allResourcePortValues.get(dynamicPortSpec.getPortName());
            if (portValue != null) {
                dynamicPortValues.put(dynamicPortSpec.getPortName(), portValue);
            }
        }
        return dynamicPortValues;
    }

    /**
     * Extracts the dynamic port values currently/previously used by the task from the provided task environment.
     * TODO(nickbp): This is deprecated in favor of port values stored in task labels. Remove this fallback on or after
     * July 2017.
     */
    @Deprecated
    public static Map<String, Integer> getAllDynamicPortValuesFromEnv(TaskSpec taskSpec, Environment environment) {
        Map<String, String> envMap = EnvUtils.toMap(environment);

        Map<String, Integer> dynamicPortValues = new HashMap<>();
        for (PortSpec dynamicPortSpec : getDynamicPortSpecs(taskSpec)) {
            String portEnvName = EnvUtils.getPortEnvName(dynamicPortSpec.getPortName(), dynamicPortSpec.getEnvKey());
            String portEnvVal = envMap.get(portEnvName);
            if (portEnvVal != null) {
                try {
                    dynamicPortValues.put(dynamicPortSpec.getPortName(), Integer.parseInt(portEnvVal));
                } catch (NumberFormatException e) {
                    // Just in case, let's be conservative about envvars: Author could have put something bogus here.
                    LOGGER.warn(String.format(
                            "Failed to extract dynamic port value from legacy task env: %s=%s",
                            portEnvName, portEnvVal), e);
                    continue; // just in case...
                }
            }
        }
        return dynamicPortValues;
    }

    private static Collection<PortSpec> getDynamicPortSpecs(TaskSpec taskSpec) {
        Collection<PortSpec> dynamicPortSpecs = new ArrayList<>();
        for (ResourceSpec resource : taskSpec.getResourceSet().getResources()) {
            if (!(resource instanceof PortsSpec)) {
                continue;
            }
            for (PortSpec portSpec : ((PortsSpec) resource).getPortSpecs()) {
                if (portSpec.getPortValue() == 0) {
                    dynamicPortSpecs.add(portSpec);
                }
            }
        }
        return dynamicPortSpecs;
    }
}
