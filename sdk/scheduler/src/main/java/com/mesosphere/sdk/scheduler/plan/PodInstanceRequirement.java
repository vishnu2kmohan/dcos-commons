package com.mesosphere.sdk.scheduler.plan;

import com.mesosphere.sdk.offer.TaskUtils;
import com.mesosphere.sdk.scheduler.recovery.RecoveryType;
import com.mesosphere.sdk.specification.PodInstance;
import org.apache.commons.collections.CollectionUtils;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * A PodInstanceRequirement encapsulates a {@link PodInstance} and the names of tasks that should be launched in it.
 */
public class PodInstanceRequirement {
    private final PodInstance podInstance;
    private final Collection<String> tasksToLaunch;
    private final Map<String, String> parameters;
    private final RecoveryType recoveryType;

    public static Builder newBuilder(PodInstanceRequirement podInstanceRequirement) {
        return new Builder(podInstanceRequirement);
    }

    public static Builder newBuilder(PodInstance podInstance, Collection<String> tasksToLaunch) {
        return new Builder(podInstance, tasksToLaunch);
    }

    /**
     * Creates a new instance with the provided permanent replacement setting.
     */
    private PodInstanceRequirement(
            PodInstance podInstance,
            Collection<String> tasksToLaunch,
            Map<String, String> parameters,
            RecoveryType recoveryType) {
        this.podInstance = podInstance;
        this.tasksToLaunch = tasksToLaunch;
        this.parameters = parameters;
        this.recoveryType = recoveryType;
    }

    /**
     * Returns the definition of the pod instance to be created.
     */
    public PodInstance getPodInstance() {
        return podInstance;
    }

    /**
     * Returns the list of tasks to be launched within this pod. This doesn't necessarily match the tasks listed in the
     * {@link PodInstance}.
     */
    public Collection<String> getTasksToLaunch() {
        return tasksToLaunch;
    }

    /**
     * Returns the map of environment variable names to values that extend the existing environments of tasks in this
     * pod.
     */
    public Map<String, String> getParameters() {
        return parameters == null ? Collections.emptyMap() : parameters;
    }

    public RecoveryType getRecoveryType() {
        return recoveryType;
    }

    public String getName() {
        return TaskUtils.getStepName(getPodInstance(), getTasksToLaunch());
    }

    /**
     * A PodInstanceRequirement conflictsWith with another it if applies to the same pod instance and some
     * tasks in that pod.
     *
     * pod-0:[task0, task1]          conflictsWith with pod-0:[task1]
     * pod-0:[task1]        does NOT conflict  with pod-0:[task0]
     * pod-0:[task0]        does NOT conflict  with pod-1:[task0]
     *
     * @param podInstanceRequirement
     * @return
     */
    public boolean conflictsWith(PodInstanceRequirement podInstanceRequirement) {
        boolean podConflicts = podInstanceRequirement.getPodInstance().conflictsWith(getPodInstance());
        boolean tasksConflict = CollectionUtils.isEqualCollection(
                podInstanceRequirement.getTasksToLaunch(),
                getTasksToLaunch());
        return podConflicts && tasksConflict;
    }

    /**
     * {@link PodInstanceRequirement} builder static inner class.
     */
    public static final class Builder {
        private PodInstance podInstance;
        private Collection<String> tasksToLaunch;
        private Map<String, String> parameters = new HashMap<>();
        private RecoveryType recoveryType = RecoveryType.NONE;

        private Builder(PodInstanceRequirement podInstanceRequirement) {
            this(podInstanceRequirement.getPodInstance(), podInstanceRequirement.getTasksToLaunch());
            parameters(podInstanceRequirement.getParameters());
            recoveryType(podInstanceRequirement.getRecoveryType());
        }

        private Builder(PodInstance podInstance, Collection<String> tasksToLaunch) {
            this.podInstance = podInstance;
            this.tasksToLaunch = tasksToLaunch;
        }

        public Builder podInstance(PodInstance podInstance) {
            this.podInstance = podInstance;
            return this;
        }

        public Builder tasksToLaunch(Collection<String> tasksToLaunch) {
            this.tasksToLaunch = tasksToLaunch;
            return this;
        }

        public Builder parameters(Map<String, String> environment) {
            this.parameters = environment;
            return this;
        }

        public Builder recoveryType(RecoveryType recoveryType) {
            this.recoveryType = recoveryType;
            return this;
        }

        public PodInstanceRequirement build() {
            return new PodInstanceRequirement(podInstance, tasksToLaunch, parameters, recoveryType);
        }
    }
}
