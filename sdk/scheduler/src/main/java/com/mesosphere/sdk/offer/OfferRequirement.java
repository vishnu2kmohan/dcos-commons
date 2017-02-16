package com.mesosphere.sdk.offer;

import com.mesosphere.sdk.offer.evaluate.placement.PlacementRule;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.mesos.Protos;
import org.apache.mesos.Protos.ExecutorInfo;
import org.apache.mesos.Protos.TaskInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * An OfferRequirement encapsulates the needed resources that an {@link org.apache.mesos.Protos.Offer} must have in
 * order to launch a task against that {@link org.apache.mesos.Protos.Offer}.
 *
 * In general these are Resource requirements, such as requiring a certain amount of cpu, memory,
 * and disk, as encapsulated by {@link TaskRequirement} and {@link ExecutorRequirement}.
 * More dynamic requirements may also be defined on the placement of the new task, as evaluated
 * by the provided {@link PlacementRule}s.
 */
public class OfferRequirement {
    private static final Logger LOGGER = LoggerFactory.getLogger(OfferRequirement.class);
    private final String type;
    private Map<String, TaskRequirement> taskRequirements;
    private ExecutorRequirement executorRequirement;
    private Optional<PlacementRule> placementRuleOptional;
    private final int index;

    /**
     * Creates a new {@link OfferRequirement}.
     *
     * @param taskType the task type name from the TaskSet, used for placement filtering
     * @param taskInfos the 'draft' {@link TaskInfo}s from which task requirements should be generated
     * @param executorInfo the executor from which an executor requirement should be
     *     generated, if any
     * @param placementRuleOptional the placement constraints which should be applied to the tasks, if any
     * @throws InvalidRequirementException if task or executor requirements could not be generated
     *     from the provided information
     */
    public static OfferRequirement create(
            String taskType,
            int index,
            Collection<TaskInfo> taskInfos,
            ExecutorInfo executorInfo,
            Optional<PlacementRule> placementRuleOptional) throws InvalidRequirementException {

        ExecutorRequirement executorRequirement =
                executorInfo != null ?
                        ExecutorRequirement.create(executorInfo) :
                        null;

        return new OfferRequirement(
                taskType,
                index,
                getTaskRequirementsInternal(taskInfos, taskType, index),
                executorRequirement,
                placementRuleOptional);
    }

    public static OfferRequirement create(
            String taskType,
            int index,
            Collection<TaskRequirement> taskRequirements,
            ExecutorRequirement executorRequirement,
            Optional<PlacementRule> placementRuleOptional) {
        return new OfferRequirement(
                taskType, index, taskRequirements, executorRequirement, placementRuleOptional);
    }

    /**
     * Creates a new {@link OfferRequirement} with provided executor requirement and empty placement constraints.
     */
    public static OfferRequirement create(
            String taskType,
            int index,
            Collection<TaskInfo> taskInfos,
            ExecutorInfo executorInfoOptional) throws InvalidRequirementException {
        return create(taskType, index, taskInfos, executorInfoOptional, Optional.empty());
    }

    /**
     * Creates a new {@link OfferRequirement} with empty executor requirement and empty placement constraints.
     */
    public static OfferRequirement create (String taskType, int index, Collection<TaskInfo> taskInfos)
            throws InvalidRequirementException {
        return create(taskType, index, taskInfos, null, Optional.empty());
    }

    /**
     * Creates and returns a new {@link OfferRequirement} with any placement rules removed.
     */
    public OfferRequirement withoutPlacementRules() {
        return new OfferRequirement(
                type, index, taskRequirements.values(), executorRequirement, Optional.empty());
    }

    private OfferRequirement(
            String type,
            int index,
            Collection<TaskRequirement> taskRequirements,
            ExecutorRequirement executorRequirement,
            Optional<PlacementRule> placementRuleOptional) {
        this.type = type;
        this.index = index;
        this.taskRequirements = taskRequirements.stream()
                .collect(Collectors.toMap(t -> t.getTaskInfo().getName(), Function.identity()));
        this.executorRequirement = executorRequirement;
        this.placementRuleOptional = placementRuleOptional;
    }

    public String getType() {
        return type;
    }

    public int getIndex() {
        return index;
    }

    public TaskRequirement getTaskRequirement(String taskName) {
        return taskRequirements.get(taskName);
    }

    public Optional<ExecutorRequirement> getExecutorRequirement() {
        return Optional.ofNullable(executorRequirement);
    }

    public void updateTaskRequirement(String taskName, TaskInfo taskInfo) {
        taskRequirements.get(taskName).update(taskInfo);
    }

    public void updateExecutorRequirement(ExecutorInfo executorInfo) {
        try {
            executorRequirement = ExecutorRequirement.createExecutorRequirement(executorInfo);
        } catch (InvalidRequirementException e) {
            LOGGER.error("Failed to update ExecutorRequirement with exception:", e);
            // TODO(mrb): Refactor to keep OfferRequirement completely immutable after creation.
            // In the meantime, we know that creation succeeded previously, and that no operation in the evaluation
            // logic will modify an ExecutorInfo in such a way as to make it invalid.
        }
    }

    public Collection<TaskRequirement> getTaskRequirements() {
        return taskRequirements.values();
    }

    public Optional<ExecutorRequirement> getExecutorRequirementOptional() {
        return Optional.ofNullable(executorRequirement);
    }

    public Optional<PlacementRule> getPlacementRuleOptional() {
        return placementRuleOptional;
    }

    public Collection<Protos.Resource> getResources() {
        Collection<Protos.Resource> resources = new ArrayList<>();

        for (TaskRequirement taskReq : taskRequirements.values()) {
            resources.addAll(taskReq.getTaskInfo().getResourcesList());
        }

        if (getExecutorRequirementOptional().isPresent()) {
            resources.addAll(executorRequirement.getExecutorInfo().getResourcesList());
        }

        return resources;
    }

    public Collection<String> getResourceIds() {
        Collection<String> resourceIds = new ArrayList<String>();

        for (TaskRequirement taskReq : taskRequirements.values()) {
            resourceIds.addAll(taskReq.getResourceIds());
        }

        if (getExecutorRequirementOptional().isPresent()) {
            resourceIds.addAll(executorRequirement.getResourceIds());
        }

        return resourceIds;
    }

    public Collection<String> getPersistenceIds() {
        Collection<String> persistenceIds = new ArrayList<String>();

        for (TaskRequirement taskReq : taskRequirements.values()) {
            persistenceIds.addAll(taskReq.getPersistenceIds());
        }

        if (getExecutorRequirementOptional().isPresent()) {
            persistenceIds.addAll(executorRequirement.getPersistenceIds());
        }

        return persistenceIds;
    }

    private static Collection<TaskRequirement> getTaskRequirementsInternal(
            Collection<TaskInfo> taskInfos, String type, int index) throws InvalidRequirementException {
        Collection<TaskRequirement> taskRequirements = new ArrayList<TaskRequirement>();
        for (TaskInfo taskInfo : taskInfos) {
            TaskInfo.Builder taskBuilder = taskInfo.toBuilder();
            taskBuilder = CommonTaskUtils.setType(taskBuilder, type);
            taskBuilder = CommonTaskUtils.setIndex(taskBuilder, index);
            taskRequirements.add(new TaskRequirement(taskBuilder.build()));
        }
        return taskRequirements;
    }

    @Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this);
    }
}
