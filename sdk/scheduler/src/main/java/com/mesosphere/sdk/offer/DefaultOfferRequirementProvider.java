package com.mesosphere.sdk.offer;

import com.google.protobuf.TextFormat;
import com.mesosphere.sdk.api.ArtifactResource;
import com.mesosphere.sdk.offer.taskdata.EnvConstants;
import com.mesosphere.sdk.offer.taskdata.EnvUtils;
import com.mesosphere.sdk.offer.taskdata.SchedulerLabelReader;
import com.mesosphere.sdk.offer.taskdata.SchedulerLabelWriter;
import com.mesosphere.sdk.scheduler.SchedulerFlags;
import com.mesosphere.sdk.scheduler.plan.PodInstanceRequirement;
import com.mesosphere.sdk.specification.*;
import com.mesosphere.sdk.specification.util.RLimit;
import com.mesosphere.sdk.state.StateStore;
import com.mesosphere.sdk.state.StateStoreUtils;
import org.apache.mesos.Protos;
import org.apache.mesos.Protos.CommandInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.mesosphere.sdk.offer.Constants.PORTS_RESOURCE_TYPE;

/**
 * A default implementation of the {@link OfferRequirementProvider} interface.
 */
public class DefaultOfferRequirementProvider implements OfferRequirementProvider {
    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultOfferRequirementProvider.class);


    private final StateStore stateStore;
    private final String serviceName;
    private final UUID targetConfigurationId;
    private final SchedulerFlags schedulerFlags;

    /**
     * Creates a new instance which relies on the provided {@link StateStore} for storing known tasks, and which
     * updates tasks which are not tagged with the provided {@code targetConfigurationId}.
     */
    public DefaultOfferRequirementProvider(
            StateStore stateStore, String serviceName, UUID targetConfigurationId, SchedulerFlags schedulerFlags) {
        this.stateStore = stateStore;
        this.serviceName = serviceName;
        this.targetConfigurationId = targetConfigurationId;
        this.schedulerFlags = schedulerFlags;
    }

    @Override
    public OfferRequirement getNewOfferRequirement(PodInstanceRequirement podInstanceRequirement)
            throws InvalidRequirementException {
        PodInstance podInstance = podInstanceRequirement.getPodInstance();
        return OfferRequirement.create(
                podInstance.getPod().getType(),
                podInstance.getIndex(),
                getNewTaskRequirements(
                        podInstance,
                        podInstanceRequirement.getTasksToLaunch(),
                        podInstanceRequirement.getEnvironment(),
                        serviceName,
                        targetConfigurationId),
                getExecutorRequirement(podInstance, serviceName, targetConfigurationId),
                podInstance.getPod().getPlacementRule());
    }

    @Override
    public OfferRequirement getExistingOfferRequirement(PodInstanceRequirement podInstanceRequirement)
            throws InvalidRequirementException {
        PodInstance podInstance = podInstanceRequirement.getPodInstance();
        List<TaskSpec> taskSpecs = podInstance.getPod().getTasks().stream()
                .filter(taskSpec -> podInstanceRequirement.getTasksToLaunch().contains(taskSpec.getName()))
                .collect(Collectors.toList());
        Map<Protos.TaskInfo, TaskSpec> taskMap = new HashMap<>();

        for (TaskSpec taskSpec : taskSpecs) {
            Optional<Protos.TaskInfo> taskInfoOptional = stateStore.fetchTask(
                    TaskSpec.getInstanceName(podInstance, taskSpec));
            if (taskInfoOptional.isPresent()) {
                Protos.TaskInfo.Builder builder = taskInfoOptional.get().toBuilder();
                taskMap.put(builder.build(), taskSpec);
            } else {
                Protos.TaskInfo taskInfo = getNewTaskInfo(
                        podInstance,
                        taskSpec,
                        podInstanceRequirement.getEnvironment(),
                        serviceName,
                        targetConfigurationId,
                        StateStoreUtils.getResources(stateStore, podInstance, taskSpec));
                LOGGER.info("Generated new TaskInfo: {}", TextFormat.shortDebugString(taskInfo));
                taskMap.put(taskInfo, taskSpec);
            }
        }

        if (taskMap.size() == 0) {
            LOGGER.warn("Attempting to get existing OfferRequirement generated 0 tasks.");
        }

        List<TaskRequirement> taskRequirements = new ArrayList<>();
        for (Map.Entry<Protos.TaskInfo, TaskSpec> e : taskMap.entrySet()) {
            taskRequirements.add(getExistingTaskRequirement(
                    podInstance, e.getKey(), e.getValue(), serviceName, targetConfigurationId));
        }
        validateTaskRequirements(taskRequirements);

        ExecutorRequirement executorRequirement = getExecutor(podInstance, serviceName, targetConfigurationId);

        return OfferRequirement.create(
                podInstance.getPod().getType(),
                podInstance.getIndex(),
                taskRequirements,
                executorRequirement,
                // Do not add placement rules to getExistingOfferRequirement
                Optional.empty()
        );
    }

    private Collection<TaskRequirement> getNewTaskRequirements(
            PodInstance podInstance,
            Collection<String> tasksToLaunch,
            Map<String, String> environment,
            String serviceName,
            UUID targetConfigurationId) throws InvalidRequirementException{
        LOGGER.info("Getting new TaskRequirements for tasks: {}", tasksToLaunch);

        ArrayList<String> usedResourceSets = new ArrayList<>();
        List<TaskRequirement> taskRequirements = new ArrayList<>();

        // Generating TaskRequirements for evaluation.
        for (TaskSpec taskSpec : podInstance.getPod().getTasks()) {
            if (!tasksToLaunch.contains(taskSpec.getName())) {
                continue;
            }

            if (!usedResourceSets.contains(taskSpec.getResourceSet().getId())) {
                LOGGER.info("Generating taskInfo to launch for: {}, with resource set: {}",
                        taskSpec.getName(), taskSpec.getResourceSet().getId());
                usedResourceSets.add(taskSpec.getResourceSet().getId());
                taskRequirements.add(getNewTaskRequirement(
                        podInstance, taskSpec, environment, serviceName, targetConfigurationId, false));
            }
        }

        // Generating TaskRequirements to complete Pod footprint.
        for (TaskSpec taskSpec : podInstance.getPod().getTasks()) {
            if (tasksToLaunch.contains(taskSpec.getName())) {
                continue;
            }

            if (!usedResourceSets.contains(taskSpec.getResourceSet().getId())) {
                LOGGER.info("Generating transient taskInfo to complete pod footprint for: {}, with resource set: {}",
                        taskSpec.getName(), taskSpec.getResourceSet().getId());
                TaskRequirement taskRequirement = getNewTaskRequirement(
                        podInstance, taskSpec, environment, serviceName, targetConfigurationId, true);
                usedResourceSets.add(taskSpec.getResourceSet().getId());
                taskRequirements.add(taskRequirement);
            }
        }

        return taskRequirements;
    }

    private TaskRequirement getNewTaskRequirement(
            PodInstance podInstance,
            TaskSpec taskSpec,
            Map<String, String> environment,
            String serviceName,
            UUID targetConfigurationId,
            boolean isTransient) throws InvalidRequirementException {
        Protos.TaskInfo taskInfo = getNewTaskInfo(
                podInstance, taskSpec, environment, serviceName, targetConfigurationId);
        if (isTransient) {
            taskInfo = taskInfo.toBuilder()
                    .setLabels(new SchedulerLabelWriter(taskInfo).setTransient().toProto())
                    .build();
        }

        Collection<ResourceRequirement> resourceRequirements = getResourceRequirements(taskSpec);

        return new TaskRequirement(
                taskSpec,
                podInstance,
                resourceRequirements);
    }

    private static Collection<ResourceRequirement> getResourceRequirements(TaskSpec taskSpec) {
        return getResourceRequirements(taskSpec, null);
    }

    private static Collection<ResourceRequirement> getResourceRequirements(
            TaskSpec taskSpec, Collection<Protos.Resource> resources) {
        ResourceSet resourceSet = taskSpec.getResourceSet();

        Map<String, Protos.Resource> resourceMap = resources == null ?
                Collections.emptyMap() :
                resources.stream()
                        .filter(resource -> !resource.hasDisk())
                        .collect(Collectors.toMap(r -> r.getName(), Function.identity()));

        Map<String, Protos.Resource> volumeMap = resources == null ?
                Collections.emptyMap() :
                resources.stream()
                        .filter(resource -> resource.hasDisk())
                        .filter(resource -> resource.getDisk().hasVolume())
                        .collect(Collectors.toMap(
                                r -> r.getDisk().getVolume().getContainerPath(),
                                Function.identity()));

        List<ResourceRequirement> resourceRequirements = new ArrayList<>();

        for (ResourceSpec r : resourceSet.getResources()) {
            resourceRequirements.add(r.getResourceRequirement(resourceMap.get(r.getName())));
        }

        for (VolumeSpec v : resourceSet.getVolumes()) {
            resourceRequirements.add(v.getResourceRequirement(volumeMap.get(v.getContainerPath())));
        }

        return resourceRequirements;
    }

    private ExecutorRequirement getExecutorRequirement(
            PodInstance podInstance,
            String serviceName,
            UUID targetConfigurationId) throws InvalidRequirementException {
        List<Protos.TaskInfo> podTasks = TaskUtils.getPodTasks(podInstance, stateStore);
        Protos.ExecutorInfo executorInfo = null;

        for (Protos.TaskInfo taskInfo : podTasks) {
            Optional<Protos.TaskStatus> taskStatusOptional = stateStore.fetchStatus(taskInfo.getName());
            if (taskStatusOptional.isPresent()
                    && taskStatusOptional.get().getState() == Protos.TaskState.TASK_RUNNING) {
                executorInfo = taskInfo.getExecutor();
                LOGGER.info(
                        "Reusing executor from task '{}': {}",
                        taskInfo.getName(),
                        TextFormat.shortDebugString(executorInfo));
            }
        }

        Map<String, Protos.Resource> volumeMap = new HashMap<>();
        if (executorInfo != null) {
            volumeMap.putAll(executorInfo.getResourcesList().stream()
                    .filter(r -> r.hasDisk() && r.getDisk().hasVolume())
                    .collect(Collectors.toMap(r -> r.getDisk().getVolume().getContainerPath(), Function.identity())));
        }

        List<ResourceRequirement> resourceRequirements = new ArrayList<>();
        for (VolumeSpec v : podInstance.getPod().getVolumes()) {
            resourceRequirements.add(v.getResourceRequirement(volumeMap.get(v.getContainerPath())));
        }

        LOGGER.info("Creating new executor for pod {}, as no RUNNING tasks were found", podInstance.getName());
        return ExecutorRequirement.createNewExecutorRequirement(podInstance.getName(), resourceRequirements);
    }

    private static Protos.TaskInfo getNewTaskInfo(
            PodInstance podInstance,
            TaskSpec taskSpec,
            Map<String, String> environment,
            String serviceName,
            UUID targetConfigurationId,
            Collection<Protos.Resource> resources) throws InvalidRequirementException {
        Protos.TaskInfo.Builder taskInfoBuilder = Protos.TaskInfo.newBuilder()
                .setName(TaskSpec.getInstanceName(podInstance, taskSpec))
                .setTaskId(CommonIdUtils.emptyTaskId())
                .setSlaveId(CommonIdUtils.emptyAgentId())
                .addAllResources(resources);

        // create default labels:
        taskInfoBuilder.setLabels(new SchedulerLabelWriter(taskInfoBuilder)
                .setTargetConfiguration(targetConfigurationId)
                .setGoalState(taskSpec.getGoal())
                .setType(podInstance.getPod().getType())
                .setIndex(podInstance.getIndex())
                .toProto());

        if (taskSpec.getCommand().isPresent()) {
            CommandSpec commandSpec = taskSpec.getCommand().get();
            taskInfoBuilder.getCommandBuilder()
                    .setValue(commandSpec.getValue())
                    .setEnvironment(getTaskEnvironment(serviceName, podInstance, taskSpec, commandSpec));
        }

        return taskInfoBuilder.build();
    }

    private static Protos.TaskInfo getNewTaskInfo(
            PodInstance podInstance,
            TaskSpec taskSpec,
            Map<String, String> environment,
            String serviceName,
            UUID targetConfigurationId) throws InvalidRequirementException {
        return getNewTaskInfo(
                podInstance, taskSpec, environment, serviceName, targetConfigurationId, getNewResources(taskSpec));
    }

    private static TaskRequirement getExistingTaskRequirement(
            PodInstance podInstance,
            Protos.TaskInfo taskInfo,
            TaskSpec taskSpec,
            String serviceName,
            UUID targetConfigurationId) throws InvalidRequirementException {
        List<Protos.Resource> diskResources = new ArrayList<>();
        List<Protos.Resource> otherResources = new ArrayList<>();
        for (Protos.Resource resource : taskInfo.getResourcesList()) {
            if (resource.hasDisk()) {
                // Disk resources may not be changed:
                diskResources.add(resource);
            } else {
                otherResources.add(resource);
            }
        }

        Protos.TaskInfo.Builder taskInfoBuilder = Protos.TaskInfo.newBuilder(taskInfo)
                .clearResources()
                .clearExecutor()
                .addAllResources(getUpdatedResources(otherResources, taskSpec))
                .addAllResources(diskResources)
                .setTaskId(CommonIdUtils.emptyTaskId())
                .setSlaveId(CommonIdUtils.emptyAgentId())
                .setLabels(new SchedulerLabelWriter(taskInfo)
                        .setTargetConfiguration(targetConfigurationId)
                        .clearTransient()
                        .toProto());

        if (taskSpec.getCommand().isPresent()) {
            CommandSpec commandSpec = taskSpec.getCommand().get();
            Protos.CommandInfo.Builder commandBuilder = Protos.CommandInfo.newBuilder()
                    .setValue(commandSpec.getValue())
                    .setEnvironment(mergeEnvironments(
                            getTaskEnvironment(serviceName, podInstance, taskSpec, commandSpec),
                            taskInfo.getCommand().getEnvironment()));
            // Overwrite any prior CommandInfo:
            taskInfoBuilder.setCommand(commandBuilder);
        }

        return new TaskRequirement(
                taskSpec,
                podInstance,
                getResourceRequirements(taskSpec, taskInfoBuilder.getResourcesList()));
    }



    private static Protos.Environment getTaskEnvironment(
            String serviceName, PodInstance podInstance, TaskSpec taskSpec, CommandSpec commandSpec) {
        Map<String, String> environment = new HashMap<>();

        // Task envvars from either of the following sources:
        // - ServiceSpec (provided by developer)
        // - TASKCFG_<podname>_* (provided by user, handled when parsing YAML, potentially overrides ServiceSpec)
        environment.putAll(commandSpec.getEnvironment());

        // Default envvars for use by executors/developers:

        // Inject Pod Instance Index
        environment.put(EnvConstants.POD_INSTANCE_INDEX_TASKENV, String.valueOf(podInstance.getIndex()));
        // Inject Framework Name
        environment.put(EnvConstants.FRAMEWORK_NAME_TASKENV, serviceName);
        // Inject TASK_NAME as KEY:VALUE
        environment.put(EnvConstants.TASK_NAME_TASKENV, TaskSpec.getInstanceName(podInstance, taskSpec));
        // Inject TASK_NAME as KEY for conditional mustache templating
        environment.put(TaskSpec.getInstanceName(podInstance, taskSpec), "true");

        return EnvUtils.fromMapToEnvironment(environment).build();
    }

    private static Protos.Environment.Builder mergeEnvironments(
            Protos.Environment primary, Protos.Environment secondary) {
        Map<String, String> primaryVariables = EnvUtils.fromEnvironmentToMap(primary);
        for (Map.Entry<String, String> secondaryEntry : EnvUtils.fromEnvironmentToMap(secondary).entrySet()) {
            if (!primaryVariables.containsKey(secondaryEntry.getKey())) {
                primaryVariables.put(secondaryEntry.getKey(), secondaryEntry.getValue());
            }
        }

        return EnvUtils.fromMapToEnvironment(primaryVariables);
    }

    private static void validateTaskRequirements(List<TaskRequirement> taskRequirements)
            throws InvalidRequirementException {
        if (taskRequirements.isEmpty()) {
            throw new InvalidRequirementException("Failed to generate any TaskRequirements.");
        }

        String taskType = taskRequirements.get(0).getType();

        for (TaskRequirement taskRequirement : taskRequirements) {
            String localTaskType = taskRequirement.getType();
            if (!localTaskType.equals(taskType)) {
                throw new InvalidRequirementException("TaskRequirements must have TaskTypes.");
            }
        }
    }

    private static Collection<Protos.Resource> getUpdatedResources(
            Collection<Protos.Resource> oldResources, TaskSpec taskSpec) throws InvalidRequirementException {
        Map<String, Protos.Resource> oldResourceMap = oldResources.stream()
                .collect(Collectors.toMap(resource -> resource.getName(), resource -> resource));

        List<Protos.Resource> updatedResources = new ArrayList<>();
        for (ResourceSpec resourceSpec : taskSpec.getResourceSet().getResources()) {
            Protos.Resource oldResource = oldResourceMap.get(resourceSpec.getName());
            if (oldResource != null) {
                // Update existing resource
                try {
                    updatedResources.add(ResourceUtils.updateResource(oldResource, resourceSpec));
                } catch (IllegalArgumentException e) {
                    LOGGER.error("Failed to update Resources with exception: ", e);
                    // On failure to update resources, keep the old resources.
                    updatedResources.add(oldResource);
                }
            } else {
                // Add newly added resource
                updatedResources.add(ResourceUtils.getExpectedResource(resourceSpec));
            }
        }

        return coalesceResources(updatedResources);
    }

    private static Collection<Protos.Resource> getNewResources(TaskSpec taskSpec)
            throws InvalidRequirementException {
        ResourceSet resourceSet = taskSpec.getResourceSet();
        Collection<Protos.Resource> resources = new ArrayList<>();

        for (ResourceSpec resourceSpec : resourceSet.getResources()) {
            resources.add(ResourceUtils.getExpectedResource(resourceSpec));
        }

        for (VolumeSpec volumeSpec : resourceSet.getVolumes()) {
            resources.add(getVolumeResource(volumeSpec));
        }

        return coalesceResources(resources);
    }

    private static Protos.Resource getVolumeResource(VolumeSpec volumeSpec) {
        Protos.Resource volume = null;
        switch (volumeSpec.getType()) {
            case ROOT:
                volume = ResourceUtils.getDesiredRootVolume(
                        volumeSpec.getRole(),
                        volumeSpec.getPrincipal(),
                        volumeSpec.getValue().getScalar().getValue(),
                        volumeSpec.getContainerPath());
                break;
            case MOUNT:
                volume = ResourceUtils.getDesiredMountVolume(
                        volumeSpec.getRole(),
                        volumeSpec.getPrincipal(),
                        volumeSpec.getValue().getScalar().getValue(),
                        volumeSpec.getContainerPath());
                break;
            default:
                LOGGER.error("Encountered unsupported disk type: " + volumeSpec.getType());
        }

        return volume;
    }

    private static List<Protos.Resource> coalesceResources(Collection<Protos.Resource> resources) {
        List<Protos.Resource> portResources = new ArrayList<>();
        List<Protos.Resource> otherResources = new ArrayList<>();
        for (Protos.Resource r : resources) {
            if (isPortResource(r)) {
                portResources.add(r);
            } else {
                otherResources.add(r);
            }
        }

        if (!portResources.isEmpty()) {
            otherResources.add(coalescePorts(portResources));
        }

        return otherResources;
    }

    private static boolean isPortResource(Protos.Resource resource) {
        return resource.getName().equals(PORTS_RESOURCE_TYPE);
    }

    private static Protos.Resource coalescePorts(List<Protos.Resource> resources) {
        // Within the SDK, each port is handled as its own resource, since they can have extra meta-data attached, but
        // we can't have multiple "ports" resources on a task info, so we combine them here. Since ports are also added
        // back onto TaskInfos during the evaluation stage (since they may be dynamic) we actually just clear the ranges
        // from that resource here to make the bookkeeping easier.
        // TODO(mrb): instead of clearing ports, keep them in OfferRequirement and build up actual TaskInfos elsewhere
        return resources.get(0).toBuilder().clearRanges().build();
    }

    /**
     * Returns the ExecutorInfo of a PodInstance if it is still running so it may be re-used, otherwise
     * it returns a new ExecutorInfo.
     * @param podInstance A PodInstance
     * @return The appropriate ExecutorInfo.
     */
    private ExecutorRequirement getExecutor(
            PodInstance podInstance, String serviceName, UUID targetConfigurationId) {

        Optional<Protos.TaskInfo> taskInfoOptional = getRunningTaskInfo(
                podInstance,
                serviceName,
                targetConfigurationId);

        // TODO: Put resource requirements back on the executor (in particular volumes)
        if (taskInfoOptional.isPresent()) {
            LOGGER.info(
                    "Reusing executor from task '{}': {}",
                    taskInfoOptional.get().getName(),
                    TextFormat.shortDebugString(taskInfoOptional.get().getExecutor()));

            Protos.ExecutorInfo executorInfo = taskInfoOptional.get().getExecutor();

            return ExecutorRequirement.createExistingExecutorRequirement(
                    executorInfo.getName(),
                    executorInfo.getExecutorId(),
                    Collections.emptyList());
        } else {
            LOGGER.info("Creating new executor for pod {}, as no RUNNING tasks were found", podInstance.getName());

            return ExecutorRequirement.createNewExecutorRequirement(
                    "executor__" + UUID.randomUUID(),
                    Collections.emptyList());
        }
    }

    private Optional<Protos.TaskInfo> getRunningTaskInfo(
            PodInstance podInstance,
            String serviceName,
            UUID targetConfigurationId) {

        List<Protos.TaskInfo> podTasks = TaskUtils.getPodTasks(podInstance, stateStore);
        for (Protos.TaskInfo taskInfo : podTasks) {
            Optional<Protos.TaskStatus> taskStatusOptional = stateStore.fetchStatus(taskInfo.getName());
            if (taskStatusOptional.isPresent()
                    && taskStatusOptional.get().getState() == Protos.TaskState.TASK_RUNNING) {
                return Optional.of(taskInfo);
            }
        }

        return Optional.empty();
    }



}
