package com.mesosphere.sdk.offer;

import com.google.protobuf.TextFormat;
import com.mesosphere.sdk.api.ArtifactResource;
import com.mesosphere.sdk.offer.taskdata.SchedulerLabelReader;
import com.mesosphere.sdk.offer.taskdata.SchedulerLabelWriter;
import com.mesosphere.sdk.offer.taskdata.SchedulerEnvWriter;
import com.mesosphere.sdk.scheduler.SchedulerFlags;
import com.mesosphere.sdk.scheduler.plan.PodInstanceRequirement;
import com.mesosphere.sdk.specification.*;
import com.mesosphere.sdk.specification.util.RLimit;
import com.mesosphere.sdk.state.StateStore;
import org.apache.mesos.Protos;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * A default implementation of the {@link OfferRequirementProvider} interface.
 */
public class DefaultOfferRequirementProvider implements OfferRequirementProvider {
    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultOfferRequirementProvider.class);

    private static final String CONFIG_TEMPLATE_DOWNLOAD_DIR = "config-templates/";

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

        LOGGER.info("Getting new TaskRequirements for tasks: {}", podInstanceRequirement.getTasksToLaunch());

        Set<String> usedResourceSets = new HashSet<>();
        List<TaskRequirement> taskRequirements = new ArrayList<>();

        for (TaskSpec taskSpec : podInstance.getPod().getTasks()) {
            String resourceSetId = taskSpec.getResourceSet().getId();
            if (usedResourceSets.contains(resourceSetId)) {
                continue;
            }
            usedResourceSets.add(resourceSetId);
            // Transient: Just filling in resources to complete Pod footprint
            // Non-Transient: Used for actual task evaluation/launch
            boolean isTransient = !podInstanceRequirement.getTasksToLaunch().contains(taskSpec.getName());
            LOGGER.info("Generating {} for: {}, with resource set: {}",
                    isTransient ? "transient taskInfo to complete pod footprint" : "taskInfo to launch",
                    taskSpec.getName(),
                    resourceSetId);
            Protos.TaskInfo taskInfo = createTaskInfo(
                    serviceName,
                    podInstance,
                    taskSpec,
                    podInstanceRequirement.getParameters(),
                    getNewResources(taskSpec),
                    Collections.emptyMap(),
                    targetConfigurationId,
                    isTransient);
            taskRequirements.add(
                    new TaskRequirement(taskInfo, getResourceRequirements(taskSpec, Collections.emptyList())));
        }

        return OfferRequirement.create(
                podInstance.getPod().getType(),
                podInstance.getIndex(),
                taskRequirements,
                createExecutorRequirement(podInstance, serviceName, targetConfigurationId),
                podInstance.getPod().getPlacementRule());
    }

    private static Collection<Protos.Resource> getNewResources(TaskSpec taskSpec)
            throws InvalidRequirementException {
        ResourceSet resourceSet = taskSpec.getResourceSet();
        Collection<Protos.Resource> resources = new ArrayList<>();

        for (ResourceSpec resourceSpec : resourceSet.getResources()) {
            resources.add(ResourceUtils.getExpectedResource(resourceSpec));
        }

        for (VolumeSpec volumeSpec : resourceSet.getVolumes()) {
            switch (volumeSpec.getType()) {
                case ROOT:
                    resources.add(
                            ResourceUtils.getDesiredRootVolume(
                                    volumeSpec.getRole(),
                                    volumeSpec.getPrincipal(),
                                    volumeSpec.getValue().getScalar().getValue(),
                                    volumeSpec.getContainerPath()));
                    break;
                case MOUNT:
                    resources.add(
                            ResourceUtils.getDesiredMountVolume(
                                    volumeSpec.getRole(),
                                    volumeSpec.getPrincipal(),
                                    volumeSpec.getValue().getScalar().getValue(),
                                    volumeSpec.getContainerPath()));
                    break;
                default:
                    LOGGER.error("Encountered unsupported disk type: " + volumeSpec.getType());
            }
        }

        return coalesceResources(resources);
    }

    private ExecutorRequirement createExecutorRequirement(
            PodInstance podInstance,
            String serviceName,
            UUID targetConfigurationId) throws InvalidRequirementException {
        List<Protos.TaskInfo> podTasks = TaskUtils.getPodTasks(podInstance, stateStore);
        Protos.ExecutorInfo executorInfo = null;

        for (Protos.TaskInfo taskInfo : podTasks) {
            Optional<Protos.TaskStatus> taskStatusOptional = stateStore.fetchStatus(taskInfo.getName());
            if (taskStatusOptional.isPresent()
                    && taskStatusOptional.get().getState() == Protos.TaskState.TASK_RUNNING) {
                LOGGER.info("Reusing executor from task '{}': {}",
                        taskInfo.getName(), TextFormat.shortDebugString(taskInfo.getExecutor()));
                return ExecutorRequirement.create(taskInfo.getExecutor());
            }
        }

        if (executorInfo == null) {
            executorInfo = getNewExecutorInfo(
                    podInstance.getPod(), serviceName, targetConfigurationId, schedulerFlags);
        }

        Map<String, Protos.Resource> volumeMap = new HashMap<>();
        volumeMap.putAll(executorInfo.getResourcesList().stream()
                .filter(r -> r.hasDisk() && r.getDisk().hasVolume())
                .collect(Collectors.toMap(r -> r.getDisk().getVolume().getContainerPath(), Function.identity())));

        List<ResourceRequirement> resourceRequirements = new ArrayList<>();
        for (VolumeSpec v : podInstance.getPod().getVolumes()) {
            resourceRequirements.add(v.getResourceRequirement(volumeMap.get(v.getContainerPath())));
        }

        LOGGER.info("Creating new executor for pod {}, as no RUNNING tasks were found", podInstance.getName());

        return ExecutorRequirement.create(executorInfo, resourceRequirements);
    }

    @Override
    public OfferRequirement getExistingOfferRequirement(PodInstanceRequirement podInstanceRequirement)
            throws InvalidRequirementException {
        PodInstance podInstance = podInstanceRequirement.getPodInstance();
        List<TaskSpec> taskSpecs = podInstance.getPod().getTasks().stream()
                .filter(taskSpec -> podInstanceRequirement.getTasksToLaunch().contains(taskSpec.getName()))
                .collect(Collectors.toList());
        List<TaskRequirement> taskRequirements = new ArrayList<>();

        for (TaskSpec taskSpec : taskSpecs) {
            Optional<Protos.TaskInfo> taskInfoOptional =
                    stateStore.fetchTask(TaskSpec.getInstanceName(podInstance, taskSpec));
            Collection<Protos.Resource> taskResources;
            Map<String, Integer> dynamicPortValues;
            if (taskInfoOptional.isPresent()) {
                // Extract resources from task to generate the new TaskInfo with
                taskResources = getResourcesFromTask(taskSpec, taskInfoOptional.get());
                // Copy any information specifying prior dynamic port values to the new task
                try {
                    dynamicPortValues = SchedulerLabelReader.getAllDynamicPortValues(
                            taskInfoOptional.get().getResourcesList(), taskSpec);
                } catch (TaskException e) {
                    throw new InvalidRequirementException(e);
                }
                if (dynamicPortValues.isEmpty()) {
                    // Fallback: extract preexisting dynamic port values from task env:
                    // TODO(nickbp): Remove this fallback on or after August 2017
                    dynamicPortValues = SchedulerLabelReader.getAllDynamicPortValuesFromEnv(
                            taskSpec, taskInfoOptional.get().getCommand().getEnvironment());
                }
            } else {
                // Fall back to getting resource information from the task's associated resource set
                taskResources = getResourcesFromResourceSet(taskSpec, podInstance, stateStore);
                // No prior task, so no prior dynamic ports to copy
                dynamicPortValues = Collections.emptyMap();
            }
            Protos.TaskInfo taskInfo = createTaskInfo(
                    serviceName,
                    podInstance,
                    taskSpec,
                    podInstanceRequirement.getParameters(),
                    taskResources,
                    dynamicPortValues,
                    targetConfigurationId,
                    false);
            taskRequirements.add(new TaskRequirement(
                    taskInfo, getResourceRequirements(taskSpec, taskInfo.getResourcesList())));
        }
        validateTaskRequirements(taskRequirements);

        return OfferRequirement.create(
                podInstance.getPod().getType(),
                podInstance.getIndex(),
                taskRequirements,
                ExecutorRequirement.create(getExecutor(podInstance, serviceName, targetConfigurationId)),
                // Do not add placement rules to getExistingOfferRequirement
                Optional.empty());
    }

    private static Collection<Protos.Resource> getResourcesFromTask(TaskSpec taskSpec, Protos.TaskInfo taskInfo) {
        Collection<Protos.Resource> taskResources = new ArrayList<>();
        List<Protos.Resource> resourcesToUpdate = new ArrayList<>();
        for (Protos.Resource resource : taskInfo.getResourcesList()) {
            if (resource.hasDisk()) {
                // Disk resources may not be changed:
                taskResources.add(resource);
            } else {
                resourcesToUpdate.add(resource);
            }
        }

        Map<String, Protos.Resource> oldResourceMap = resourcesToUpdate.stream()
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

        taskResources.addAll(coalesceResources(updatedResources));
        return taskResources;
    }

    private static Collection<Protos.Resource> getResourcesFromResourceSet(
            TaskSpec taskSpec, PodInstance podInstance, StateStore stateStore) {
        // Exact task not found. Fall back to finding another task using the same resource set.
        Collection<String> tasksWithResourceSet = podInstance.getPod().getTasks().stream()
                .filter(podTask -> taskSpec.getResourceSet().getId().equals(podTask.getResourceSet().getId()))
                .map(podTask -> TaskSpec.getInstanceName(podInstance, podTask))
                .distinct()
                .collect(Collectors.toList());

        Collection<Protos.TaskInfo> taskInfosForPod = stateStore.fetchTasks().stream()
                .filter(taskInfo -> {
                    try {
                        return TaskUtils.isSamePodInstance(taskInfo, podInstance);
                    } catch (TaskException e) {
                        return false;
                    }
                })
                .collect(Collectors.toList());

        Optional<Protos.TaskInfo> matchingTaskInfoOptional = taskInfosForPod.stream()
                .filter(taskInfo -> tasksWithResourceSet.contains(taskInfo.getName()))
                .findFirst();

        if (!matchingTaskInfoOptional.isPresent()) {
            LOGGER.error("Failed to find a Task with resource set: {}", taskSpec.getResourceSet().getId());
            return Collections.emptyList();
        }
        return matchingTaskInfoOptional.get().getResourcesList();
    }

    private static void validateTaskRequirements(List<TaskRequirement> taskRequirements)
            throws InvalidRequirementException {
        if (taskRequirements.isEmpty()) {
            throw new InvalidRequirementException("Failed to generate any TaskRequirements.");
        }

        String taskType = "";
        try {
            taskType = new SchedulerLabelReader(taskRequirements.get(0).getTaskInfo()).getType();
        } catch (TaskException e) {
            throw new InvalidRequirementException(e);
        }

        for (TaskRequirement taskRequirement : taskRequirements) {
            try {
                String localTaskType = new SchedulerLabelReader(taskRequirement.getTaskInfo()).getType();
                if (!localTaskType.equals(taskType)) {
                    throw new InvalidRequirementException("TaskRequirements must have TaskTypes.");
                }
            } catch (TaskException e) {
                throw new InvalidRequirementException(e);
            }
        }
    }

    /**
     * Creates a new {@link TaskInfo} object, either representing a new task to be launched or an existing task to be
     * relaunched.
     *
     * @param resources the resources to be used by this task. may be carried over from the prior version of the task
     * @param isTransient whether this task is just a placeholder to fill space in a pod
     */
    private static Protos.TaskInfo createTaskInfo(
            String serviceName,
            PodInstance podInstance,
            TaskSpec taskSpec,
            Map<String, String> planParameters,
            Collection<Protos.Resource> resources,
            Map<String, Integer> dynamicPortValues,
            UUID targetConfigurationId,
            boolean isTransient) {
        Protos.TaskInfo.Builder taskInfoBuilder = Protos.TaskInfo.newBuilder()
                .addAllResources(resources)
                .setName(TaskSpec.getInstanceName(podInstance, taskSpec))
                .setTaskId(CommonIdUtils.emptyTaskId())
                .setSlaveId(CommonIdUtils.emptyAgentId());

        SchedulerEnvWriter envWriter = new SchedulerEnvWriter();
        if (taskSpec.getCommand().isPresent()) {
            envWriter.setEnv(
                    serviceName,
                    podInstance,
                    taskSpec,
                    taskSpec.getCommand().get(),
                    CONFIG_TEMPLATE_DOWNLOAD_DIR,
                    planParameters);
            taskInfoBuilder.getCommandBuilder().setEnvironment(envWriter.getTaskEnv());
        }

        if (taskSpec.getDiscovery().isPresent()) {
            DiscoverySpec discoverySpec = taskSpec.getDiscovery().get();
            Protos.DiscoveryInfo.Builder discoveryInfoBuilder = Protos.DiscoveryInfo.newBuilder();
            if (discoverySpec.getPrefix().isPresent()) {
                discoveryInfoBuilder.setName(
                        String.format("%s-%d", discoverySpec.getPrefix().get(), podInstance.getIndex()));
            }
            if (discoverySpec.getVisibility().isPresent()) {
                discoveryInfoBuilder.setVisibility(discoverySpec.getVisibility().get());
            } else {
                discoveryInfoBuilder.setVisibility(Protos.DiscoveryInfo.Visibility.CLUSTER);
            }
            taskInfoBuilder.setDiscovery(discoveryInfoBuilder);
        }

        if (taskSpec.getHealthCheck().isPresent()) {
            // Health check is stored directly against the TaskInfo:
            taskInfoBuilder.setHealthCheck(
                    toHealthCheck(taskSpec.getHealthCheck().get(), envWriter.getHealthCheckEnv()));
        }

        SchedulerLabelWriter labelWriter = new SchedulerLabelWriter()
                .setTargetConfiguration(targetConfigurationId)
                .setGoalState(taskSpec.getGoal())
                .setType(podInstance.getPod().getType())
                .setIndex(podInstance.getIndex());
        if (isTransient) {
            labelWriter.setTransient();
        }
        if (taskSpec.getReadinessCheck().isPresent()) {
            // Readiness check is packed as a task label:
            labelWriter.setReadinessCheck(
                    toHealthCheck(taskSpec.getReadinessCheck().get(), envWriter.getHealthCheckEnv()));
        }
        taskInfoBuilder.setLabels(labelWriter.toProto());

        return taskInfoBuilder.build();
    }

    private static Collection<ResourceRequirement> getResourceRequirements(
            TaskSpec taskSpec, Collection<Protos.Resource> existingResources) {
        ResourceSet resourceSet = taskSpec.getResourceSet();

        Map<String, Protos.Resource> existingResourceMap =
                existingResources.stream()
                        .filter(resource -> !resource.hasDisk())
                        .collect(Collectors.toMap(r -> r.getName(), Function.identity()));

        Map<String, Protos.Resource> existingVolumeMap =
                existingResources.stream()
                        .filter(resource -> resource.hasDisk())
                        .filter(resource -> resource.getDisk().hasVolume())
                        .collect(Collectors.toMap(
                                r -> r.getDisk().getVolume().getContainerPath(),
                                Function.identity()));

        List<ResourceRequirement> resourceRequirements = new ArrayList<>();

        for (ResourceSpec spec : resourceSet.getResources()) {
            resourceRequirements.add(spec.getResourceRequirement(existingResourceMap.get(spec.getName())));
        }

        for (VolumeSpec spec : resourceSet.getVolumes()) {
            resourceRequirements.add(spec.getResourceRequirement(existingVolumeMap.get(spec.getContainerPath())));
        }

        return resourceRequirements;
    }

    private static Protos.Resource getVolumeResource(VolumeSpec volumeSpec) {
        return volumeSpec.getResourceRequirement(null).getResource();
    }

    private static List<Protos.Resource> coalesceResources(Collection<Protos.Resource> resources) {
        List<Protos.Resource> portResources = new ArrayList<>();
        List<Protos.Resource> otherResources = new ArrayList<>();
        for (Protos.Resource r : resources) {
            if (r.getName().equals(Constants.PORTS_RESOURCE_TYPE)) {
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
    private Protos.ExecutorInfo getExecutor(
            PodInstance podInstance, String serviceName, UUID targetConfigurationId) {
        List<Protos.TaskInfo> podTasks = TaskUtils.getPodTasks(podInstance, stateStore);

        for (Protos.TaskInfo taskInfo : podTasks) {
            Optional<Protos.TaskStatus> taskStatusOptional = stateStore.fetchStatus(taskInfo.getName());
            if (taskStatusOptional.isPresent()
                    && taskStatusOptional.get().getState() == Protos.TaskState.TASK_RUNNING) {
                LOGGER.info(
                        "Reusing executor from task '{}': {}",
                        taskInfo.getName(),
                        TextFormat.shortDebugString(taskInfo.getExecutor()));
                return taskInfo.getExecutor();
            }
        }

        LOGGER.info("Creating new executor for pod {}, as no RUNNING tasks were found", podInstance.getName());
        return getNewExecutorInfo(podInstance.getPod(), serviceName, targetConfigurationId, schedulerFlags);
    }

    private static Protos.NetworkInfo getNetworkInfo(NetworkSpec networkSpec) {
        LOGGER.info("Loading NetworkInfo for network named \"{}\"", networkSpec.getName());
        Protos.NetworkInfo.Builder netInfoBuilder = Protos.NetworkInfo.newBuilder();
        netInfoBuilder.setName(networkSpec.getName());

        if (!networkSpec.getPortMappings().isEmpty()) {
            for (Map.Entry<Integer, Integer> e : networkSpec.getPortMappings().entrySet()) {
                Integer hostPort = e.getKey();
                Integer containerPort = e.getValue();
                netInfoBuilder.addPortMappings(Protos.NetworkInfo.PortMapping.newBuilder()
                        .setHostPort(hostPort)
                        .setContainerPort(containerPort)
                        .build());
            }
        }

        if (!networkSpec.getNetgroups().isEmpty()) {
            netInfoBuilder.addAllGroups(networkSpec.getNetgroups());
        }

        if (!networkSpec.getIpAddresses().isEmpty()) {
            for (String ipAddressString : networkSpec.getIpAddresses()) {
                netInfoBuilder.addIpAddresses(
                        Protos.NetworkInfo.IPAddress.newBuilder()
                                .setIpAddress(ipAddressString)
                                .setProtocol(Protos.NetworkInfo.Protocol.IPv4)
                                .build());
            }
        }

        return netInfoBuilder.build();
    }

    private static Protos.RLimitInfo getRLimitInfo(Collection<RLimit> rlimits) {
        Protos.RLimitInfo.Builder rLimitInfoBuilder = Protos.RLimitInfo.newBuilder();

        for (RLimit rLimit : rlimits) {
            Optional<Long> soft = rLimit.getSoft();
            Optional<Long> hard = rLimit.getHard();
            Protos.RLimitInfo.RLimit.Builder rLimitsBuilder = Protos.RLimitInfo.RLimit.newBuilder()
                    .setType(rLimit.getEnum());

            // RLimit itself validates that both or neither of these are present.
            if (soft.isPresent() && hard.isPresent()) {
                rLimitsBuilder.setSoft(soft.get()).setHard(hard.get());
            }
            rLimitInfoBuilder.addRlimits(rLimitsBuilder);
        }

        return rLimitInfoBuilder.build();
    }

    private static Protos.ExecutorInfo getNewExecutorInfo(
            PodSpec podSpec,
            String serviceName,
            UUID targetConfigurationId,
            SchedulerFlags schedulerFlags) throws IllegalStateException {
        Protos.ExecutorInfo.Builder executorInfoBuilder = Protos.ExecutorInfo.newBuilder()
                .setName(podSpec.getType())
                .setExecutorId(Protos.ExecutorID.newBuilder().setValue("").build()); // Set later by ExecutorRequirement
        // Populate ContainerInfo with the appropriate information from PodSpec
        Protos.ContainerInfo containerInfo = getContainerInfo(podSpec);
        if (containerInfo != null) {
            executorInfoBuilder.setContainer(containerInfo);
        }

        // command and user:
        Protos.CommandInfo.Builder executorCommandBuilder = executorInfoBuilder.getCommandBuilder().setValue(
                "export LD_LIBRARY_PATH=$MESOS_SANDBOX/libmesos-bundle/lib:$LD_LIBRARY_PATH && " +
                "export MESOS_NATIVE_JAVA_LIBRARY=$(ls $MESOS_SANDBOX/libmesos-bundle/lib/libmesos-*.so) && " +
                "export JAVA_HOME=$(ls -d $MESOS_SANDBOX/jre*/) && " +
         // Remove Xms/Xmx if +UseCGroupMemoryLimitForHeap or equivalent detects cgroups memory limit
                "export JAVA_OPTS=\"-Xms128M -Xmx128M\" && " +
                "$MESOS_SANDBOX/executor/bin/executor");

        if (podSpec.getUser().isPresent()) {
            executorCommandBuilder.setUser(podSpec.getUser().get());
        }

        // Required URIs from the scheduler environment:
        executorCommandBuilder.addUrisBuilder().setValue(schedulerFlags.getLibmesosURI());
        executorCommandBuilder.addUrisBuilder().setValue(schedulerFlags.getJavaURI());

        // Any URIs defined in PodSpec itself.
        for (URI uri : podSpec.getUris()) {
            executorCommandBuilder.addUrisBuilder().setValue(uri.toString());
        }

        // Volumes for the pod to share.
        for (VolumeSpec v : podSpec.getVolumes()) {
            executorInfoBuilder.addResources(getVolumeResource(v));
        }

        // Finally any URIs for config templates defined in TaskSpecs.
        for (TaskSpec taskSpec : podSpec.getTasks()) {
            for (ConfigFileSpec config : taskSpec.getConfigFiles()) {
                executorCommandBuilder.addUrisBuilder()
                        .setValue(ArtifactResource.getTemplateUrl(
                                serviceName,
                                targetConfigurationId,
                                podSpec.getType(),
                                taskSpec.getName(),
                                config.getName()))
                        .setOutputFile(CONFIG_TEMPLATE_DOWNLOAD_DIR + config.getName())
                        .setExtract(false);
            }
        }

        return executorInfoBuilder.build();
    }

    private static Protos.ContainerInfo getContainerInfo(PodSpec podSpec) {
        if (!podSpec.getImage().isPresent() && podSpec.getNetworks().isEmpty() && podSpec.getRLimits().isEmpty()) {
            return null;
        }

        Protos.ContainerInfo.Builder containerInfo = Protos.ContainerInfo.newBuilder()
                .setType(Protos.ContainerInfo.Type.MESOS);

        if (podSpec.getImage().isPresent()) {
            containerInfo.getMesosBuilder()
            .setImage(Protos.Image.newBuilder()
                    .setType(Protos.Image.Type.DOCKER)
                    .setDocker(Protos.Image.Docker.newBuilder()
                            .setName(podSpec.getImage().get())));
        }

        if (!podSpec.getNetworks().isEmpty()) {
            containerInfo.addAllNetworkInfos(
                    podSpec.getNetworks().stream().map(n -> getNetworkInfo(n)).collect(Collectors.toList()));
        }

        if (!podSpec.getRLimits().isEmpty()) {
            containerInfo.setRlimitInfo(getRLimitInfo(podSpec.getRLimits()));
        }

        return containerInfo.build();
    }

    private static Protos.HealthCheck toHealthCheck(
            ReadinessCheckSpec readinessCheckSpec, Protos.Environment envToUse) {
        Protos.HealthCheck.Builder builder = Protos.HealthCheck.newBuilder()
                .setDelaySeconds(readinessCheckSpec.getDelay())
                .setIntervalSeconds(readinessCheckSpec.getInterval())
                .setTimeoutSeconds(readinessCheckSpec.getTimeout());
        if (readinessCheckSpec instanceof HealthCheckSpec) {
            HealthCheckSpec healthCheckSpec = (HealthCheckSpec) readinessCheckSpec;
            builder
                    .setConsecutiveFailures(healthCheckSpec.getMaxConsecutiveFailures())
                    .setGracePeriodSeconds(healthCheckSpec.getGracePeriod());
        } else {
            builder
                    .setConsecutiveFailures(0)
                    .setGracePeriodSeconds(0);
        }
        builder.getCommandBuilder()
                .setValue(readinessCheckSpec.getCommand())
                .setEnvironment(envToUse);
        return builder.build();
    }
}
