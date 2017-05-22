package com.mesosphere.sdk.offer;

import com.google.protobuf.TextFormat;
import com.mesosphere.sdk.offer.taskdata.SchedulerResourceLabelWriter;
import com.mesosphere.sdk.specification.ResourceSpec;
import org.apache.mesos.Protos.*;
import org.apache.mesos.Protos.Resource.DiskInfo;
import org.apache.mesos.Protos.Value.Range;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * This class encapsulates common methods for manipulating Resources.
 */
public class ResourceUtils {
    private static final Logger LOGGER = LoggerFactory.getLogger(ResourceUtils.class);

    public static final String VIP_PREFIX = "VIP_";
    public static final String VIP_HOST_TLD = "l4lb.thisdcos.directory";

    public static Resource.Builder getUnreservedResource(String name, Value value, String role) {
        return setResource(Resource.newBuilder().setRole(role), name, value);
    }

    public static Resource getDesiredResource(ResourceSpec resourceSpec) {
        Resource.Builder resBuilder =
                getUnreservedResource(resourceSpec.getName(), resourceSpec.getValue(), resourceSpec.getRole());
        resBuilder.getReservationBuilder().setPrincipal(resourceSpec.getPrincipal());
        return resBuilder.build();
    }

    public static Resource getExpectedResource(ResourceSpec resourceSpec) {
        return getExpectedResource(
                resourceSpec.getRole(),
                resourceSpec.getPrincipal(),
                resourceSpec.getName(),
                resourceSpec.getValue());
    }

    public static Resource getUnreservedMountVolume(double diskSize, String mountRoot) {
        return getUnreservedResource(Constants.DISK_RESOURCE_TYPE, getScalar(diskSize), Constants.ANY_ROLE)
                .setDisk(getUnreservedMountVolumeDiskInfo(mountRoot))
                .build();
    }

    public static Resource getDesiredMountVolume(String role, String principal, double diskSize, String containerPath) {
        Resource.Builder resBuilder = getUnreservedResource(Constants.DISK_RESOURCE_TYPE, getScalar(diskSize), role)
                .setDisk(getDesiredMountVolumeDiskInfo(principal, containerPath));
        resBuilder.getReservationBuilder().setPrincipal(principal);
        return resBuilder.build();
    }

    public static Resource getExpectedMountVolume(
            double diskSize,
            String resourceId,
            String role,
            String principal,
            String mountRoot,
            String containerPath,
            String persistenceId) {
        Resource.Builder resBuilder = getUnreservedResource(Constants.DISK_RESOURCE_TYPE, getScalar(diskSize), role)
                .setDisk(getExpectedMountVolumeDiskInfo(mountRoot, containerPath, persistenceId, principal));
        resBuilder.getReservationBuilder().setPrincipal(principal);
        return new SchedulerResourceLabelWriter(resBuilder)
                .setResourceId(resourceId)
                .toProto();
    }

    public static Resource.Builder withValue(Resource.Builder resBuilder, Value value) {
        if (resBuilder.getType() != value.getType()) {
            throw new IllegalArgumentException(
                    String.format("Resource type %s does not equal value type %s",
                            resBuilder.getType().toString(), value.getType().toString()));
        }

        switch (resBuilder.getType()) {
            case SCALAR:
                return resBuilder.setScalar(value.getScalar());
            case RANGES:
                return resBuilder.setRanges(value.getRanges());
            case SET:
                return resBuilder.setSet(value.getSet());
            default:
                throw new IllegalArgumentException(String.format("Unknown resource type: %s", resBuilder.getType()));
        }
    }

    public static Resource updateResource(Resource resource, ResourceSpec resourceSpec)
            throws IllegalArgumentException {
        return withValue(resource.toBuilder(), resourceSpec.getValue()).build();
    }

    private static Resource.Builder setResource(Resource.Builder resBuilder, String name, Value value) {
        Value.Type type = value.getType();

        resBuilder
                .setName(name)
                .setType(type);

        switch (type) {
            case SCALAR:
                return resBuilder.setScalar(value.getScalar());
            case RANGES:
                return resBuilder.setRanges(value.getRanges());
            case SET:
                return resBuilder.setSet(value.getSet());
            default:
                throw new IllegalArgumentException(String.format("Unsupported resource value type: %s", type));
        }
    }

    public static Resource getUnreservedRootVolume(double diskSize) {
        return getUnreservedScalar(Constants.DISK_RESOURCE_TYPE, diskSize);
    }

    public static Resource getDesiredRootVolume(String role, String principal, double diskSize, String containerPath) {
        Resource.Builder resBuilder = getUnreservedResource(Constants.DISK_RESOURCE_TYPE, getScalar(diskSize), role)
                .setDisk(getExpectedRootVolumeDiskInfo("", containerPath, principal));
        resBuilder.getReservationBuilder().setPrincipal(principal);
        return resBuilder.build();
    }

    public static Resource getExpectedRootVolume(
            double diskSize,
            String resourceId,
            String containerPath,
            String role,
            String principal,
            String persistenceId) {
        Resource.Builder resBuilder = getUnreservedResource(Constants.DISK_RESOURCE_TYPE, getScalar(diskSize), role)
                .setDisk(getExpectedRootVolumeDiskInfo(persistenceId, containerPath, principal));
        resBuilder.getReservationBuilder().setPrincipal(principal);
        return new SchedulerResourceLabelWriter(resBuilder).setResourceId(resourceId).toProto();
    }

    public static Resource getExpectedResource(String role, String principal, String name, Value value) {
        return getExpectedResource(role, principal, name, value, "");
    }

    public static Resource getExpectedResource(String role,
                                               String principal,
                                               String name,
                                               Value value,
                                               String resourceId) {
        Resource.Builder resBuilder = getUnreservedResource(name, value, role);
        resBuilder.getReservationBuilder().setPrincipal(principal);
        return new SchedulerResourceLabelWriter(resBuilder).setResourceId(resourceId).toProto();
    }

    public static Resource getUnreservedScalar(String name, double value) {
        return getUnreservedResource(name, getScalar(value), Constants.ANY_ROLE).build();
    }

    public static Resource getExpectedScalar(
            String name, double value, String resourceId, String role, String principal) {
        return getExpectedResource(role, principal, name, getScalar(value), resourceId);
    }

    public static Resource getDesiredScalar(String role, String principal, String name, double value) {
        return getExpectedResource(role, principal, name, getScalar(value));
    }

    public static Resource getUnreservedRanges(String name, List<Range> ranges) {
        return getUnreservedResource(name, getRanges(ranges), Constants.ANY_ROLE).build();
    }

    public static Resource getDesiredRanges(String role, String principal, String name, List<Range> ranges) {
        return getExpectedResource(role, principal, name, getRanges(ranges));
    }

    public static Resource getExpectedRanges(
            String name, List<Range> ranges, String resourceId, String role, String principal) {
        Resource.Builder resBuilder = getUnreservedResource(name, getRanges(ranges), role);
        resBuilder.getReservationBuilder().setPrincipal(principal);
        return new SchedulerResourceLabelWriter(resBuilder).setResourceId(resourceId).toProto();
    }

    public static TaskInfo.Builder addVIP(
            TaskInfo.Builder builder,
            String vipName,
            Integer vipPort,
            String protocol,
            DiscoveryInfo.Visibility visibility,
            Resource resource) {
        if (builder.hasDiscovery()) {
            addVIP(
                    builder.getDiscoveryBuilder(),
                    vipName,
                    protocol,
                    visibility,
                    vipPort,
                    (int) resource.getRanges().getRange(0).getBegin());
        } else {
            builder.setDiscovery(getVIPDiscoveryInfo(
                    builder.getName(),
                    vipName,
                    vipPort,
                    protocol,
                    visibility,
                    resource));
        }

        return builder;
    }

    public static ExecutorInfo.Builder addVIP(
            ExecutorInfo.Builder builder,
            String vipName,
            Integer vipPort,
            String protocol,
            DiscoveryInfo.Visibility visibility,
            Resource resource) {
        if (builder.hasDiscovery()) {
            addVIP(
                    builder.getDiscoveryBuilder(),
                    vipName,
                    protocol,
                    visibility,
                    vipPort,
                    (int) resource.getRanges().getRange(0).getBegin());
        } else {
            builder.setDiscovery(getVIPDiscoveryInfo(
                    builder.getName(),
                    vipName,
                    vipPort,
                    protocol,
                    visibility,
                    resource));
        }

        return builder;
    }

    private static DiscoveryInfo.Builder addVIP(
            DiscoveryInfo.Builder builder,
            String vipName,
            String protocol,
            DiscoveryInfo.Visibility visibility,
            Integer vipPort,
            int destPort) {
        builder.getPortsBuilder()
                .addPortsBuilder()
                .setNumber(destPort)
                .setProtocol(protocol)
                .setVisibility(visibility)
                .getLabelsBuilder()
                .addLabels(getVIPLabel(vipName, vipPort));

        // Ensure Discovery visibility is always CLUSTER. This is to update visibility if prior info
        // (i.e. upgrading an old service with a previous version of SDK) has different visibility.
        builder.setVisibility(DiscoveryInfo.Visibility.CLUSTER);
        return builder;
    }

    public static DiscoveryInfo getVIPDiscoveryInfo(
            String taskName,
            String vipName,
            Integer vipPort,
            String protocol,
            DiscoveryInfo.Visibility visibility,
            Resource r) {
        DiscoveryInfo.Builder discoveryInfoBuilder = DiscoveryInfo.newBuilder()
                .setVisibility(DiscoveryInfo.Visibility.CLUSTER)
                .setName(taskName);

        discoveryInfoBuilder.getPortsBuilder().addPortsBuilder()
                .setNumber((int) r.getRanges().getRange(0).getBegin())
                .setProtocol(protocol)
                .setVisibility(visibility)
                .getLabelsBuilder()
                .addLabels(getVIPLabel(vipName, vipPort));

        return discoveryInfoBuilder.build();
    }

    public static Label getVIPLabel(String vipName, Integer vipPort) {
        return Label.newBuilder()
                .setKey(String.format("%s%s", VIP_PREFIX, UUID.randomUUID().toString()))
                .setValue(String.format("%s:%d", vipName, vipPort))
                .build();
    }

    public static Resource setValue(Resource resource, Value value) {
        return setResource(Resource.newBuilder(resource), resource.getName(), value).build();
    }

    public static String getPersistenceId(Resource resource) {
        if (resource.hasDisk() && resource.getDisk().hasPersistence()) {
            return resource.getDisk().getPersistence().getId();
        }

        return null;
    }

    public static TaskInfo clearResourceIds(TaskInfo taskInfo) {
        List<Resource> clearedTaskResources = clearResourceIds(taskInfo.getResourcesList());
        TaskInfo.Builder taskInfoBuilder = TaskInfo.newBuilder(taskInfo)
                .clearResources()
                .addAllResources(clearedTaskResources);

        if (taskInfo.hasExecutor()) {
            taskInfoBuilder.setExecutor(clearResourceIds(taskInfo.getExecutor()));
        }

        return taskInfoBuilder.build();
    }

    public static ExecutorInfo clearResourceIds(ExecutorInfo executorInfo) {
        List<Resource> clearedResources = clearResourceIds(executorInfo.getResourcesList());
        return ExecutorInfo.newBuilder(executorInfo)
                .clearResources()
                .addAllResources(clearedResources)
                .build();
    }

    public static TaskInfo clearPersistence(TaskInfo taskInfo) {
        List<Resource> resources = new ArrayList<>();
        for (Resource resource : taskInfo.getResourcesList()) {
            if (resource.hasDisk()) {
                resource = Resource.newBuilder(resource)
                        .setDisk(resource.getDisk().toBuilder()
                                .setPersistence(DiskInfo.Persistence.newBuilder().setId(""))
                ).build();
            }
            resources.add(resource);
        }
        return TaskInfo.newBuilder(taskInfo).clearResources().addAllResources(resources).build();
    }

    public static boolean areDifferent(
            ResourceSpec oldResourceSpec,
            ResourceSpec newResourceSpec) {

        Value oldValue = oldResourceSpec.getValue();
        Value newValue = newResourceSpec.getValue();
        if (!ValueUtils.equal(oldValue, newValue)) {
            LOGGER.info(String.format("Values '%s' and '%s' are different.", oldValue, newValue));
            return true;
        }

        String oldRole = oldResourceSpec.getRole();
        String newRole = newResourceSpec.getRole();
        if (!Objects.equals(oldRole, newRole)) {
            LOGGER.info(String.format("Roles '%s' and '%s' are different.", oldRole, newRole));
            return true;
        }

        String oldPrincipal = oldResourceSpec.getPrincipal();
        String newPrincipal = newResourceSpec.getPrincipal();
        if (!Objects.equals(oldPrincipal, newPrincipal)) {
            LOGGER.info(String.format("Principals '%s' and '%s' are different.", oldPrincipal, newPrincipal));
            return true;
        }

        return false;
    }

    /**
     * This method gets the {@link Resource} with the supplied resourceName from the supplied {@link TaskInfo}, throwing
     * an {@link IllegalArgumentException} if not found.
     * @param taskInfo the task info whose resource will be returned
     * @param resourceName the resourceName of the resource to return
     * @return the resource with the supplied resourceName
     */
    public static Resource getResource(TaskInfo taskInfo, String resourceName) {
        for (Resource r : taskInfo.getResourcesList()) {
            if (r.getName().equals(resourceName)) {
                return r;
            }
        }

        throw new IllegalArgumentException(
                String.format(
                        "Task has no resource with name '%s': %s",
                        resourceName, TextFormat.shortDebugString(taskInfo)));
    }

    /**
     * This method gets the {@link Resource} with the supplied resourceName from the supplied {@link TaskInfo.Builder},
     * throwing an {@link IllegalArgumentException} if not found.
     * @param taskBuilder the task builder whose resource will be returned
     * @param resourceName the resourceName of the resource to return
     * @return the resource with the supplied resourceName
     */
    public static Resource getResource(TaskInfo.Builder taskBuilder, String resourceName) {
        for (Resource r : taskBuilder.getResourcesList()) {
            if (r.getName().equals(resourceName)) {
                return r;
            }
        }

        throw new IllegalArgumentException(
                String.format(
                        "Task has no resource with name '%s': %s",
                        resourceName, TextFormat.shortDebugString(taskBuilder)));
    }

    public static Resource getResource(ExecutorInfo executorInfo, String name) {
        for (Resource r : executorInfo.getResourcesList()) {
            if (r.getName().equals(name)) {
                return r;
            }
        }

        throw new IllegalArgumentException(String.format(
                "Executor has no resource with name '%s': %s",
                name, TextFormat.shortDebugString(executorInfo)));
    }

    /**
     * This method gets the {@link Resource} with the supplied resourceName from the supplied
     * {@link ExecutorInfo.Builder}, throwing an {@link IllegalArgumentException} if not found.
     * @param executorBuilder the executor builder whose resource will be returned
     * @param resourceName the resourceName of the resource to return
     * @return the resource with the supplied resourceName
     */
    public static Resource getResource(ExecutorInfo.Builder executorBuilder, String resourceName) {
        for (Resource r : executorBuilder.getResourcesList()) {
            if (r.getName().equals(resourceName)) {
                return r;
            }
        }

        throw new IllegalArgumentException(
                String.format(
                        "Task has no resource with name '%s': %s",
                        resourceName, TextFormat.shortDebugString(executorBuilder)));
    }

    /**
     * Returns a list of all the resources associated with a task, including {@link Executor} resources.
     *
     * @param taskInfo The {@link Protos.TaskInfo} containing the {@link Protos.Resource}
     * @return a list of {@link Protos.Resource}s
     */
    public static List<Resource> getAllResources(TaskInfo taskInfo) {
        List<Resource> resources = new ArrayList<>();
        // Get all resources from both the task level and the executor level
        resources.addAll(taskInfo.getResourcesList());
        if (taskInfo.hasExecutor()) {
            resources.addAll(taskInfo.getExecutor().getResourcesList());
        }
        return resources;
    }

    private static List<Resource> clearResourceIds(List<Resource> resources) {
        List<Resource> clearedResources = new ArrayList<>();
        for (Resource resource : resources) {
            clearedResources.add(new SchedulerResourceLabelWriter(resource).clearResourceId().toProto());
        }
        return clearedResources;
    }

    public static DiskInfo getUnreservedMountVolumeDiskInfo(String mountRoot) {
        DiskInfo.Builder diskBuilder = DiskInfo.newBuilder();
        diskBuilder.getSourceBuilder()
                .setType(DiskInfo.Source.Type.MOUNT)
                .getMountBuilder().setRoot(mountRoot);
        return diskBuilder.build();
    }

    private static DiskInfo getDesiredMountVolumeDiskInfo(String principal, String containerPath) {
        DiskInfo.Builder diskBuilder = DiskInfo.newBuilder()
                .setSource(getDesiredMountVolumeSource());
        diskBuilder.getPersistenceBuilder()
                .setId("")
                .setPrincipal(principal);
        diskBuilder.getVolumeBuilder()
                .setContainerPath(containerPath)
                .setMode(Volume.Mode.RW);
        return diskBuilder.build();
    }

    private static DiskInfo getExpectedMountVolumeDiskInfo(
            String mountRoot,
            String containerPath,
            String persistenceId,
            String principal) {
        DiskInfo.Builder diskBuilder = DiskInfo.newBuilder(getUnreservedMountVolumeDiskInfo(mountRoot));
        diskBuilder.getPersistenceBuilder()
                .setId(persistenceId)
                .setPrincipal(principal);
        diskBuilder.getVolumeBuilder()
                .setContainerPath(containerPath)
                .setMode(Volume.Mode.RW);
        return diskBuilder.build();
    }

    private static DiskInfo getExpectedRootVolumeDiskInfo(
            String persistenceId,
            String containerPath,
            String principal) {
        DiskInfo.Builder diskBuilder = DiskInfo.newBuilder();
        diskBuilder.getPersistenceBuilder()
                .setId(persistenceId)
                .setPrincipal(principal);
        diskBuilder.getVolumeBuilder()
                .setContainerPath(containerPath)
                .setMode(Volume.Mode.RW);
        return diskBuilder.build();
    }

    private static DiskInfo.Source getDesiredMountVolumeSource() {
        return DiskInfo.Source.newBuilder().setType(DiskInfo.Source.Type.MOUNT).build();
    }

    public static Resource removeLabel(Resource resource, String key) {
        Resource.Builder builder = resource.toBuilder();
        builder.getReservationBuilder().clearLabels();
        for (Label l : resource.getReservation().getLabels().getLabelsList()) {
            if (!l.getKey().equals(key)) {
                builder.getReservationBuilder().getLabelsBuilder().addLabels(l);
            }
        }

        return builder.build();
    }

    private static Value getRanges(Collection<Range> ranges) {
        Value.Builder valueBuilder = Value.newBuilder().setType(Value.Type.RANGES);
        valueBuilder.getRangesBuilder().addAllRange(ranges);
        return valueBuilder.build();
    }

    private static Value getScalar(double value) {
        Value.Builder valueBuilder = Value.newBuilder().setType(Value.Type.SCALAR);
        valueBuilder.getScalarBuilder().setValue(value);
        return valueBuilder.build();
    }
}
