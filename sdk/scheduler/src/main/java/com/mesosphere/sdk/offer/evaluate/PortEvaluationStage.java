package com.mesosphere.sdk.offer.evaluate;

import com.mesosphere.sdk.offer.*;
import com.mesosphere.sdk.offer.taskdata.SchedulerEnvWriter;
import com.mesosphere.sdk.offer.taskdata.SchedulerLabelReader;
import com.mesosphere.sdk.offer.taskdata.SchedulerResourceLabelReader;

import org.apache.commons.lang3.StringUtils;
import org.apache.mesos.Protos;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.IntStream;

import static com.mesosphere.sdk.offer.evaluate.EvaluationOutcome.*;

/**
 * This class evaluates an offer for a single port against an {@link OfferRequirement}, finding a port dynamically
 * in the offer where specified by the framework and modifying {@link org.apache.mesos.Protos.TaskInfo} and
 * {@link org.apache.mesos.Protos.ExecutorInfo} where appropriate so that the port is available in their respective
 * environments.
 */
public class PortEvaluationStage extends ResourceEvaluationStage {
    private static final Logger LOGGER = LoggerFactory.getLogger(PortEvaluationStage.class);

    private final String portName;
    private final int port;
    private final Optional<String> customEnvKey;

    private String resourceId;

    public PortEvaluationStage(
            Protos.Resource resource, String taskName, String portName, int port, Optional<String> customEnvKey) {
        super(resource, taskName);
        this.portName = portName;
        this.port = port;
        this.customEnvKey = customEnvKey;
    }

    @Override
    public EvaluationOutcome evaluate(MesosResourcePool mesosResourcePool, PodInfoBuilder podInfoBuilder) {
        int portToUse;
        if (port != 0) {
            // Fixed port
            portToUse = port;
        } else {
            // Dynamic port: reuse previous selection if any, or do a new selection
            List<Protos.Resource> resourcesToCheck = getTaskName().isPresent()
                    ? podInfoBuilder.getTaskBuilder(getTaskName().get()).getResourcesList()
                    : podInfoBuilder.getExecutorBuilder().get().getResourcesList();
            // TODO should be getting the prior port value from mesosResourcePool, not podInfoBuilder!!!

            mesosResourcePool.getReservedPool().get(Constants.PORTS_RESOURCE_TYPE);
            SchedulerEnvR
            Optional<Integer> taskPort = SchedulerLabelReader.getDynamicPortValue(resourcesToCheck, portName);
            if (taskPort.isPresent()) {
                // Keep/reuse the existing port.
                portToUse = taskPort.get();
            } else {
                // No previous port was found, select a port from what's available.
                Protos.Value availablePorts =
                        mesosResourcePool.getUnreservedMergedPool().get(Constants.PORTS_RESOURCE_TYPE);
                if (availablePorts == null) {
                    return fail(this, "No unreserved ports were available in offer resource pool: %s",
                            mesosResourcePool.getOffer().toString());
                }
                List<Protos.Value.Range> availablePortRanges = availablePorts.getRanges().getRangeList();
                Optional<Integer> selectedDynamicPort = selectDynamicPort(availablePortRanges, podInfoBuilder);
                if (!selectedDynamicPort.isPresent()) {
                    return fail(this,
                            "No ports were available for dynamic port in offer: %s, ranges: %s",
                            mesosResourcePool.getOffer().toString(), availablePortRanges);
                }
                portToUse = selectedDynamicPort.get();
            }
        }

        // If this is not the first port evaluation stage in this evaluation run, and this is a new pod being launched,
        // we want to use the reservation ID we created for the first port in this cycle for all subsequent ports.
        try {
            Protos.Resource resource = getTaskName().isPresent()
                    ? ResourceUtils.getResource(
                            podInfoBuilder.getTaskBuilder(getTaskName().get()), Constants.PORTS_RESOURCE_TYPE)
                    : ResourceUtils.getResource(
                            podInfoBuilder.getExecutorBuilder().get(), Constants.PORTS_RESOURCE_TYPE);
            resourceId = new SchedulerResourceLabelReader(resource).getResourceId().get();
        } catch (IllegalArgumentException e) {
            // There have been no previous ports in this evaluation cycle, so there's no resource on the task builder
            // to get the resource id from.
            resourceId = "";
        }
        super.setResourceRequirement(getPortRequirement(getResourceRequirement(), portToUse));

        return super.evaluate(mesosResourcePool, podInfoBuilder);
    }

    @Override
    protected void setProtos(PodInfoBuilder podInfoBuilder, Protos.Resource resource) {
        long port = resource.getRanges().getRange(0).getBegin();
        if (getTaskName().isPresent()) {
            // Update resource, and include a label listing the port assignment
            Protos.TaskInfo.Builder taskBuilder = podInfoBuilder.getTaskBuilder(getTaskName().get());
            taskBuilder
                    .clearResources()
                    .addAllResources(
                            updateOrAddRangedResource(taskBuilder.getResourcesList(), resource, portName, port));

            // Update advertised port in task env (and readiness/health check env if applicable):
            try {
                SchedulerEnvWriter.setPort(taskBuilder, portName, customEnvKey, port);
            } catch (TaskException e) {
                LOGGER.error(String.format("Failed to add PORT envvar to Task %s", getTaskName().get()), e);
            }
        } else {
            // Update resource, and include a label listing the port assignment
            Protos.ExecutorInfo.Builder executorBuilder = podInfoBuilder.getExecutorBuilder().get();
            executorBuilder
                    .clearResources()
                    .addAllResources(
                            updateOrAddRangedResource(executorBuilder.getResourcesList(), resource, portName, port));

            // Update advertised port in executor env:
            SchedulerEnvWriter.setPort(executorBuilder, portName, customEnvKey, port);
        }
    }

    private static Optional<Integer> getPriorDynamicPortValue(
            MesosResourcePool resources, Protos.Environment priorEnvironment) {
        // TODO(nickbp): Remove this fallback on or after August 2017
        dynamicPortValues = SchedulerLabelReader.getAllDynamicPortValuesFromEnv(taskSpec, priorEnvironment);
    }

    private static List<Protos.Resource> updateOrAddRangedResource(
            List<Protos.Resource> existingResources, Protos.Resource resource, String portName, long portValue) {
        List<Protos.Resource> updatedResources = new ArrayList<>();
        boolean updateOccurred = false;
        for (Protos.Resource existingResource : existingResources) {
            if (existingResource.getName().equals(resource.getName())) {
                // Merge onto matching resource
                updateOccurred = true;
                Protos.Value.Builder valueBuilder = Protos.Value.newBuilder().setType(Protos.Value.Type.RANGES);
                valueBuilder.getRangesBuilder().addAllRange(
                        RangeAlgorithms.mergeRanges(
                                existingResource.getRanges().getRangeList(), resource.getRanges().getRangeList()));
                updatedResources.add(ResourceBuilder.fromExistingResource(existingResource)
                        .setValue(valueBuilder.build())
                        .setPort(portName, portValue)
                        .build());
            } else {
                // Passthrough
                updatedResources.add(existingResource);
            }
        }
        if (!updateOccurred) {
            // Match not found, add new resource to end
            updatedResources.add(resource);
        }
        return updatedResources;
    }

    @Override
    protected Protos.Resource toFulfilledResource(Protos.Resource resource) {
        Protos.Resource reservedResource = super.toFulfilledResource(resource);
        if (!StringUtils.isBlank(resourceId)) {
            reservedResource = ResourceBuilder.fromExistingResource(reservedResource)
                    .setResourceId(resourceId)
                    .build();
        }
        return reservedResource;
    }

    private static Optional<Integer> selectDynamicPort(
            List<Protos.Value.Range> availablePortRanges, PodInfoBuilder podInfoBuilder) {
        // We don't want to accidentally dynamically consume a port that's explicitly claimed by this pod, so compile a
        // list of those to check against the offered ports.
        Set<Integer> consumedPorts = new HashSet<>();
        for (Protos.Resource resource : podInfoBuilder.getOfferRequirement().getResources()) {
            if (resource.getName().equals(Constants.PORTS_RESOURCE_TYPE)) {
                resource.getRanges().getRangeList().stream()
                        .flatMap(r -> IntStream.rangeClosed((int) r.getBegin(), (int) r.getEnd()).boxed())
                        .filter(p -> p != 0)
                        .forEach(consumedPorts::add);
            }
        }

        // Also check other dynamically allocated ports which had been taken by earlier stages of this evaluation round.
        for (Protos.Resource.Builder resourceBuilder : podInfoBuilder.getResourceBuilders()) {
            if (resourceBuilder.getName().equals(Constants.PORTS_RESOURCE_TYPE)) {
                resourceBuilder.getRanges().getRangeList().stream()
                        .flatMap(r -> IntStream.rangeClosed((int) r.getBegin(), (int) r.getEnd()).boxed())
                        .filter(p -> p != 0)
                        .forEach(consumedPorts::add);
            }
        }

        return availablePortRanges.stream()
                .flatMap(r -> IntStream.rangeClosed((int) r.getBegin(), (int) r.getEnd()).boxed())
                .filter(p -> !consumedPorts.contains(p))
                .findFirst();
    }

    private static ResourceRequirement getPortRequirement(ResourceRequirement resourceRequirement, int port) {
        Protos.Resource.Builder builder = resourceRequirement.getResource().toBuilder();
        builder.clearRanges().getRangesBuilder().addRange(Protos.Value.Range.newBuilder().setBegin(port).setEnd(port));

        return new ResourceRequirement(builder.build());
    }
}
